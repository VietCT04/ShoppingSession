#pragma once
#include "core.hpp"
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#include <immintrin.h>
#endif
namespace shop {
// Fused record/field scanning for this track's dialect: LF always terminates a
// record, including malformed quoted records. No intermediate line scan/index.
// The callback receives views into the mapped input and performs processing
// immediately. Inspired by the fused accumulate model in csveee (VLDB 2026).
template<size_t N,class Consume>void scan_csv(Chunk chunk,Consume consume) {
    std::array<SV,N> fields;
    const char* record=chunk.begin;
    const char* field_start=record;
    size_t field=0;unsigned quotes=0;bool bad=false;
    auto finish_field=[&](const char* at) {
        SV value(field_start,at-field_start);
        if(quotes) {
            if(quotes!=2||value.size()<2||value.front()!='"'||value.back()!='"')return false;
            value.remove_prefix(1);value.remove_suffix(1);
        }
        fields[field++]=value;quotes=0;return true;
    };
    auto special=[&](const char* at) {
        if(*at=='\n') {
            if(!bad&&field==N-1&&finish_field(at))consume(fields);
            record=at+1;field_start=record;field=0;quotes=0;bad=false;
        } else if(!bad) {
            if(*at=='"'){if(++quotes>2)bad=true;}
            else if(field>=N-1||!finish_field(at))bad=true;
            else field_start=at+1;
        }
    };
    const char* at=chunk.begin;
#ifdef __SSE2__
    auto comma=_mm_set1_epi8(','),quote=_mm_set1_epi8('"'),lf=_mm_set1_epi8('\n');
    while(chunk.end-at>=16) {
        auto bytes=_mm_loadu_si128(reinterpret_cast<const __m128i*>(at));
        auto separators=_mm_or_si128(_mm_cmpeq_epi8(bytes,comma),_mm_or_si128(_mm_cmpeq_epi8(bytes,quote),_mm_cmpeq_epi8(bytes,lf)));
        unsigned mask=_mm_movemask_epi8(separators);
        while(mask){auto bit=std::countr_zero(mask);special(at+bit);mask&=mask-1;}
        at+=16;
    }
#endif
    for(;at<chunk.end;++at)if(*at==','||*at=='"'||*at=='\n')special(at);
    if(record<chunk.end&&!bad&&field==N-1&&finish_field(chunk.end))consume(fields);
}
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
// Enumerate comma/newline positions only. Count quotes in each field from a
// second mask, including fields that cross SIMD blocks.
template<size_t N,class Consume>
__attribute__((target("avx2,popcnt,bmi"))) void scan_csv_avx2(Chunk chunk,Consume consume) {
    std::array<SV,N> fields;
    const char* record=chunk.begin;
    const char* start=record;
    size_t field=0;
    unsigned quotes=0;
    bool bad=false;
    auto extract=[&](const char* end) {
        SV value(start,end-start);
        if(quotes) {
            if(quotes!=2||value.size()<2||value.front()!='"'||value.back()!='"')return false;
            value.remove_prefix(1);
            value.remove_suffix(1);
        }
        fields[field++]=value;
        return true;
    };
    auto separator=[&](const char* at,bool newline) {
        if(newline) {
            if(!bad&&field==N-1&&extract(at))consume(fields);
            record=at+1;
            field=0;
            bad=false;
        } else if(!bad) {
            bad=field==N-1||!extract(at);
        }
        start=at+1;
        quotes=0;
    };
    const auto comma=_mm256_set1_epi8(','),quote=_mm256_set1_epi8('"'),lf=_mm256_set1_epi8('\n');
    const char* at=chunk.begin;
    while(chunk.end-at>=32) {
        auto bytes=_mm256_loadu_si256(reinterpret_cast<const __m256i*>(at));
        uint32_t newlines=_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,lf));
        uint32_t separators=newlines|uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,comma)));
        uint32_t quote_mask=_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,quote));
        while(separators) {
            uint32_t bit=separators&(0u-separators);
            uint32_t before=quote_mask&(bit-1);
            quotes+=unsigned(__builtin_popcount(before));
            quote_mask^=before;
            separator(at+std::countr_zero(bit),bool(newlines&bit));
            separators^=bit;
        }
        quotes=std::min(3u,quotes+unsigned(__builtin_popcount(quote_mask)));
        at+=32;
    }
    for(;at<chunk.end;++at) {
        if(*at==','||*at=='\n')separator(at,*at=='\n');
        else if(*at=='"'&&quotes<3)++quotes;
    }
    if(record<chunk.end&&!bad&&field==N-1&&extract(chunk.end))consume(fields);
}
inline bool avx2_csv_available() {
    return __builtin_cpu_supports("avx2")&&__builtin_cpu_supports("popcnt")&&__builtin_cpu_supports("bmi");
}

__attribute__((target("bmi2,popcnt"))) inline uint32_t extract_csv_bits(__uint128_t quotes,__uint128_t positions) {
    uint64_t lower=uint64_t(positions);
    return uint32_t(_pext_u64(uint64_t(quotes),lower)|
                    (_pext_u64(uint64_t(quotes>>64),uint64_t(positions>>64))<<std::popcount(lower)));
}

// For short rows, compare the ranks of all opening and closing quotes before
// extracting the fields. Boundary masks include empty fields between commas.
template<size_t N,class Consume>
__attribute__((target("avx2,popcnt,bmi,bmi2"))) void scan_csv_rows(Chunk chunk,Consume consume) {
    static_assert(N>=2&&N<=32);
    const auto comma=_mm256_set1_epi8(','),quote=_mm256_set1_epi8('"'),lf=_mm256_set1_epi8('\n');
    const char* row=chunk.begin;
    while(chunk.end-row>=128) {
        __uint128_t commas=0;
        __uint128_t quotes=0;
        const char* end=nullptr;
        for(unsigned offset=0;offset<128;offset+=32) {
            auto bytes=_mm256_loadu_si256(reinterpret_cast<const __m256i*>(row+offset));
            uint32_t newlines=_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,lf));
            uint32_t before=(newlines&(0u-newlines))-1;
            uint32_t fields=_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,comma));
            commas|=__uint128_t(fields&before)<<offset;
            uint32_t quote_bits=_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,quote));
            quotes|=__uint128_t(quote_bits&before)<<offset;
            if(newlines) {
                end=row+offset+std::countr_zero(newlines);
                break;
            }
        }
        std::array<SV,N> fields;
        if(!end) {
            end=static_cast<const char*>(std::memchr(row+128,'\n',chunk.end-row-128));
            if(!end)end=chunk.end;
            if(csv(SV(row,end-row),fields))consume(fields);
        } else {
            uint64_t low=uint64_t(commas),high=uint64_t(commas>>64);
            if(std::popcount(low)+std::popcount(high)==N-1) {
                uint32_t quoted=0;
                bool valid=true;
                if(quotes) {
                    __uint128_t starts=(commas<<1)|1;
                    __uint128_t finishes=(commas|(__uint128_t{1}<<(end-row)))>>1;
                    quoted=extract_csv_bits(quotes,starts);
                    uint32_t closed=extract_csv_bits(quotes,finishes)<<unsigned(bool(commas&1));
                    auto total_quotes=std::popcount(uint64_t(quotes))+std::popcount(uint64_t(quotes>>64));
                    valid=quoted==closed&&total_quotes==2*std::popcount(quoted)&&!(quotes&starts&finishes);
                }
                if(!valid) {
                    row=end+1;
                    continue;
                }
                const char* start=row;
                for(size_t i=0;i<N-1;++i) {
                    unsigned offset;
                    if(low){offset=std::countr_zero(low);low&=low-1;}
                    else {offset=64+std::countr_zero(high);high&=high-1;}
                    const char* delimiter=row+offset;
                    size_t trim=quoted&1;
                    quoted>>=1;
                    fields[i]=SV(start+trim,delimiter-start-2*trim);
                    start=delimiter+1;
                }
                fields.back()=SV(start+quoted,end-start-2*quoted);
                consume(fields);
            }
        }
        row=end==chunk.end?end:end+1;
    }
    scan_csv_avx2<N>(Chunk{row,chunk.end},consume);
}
inline bool row_csv_available() {
    // PEXT is microcoded on early Zen, where the streaming scanner is faster.
    return avx2_csv_available()&&__builtin_cpu_supports("bmi2")&&
        !__builtin_cpu_is("znver1")&&!__builtin_cpu_is("znver2");
}
#endif
}
