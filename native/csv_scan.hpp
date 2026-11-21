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
#endif
}
