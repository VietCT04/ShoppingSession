#pragma once
#include "core.hpp"
#include <deque>

namespace shop {
// Nine nonzero ASCII bytes fit in 63 bits with zero padding. Other IDs keep
// their exact original bytes and use the high bit to tag a mapped view.
struct ShortId {
    static constexpr uint64_t indirect_bit=uint64_t{1}<<63;
    static constexpr uint64_t extra_bit=uint64_t{1}<<62;
    static constexpr uint64_t pointer_mask=(uint64_t{1}<<47)-1;
    using Buffer=std::array<char,9>;
    uint64_t word=0;

    ShortId()=default;
    explicit ShortId(SV text) {
        if(text.size()==9) {
            uint64_t bytes;
            std::memcpy(&bytes,text.data(),8);
            constexpr uint64_t high_bits=0x8080808080808080ULL;
            constexpr uint64_t byte_ones=0x0101010101010101ULL;
            unsigned last=static_cast<unsigned char>(text[8]);
            bool contains_zero=((bytes-byte_ones)&~bytes&high_bits)!=0;
            if(!(bytes&high_bits)&&!contains_zero&&last&&last<128) {
                // Remove the high bit in every byte while preserving the
                // order, combining pairs, quartets, then both groups.
                bytes=__builtin_bswap64(bytes);
                bytes=(bytes&0x007f007f007f007fULL)|((bytes&0x7f007f007f007f00ULL)>>1);
                bytes=(bytes&0x00003fff00003fffULL)|((bytes&0x3fff00003fff0000ULL)>>2);
                bytes=(bytes&0x000000000fffffffULL)|((bytes&0x0fffffff00000000ULL)>>4);
                word=(bytes<<7)|last;
                return;
            }
        }
        if(text.size()<=9) {
            for(unsigned char ch:text) {
                if(ch==0||ch>127) {
                    word=encode_view(text);
                    return;
                }
                word=(word<<7)|ch;
            }
            word<<=7*(9-text.size());
        } else {
            word=encode_view(text);
        }
    }
    bool empty()const{return word==0;}
    bool indirect()const{return (word&indirect_bit)!=0;}
    SV mapped_view()const {
        if(word&extra_bit)return *reinterpret_cast<const SV*>(word&~(indirect_bit|extra_bit));
        return SV(reinterpret_cast<const char*>(word&pointer_mask),(word>>47)&0x7fff);
    }
    SV view(Buffer& buffer)const {
        if(indirect())return mapped_view();
        size_t count=0;
        for(;count<buffer.size();++count) {
            char ch=char((word>>(7*(8-count)))&127);
            if(!ch)break;
            buffer[count]=ch;
        }
        return SV(buffer.data(),count);
    }
    uint64_t prefix()const {
        Buffer buffer;
        SV value=view(buffer);
        uint64_t result=0;
        std::memcpy(&result,value.data(),std::min(value.size(),size_t(8)));
        return __builtin_bswap64(result);
    }
    bool operator==(const ShortId& other)const {
        if(!indirect()&&!other.indirect())return word==other.word;
        if(indirect()!=other.indirect())return false;
        return mapped_view()==other.mapped_view();
    }
    bool operator<(const ShortId& other)const {
        if(!indirect()&&!other.indirect())return word<other.word;
        Buffer a,b;
        return view(a)<other.view(b);
    }
    bool operator>(const ShortId& other)const{return other<*this;}
private:
    static uint64_t encode_view(SV value) {
        auto address=reinterpret_cast<uintptr_t>(value.data());
        if(address<=pointer_mask&&value.size()<=0x7fff)
            return indirect_bit|(uint64_t(value.size())<<47)|address;
        // Preserve arbitrary field lengths and x86-64 five-level addresses.
        // Deque insertion does not invalidate references to existing views.
        static std::mutex mutex;
        static std::deque<SV> exceptional_views;
        std::lock_guard guard(mutex);
        exceptional_views.push_back(value);
        address=reinterpret_cast<uintptr_t>(&exceptional_views.back());
        if(address>=extra_bit)throw std::runtime_error("unsupported user-space address");
        return indirect_bit|extra_bit|address;
    }
};
static_assert(sizeof(ShortId)==8);

struct ShortIdHash {
    size_t operator()(const ShortId& id)const noexcept {
        if(id.indirect())return Hash{}(id.mapped_view());
        uint64_t hash=id.word;
        hash^=hash>>32;
        hash*=0xd6e8feb86659fd93ULL;
        hash^=hash>>32;
        hash*=0xd6e8feb86659fd93ULL;
        return hash^(hash>>32);
    }
};
}
