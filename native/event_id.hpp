#pragma once
#include "short_id.hpp"

namespace shop {
// Inline event IDs mix their bits once, when parsed. Every step is a
// bijection on 63 bits, keeping the mixed value suitable for exact equality.
// Event IDs need only a consistent order for grouping equal dedup keys.
class ReversibleEventId {
    ShortId value;
public:
    ReversibleEventId()=default;
    explicit ReversibleEventId(SV input):value(input) {
        if(value.indirect())return;
        constexpr uint64_t mask=ShortId::indirect_bit-1;
        constexpr uint64_t multiplier=0xd6e8feb86659fd93ULL;
        auto bits=value.word;
        bits=((bits^(bits>>32))*multiplier)&mask;
        bits=((bits^(bits>>32))*multiplier)&mask;
        value.word=bits^(bits>>32);
    }
    bool empty()const{return value.empty();}
    bool operator==(const ReversibleEventId& other)const{return value==other.value;}
    bool operator<(const ReversibleEventId& other)const {
        const bool indirect=value.indirect(),other_indirect=other.value.indirect();
        if(indirect!=other_indirect)return other_indirect;
        if(indirect)return value.mapped_view()<other.value.mapped_view();
        return value.word<other.value.word;
    }
    size_t hash()const noexcept{return value.indirect()?ShortIdHash{}(value):value.word;}
};
static_assert(sizeof(ReversibleEventId)==sizeof(ShortId));

struct ReversibleEventHash {
    size_t operator()(const ReversibleEventId& id)const noexcept{return id.hash();}
};
}
