#pragma once
#include "core.hpp"

namespace shop {
// Exact string keys with contiguous, open-addressed storage. The key's bytes
// are always compared; a hash collision can never merge two IDs.
template<class V> class FlatMap {
    using Slot=std::pair<SV,V>;
    std::vector<Slot> slots;
    size_t used=0;
public:
    void reserve(size_t n) {
        size_t capacity=16;
        while(capacity<n+n/2+1)capacity*=2;
        if(capacity<=slots.size())return;
        auto old=std::move(slots);slots.resize(capacity);used=0;
        for(auto& [key,value]:old)if(key.data())try_emplace(key,value);
    }
    std::pair<Slot*,bool> try_emplace(SV key,V value) {
        if(!key.data())throw std::runtime_error("null string_view key");
        if((used+1)*3>=slots.size()*2)reserve(used+1);
        size_t at=Hash{}(key)&(slots.size()-1);
        while(slots[at].first.data()) {
            if(slots[at].first==key)return {&slots[at],false};
            at=(at+1)&(slots.size()-1);
        }
        slots[at]={key,value};++used;return {&slots[at],true};
    }
    Slot* find(SV key) {
        if(slots.empty())return nullptr;
        size_t at=Hash{}(key)&(slots.size()-1);
        while(slots[at].first.data()) {
            if(slots[at].first==key)return &slots[at];
            at=(at+1)&(slots.size()-1);
        }
        return nullptr;
    }
    const Slot* find(SV key)const{return const_cast<FlatMap*>(this)->find(key);}
    Slot* end(){return nullptr;}
    const Slot* end()const{return nullptr;}
    template<class F>void each(F f)const{for(auto& [key,value]:slots)if(key.data())f(key,value);}
    size_t size()const{return used;}
};
}
