#pragma once
#include "core.hpp"

namespace shop {
// Exact string keys with contiguous, open-addressed storage. The key's bytes
// are always compared; a hash collision can never merge two IDs.
template<class V,class Key=SV,class Hasher=Hash> class FlatMap {
    using Slot=std::pair<Key,V>;
    std::vector<Slot> slots;
    size_t used=0;
    static bool empty(const Key& key) {
        if constexpr(std::is_same_v<Key,SV>)return !key.data();
        else return key.empty();
    }
    static size_t bucket(const Key& key,size_t mask) {
        auto h=Hasher{}(key);
        // Partitioning already fixes the low hash bits. Reusing those bits
        // here creates long probe clusters inside every partition.
        return (h^(h>>32))&mask;
    }
public:
    void reserve(size_t n) {
        size_t capacity=16;
        while(capacity<n+n/2+1)capacity*=2;
        if(capacity<=slots.size())return;
        auto old=std::move(slots);slots.resize(capacity);used=0;
        for(auto& [key,value]:old)if(!empty(key))try_emplace(key,value);
    }
    std::pair<Slot*,bool> try_emplace(Key key,V value) {
        if(empty(key))throw std::runtime_error("empty map key");
        if((used+1)*3>=slots.size()*2)reserve(used+1);
        size_t at=bucket(key,slots.size()-1);
        while(!empty(slots[at].first)) {
            if(slots[at].first==key)return {&slots[at],false};
            at=(at+1)&(slots.size()-1);
        }
        slots[at]={key,value};++used;return {&slots[at],true};
    }
    Slot* find(const Key& key) {
        if(slots.empty())return nullptr;
        size_t at=bucket(key,slots.size()-1);
        while(!empty(slots[at].first)) {
            if(slots[at].first==key)return &slots[at];
            at=(at+1)&(slots.size()-1);
        }
        return nullptr;
    }
    const Slot* find(const Key& key)const{return const_cast<FlatMap*>(this)->find(key);}
    Slot* end(){return nullptr;}
    const Slot* end()const{return nullptr;}
    template<class F>void each(F f)const{for(auto& [key,value]:slots)if(!empty(key))f(key,value);}
    template<class F>void each_mutable(F f){for(auto& [key,value]:slots)if(!empty(key))f(key,value);}
    size_t size()const{return used;}
};
}
