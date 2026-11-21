#pragma once
#include "core.hpp"

namespace shop {
// Exact string keys with contiguous, open-addressed storage. The key's bytes
// are always compared; a hash collision can never merge two IDs.
template<class V,class Key=SV,class Hasher=Hash> class FlatMap {
    using Slot=std::pair<Key,V>;
    std::vector<Slot> slots;
    std::vector<uint8_t> tags;
    size_t used=0;
    static bool empty(const Key& key) {
        if constexpr(std::is_same_v<Key,SV>)return !key.data();
        else return key.empty();
    }
    static size_t bucket(size_t h,size_t mask) {
        // Partitioning already fixes the low hash bits. Reusing those bits
        // here creates long probe clusters inside every partition.
        return (h^(h>>32))&mask;
    }
    static uint8_t tag(size_t hash) {
        return 1+((uint64_t(hash)>>57)&127);
    }
#ifdef __SSE2__
    Slot* find_tagged(const Key& key,size_t hash,size_t at) {
        auto wanted=_mm_set1_epi8(static_cast<char>(tag(hash)));
        auto zero=_mm_setzero_si128();
        const auto mask=slots.size()-1;
        for(;;) {
            auto control=_mm_loadu_si128(reinterpret_cast<const __m128i*>(tags.data()+at));
            unsigned matches=_mm_movemask_epi8(_mm_cmpeq_epi8(control,wanted));
            while(matches) {
                auto& slot=slots[(at+std::countr_zero(matches))&mask];
                if(slot.first==key)return &slot;
                matches&=matches-1;
            }
            if(_mm_movemask_epi8(_mm_cmpeq_epi8(control,zero)))return nullptr;
            at=(at+16)&mask;
        }
    }
#endif
public:
    // Read-only SIMD probe metadata, inspired by Swiss tables. Exact key
    // equality remains required. The extra bytes mirror the wraparound group.
    void freeze() {
#ifdef __SSE2__
        if(slots.empty())return;
        tags.assign(slots.size()+16,0);
        for(size_t i=0;i<slots.size();++i) {
            if(!empty(slots[i].first))tags[i]=tag(Hasher{}(slots[i].first));
        }
        std::copy_n(tags.begin(),16,tags.begin()+slots.size());
#endif
    }
    void reserve(size_t n) {
        size_t capacity=16;
        while(capacity<n+n/2+1)capacity*=2;
        if(capacity<=slots.size())return;
        tags.clear();
        auto old=std::move(slots);slots.resize(capacity);used=0;
        for(auto& [key,value]:old)if(!empty(key))try_emplace(key,value);
    }
    std::pair<Slot*,bool> try_emplace(Key key,V value) {
        tags.clear();
        if(empty(key))throw std::runtime_error("empty map key");
        if((used+1)*3>=slots.size()*2)reserve(used+1);
        size_t at=bucket(Hasher{}(key),slots.size()-1);
        while(!empty(slots[at].first)) {
            if(slots[at].first==key)return {&slots[at],false};
            at=(at+1)&(slots.size()-1);
        }
        slots[at]={key,value};++used;return {&slots[at],true};
    }
    Slot* find(const Key& key,size_t hash) {
        if(slots.empty())return nullptr;
        size_t at=bucket(hash,slots.size()-1);
#ifdef __SSE2__
        if(!tags.empty())return find_tagged(key,hash,at);
#endif
        while(!empty(slots[at].first)) {
            if(slots[at].first==key)return &slots[at];
            at=(at+1)&(slots.size()-1);
        }
        return nullptr;
    }
    Slot* find(const Key& key){return slots.empty()?nullptr:find(key,Hasher{}(key));}
    const Slot* find(const Key& key,size_t hash)const{return const_cast<FlatMap*>(this)->find(key,hash);}
    const Slot* find(const Key& key)const{return const_cast<FlatMap*>(this)->find(key);}
    void prefetch(size_t hash)const {
        if(slots.empty())return;
        size_t at=bucket(hash,slots.size()-1);
        if(!tags.empty())__builtin_prefetch(tags.data()+at,0,1);
        __builtin_prefetch(slots.data()+at,0,1);
    }
    Slot* end(){return nullptr;}
    const Slot* end()const{return nullptr;}
    template<class F>void each(F f)const{for(auto& [key,value]:slots)if(!empty(key))f(key,value);}
    template<class F>void each_mutable(F f){for(auto& [key,value]:slots)if(!empty(key))f(key,value);}
    size_t size()const{return used;}
};
}
