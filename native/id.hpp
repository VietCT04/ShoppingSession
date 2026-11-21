#pragma once
#include "core.hpp"
#include "flat_map.hpp"
namespace shop {
// Inline up to 15 UTF8 bytes. Longer values retain an exact mapped view. No
// assumption about a generator's ID alphabet, casing, or numeric structure.
struct Id {
    uint64_t lo=0,hi=0;
    Id()=default;
    Id(SV s) {
        if(s.size()<=15) {
            if(!s.empty())std::memcpy(this,s.data(),s.size());
            hi|=uint64_t(s.size())<<56;
        } else {lo=reinterpret_cast<uintptr_t>(s.data());hi=(0xffULL<<56)|s.size();}
    }
    bool indirect()const{return (hi>>56)==255;}
    bool empty()const{return !lo&&!hi;}
    SV view()const {
        return indirect()?SV(reinterpret_cast<const char*>(lo),hi&0x00ffffffffffffffULL):SV(reinterpret_cast<const char*>(this),hi>>56);
    }
    uint64_t prefix()const {
        if(!indirect())return __builtin_bswap64(lo);
        uint64_t v;std::memcpy(&v,reinterpret_cast<const char*>(lo),8);return __builtin_bswap64(v);
    }
    bool operator==(const Id& b)const {
        if(!indirect()&&!b.indirect())return lo==b.lo&&hi==b.hi;
        return view()==b.view();
    }
    bool operator!=(const Id& b)const{return !(*this==b);}
    bool operator<(const Id& b)const {
        if(indirect()||b.indirect())return view()<b.view();
        auto a0=__builtin_bswap64(lo),b0=__builtin_bswap64(b.lo);
        return a0!=b0?a0<b0:__builtin_bswap64(hi)<__builtin_bswap64(b.hi);
    }
    bool operator>(const Id& b)const{return b<*this;}
};
static_assert(sizeof(Id)==16);
struct IdHash {
    size_t operator()(const Id& id)const noexcept {
        if(id.indirect())return Hash{}(id.view());
        uint64_t h=id.lo*0x9e3779b97f4a7c15ULL ^ std::rotl(id.hi,31);
        h^=h>>32;h*=0xd6e8feb86659fd93ULL;h^=h>>32;h*=0xd6e8feb86659fd93ULL;return h^(h>>32);
    }
};
struct PackedPeople {
    FlatMap<uint32_t,Id,IdHash> index;
    std::vector<uint32_t> parents,sizes;
    std::vector<Id> names;
    uint32_t id(Id s) {
        auto [it,added]=index.try_emplace(s,index.size());
        if(added){parents.push_back(it->second);sizes.push_back(1);names.push_back(s);}return it->second;
    }
    uint32_t root(uint32_t n){while(parents[n]!=n){parents[n]=parents[parents[n]];n=parents[n];}return n;}
    void join(Id a,Id b) {
        auto x=root(id(a)),y=root(id(b));if(x==y)return;
        if(sizes[x]<sizes[y])std::swap(x,y);
        parents[y]=x;sizes[x]+=sizes[y];if(names[y]<names[x])names[x]=names[y];
    }
    void finish(){index.each_mutable([&](Id,uint32_t& n){n=root(n);});}
    Id person(Id s)const{auto it=index.find(s);return it==index.end()?s:names[it->second];}
};
}
