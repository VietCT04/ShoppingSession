#pragma once
#include "hash.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace shop {
namespace fs=std::filesystem;
using SV=std::string_view;
constexpr int64_t DAY=86400000, WEEK=7*DAY, GAP=1800000;
struct Hash {
    size_t operator()(SV s) const noexcept {
        // Full bytes determine identity; hashes only select buckets.
#ifndef SHOP_LEGACY_HASH
        return duckdb_hash(s);
#else
        uint64_t h=0xa0761d6478bd642fULL;
        for(unsigned char c:s) h=(h^c)*0x100000001b3ULL;
        h^=h>>32; h*=0xd6e8feb86659fd93ULL; h^=h>>32;
        return h;
#endif
    }
};
template<class V> using Dict=std::unordered_map<SV,V,Hash>;
struct Mapping {
    const char* data=nullptr;
    size_t size=0;
    explicit Mapping(const fs::path& p) {
        int fd=open(p.c_str(),O_RDONLY);
        if(fd<0) throw std::runtime_error("cannot open "+p.string());
        struct stat st{};
        if(fstat(fd,&st)!=0) { close(fd); throw std::runtime_error("cannot stat "+p.string()); }
        size=st.st_size;
        if(size) {
            void* m=mmap(nullptr,size,PROT_READ,MAP_PRIVATE,fd,0);
            if(m==MAP_FAILED) {close(fd);throw std::runtime_error("cannot mmap "+p.string());}
            data=static_cast<const char*>(m);
        }
        close(fd);
    }
    ~Mapping(){if(data) munmap(const_cast<char*>(data),size);}
    Mapping(const Mapping&)=delete;
};
inline std::vector<fs::path> files(const fs::path& dir) {
    std::vector<fs::path> r;
    for(const auto& p:fs::directory_iterator(dir)) if(p.is_regular_file() && p.path().extension()==".csv") r.push_back(p.path());
    std::sort(r.begin(),r.end()); return r;
}
// Only LF ends a record. Every comma separates fields. Whole-field quotes only.
template<size_t N> bool csv(SV line,std::array<SV,N>& f) {
    size_t start=0;
    for(size_t i=0;i<N;++i) {
        size_t end=line.find(',',start);
        if((i==N-1)!=(end==SV::npos)) return false;
        if(end==SV::npos) end=line.size();
        SV v=line.substr(start,end-start);
        if(!v.empty() && v.front()=='"') {
            if(v.size()<2 || v.back()!='"') return false;
            v.remove_prefix(1);v.remove_suffix(1);
        }
        if(v.find('"')!=SV::npos) return false;
        f[i]=v;start=end+1;
    }
    return true;
}
inline bool digits(SV s,uint64_t& n,uint64_t limit=INT64_MAX) {
    if(s.empty())return false;
    n=0;
    for(unsigned char c:s) {
        if(c<'0'||c>'9'||n>limit/10||(n==limit/10&&uint64_t(c-'0')>limit%10)) return false;
        n=n*10+c-'0';
    }
    return true;
}
// Howard Hinnant's civil-date arithmetic, proleptic Gregorian calendar.
inline int64_t civil(int y,unsigned m,unsigned d) {
    y-=m<=2; int era=(y>=0?y:y-399)/400; unsigned yoe=y-era*400;
    unsigned doy=(153*(m+(m>2?-3:9))+2)/5+d-1;
    return era*146097+int(yoe*365+yoe/4-yoe/100+doy)-719468;
}
inline int64_t timestamp(SV s) {
    if(s.size()!=20 && s.size()!=25) {uint64_t n;return digits(s,n)?int64_t(n):-1;}
    // Epoch milliseconds can also have exactly 20 or 25 characters (leading zeros).
    if(s[4]!='-') {uint64_t n;return digits(s,n)?int64_t(n):-1;}
    if(s[7]!='-'||s[10]!='T'||s[13]!=':'||s[16]!=':')return -1;
    uint64_t y,m,d,h,mi,se;
    if(!digits(s.substr(0,4),y)||!digits(s.substr(5,2),m)||!digits(s.substr(8,2),d)||
       !digits(s.substr(11,2),h)||!digits(s.substr(14,2),mi)||!digits(s.substr(17,2),se))return -1;
    static const unsigned days[]={0,31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap=y%4==0&&(y%100!=0||y%400==0);
    if(m<1||m>12||d<1||d>days[m]+(m==2&&leap)||h>23||mi>59||se>59)return -1;
    int64_t offset=0;
    if(s.size()==20) {if(s[19]!='Z')return -1;}
    else {
        uint64_t oh,om;
        if((s[19]!='+'&&s[19]!='-')||s[22]!=':'||!digits(s.substr(20,2),oh)||!digits(s.substr(23,2),om)||oh>23||om>59)return -1;
        offset=int64_t(oh*60+om)*60000*(s[19]=='+'?1:-1);
    }
    return civil(y,m,d)*DAY+int64_t(h*3600+mi*60+se)*1000-offset;
}
inline int64_t dayof(int64_t ts) {return ts>=0?ts/DAY:(ts-(DAY-1))/DAY;}
struct Chunk {const char* begin;const char* end;};
struct Inputs {
    std::vector<std::unique_ptr<Mapping>> maps;
    std::vector<Chunk> events,clicks,links;
    void add(const fs::path& p,std::vector<Chunk>& chunks,size_t bytes=1<<20) {
        auto m=std::make_unique<Mapping>(p);
        if(m->size) {
            const char* end=m->data+m->size;
            const char* first=static_cast<const char*>(memchr(m->data,'\n',m->size));
            const char* b=first?first+1:end;
            while(b<end) {
                const char* e=b+std::min<size_t>(bytes,end-b);
                if(e<end) {const char* nl=static_cast<const char*>(memchr(e,'\n',end-e));e=nl?nl+1:end;}
                chunks.push_back({b,e});b=e;
            }
        }
        maps.push_back(std::move(m));
    }
    template<class F> static void lines(Chunk c,F f) {
        const char* p=c.begin;
        while(p<c.end) {
            const char* e=static_cast<const char*>(memchr(p,'\n',c.end-p));
            if(!e)e=c.end;
            f(SV(p,e-p));p=e==c.end?e:e+1;
        }
    }
};
template<class F> void parallel(size_t jobs,int threads,F f) {
    std::atomic<size_t> next{0};std::exception_ptr failure;std::mutex mu;
    std::vector<std::thread> pool;
    for(int t=0;t<threads;++t)pool.emplace_back([&,t]{
        try {while(true){auto i=next.fetch_add(1,std::memory_order_relaxed);if(i>=jobs)break;f(i,t);}}
        catch(...){std::lock_guard<std::mutex> lock(mu);if(!failure)failure=std::current_exception();next=jobs;}
    });
    for(auto& t:pool)t.join();
    if(failure)std::rethrow_exception(failure);
}
struct People {
    Dict<uint32_t> index;
    std::vector<uint32_t> parents,sizes;
    std::vector<SV> names;
    uint32_t id(SV s) {
        auto [it,added]=index.try_emplace(s,index.size());
        if(added){parents.push_back(it->second);sizes.push_back(1);names.push_back(s);}return it->second;
    }
    uint32_t root(uint32_t n){while(parents[n]!=n){parents[n]=parents[parents[n]];n=parents[n];}return n;}
    void join(SV a,SV b) {
        auto x=root(id(a)),y=root(id(b));if(x==y)return;
        if(sizes[x]<sizes[y])std::swap(x,y);
        parents[y]=x;sizes[x]+=sizes[y];names[x]=std::min(names[x],names[y]);
    }
    void finish(){for(auto& [s,n]:index)n=root(n);}
    SV person(SV s) const {auto it=index.find(s);return it==index.end()?s:names[it->second];}
};
struct Timer {
    std::chrono::steady_clock::time_point start=std::chrono::steady_clock::now(),last=start;
    void mark(const char* label) {auto now=std::chrono::steady_clock::now();std::cerr<<label<<" "<<std::chrono::duration<double>(now-last).count()<<"s (total "<<std::chrono::duration<double>(now-start).count()<<"s)\n";last=now;}
};
}
