#pragma once
#include "core.hpp"
namespace shop {
// Dense counters with sparse membership/reset. Work is proportional to touched
// IDs; there is no allocation or scan over every campaign for each person.
struct CampaignWindow {
    std::vector<size_t> count;
    std::vector<uint32_t> position,present;
    explicit CampaignWindow(size_t n):count(n),position(n){present.reserve(n);}
    void clear(){for(auto c:present)count[c]=0;present.clear();}
    void add(uint32_t c){if(!count[c]++){position[c]=present.size();present.push_back(c);}}
    void remove(uint32_t c) {
        if(!--count[c]) {
            auto last=present.back();auto at=position[c];
            present[at]=last;position[last]=at;present.pop_back();
        }
    }
};
struct CategoryRevenue {
    std::vector<int64_t> sum;
    std::vector<uint32_t> present;
    explicit CategoryRevenue(size_t n):sum(n){present.reserve(n);}
    void add(uint32_t c,uint32_t price){if(!sum[c])present.push_back(c);sum[c]+=price;}
    void clear(){for(auto c:present)sum[c]=0;present.clear();}
    uint32_t top(const std::vector<SV>& names)const {
        uint32_t best=present[0];
        for(auto c:present)if(sum[c]>sum[best]||(sum[c]==sum[best]&&names[c]<names[best]))best=c;
        return best;
    }
};
}
