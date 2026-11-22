#pragma once
#include "core.hpp"

namespace shop {
struct GapSession {uint32_t ts,last;size_t count;};

// Within a GAP-wide bin the events are connected. Only the minimum and
// maximum can determine whether neighboring bins belong to the same session.
template<bool Sparse=false>class GapSessions {
    struct Bin {uint32_t first=UINT32_MAX,last=0;size_t count=0;};
    std::vector<Bin> bins;
    std::vector<GapSession> sessions;
    std::vector<uint64_t> occupied;
    void append(const Bin& bin) {
        if(sessions.empty()||bin.first-sessions.back().last>GAP)
            sessions.push_back({bin.first,bin.last,bin.count});
        else {
            auto& session=sessions.back();
            session.last=bin.last;
            session.count+=bin.count;
        }
    }
    // Sparse groups touch only a few bins. Visit those in time order and
    // clear their counts before the same worker processes the next person.
    template<class Event>std::span<const GapSession> summarize_sparse(std::span<Event> events,uint32_t last_offset) {
        bins.resize(last_offset/GAP+1);
        occupied.resize((bins.size()+63)/64);
        sessions.clear();
        for(const auto& event:events) {
            const size_t index=event.ts/GAP;
            auto& bin=bins[index];
            if(!bin.count) {
                bin={event.ts,event.ts,1};
                occupied[index/64]|=uint64_t{1}<<(index%64);
            } else {
                bin.first=std::min(bin.first,event.ts);
                bin.last=std::max(bin.last,event.ts);
                ++bin.count;
            }
        }
        for(size_t word=0;word<occupied.size();++word) {
            uint64_t active=occupied[word];
            if(!active)continue;
            occupied[word]=0;
            while(active) {
                auto& bin=bins[word*64+std::countr_zero(active)];
                append(bin);
                bin.count=0;
                active&=active-1;
            }
        }
        return sessions;
    }
public:
    template<class Event>std::span<const GapSession> summarize(std::span<Event> events,uint32_t last_offset) {
        if constexpr(Sparse)return summarize_sparse(events,last_offset);
        else {
            bins.assign(last_offset/GAP+1,Bin{});
            sessions.clear();
            for(const auto& event:events) {
                auto& bin=bins[event.ts/GAP];
                bin.first=std::min(bin.first,event.ts);
                bin.last=std::max(bin.last,event.ts);
                ++bin.count;
            }
            for(const auto& bin:bins)if(bin.count)append(bin);
            return sessions;
        }
    }
};
}
