#pragma once
#include "core.hpp"

namespace shop {
struct GapSession {uint32_t ts,last;size_t count;};

// Within a GAP-wide bin the events are connected. Only the minimum and
// maximum can determine whether neighboring bins belong to the same session.
class GapSessions {
    struct Bin {uint32_t first=UINT32_MAX,last=0;size_t count=0;};
    std::vector<Bin> bins;
    std::vector<GapSession> sessions;
public:
    template<class Event>std::span<const GapSession> summarize(std::span<Event> events,uint32_t last_offset) {
        bins.assign(last_offset/GAP+1,Bin{});
        sessions.clear();
        for(const auto& event:events) {
            auto& bin=bins[event.ts/GAP];
            bin.first=std::min(bin.first,event.ts);
            bin.last=std::max(bin.last,event.ts);
            ++bin.count;
        }
        for(const auto& bin:bins)if(bin.count) {
            if(sessions.empty()||bin.first-sessions.back().last>GAP)
                sessions.push_back({bin.first,bin.last,bin.count});
            else {
                auto& session=sessions.back();
                session.last=bin.last;
                session.count+=bin.count;
            }
        }
        return sessions;
    }
};
}
