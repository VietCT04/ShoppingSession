#pragma once
#include "core.hpp"
namespace shop {
// ingest_ts is guaranteed valid whole-second UTC F1. A monotonic calendar key
// suffices for both earliest-copy ordering and the 24-hour cutoff. This avoids
// Gregorian-to-epoch conversion on every event, while event/click ts parsing
// remains unchanged and strict.
struct IngestOrder {
    int64_t first_day=0;
    std::vector<int64_t> calendar;
    static unsigned pair(const char* p){return unsigned(p[0]-'0')*10+unsigned(p[1]-'0');}
    static int64_t pack(SV s) {
        const char* p=s.data();
        unsigned year=pair(p)*100+pair(p+2),month=pair(p+5),day=pair(p+8);
        return int64_t(year*512+month*32+day)*86400+pair(p+11)*3600+pair(p+14)*60+pair(p+17);
    }
    void init(int64_t start,int64_t end) {
        first_day=dayof(start);
        size_t count=dayof(end-1)-first_day+2;
        calendar.resize(count);
        for(size_t i=0;i<count;++i) {
            std::chrono::year_month_day date{std::chrono::sys_days{std::chrono::days{first_day+int64_t(i)}}};
            calendar[i]=int64_t(int(date.year())*512+unsigned(date.month())*32+unsigned(date.day()))*86400;
        }
    }
    bool on_time(int64_t order,int64_t event_ts)const {
        int64_t day=event_ts/DAY;
        int64_t second=(event_ts%DAY)/1000;
        // Ingest has whole seconds. Flooring the millisecond cutoff is exact,
        // including millisecond-precision events and exactly 24h.
        return order<=calendar[size_t(day-first_day)+1]+second;
    }
};
}
