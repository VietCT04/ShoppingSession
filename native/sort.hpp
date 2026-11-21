#pragma once
#include "core.hpp"
// Standalone adapters for DuckDB's vendored sort components. No engine state.
#define DUCKDB_EXPLICIT_FALLTHROUGH [[fallthrough]]
namespace duckdb {
template<class T,class U> constexpr T UnsafeNumericCast(U value){return static_cast<T>(value);}
}
#include "ska_sort.hpp"
#undef DUCKDB_EXPLICIT_FALLTHROUGH
namespace shop {
struct CompactEvent {
    int64_t ts;uint32_t price,category,person;
    bool operator<(const CompactEvent& b)const{return person!=b.person?person<b.person:ts<b.ts;}
};
struct EventSortKey {
    int64_t start;
    bool requires_next_sort;
    static constexpr size_t ska_sort_width=8;
    bool Interrupted()const{return false;}
    bool ByteIsSkippable(size_t)const{return false;}
    uint64_t operator()(const CompactEvent& e)const {
        uint64_t time=uint64_t(e.ts-start);
        // A <=49-day window fits exactly. For longer windows, use the high
        // timestamp bits and DuckDB's full-comparison fallback on equal keys.
        return (uint64_t(e.person)<<32)|(requires_next_sort?time>>32:time);
    }
};
inline void duckdb_sort_events(std::vector<CompactEvent>& ev,int64_t start,int64_t end) {
    if(ev.size()>1)duckdb_ska_sort::ska_sort(ev.begin(),ev.end(),EventSortKey{start,uint64_t(end-start)>UINT32_MAX});
}
}
