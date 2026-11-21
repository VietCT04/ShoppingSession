#pragma once
#include "core.hpp"

namespace shop {
struct NarrowLocation {uint32_t ingest,position;};

class LocationCodec {
    static constexpr uint32_t wide=UINT32_MAX;
    struct Overflow {int64_t ingest;uint64_t position;};
    int64_t base;
    unsigned shard_bits,row_bits,spill_bits;
    uint64_t row_mask,shard_mask,spill_mask;
    std::vector<std::vector<Overflow>> overflow;

    const Overflow& exceptional(NarrowLocation location)const {
        auto index=uint64_t(location.position);
        return overflow[index>>spill_bits][index&spill_mask];
    }
    int64_t time(NarrowLocation location)const {
        return location.ingest==wide?exceptional(location).ingest:base+location.ingest;
    }
public:
    LocationCodec(int64_t base_ms,unsigned workers,unsigned shards):
        base(base_ms),shard_bits(std::countr_zero(shards)),
        row_bits(32u-std::bit_width(workers-1)-shard_bits),spill_bits(32u-std::bit_width(workers-1)),
        row_mask((uint64_t{1}<<row_bits)-1),shard_mask(shards-1),spill_mask((uint64_t{1}<<spill_bits)-1),overflow(workers){}

    static uint64_t wide_position(uint64_t worker,uint64_t shard,uint64_t index) {
        return index?(worker<<48)|(shard<<32)|index:0;
    }
    NarrowLocation encode(int64_t ingest,unsigned worker,unsigned shard,uint64_t index) {
        uint64_t relative=uint64_t(ingest)-uint64_t(base);
        if(relative<wide&&index<=row_mask) {
            uint64_t slot=(uint64_t(worker)<<shard_bits)|shard;
            return {uint32_t(relative),index?uint32_t((slot<<row_bits)|index):0};
        }
        auto& entries=overflow[worker];
        if(entries.size()>spill_mask)throw std::runtime_error("too many wide event locations");
        uint32_t position=uint32_t((uint64_t(worker)<<spill_bits)|entries.size());
        entries.push_back({ingest,wide_position(worker,shard,index)});
        return {wide,position};
    }
    bool earlier(NarrowLocation a,NarrowLocation b)const {
        if(a.ingest!=wide&&b.ingest!=wide)return a.ingest<b.ingest;
        return time(a)<time(b);
    }
    uint64_t decode(NarrowLocation location)const {
        if(location.ingest==wide)return exceptional(location).position;
        if(!location.position)return 0;
        uint64_t position=location.position;
        uint64_t slot=position>>row_bits;
        return wide_position(slot>>shard_bits,slot&shard_mask,position&row_mask);
    }
};
}
