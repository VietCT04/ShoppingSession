#pragma once
#include "core.hpp"
#include <memory>
#include <type_traits>

namespace shop {
// Each partition has an untouched virtual range. It acquires physical pages
// as its worker appends rows and only copies data if that range fills.
template<class Row>class ArenaBucket {
    static_assert(std::is_trivially_copyable_v<Row>);
    Row* reserved=nullptr;
    size_t capacity=0,count=0;
    std::vector<Row> expanded;
public:
    ArenaBucket(Row* storage,size_t rows):reserved(storage),capacity(rows){}
    void push_back(const Row& row) {
        if(count<capacity)std::construct_at(reserved+count,row);
        else {
            if(expanded.empty()) {
                expanded.reserve(capacity+capacity/2+1);
                expanded.insert(expanded.end(),reserved,reserved+capacity);
            }
            expanded.push_back(row);
        }
        ++count;
    }
    const Row* begin()const{return expanded.empty()?reserved:expanded.data();}
    const Row* end()const{return begin()+count;}
    size_t size()const{return count;}
    void release() {
        std::vector<Row>().swap(expanded);
        count=0;
    }
};

template<class Row>class IngestArena {
    static constexpr size_t Page=4096;
    struct Free {void operator()(std::byte* address)const{std::free(address);}};
    std::vector<std::unique_ptr<std::byte,Free>> memory;
    static size_t aligned(size_t value,size_t boundary) {
        if(value>SIZE_MAX-boundary+1)throw std::bad_alloc();
        return (value+boundary-1)&~(boundary-1);
    }
public:
    std::vector<std::vector<ArenaBucket<Row>>> buckets;
    IngestArena(size_t workers,size_t partitions,size_t input_bytes):buckets(workers) {
        size_t stride=aligned(std::max(Page,input_bytes/(workers*partitions)),Page);
        if(stride>SIZE_MAX/partitions)throw std::bad_alloc();
        size_t arena_bytes=stride*partitions;
        memory.reserve(workers);
        for(auto& worker:buckets) {
            auto* region=static_cast<std::byte*>(std::aligned_alloc(Page,arena_bytes));
            if(!region)throw std::bad_alloc();
            memory.emplace_back(region);
            worker.reserve(partitions);
            for(size_t s=0;s<partitions;++s)worker.emplace_back(reinterpret_cast<Row*>(region+s*stride),stride/sizeof(Row));
        }
    }
};
}
