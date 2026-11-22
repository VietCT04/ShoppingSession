#pragma once
#include "flat_map.hpp"

namespace shop {
// The link graph and subsequent grouping share one table per source shard.
// Until grouping starts, values contain only the component ordinal.
template<class Key,class Hash,class GroupHash>class ShardedPeople {
    std::vector<uint32_t> parents,sizes;
    size_t shard_mask=0;

    uint32_t root(uint32_t node) {
        while(parents[node]!=node) {
            parents[node]=parents[parents[node]];
            node=parents[node];
        }
        return node;
    }
    uint32_t intern(Key name) {
        const uint64_t hash=Hash{}(name);
        auto& table=index[hash&shard_mask];
        auto [entry,inserted]=table.template try_emplace<true>(name,names.size(),hash>>32);
        if(inserted) {
            if(names.size()>=UINT32_MAX)throw std::runtime_error("too many linked aliases");
            parents.push_back(uint32_t(names.size()));
            sizes.push_back(1);
            names.push_back(name);
        }
        return uint32_t(entry->second);
    }

public:
    using Index=FlatMap<uint64_t,Key,GroupHash>;
    std::vector<Index> index;
    std::vector<Key> names;

    void init(size_t shards,size_t edges) {
        shard_mask=shards-1;
        index.resize(shards);
        if(edges) {
            parents.reserve(edges);
            sizes.reserve(edges);
            names.reserve(edges);
            for(auto& table:index)table.reserve(edges*2/shards+1);
        }
    }
    void join(Key a,Key b) {
        auto left=root(intern(a)),right=root(intern(b));
        if(left==right)return;
        if(sizes[left]<sizes[right])std::swap(left,right);
        parents[right]=left;
        sizes[left]+=sizes[right];
        if(names[right]<names[left])names[left]=names[right];
    }
    void finish() {
        std::vector<uint32_t>().swap(sizes);
    }
    uint32_t component(uint32_t node)const {
        // The graph is immutable at this point. An alias resolves its root
        // only when the first surviving event or click reaches its shard.
        while(parents[node]!=node)node=parents[node];
        return node;
    }
};
}
