#pragma once
#include <cstdint>
#include <cstring>
#include <string_view>
// Adapted from DuckDB v1.4.3, src/common/types/hash.cpp (HashBytes) and
// src/include/duckdb/common/types/hash.hpp (MurmurHash64). DuckDB MIT license:
// third_party/duckdb_sort/DUCKDB_LICENSE. Uses safe loads and standard C++ types.
// The seed credit in upstream is Martin Ankerl, MIT, copyright 2018-2021.
namespace shop {
inline uint64_t duckdb_hash(std::string_view s)noexcept {
    uint64_t h=0xe17a1465U ^ (uint64_t(s.size())*0xc6a4a7935bd1e995ULL);
    size_t offset=0;
    for(;offset+8<=s.size();offset+=8) {
        uint64_t word;std::memcpy(&word,s.data()+offset,8);
        h^=word;h*=0xd6e8feb86659fd93ULL;
    }
    if(offset<s.size()) {
        uint64_t word=0;std::memcpy(&word,s.data()+offset,s.size()-offset);
        h^=word;h*=0xd6e8feb86659fd93ULL;
    }
    h^=h>>32;h*=0xd6e8feb86659fd93ULL;
    h^=h>>32;h*=0xd6e8feb86659fd93ULL;
    return h^(h>>32);
}
}
