#pragma once
// Minimal, dependency-free Parquet v1 writer. PLAIN values, uncompressed pages,
// RLE definition levels, UTF8 strings and DATE logical annotation.
// Wire formats: apache/parquet-format and Apache Thrift compact protocol.
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace parquet {
using Bytes = std::string;
inline void varint(Bytes& b, uint64_t n) {
    while (n >= 128) { b += char((n & 127) | 128); n >>= 7; }
    b += char(n);
}
inline void integer(Bytes& b, int64_t n) { varint(b, (uint64_t(n) << 1) ^ uint64_t(n >> 63)); }
inline void binary(Bytes& b, std::string_view s) { varint(b, s.size()); b.append(s); }
template<class T> inline void little(Bytes& b, T n) {
    for (size_t i=0;i<sizeof(T);++i) b += char(uint64_t(n) >> (8*i));
}
struct Compact {
    Bytes b;
    int prev = 0;
    void field(int id, int type) {
        int delta=id-prev;
        if(delta>0 && delta<16) b+=char(delta*16+type);
        else { b+=char(type); integer(b,id); }
        prev=id;
    }
    void num(int id, int64_t v, int type=5) { field(id,type); integer(b,v); }
    void str(int id, std::string_view v) { field(id,8); binary(b,v); }
    void structure(int id, const Compact& s) { field(id,12); b+=s.b; b+=char(0); }
    void list(int id,int type,size_t n) {
        field(id,9);
        if(n<15) b+=char(n*16+type);
        else { b+=char(0xf0+type); varint(b,n); }
    }
    void item(const Compact& s) { b+=s.b; b+=char(0); }
};
struct Column {
    std::string name;
    int type; // Parquet INT32=1, INT64=2, BYTE_ARRAY=6
    bool nullable=false;
    bool date=false;
    Bytes values, levels;
    uint64_t count=0;
    void present(bool yes) { if(nullable) levels+=char(yes); ++count; }
    void number(int64_t n) { present(true); if(type==1) little<int32_t>(values,n); else little<int64_t>(values,n); }
    void text(std::string_view s) { present(true); little<uint32_t>(values,s.size()); values.append(s); }
    void null() { if(!nullable) throw std::runtime_error("null in required column"); present(false); }
};
class File {
    std::ofstream out;
    std::vector<Compact> groups;
    uint64_t rows=0;

public:
    explicit File(const std::string& path):out(path,std::ios::binary) {
        out.exceptions(std::ios::badbit | std::ios::failbit);
        out.write("PAR1",4);
    }

    void append(const std::vector<Column>& columns) {
        if(columns.empty()) throw std::runtime_error("empty schema");
        Compact chunks;
        uint64_t total=0, group_rows=columns[0].count;
        for(const auto& c:columns) {
            if(c.count!=group_rows) throw std::runtime_error("unequal column lengths");
            Bytes page;
            if(c.nullable) {
                Bytes rle;
                for(size_t i=0;i<c.levels.size();) {
                    size_t j=i+1;
                    while(j<c.levels.size() && c.levels[j]==c.levels[i]) ++j;
                    varint(rle,(j-i)*2); rle+=c.levels[i]; i=j;
                }
                little<uint32_t>(page,rle.size()); page+=rle;
            }
            page+=c.values;
            if(page.size()>INT32_MAX || group_rows>INT32_MAX) throw std::runtime_error("Parquet page too large; use more output partitions");
            Compact data; data.num(1,group_rows); data.num(2,0); data.num(3,3); data.num(4,3);
            Compact header; header.num(1,0); header.num(2,page.size()); header.num(3,page.size()); header.structure(5,data);
            header.b+=char(0);
            uint64_t offset=uint64_t(out.tellp()), size=header.b.size()+page.size();
            out.write(header.b.data(),header.b.size()); out.write(page.data(),page.size());
            Compact meta; meta.num(1,c.type); meta.list(2,5,2); integer(meta.b,0); integer(meta.b,3);
            meta.list(3,8,1); binary(meta.b,c.name); meta.num(4,0); meta.num(5,group_rows,6);
            meta.num(6,size,6); meta.num(7,size,6); meta.num(9,offset,6);
            Compact chunk; chunk.num(2,offset,6); chunk.structure(3,meta); chunks.item(chunk); total+=size;
        }
        Compact group; group.list(1,12,columns.size()); group.b+=chunks.b;
        group.num(2,total,6); group.num(3,group_rows,6);
        groups.push_back(std::move(group));
        rows+=group_rows;
    }

    void finish(const std::vector<Column>& columns) {
        Compact root; root.str(4,"schema"); root.num(5,columns.size());
        Compact footer; footer.num(1,1); footer.list(2,12,columns.size()+1); footer.item(root);
        for(const auto& c:columns) {
            Compact schema; schema.num(1,c.type); schema.num(3,c.nullable?1:0); schema.str(4,c.name);
            if(c.type==6) schema.num(6,0); else if(c.date) schema.num(6,6);
            footer.item(schema);
        }
        footer.num(3,rows,6); footer.list(4,12,groups.size());
        for(const auto& group:groups)footer.item(group);
        footer.str(6,"shopping-sessions-cpp"); footer.b+=char(0);
        out.write(footer.b.data(),footer.b.size());
        Bytes length; little<uint32_t>(length,footer.b.size()); out.write(length.data(),4); out.write("PAR1",4);
        out.close();
    }
};

inline void write(const std::string& path, const std::vector<Column>& columns) {
    File file(path);
    file.append(columns);
    file.finish(columns);
}
}
