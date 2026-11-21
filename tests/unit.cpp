#include "../native/core.hpp"
#include "../native/sort.hpp"
#include <cstdlib>
#include <random>
using namespace shop;
void require(bool v,const char* label){if(!v)throw std::runtime_error(label);}
int main(){try{
    std::array<SV,2> fields;
    require(csv("\" a\",\"b \"",fields)&&fields[0]==" a"&&fields[1]=="b ","quoted spaces");
    require(csv("a,b\r",fields)&&fields[1]=="b\r","CR preserved");
    require(csv("a,",fields)&&fields[1].empty(),"empty field");
    for(auto s:{"a,b,c","a,\"b","a,b\"","a,\"b\" ","a\"a,b","a,\"\"\"",""})require(!csv(s,fields),"malformed CSV");
    const int64_t ts=timestamp("2026-10-03T23:58:10Z");
    require(ts==1791071890000LL,"epoch conversion");
    require(timestamp("2026-10-03T19:58:10-04:00")==ts,"offset conversion");
    require(timestamp("00000001791071890000")==ts,"20-digit epoch");
    require(timestamp("0000000000001791071890000")==ts,"25-digit epoch");
    require(timestamp("2026-10-03T23:58:10-00:00")==ts,"negative zero offset");
    require(timestamp("2028-02-29T00:00:00Z")>=0,"leap day");
    for(auto s:{"2026-02-29T00:00:00Z","2028-02-30T00:00:00Z","2026-10-03T24:00:00Z","2026-10-03T23:58:60Z","2026-10-03T23:58:10z","2026-10-03T23:58:10Z\r","2026-10-03T23:58:10+24:00","2026-10-03T23:58:10+05:60","2026-10-03T23:58:10+0200","1791071890000.0","-5","9999999999999999999999999999999999999999999999999999999","２５０"})require(timestamp(s)==-1,"invalid timestamp");
    require(timestamp("0000000000000000000000000000000000000000000000000001")==1,"arbitrary leading zeros");
    uint64_t n=0;require(digits("000250",n,10000000)&&n==250,"price leading zero");
    require(!digits("10000001",n,10000000)&&!digits("٢٥٠",n),"price bounds and ASCII");
    People p;p.join("z","y");p.join("y","x");p.join("A","z");p.join("z","z");p.join("é","A");p.finish();
    require(p.person("x")=="A"&&p.person("é")=="A"&&p.person("independent")=="independent","union find minimum");
    std::mt19937_64 rng(938151);
    for(uint64_t width:{uint64_t(14*DAY),uint64_t(UINT32_MAX)+1,uint64_t(500*DAY)}) {
        std::vector<CompactEvent> rows;
        for(size_t i=0;i<100000;++i)rows.push_back({int64_t(1700000000000LL+rng()%width),0,0,uint32_t(rng()%8192)});
        rows.push_back(rows[0]);
        auto expected=rows;std::sort(expected.begin(),expected.end());
        duckdb_sort_events(rows,1700000000000LL,1700000000000LL+width);
        for(size_t i=0;i<rows.size();++i)require(rows[i].person==expected[i].person&&rows[i].ts==expected[i].ts,"ported DuckDB sort matches std::sort");
    }
    std::cout<<"unit checks passed\n";
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
