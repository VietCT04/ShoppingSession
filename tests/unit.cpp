#include "../native/core.hpp"
#include "../native/sort.hpp"
#include "../native/flat_map.hpp"
#include "../native/id.hpp"
#include "../native/short_id.hpp"
#include "../native/event_id.hpp"
#include "../native/worker_pool.hpp"
#include "../native/csv_scan.hpp"
#include "../native/ingest_order.hpp"
#include "../native/sparse_state.hpp"
#include "../native/fast_time.hpp"
#include "../native/ingest_arena.hpp"
#include "../native/location_codec.hpp"
#include "../native/gap_sessions.hpp"
#include <cstdlib>
#include <cstdio>
#include <random>
using namespace shop;
void require(bool v,const char* label){if(!v)throw std::runtime_error(label);}
int main(){try{
    {
        LocationCodec codec(1000,8,256);
        auto regular=codec.encode(2000,5,42,123);
        auto earlier=codec.encode(-2000,5,42,0);
        auto wide_row=codec.encode(3000,5,42,uint64_t{1}<<22);
        require(codec.decode(regular)==LocationCodec::wide_position(5,42,123),"compact partition position");
        require(!codec.decode(earlier)&&codec.earlier(earlier,regular),"invalid early copy keeps exact ingest order");
        require(codec.decode(wide_row)==LocationCodec::wide_position(5,42,uint64_t{1}<<22)&&codec.earlier(regular,wide_row),
                "wide partition position and timestamp fallback");
    }
    {
        std::array<uint64_t,2> storage;
        ArenaBucket<uint64_t> bucket(storage.data(),storage.size());
        for(uint64_t value:{1,2,3,4})bucket.push_back(value);
        require(std::vector<uint64_t>(bucket.begin(),bucket.end())==std::vector<uint64_t>{1,2,3,4},
                "arena overflow preserves all raw rows");
        bucket.release();
        bucket.push_back(9);
        require(bucket.size()==1&&*bucket.begin()==9,"arena bucket can reuse its original range");
    }
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
    {
        WorkerPool pool(3);
        bool propagated=false;
        try {pool.run(8,[](size_t i,int){if(i==3)throw std::runtime_error("worker test");});}
        catch(const std::runtime_error&){propagated=true;}
        require(propagated,"pool propagates worker failures");
        for(int round=0;round<3;++round) {
            std::vector<int> assigned(17,-1);
            pool.run(assigned.size(),[&](size_t i,int worker){assigned[i]=worker;});
            require(std::all_of(assigned.begin(),assigned.end(),[](int worker){return worker>=0&&worker<3;}),
                "pool assigns every job in each stage");
        }
    }
    FlatMap<uint32_t> map;
    std::vector<std::string> keys;keys.reserve(10000);
    for(size_t i=0;i<10000;++i)keys.push_back("exact key "+std::to_string(i));
    for(size_t i=0;i<keys.size();++i) {
        const auto added=i%2?map.try_emplace<true>(keys[i],i,Hash{}(keys[i])).second:map.try_emplace(keys[i],i).second;
        require(added,"flat insert with computed and supplied hashes");
    }
    for(size_t i=0;i<keys.size();++i)require(map.find(keys[i])&&map.find(keys[i])->second==i,"flat growth preserves keys");
    require(!map.find("missing")&&!map.try_emplace(keys[9],55).second,"flat lookup and duplicate");
    map.freeze();
    for(size_t i=0;i<keys.size();++i)require(map.find(keys[i])&&map.find(keys[i])->second==i,"tagged probe finds exact keys");
    for(size_t i=0;i<10000;++i)require(!map.find("missing key "+std::to_string(i)),"tagged probe rejects absent keys");
    require(map.try_emplace("insert after freeze",112).second&&map.find("insert after freeze")->second==112,
            "insertion invalidates read-only probe metadata");
    std::mt19937_64 rng(938151);
    for(size_t i=0;i<30000;++i) {
        std::string a,b;
        size_t len=i%3?9:rng()%20;
        for(size_t j=0;j<len;++j)a+=char(i%4?1+rng()%127:rng()%256);
        b=a;
        if(!b.empty()&&i%2)b[rng()%b.size()]=char(rng()%256);
        ShortId x(a),y(b);
        ShortId::Buffer left,right;
        require(x.view(left)==a&&y.view(right)==b,"8-byte keys preserve exact bytes");
        require((x==y)==(a==b)&&(x<y)==(SV(a)<SV(b)),"8-byte keys preserve lexical order");
        if(a==b)require(ShortIdHash{}(x)==ShortIdHash{}(y),"equal 8-byte keys hash equally");
        ReversibleEventId event_a(a),event_b(b);
        require((event_a==event_b)==(a==b),"mixed event ID keeps exact equality");
        require((!(event_a<event_b)&&!(event_b<event_a))==(a==b),"mixed event ID comparison groups equal keys");
        if(a==b)require(event_a.hash()==event_b.hash(),"equal mixed event IDs hash equally");
    }
    {
        std::string large(40000,'A');
        ShortId key(large);
        ShortId::Buffer buffer;
        require(key.view(buffer)==large,"large IDs retain complete mapped field");
    }
    {
        StrictTimeParser parser;parser.init(timestamp("2028-02-20T00:00:00Z"),timestamp("2028-03-05T00:00:00Z"),true);
        for(size_t i=0;i<300000;++i) {
            char value[48];
            unsigned y=i%3?2027+rng()%3:rng()%10000,m=rng()%15,d=rng()%34,h=rng()%26,mi=rng()%63,sec=rng()%63;
            if(i%2)std::snprintf(value,sizeof(value),"%04u-%02u-%02uT%02u:%02u:%02uZ",y,m,d,h,mi,sec);
            else std::snprintf(value,sizeof(value),"%04u-%02u-%02uT%02u:%02u:%02u%c%02u:%02u",y,m,d,h,mi,sec,rng()%2?'+':'-',unsigned(rng()%26),unsigned(rng()%63));
            std::string input=value;
            if(i%4==0)input[rng()%input.size()]=char(rng()%256);
            require(parser.parse(input)==timestamp(input),"strict cached ISO parser equals general parser");
            require(parser.parse(input,true)==timestamp(input),"packed UTC parser equals general parser");
            std::string epoch=std::to_string(rng()&INT64_MAX);
            if(i%3==0)epoch.insert(0,rng()%25,'0');
            if(i%5==0)epoch[rng()%epoch.size()]=char(rng()%256);
            require(parser.parse(epoch)==timestamp(epoch),"SWAR epoch parser equals general parser");
        }
        const std::string leap_day="2028-02-29T23:59:59Z";
        const auto empty_date=std::string(10,'\0')+"T12:34:56Z";
        require(parser.parse(empty_date,true)==-1,"unused UTC cache slots reject an invalid date");
        for(size_t position=0;position<leap_day.size();++position) {
            for(unsigned byte=0;byte<256;++byte) {
                auto input=leap_day;
                input[position]=char(byte);
                require(parser.parse(input,true)==timestamp(input),"all bytes of cached UTC leap day");
            }
        }
        for(size_t i=0;i<100000;++i) {
            char eight[9];unsigned n=rng()%100000000;
            std::snprintf(eight,sizeof(eight),"%08u",n);
            require(StrictTimeParser::eight_digits(eight)&&StrictTimeParser::decimal8(eight)==n,"eight-digit conversion");
        }
    }
    {
        struct Event {uint32_t ts;};
        GapSessions<> bins;
        GapSessions<true> sparse_bins;
        const uint32_t gap=uint32_t(GAP),limit=100*gap;
        auto compare_sessions=[&](std::vector<Event> events) {
            auto summarized=bins.summarize(std::span(events),limit);
            auto sparse=sparse_bins.summarize(std::span(events),limit);
            require(sparse.size()==summarized.size(),"occupied bins produce the same session count");
            std::sort(events.begin(),events.end(),[](const auto& a,const auto& b){return a.ts<b.ts;});
            size_t session=0;
            for(size_t first=0;first<events.size();) {
                size_t last=first+1;
                while(last<events.size()&&events[last].ts-events[last-1].ts<=gap)++last;
                require(session<summarized.size(),"every sorted session has a gap summary");
                const auto& actual=summarized[session];
                const auto& occupied=sparse[session++];
                require(actual.ts==events[first].ts&&actual.last==events[last-1].ts&&actual.count==last-first,
                        "gap summaries preserve exact boundaries and event counts");
                require(occupied.ts==actual.ts&&occupied.last==actual.last&&occupied.count==actual.count,
                        "occupied bins preserve boundaries and reset between users");
                first=last;
            }
            require(session==summarized.size(),"gap bins produce no extra sessions");
        };
        compare_sessions({{3*gap+1},{gap},{limit},{2*gap},{0},{2*gap}});
        compare_sessions({});
        for(size_t repeat=0;repeat<100;++repeat) {
            std::vector<Event> events;
            for(size_t i=0,n=rng()%500;i<n;++i)events.push_back({uint32_t(rng()%limit)});
            compare_sessions(std::move(events));
        }
    }
    {
        CampaignWindow window(257);std::vector<size_t> counts(257);
        for(size_t i=0;i<100000;++i) {
            uint32_t c=rng()%257;
            if(counts[c]&&rng()%2){window.remove(c);--counts[c];}else{window.add(c);++counts[c];}
            require(window.count==counts,"campaign window exact multiplicity");
            auto active=window.present;std::sort(active.begin(),active.end());
            std::vector<uint32_t> expected;for(uint32_t j=0;j<counts.size();++j)if(counts[j])expected.push_back(j);
            require(active==expected,"campaign window unique active membership");
            if(i%1000==999){window.clear();std::fill(counts.begin(),counts.end(),0);}
        }
        CategoryRevenue revenue(3);std::vector<SV> names={"unknown","z","a"};
        revenue.add(0,5);revenue.add(1,10);revenue.add(2,10);
        require(revenue.top(names)==2,"array category revenue byte tie");
        revenue.add(0,100);require(revenue.top(names)==0,"array unknown category can win");
        revenue.clear();require(revenue.present.empty()&&revenue.sum==std::vector<int64_t>(3),"category reset");
    }
    {
        auto format=[](int64_t epoch) {
            int64_t day=dayof(epoch),second=(epoch-day*DAY)/1000;
            std::chrono::year_month_day date{std::chrono::sys_days{std::chrono::days{day}}};
            char out[32];std::snprintf(out,sizeof(out),"%04d-%02u-%02uT%02d:%02d:%02dZ",int(date.year()),unsigned(date.month()),unsigned(date.day()),int(second/3600),int(second/60%60),int(second%60));
            return std::string(out);
        };
        for(SV start_text:{"2026-12-25T00:00:00Z","2028-02-20T00:00:00Z"}) {
            auto start=timestamp(start_text),end=start+14*DAY;IngestOrder order;order.init(start,end);
            for(size_t i=0;i<100000;++i) {
                int64_t event_ts=start+int64_t(rng()%(end-start));
                int64_t ingested=start-7*DAY+int64_t(rng()%(28*DAY));
                ingested=ingested/1000*1000;
                auto f1=format(ingested);
                require(order.on_time(IngestOrder::pack(f1),event_ts)==(ingested-event_ts<=DAY),"ingest-order exact late cutoff");
                auto other=format(ingested+int64_t(rng()%1000000)*1000);
                require(IngestOrder::pack(f1)<IngestOrder::pack(other)||f1==other,"ingest-order monotonicity");
            }
            for(int64_t offset:{-1000LL,0LL,1LL,500LL,999LL,1000LL}) {
                int64_t event_ts=start+500,ingested=(event_ts+DAY+offset)/1000*1000;
                require(order.on_time(IngestOrder::pack(format(ingested)),event_ts)==(ingested-event_ts<=DAY),"ingest-order fractional boundary");
            }
        }
        require(IngestOrder::pack("0000-01-01T00:00:00Z")<IngestOrder::pack("9999-12-31T23:59:59Z"),"ingest-order year range");
    }
    for(size_t round=0;round<3000;++round) {
        std::string buffer;
        for(size_t row=0;row<20;++row) {
            std::string record;
            for(size_t field=0;field<7;++field) {
                if(field)record+=',';
                bool quoted=(row%2==0)&&rng()%2;
                if(quoted)record+='"';
                for(size_t j=0,n=rng()%30;j<n;++j)record+="abXYZ012 \t\r"[rng()%11];
                if(quoted)record+='"';
            }
            if(rng()%4==0)record.insert(rng()%(record.size()+1),1,',');
            if(rng()%4==0)record.insert(rng()%(record.size()+1),1,'"');
            buffer+=record;if(row!=19||rng()%2)buffer+='\n';
        }
        std::vector<std::array<SV,7>> expected,actual;
        Chunk chunk{buffer.data(),buffer.data()+buffer.size()};
        Inputs::lines(chunk,[&](SV line){std::array<SV,7> f;if(csv_reference(line,f))expected.push_back(f);});
        scan_csv<7>(chunk,[&](const std::array<SV,7>& f){actual.push_back(f);});
        require(expected==actual,"fused CSV equals line/scalar parsing");
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
        if(avx2_csv_available()) {
            actual.clear();
            scan_csv_avx2<7>(chunk,[&](const std::array<SV,7>& f){actual.push_back(f);});
            require(expected==actual,"AVX2 CSV equals line/scalar parsing");
            if(row_csv_available()) {
                actual.clear();
                scan_csv_rows<7>(chunk,[&](const std::array<SV,7>& f){actual.push_back(f);});
                require(expected==actual,"AVX2 row CSV equals line/scalar parsing");
            }
        }
#endif
    }
    for(size_t round=0;round<200000;++round) {
        std::string a,b;
        for(size_t i=0,n=rng()%40;i<n;++i)a+=char(rng()%256);
        if(round%2)b=a;else for(size_t i=0,n=rng()%40;i<n;++i)b+=char(rng()%256);
        Id x(a),y(b),copy=x;
        require(x.view()==a&&copy.view()==a&&y.view()==b,"packed ID exact storage and copy");
        require((x==y)==(a==b)&&(x<y)==(SV(a)<SV(b)),"packed ID byte order and equality");
        if(x==y)require(IdHash{}(x)==IdHash{}(y),"equal packed IDs hash equally");
    }
    PackedPeople packed_people;
    packed_people.join(Id("long-id-with-more-than-15-bytes"),Id("u1"));
    packed_people.join(Id("u1"),Id("A"));packed_people.join(Id("A"),Id("é"));packed_people.finish();
    require(packed_people.person(Id("long-id-with-more-than-15-bytes")).view()=="A","packed union-find minimum");
    for(size_t round=0;round<200000;++round) {
        const char alphabet[]="ab\"\",,, 0123\t\rXYZ";
        std::string line;size_t length=rng()%150;
        for(size_t i=0;i<length;++i)line+=alphabet[rng()%(sizeof(alphabet)-1)];
        std::array<SV,7> scalar,simd;
        bool a=csv_reference(line,scalar),b=csv(line,simd);
        require(a==b&&(!a||scalar==simd),"SIMD CSV equals scalar parser");
    }
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
