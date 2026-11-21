#include "core.hpp"
#include "parquet.hpp"
#include "flat_map.hpp"
#include "sort.hpp"
#include "id.hpp"
#include "csv_scan.hpp"
#include "ingest_order.hpp"
#include "sparse_state.hpp"
#include "fast_time.hpp"
#include "../third_party/unordered_dense/ankerl/unordered_dense.h"
#ifdef SHOP_IPS_SORTS
#include "ips4o.hpp"
#include "ips2ra.hpp"
#endif
#include <cstdlib>
#ifndef SHOP_VIEW_IDS
#define SHOP_PACKED_IDS
#endif

using namespace shop;
#ifdef SHOP_PACKED_IDS
using Key=Id;using KeyHash=IdHash;using PeopleType=PackedPeople;
SV text(const Id& s){return s.view();}
#else
using Key=SV;using KeyHash=Hash;using PeopleType=People;
SV text(SV s){return s;}
#endif
template<class V>using KeyDict=std::unordered_map<Key,V,KeyHash>;
template<class V>using KeyFlatMap=FlatMap<V,Key,KeyHash>;
struct DenseKeyHash {
    using is_avalanching=void;
    size_t operator()(const Key& key)const noexcept {auto h=KeyHash{}(key);return h^(h>>32);}
};
struct Raw {Key id,person;int64_t ts,ingest;uint32_t price,category;};
struct CompactRaw {
    Key id,person;
    int64_t ingest;
    uint32_t offset,purchase;
    static constexpr uint32_t invalid=UINT32_MAX;
    static constexpr uint32_t price_mask=(1u<<24)-1;
};
static_assert(sizeof(CompactRaw)==48);
struct Event {
    Key person;int64_t ts;uint32_t price,category;
    bool operator<(const Event& b)const{return person!=b.person?person<b.person:ts<b.ts;}
};
struct Click {Key person,id;int64_t ts;uint32_t campaign;};
struct RelativeEvent {
    uint32_t ts,price,category,person;
    bool operator<(const RelativeEvent& other)const{return ts<other.ts;}
};
static_assert(sizeof(RelativeEvent)==16);
#ifdef SHOP_PACKED_IDS
struct PackedEventSortKey {
    bool requires_next_sort=true;
    static constexpr size_t ska_sort_width=8;
    bool Interrupted()const{return false;}
    bool ByteIsSkippable(size_t)const{return false;}
    uint64_t operator()(const Event& e)const{return e.person.prefix();}
};
#endif
struct CompactClick {
    Key id;int64_t ts;uint32_t campaign,person;
    bool operator<(const CompactClick& b)const{return ts!=b.ts?ts<b.ts:id>b.id;}
};
struct TimeSortKey {
    bool requires_next_sort=false;
    static constexpr size_t ska_sort_width=8;
    bool Interrupted()const{return false;}
    bool ByteIsSkippable(size_t)const{return false;}
    template<class E>uint64_t operator()(const E& e)const{return uint64_t(e.ts)^0x8000000000000000ULL;}
};
struct Aggregate {int64_t sessions=0,purchasing=0,revenue=0,assisted=0;};
struct WorkProfile {double gather=0,intern=0,sort=0,aggregate=0,parquet=0;};
using CampaignTotals=std::map<std::pair<int64_t,uint32_t>,Aggregate>;
template<class T> using Buckets=std::vector<std::vector<T>>;
template<class T> using Workers=std::vector<Buckets<T>>;

struct Options {
    fs::path data="/data",out="/output";
    int threads=8,shards=256,dedup_shards=4096,ingest_shards=-1;
    bool baseline=false;
#ifdef SHOP_PACKED_IDS
    bool compact=false;
#else
    bool compact=true;
#endif
    bool flat_links=false;
    bool dense_totals=true;
    bool group_person=true;
    bool fused_csv=true;
    bool ordered_ingest=false;
    bool array_active=true,array_categories=true;
    bool fast_time=true;
    bool narrow_events=true;
    bool direct_dedup=false,direct_encoding=true;
    bool overlap_links=true;
    bool avx2_csv=true;
    bool tag_links=true,heavy_shards_first=false;
    bool sparse_duplicates=true;
    bool compact_raw=true;
    std::string dedup="flat";
    std::string sorter="duckdb";
    std::string group_sort="duckdb";
};
Options options(int argc,char** argv) {
    Options o;
    if(auto s=getenv("DATA_DIR"))o.data=s;
    if(auto s=getenv("OUTPUT_DIR"))o.out=s;
    if(auto s=getenv("NCPU"))o.threads=std::stoi(s);
    int positional=0;
    for(int i=1;i<argc;++i) {
        std::string arg=argv[i];
        if(arg=="--baseline")o.baseline=true;
        else if(arg=="--compact")o.compact=true;
        else if(arg=="--no-compact")o.compact=false;
        else if(arg=="--flat-links")o.flat_links=true;
        else if(arg=="--dense-totals")o.dense_totals=true;
        else if(arg=="--no-dense-totals")o.dense_totals=false;
        else if(arg=="--group-person")o.group_person=true;
        else if(arg=="--no-group-person")o.group_person=false;
        else if(arg=="--fused-csv")o.fused_csv=true;
        else if(arg=="--no-fused-csv")o.fused_csv=false;
        else if(arg=="--ordered-ingest")o.ordered_ingest=true;
        else if(arg=="--array-active")o.array_active=true;
        else if(arg=="--array-categories")o.array_categories=true;
        else if(arg=="--no-array-active")o.array_active=false;
        else if(arg=="--no-array-categories")o.array_categories=false;
        else if(arg=="--fast-time")o.fast_time=true;
        else if(arg=="--no-fast-time")o.fast_time=false;
        else if(arg=="--narrow-events")o.narrow_events=true;
        else if(arg=="--wide-events")o.narrow_events=false;
        else if(arg=="--direct-dedup")o.direct_dedup=true;
        else if(arg=="--direct-encoding")o.direct_encoding=true;
        else if(arg=="--no-direct-encoding")o.direct_encoding=false;
        else if(arg=="--overlap-links")o.overlap_links=true;
        else if(arg=="--no-overlap-links")o.overlap_links=false;
        else if(arg=="--avx2-csv")o.avx2_csv=true;
        else if(arg=="--no-avx2-csv")o.avx2_csv=false;
        else if(arg=="--tag-links")o.tag_links=true;
        else if(arg=="--no-tag-links")o.tag_links=false;
        else if(arg=="--heavy-shards-first")o.heavy_shards_first=true;
        else if(arg=="--sparse-duplicates")o.sparse_duplicates=true;
        else if(arg=="--no-sparse-duplicates")o.sparse_duplicates=false;
        else if(arg=="--compact-raw")o.compact_raw=true;
        else if(arg=="--wide-raw")o.compact_raw=false;
        else if(arg=="--threads"&&i+1<argc)o.threads=std::stoi(argv[++i]);
        else if(arg=="--shards"&&i+1<argc)o.shards=std::stoi(argv[++i]);
        else if(arg=="--dedup-shards"&&i+1<argc)o.dedup_shards=std::stoi(argv[++i]);
        else if(arg=="--ingest-shards"&&i+1<argc)o.ingest_shards=std::stoi(argv[++i]);
        else if(arg=="--dedup"&&i+1<argc)o.dedup=argv[++i];
        else if(arg=="--sorter"&&i+1<argc)o.sorter=argv[++i];
        else if(arg=="--group-sort"&&i+1<argc)o.group_sort=argv[++i];
        else if(arg=="--help") {std::cout<<"shopping [DATA OUTPUT] [--baseline] [--threads N] [--shards N] [--dedup sort|hash|flat] [--compact|--no-compact] [--sorter std|duckdb]\n";std::exit(0);}
        else if(arg.starts_with("--"))throw std::runtime_error("unknown option "+arg);
        else if(positional++==0)o.data=arg;
        else if(positional==2)o.out=arg;
        else throw std::runtime_error("too many paths");
    }
    if(o.baseline){o.threads=1;o.shards=1;o.dedup="sort";o.compact=false;o.flat_links=false;o.dense_totals=false;o.group_person=false;o.fused_csv=false;o.ordered_ingest=false;o.array_active=false;o.array_categories=false;o.sorter="std";}
    if(!o.dedup_shards||o.baseline)o.dedup_shards=o.shards;
    if(o.ingest_shards==-1)o.ingest_shards=std::min(256,o.dedup_shards);
    if(!o.ingest_shards||o.baseline)o.ingest_shards=o.dedup_shards;
    if(o.baseline)o.fast_time=false;
    if(o.baseline)o.overlap_links=false;
    if(o.baseline)o.tag_links=false;
    if(o.baseline)o.compact_raw=false;
    if(o.threads<1||o.threads>256||o.shards<1||o.shards>4096||(o.shards&(o.shards-1)))throw std::runtime_error("threads must be 1..256; shards a power of two in 1..4096");
    if(o.dedup_shards<1||o.dedup_shards>16384||(o.dedup_shards&(o.dedup_shards-1)))throw std::runtime_error("dedup-shards must be a power of two in 1..16384");
    if(o.ingest_shards<1||o.ingest_shards>o.dedup_shards||(o.ingest_shards&(o.ingest_shards-1)))
        throw std::runtime_error("ingest-shards must be a power of two no larger than dedup-shards");
    if(o.dedup!="sort"&&o.dedup!="hash"&&o.dedup!="flat"&&o.dedup!="dense")throw std::runtime_error("dedup must be sort, hash, flat, or dense");
    if(o.sorter!="std"&&o.sorter!="duckdb")throw std::runtime_error("sorter must be std or duckdb");
    if(o.group_sort!="duckdb"&&o.group_sort!="ips4o"&&o.group_sort!="ips2ra")throw std::runtime_error("group-sort must be duckdb, ips4o, or ips2ra");
#ifndef SHOP_IPS_SORTS
    if(o.group_sort!="duckdb")throw std::runtime_error("IPS experiments require make build/shopping-ips");
#endif
    return o;
}
struct Pipeline {
    Options o;
    Inputs in;
    PeopleType people;
    KeyFlatMap<uint32_t> person_lookup;
    Dict<uint32_t> products,campaigns,category_index;
    std::vector<SV> categories={"unknown"},campaign_names={"organic"},channels={"organic"};
    int64_t start,end;
    IngestOrder ingest_order;
    StrictTimeParser strict_time;
    Workers<Event> events;
    Workers<Click> clicks;
    std::vector<CampaignTotals> totals;
    std::vector<std::vector<Aggregate>> dense_totals;
    std::vector<WorkProfile> profiles;
    explicit Pipeline(Options options):o(std::move(options)),
        events(o.threads,Buckets<Event>(o.shards)),clicks(o.threads,Buckets<Click>(o.shards)),totals(o.threads),profiles(o.threads){}
    using Clock=std::chrono::steady_clock;
    static double elapsed(Clock::time_point t){return std::chrono::duration<double>(Clock::now()-t).count();}
    size_t shard(Key s)const{return KeyHash{}(s)&(o.shards-1);}
    size_t event_shard(Key s)const{return KeyHash{}(s)&(o.dedup_shards-1);}
    void load() {
        std::ifstream mf(o.data/"manifest.json");
        if(!mf)throw std::runtime_error("cannot open manifest.json");
        std::string manifest((std::istreambuf_iterator<char>(mf)),{});
        auto window=[&](const char* key){
            std::smatch match;std::regex pattern(std::string("\"")+key+"\"\\s*:\\s*\"([^\"]+)\"");
            if(!std::regex_search(manifest,match,pattern))throw std::runtime_error(std::string("missing ")+key);
            return timestamp(match[1].str());
        };
        start=window("window_start");end=window("window_end");
        if(start<0||end<=start)throw std::runtime_error("invalid manifest window");
        if(o.ordered_ingest)ingest_order.init(start,end);
        if(o.fast_time)strict_time.init(start,end);
        std::vector<Chunk> lookup;
        in.add(o.data/"products.csv",lookup);
        category_index.emplace("unknown",0);
        for(auto chunk:lookup)Inputs::lines(chunk,[&](SV line){
            std::array<SV,2> f;if(!csv(line,f))throw std::runtime_error("malformed products.csv");
            auto [it,added]=category_index.try_emplace(f[1],categories.size());
            if(added)categories.push_back(f[1]);
            products.emplace(f[0],it->second);
        });
        lookup.clear();in.add(o.data/"campaigns.csv",lookup);
        for(auto chunk:lookup)Inputs::lines(chunk,[&](SV line){
            std::array<SV,2> f;if(!csv(line,f))throw std::runtime_error("malformed campaigns.csv");
            campaigns.emplace(f[0],campaign_names.size());campaign_names.push_back(f[0]);channels.push_back(f[1]);
        });
        if(o.dense_totals) {
            auto days=dayof(end-1)-dayof(start)+1;
            dense_totals.resize(o.threads);
            for(auto& t:dense_totals)t.resize(size_t(days)*campaign_names.size());
        }
        for(auto p:files(o.data/"links"))in.add(p,in.links);
        if(!o.overlap_links)build_links();
        for(auto p:files(o.data/"events"))in.add(p,in.events);
        for(auto p:files(o.data/"clicks"))in.add(p,in.clicks);
        fs::create_directories(o.out/"user_daily");fs::create_directories(o.out/"campaign_daily");
        // Replace only our table outputs, so rerunning never leaves extra rows.
        for(auto table:{"user_daily","campaign_daily"})for(auto p:files_parquet(o.out/table))fs::remove(p);
    }
    void build_links() {
        for(auto chunk:in.links)Inputs::lines(chunk,[&](SV line){
            std::array<SV,2> f;if(csv(line,f)&&!f[0].empty()&&!f[1].empty())people.join(f[0],f[1]);
        });
        people.finish();
#ifdef SHOP_PACKED_IDS
        if(o.tag_links)people.index.freeze();
#endif
#ifndef SHOP_PACKED_IDS
        if(o.flat_links) {
            person_lookup.reserve(people.index.size());
            for(auto [name,root]:people.index)person_lookup.try_emplace(name,root);
            Dict<uint32_t>().swap(people.index);
            std::vector<uint32_t>().swap(people.parents);std::vector<uint32_t>().swap(people.sizes);
        }
#endif
    }
    static std::vector<fs::path> files_parquet(const fs::path& dir) {
        std::vector<fs::path> r;for(auto& p:fs::directory_iterator(dir))if(p.is_regular_file()&&p.path().extension()==".parquet")r.push_back(p.path());return r;
    }
    Key canonical(Key s)const {
#ifdef SHOP_PACKED_IDS
        return people.person(s);
#else
        if(!o.flat_links)return people.person(s);
        auto it=person_lookup.find(s);return it==person_lookup.end()?s:people.names[it->second];
#endif
    }
    Aggregate& total(int worker,int64_t day,uint32_t campaign) {
        if(o.dense_totals)return dense_totals[worker][size_t(day-dayof(start))*campaign_names.size()+campaign];
        return totals[worker][{day,campaign}];
    }
    template<size_t N,class F>void records(Chunk chunk,F consume)const {
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
        if(o.fused_csv&&o.avx2_csv&&avx2_csv_available()) {
            scan_csv_avx2<N>(chunk,consume);
            return;
        }
#endif
        if(o.fused_csv)scan_csv<N>(chunk,consume);
        else Inputs::lines(chunk,[&](SV line){std::array<SV,N> fields;if(csv(line,fields))consume(fields);});
    }
    int64_t parse_time(SV s)const{return o.fast_time?strict_time.parse(s):timestamp(s);}
    template<class R> void parse_events(Workers<R>& raw) {
        parallel(in.events.size()+size_t(o.overlap_links),o.threads,[&](size_t i,int worker){
            if(o.overlap_links) {
                // This job owns the link graph. The other jobs carry raw user
                // IDs until the barrier, and therefore never read it early.
                if(!i){build_links();return;}
                --i;
            }
            records<7>(in.events[i],[&](const std::array<SV,7>& f){
                if(f[0].empty())return;
                // Invalid winners remain in the dedup input. A later valid copy
                // must never rescue an invalid earliest copy.
                int64_t ts=parse_time(f[2]),ingest=o.ordered_ingest?IngestOrder::pack(f[3]):parse_time(f[3]);
                uint64_t price=0;uint32_t category=0;
                bool purchase=f[4]=="purchase";
                bool valid=!f[1].empty()&&!f[5].empty()&&(purchase||f[4]=="view"||f[4]=="cart")&&
                    ts>=start&&ts<end&&(o.ordered_ingest?ingest_order.on_time(ingest,ts):ingest-ts<=DAY);
                if(purchase) {
                    valid=valid&&digits(f[6],price,10000000)&&price>0;
                    if(valid){auto it=products.find(f[5]);if(it!=products.end())category=it->second;}
                }
                if(!valid)ts=-1;
                Key event_id(f[0]);
                Key user_id(f[1]);
                Key person=o.overlap_links?user_id:canonical(user_id);
                size_t bucket=KeyHash{}(event_id)&(o.ingest_shards-1);
                if constexpr(std::is_same_v<R,CompactRaw>) {
                    uint32_t offset=valid?uint32_t(ts-start):CompactRaw::invalid;
                    uint32_t purchase_info=(uint32_t(price)&CompactRaw::price_mask)|(category<<24);
                    raw[worker][bucket].push_back({event_id,person,ingest,offset,purchase_info});
                } else {
                    raw[worker][bucket].push_back({event_id,person,ts,ingest,uint32_t(price),category});
                }
            });
        });
    }
    template<class R> void dedup(Workers<R>& raw) {
        parallel(o.ingest_shards,o.threads,[&](size_t s,int worker){
            size_t count=0;for(auto& w:raw)count+=w[s].size();
            if(!count)return;
            auto emit=[&](const R& r) {
                int64_t ts;
                uint32_t price,category;
                if constexpr(std::is_same_v<R,CompactRaw>) {
                    if(r.offset==CompactRaw::invalid)return;
                    ts=start+r.offset;
                    price=r.purchase&CompactRaw::price_mask;
                    category=r.purchase>>24;
                } else {
                    if(r.ts<start)return;
                    ts=r.ts;
                    price=r.price;
                    category=r.category;
                }
                Key person=o.overlap_links?canonical(r.person):r.person;
                events[worker][shard(person)].push_back({person,ts,price,category});
            };
            if(o.ingest_shards!=o.dedup_shards) {
                size_t partitions=o.dedup_shards/o.ingest_shards;
                size_t bits=std::countr_zero(unsigned(o.ingest_shards));
                std::vector<std::vector<R>> subpartitions(partitions);
                size_t average=count/partitions;
                for(auto& rows:subpartitions)rows.reserve(average+average/8+16);
                for(auto& buckets:raw) {
                    for(const R& row:buckets[s]) {
                        size_t subpartition=event_shard(row.id)>>bits;
                        subpartitions[subpartition].push_back(row);
                    }
                    std::vector<R>().swap(buckets[s]);
                }
                for(auto& rows:subpartitions)if(!rows.empty())dedup_rows(rows,emit);
                return;
            }
            if(o.direct_dedup&&o.dedup=="flat") {
                KeyFlatMap<const R*> winners;
                winners.reserve(count);
                for(const auto& buckets:raw) {
                    for(const auto& event:buckets[s]) {
                        auto [entry,inserted]=winners.try_emplace(event.id,&event);
                        if(!inserted&&event.ingest<entry->second->ingest)entry->second=&event;
                    }
                }
                winners.each([&](const Key&,const R* event){emit(*event);});
                for(auto& buckets:raw)std::vector<R>().swap(buckets[s]);
                return;
            }
            std::vector<R> rows;rows.reserve(count);
            for(auto& w:raw){rows.insert(rows.end(),w[s].begin(),w[s].end());std::vector<R>().swap(w[s]);}
            dedup_rows(rows,emit);
        });
    }
    template<class R,class Emit> void dedup_rows(std::vector<R>& rows,Emit emit) {
        if(o.sparse_duplicates&&o.dedup=="flat"&&rows.size()>=1024) {
            dedup_sparse(rows,emit);
            return;
        }
        if(o.dedup=="dense") {
            ankerl::unordered_dense::map<Key,size_t,DenseKeyHash> winners;winners.reserve(rows.size());
            for(size_t i=0;i<rows.size();++i){auto [it,added]=winners.try_emplace(rows[i].id,i);if(!added&&rows[i].ingest<rows[it->second].ingest)it->second=i;}
            for(auto [id,i]:winners)emit(rows[i]);
        } else if(o.dedup=="flat") {
            KeyFlatMap<size_t> winners;winners.reserve(rows.size());
            for(size_t i=0;i<rows.size();++i){auto [it,added]=winners.try_emplace(rows[i].id,i);if(!added&&rows[i].ingest<rows[it->second].ingest)it->second=i;}
            winners.each([&](Key,size_t i){emit(rows[i]);});
        } else if(o.dedup=="hash") {
            KeyDict<size_t> winners;winners.reserve(rows.size());
            for(size_t i=0;i<rows.size();++i){auto [it,added]=winners.try_emplace(rows[i].id,i);if(!added&&rows[i].ingest<rows[it->second].ingest)it->second=i;}
            for(auto [id,i]:winners)emit(rows[i]);
        } else {
            std::sort(rows.begin(),rows.end(),[](const R& a,const R& b){return a.id!=b.id?a.id<b.id:a.ingest<b.ingest;});
            for(size_t i=0;i<rows.size();){emit(rows[i]);size_t j=i+1;while(j<rows.size()&&rows[j].id==rows[i].id)++j;i=j;}
        }
    }
    template<class R,class Emit> static void dedup_sparse(const std::vector<R>& rows,Emit emit) {
        // The bitmap can prove a hash tag occurs once. Shared tags always go
        // through exact key comparison, including collisions of distinct IDs.
        std::array<uint64_t,1024> seen{}, shared{};
        std::vector<uint16_t> tags;
        tags.reserve(rows.size());
        for(const auto& event:rows) {
            uint16_t tag=uint64_t(KeyHash{}(event.id))>>32;
            tags.push_back(tag);
            const uint64_t bit=uint64_t{1}<<(tag&63);
            shared[tag>>6]|=seen[tag>>6]&bit;
            seen[tag>>6]|=bit;
        }
        KeyFlatMap<size_t> winners;
        winners.reserve(rows.size()/8);
        for(size_t i=0;i<rows.size();++i) {
            const auto tag=tags[i];
            const auto& event=rows[i];
            if(!(shared[tag>>6]&(uint64_t{1}<<(tag&63)))) {
                emit(event);
                continue;
            }
            auto [entry,inserted]=winners.try_emplace(event.id,i);
            if(!inserted&&event.ingest<rows[entry->second].ingest)entry->second=i;
        }
        winners.each([&](const Key&,size_t row){emit(rows[row]);});
    }
    void parse_clicks() {
        parallel(in.clicks.size(),o.threads,[&](size_t i,int worker){
            records<4>(in.clicks[i],[&](const std::array<SV,4>& f){
                if(f[0].empty()||f[1].empty())return;
                auto it=campaigns.find(f[2]);if(it==campaigns.end())return;
                auto ts=parse_time(f[3]);if(ts<start-WEEK||ts>=end)return;
                auto p=canonical(Key(f[1]));clicks[worker][shard(p)].push_back({p,Key(f[0]),ts,it->second});
            });
        });
        // Identical duplicate clicks need no materialized dedup: they cannot
        // change the winning (timestamp,id), nor campaign membership in a window.
    }
    template<class T> static std::vector<T> gather(Workers<T>& workers,size_t s) {
        size_t n=0;for(auto& w:workers)n+=w[s].size();std::vector<T> r;r.reserve(n);
        for(auto& w:workers){r.insert(r.end(),w[s].begin(),w[s].end());std::vector<T>().swap(w[s]);}return r;
    }
    template<class T> static size_t bucket_size(const Workers<T>& workers,size_t shard) {
        size_t count=0;
        for(const auto& buckets:workers)count+=buckets[shard].size();
        return count;
    }
    template<class T,class F> static void consume_bucket(Workers<T>& workers,size_t shard,F consume) {
        for(auto& buckets:workers) {
            auto& records=buckets[shard];
            for(const auto& record:records)consume(record);
            std::vector<T>().swap(records);
        }
    }
    template<class E> void encode_shard(size_t shard,int worker,int64_t time_base) {
        auto before=Clock::now();
        if(o.direct_encoding) {
            auto consume_events=[&](auto f){consume_bucket(events,shard,f);};
            auto consume_clicks=[&](auto f){consume_bucket(clicks,shard,f);};
            encode_partition<E>(shard,worker,bucket_size(events,shard),bucket_size(clicks,shard),
                                consume_events,consume_clicks,time_base,before);
        } else {
            auto event_rows=gather(events,shard);
            auto click_rows=gather(clicks,shard);
            profiles[worker].gather+=elapsed(before);
            auto consume_events=[&](auto f){for(const auto& event:event_rows)f(event);std::vector<Event>().swap(event_rows);};
            auto consume_clicks=[&](auto f){for(const auto& click:click_rows)f(click);std::vector<Click>().swap(click_rows);};
            encode_partition<E>(shard,worker,event_rows.size(),click_rows.size(),
                                consume_events,consume_clicks,time_base,Clock::now());
        }
    }
    void sessionize() {
        std::vector<size_t> order;
        if(o.heavy_shards_first) {
            std::vector<size_t> weights(o.shards);
            order.reserve(o.shards);
            for(int shard=0;shard<o.shards;++shard) {
                order.push_back(shard);
                weights[shard]=bucket_size(events,shard)+bucket_size(clicks,shard);
            }
            std::sort(order.begin(),order.end(),[&](size_t a,size_t b){return weights[a]>weights[b];});
        }
        parallel(o.shards,o.threads,[&](size_t job,int worker){
            size_t s=order.empty()?job:order[job];
            if(o.group_person&&o.narrow_events&&uint64_t(end-start-1)<=UINT32_MAX) {
                encode_shard<RelativeEvent>(s,worker,start);
            } else if(o.compact||o.group_person) {
                encode_shard<CompactEvent>(s,worker,0);
            } else {
                auto before=Clock::now();
                auto ev=gather(events,s);
                auto cl=gather(clicks,s);
                profiles[worker].gather+=elapsed(before);
                process(s,worker,std::span(ev),std::span(cl),[](Key person){return person;});
            }
        });
        WorkProfile sum;
        for(auto p:profiles){sum.gather+=p.gather;sum.intern+=p.intern;sum.sort+=p.sort;sum.aggregate+=p.aggregate;sum.parquet+=p.parquet;}
        std::cerr<<"session_work_seconds gather="<<sum.gather<<" intern="<<sum.intern<<" sort="<<sum.sort<<" aggregate="<<sum.aggregate<<" parquet="<<sum.parquet<<'\n';
    }
    template<class E,class ConsumeEvents,class ConsumeClicks>
    void encode_partition(size_t s,int worker,size_t event_count,size_t click_count,
                          ConsumeEvents consume_events,ConsumeClicks consume_clicks,
                          int64_t time_base,Clock::time_point before) {
        KeyFlatMap<uint32_t> codes;
        std::vector<Key> names;
        std::vector<size_t> event_counts;
        std::vector<E> compact_events;compact_events.reserve(event_count);
        consume_events([&](const Event& e) {
            auto [it,added]=codes.try_emplace(e.person,names.size());
            if(added){names.push_back(e.person);if(o.group_person)event_counts.push_back(0);}
            if(o.group_person)++event_counts[it->second];
            compact_events.push_back({static_cast<decltype(E::ts)>(e.ts-time_base),e.price,e.category,it->second});
        });
        std::vector<CompactClick> compact_clicks;compact_clicks.reserve(click_count);
        std::vector<size_t> click_counts(o.group_person?names.size():0);
        consume_clicks([&](const Click& c) {
            auto it=codes.find(c.person);
            if(it!=codes.end()){
                compact_clicks.push_back({c.id,c.ts,c.campaign,it->second});
                if(o.group_person)++click_counts[it->second];
            }
        });
        profiles[worker].intern+=elapsed(before);
        if(o.group_person) {
            auto group_begin=Clock::now();
            auto grouped_events=group_records(compact_events,event_counts);
            auto grouped_clicks=group_records(compact_clicks,click_counts);
            profiles[worker].gather+=elapsed(group_begin);
            auto sort_begin=Clock::now();
            for(size_t person=0;person<names.size();++person) {
                auto eb=grouped_events.data.get()+event_counts[person],ee=grouped_events.data.get()+event_counts[person+1];
                sort_group(eb,ee,[](auto& a,auto& b){return a.ts<b.ts;});
                auto cb=grouped_clicks.data.get()+click_counts[person],ce=grouped_clicks.data.get()+click_counts[person+1];
                sort_group(cb,ce,[](auto& a,auto& b){return a.ts!=b.ts?a.ts<b.ts:a.id>b.id;});
            }
            profiles[worker].sort+=elapsed(sort_begin);
            process<true>(s,worker,std::span(grouped_events.data.get(),grouped_events.size),std::span(grouped_clicks.data.get(),grouped_clicks.size),[&](uint32_t code){return names[code];},time_base);
        } else if constexpr(std::is_same_v<E,CompactEvent>) {
            process(s,worker,std::span(compact_events),std::span(compact_clicks),[&](uint32_t code){return names[code];});
        }
    }
    template<class T>struct Grouped {std::unique_ptr<T[]> data;size_t size;};
    template<class T>static Grouped<T> group_records(std::vector<T>& input,std::vector<size_t>& counts) {
        size_t sum=0;
        for(auto& count:counts){auto n=count;count=sum;sum+=n;}
        counts.push_back(sum);
        auto positions=counts;
        // Default-initialize trivial records: avoid an extra zero-fill pass.
        std::unique_ptr<T[]> output(new T[sum]);
        for(auto& row:input)output[positions[row.person]++]=row;
        std::vector<T>().swap(input);
        return {std::move(output),sum};
    }
    template<class T,class Less>void sort_group(T* begin,T* end,Less less) {
        size_t n=end-begin;
        if(n<2)return;
        if(n<=24) {
            for(T* i=begin+1;i<end;++i){T value=*i;T* j=i;while(j>begin&&less(value,*(j-1))){*j=*(j-1);--j;}*j=value;}
        } else if(n<1024)duckdb_pdqsort::pdqsort_branchless(begin,end,less);
        else {
#ifdef SHOP_IPS_SORTS
            if(o.group_sort=="ips4o")ips4o::sort(begin,end,less);
            else if(o.group_sort=="ips2ra") {
                ips2ra::sort(begin,end,TimeSortKey{});
                if constexpr(std::is_same_v<T,CompactClick>) {
                    for(T* b=begin;b<end;){T* e=b+1;while(e<end&&e->ts==b->ts)++e;if(e-b>1)duckdb_pdqsort::pdqsort(b,e,less);b=e;}
                }
            } else
#endif
            duckdb_ska_sort::ska_sort(begin,end,TimeSortKey{std::is_same_v<T,CompactClick>});
        }
    }
    template<bool pre_sorted=false,class E,class C,class Name> void process(size_t s,int worker,std::span<E> ev,std::span<C> cl,Name name,int64_t time_base=0) {
            auto before=Clock::now();
            if constexpr(!pre_sorted) {
            auto event_less=[](auto& a,auto& b){return a.person!=b.person?a.person<b.person:a.ts<b.ts;};
            if(o.sorter=="duckdb") {
                if constexpr(std::is_same_v<E,CompactEvent>) {
                    duckdb_sort_events(ev,start,end);
                } else {
#ifdef SHOP_PACKED_IDS
                    if(ev.size()>1)duckdb_ska_sort::ska_sort(ev.begin(),ev.end(),PackedEventSortKey{});
#else
                    duckdb_pdqsort::pdqsort(ev.begin(),ev.end(),event_less);
#endif
                }
            } else std::sort(ev.begin(),ev.end(),event_less);
            // Within a timestamp, reverse click_id order puts the winning ID last.
            auto click_less=[](auto& a,auto& b){return a.person!=b.person?a.person<b.person:(a.ts!=b.ts?a.ts<b.ts:a.id>b.id);};
            if(o.sorter=="duckdb")duckdb_pdqsort::pdqsort(cl.begin(),cl.end(),click_less);
            else std::sort(cl.begin(),cl.end(),click_less);
            }
            profiles[worker].sort+=elapsed(before);before=Clock::now();
            using parquet::Column;
            std::vector<Column> cols={{"user_id",6},{"day",1,false,true},{"sessions",2},{"events",2},{"revenue_cents",2},{"top_category",6,true}};
            size_t cb=0;
            CampaignWindow campaign_window(o.array_active?campaign_names.size():0);
            CategoryRevenue category_revenue(o.array_categories?categories.size():0);
            for(size_t begin=0;begin<ev.size();) {
                size_t finish=begin+1;auto person_key=ev[begin].person;auto person_name=name(person_key);SV person=text(person_name);
                while(finish<ev.size()&&ev[finish].person==person_key)++finish;
                while(cb<cl.size()&&cl[cb].person<person_key)++cb;
                size_t ce=cb;while(ce<cl.size()&&cl[ce].person==person_key)++ce;
                size_t hi=cb,lo=cb;
                std::unordered_map<uint32_t,size_t> active;
                campaign_window.clear();
                int64_t day=-1,ns=0,ne=0,nr=0;
                std::unordered_map<uint32_t,int64_t> cat_revenue;
                auto flush=[&]{
                    if(!ns)return;
                    cols[0].text(person);cols[1].number(day);cols[2].number(ns);cols[3].number(ne);cols[4].number(nr);
                    if(o.array_categories) {
                        if(category_revenue.present.empty())cols[5].null();
                        else cols[5].text(categories[category_revenue.top(categories)]);
                    } else if(cat_revenue.empty())cols[5].null();
                    else {uint32_t top=0;int64_t amount=-1;for(auto [c,r]:cat_revenue)if(r>amount||(r==amount&&categories[c]<categories[top])){top=c;amount=r;}cols[5].text(categories[top]);}
                    ns=ne=nr=0;cat_revenue.clear();category_revenue.clear();
                };
                for(size_t i=begin;i<finish;) {
                    const int64_t st=time_base+ev[i].ts,sd=dayof(st);
                    if(sd!=day){flush();day=sd;}
                    size_t j=i+1;while(j<finish&&ev[j].ts-ev[j-1].ts<=GAP)++j;
                    int64_t revenue=0;
                    for(size_t k=i;k<j;++k)if(ev[k].price){
                        revenue+=ev[k].price;
                        if(o.array_categories)category_revenue.add(ev[k].category,ev[k].price);
                        else cat_revenue[ev[k].category]+=ev[k].price;
                    }
                    ++ns;ne+=j-i;nr+=revenue;
                    uint32_t credited=0;
                    if(o.baseline) {
                        // Intentionally simple oracle: rescan clicks per session.
                        std::unordered_map<uint32_t,bool> assisted;
                        for(size_t k=cb;k<ce;++k)if(cl[k].ts<=st&&cl[k].ts>=st-WEEK){credited=cl[k].campaign;assisted[credited]=true;}
                        for(auto [cam,unused]:assisted)++total(worker,sd,cam).assisted;
                    } else if(o.array_active) {
                        while(hi<ce&&cl[hi].ts<=st){campaign_window.add(cl[hi].campaign);++hi;}
                        while(lo<hi&&cl[lo].ts<st-WEEK){campaign_window.remove(cl[lo].campaign);++lo;}
                        if(hi>lo)credited=cl[hi-1].campaign;
                        for(auto cam:campaign_window.present)++total(worker,sd,cam).assisted;
                    } else {
                        while(hi<ce&&cl[hi].ts<=st){++active[cl[hi].campaign];++hi;}
                        while(lo<hi&&cl[lo].ts<st-WEEK){auto it=active.find(cl[lo].campaign);if(!--it->second)active.erase(it);++lo;}
                        if(hi>lo)credited=cl[hi-1].campaign;
                        for(auto [cam,count]:active)++total(worker,sd,cam).assisted;
                    }
                    auto& credited_total=total(worker,sd,credited);++credited_total.sessions;credited_total.purchasing+=revenue>0;credited_total.revenue+=revenue;
                    i=j;
                }
                flush();cb=ce;begin=finish;
            }
            profiles[worker].aggregate+=elapsed(before);before=Clock::now();
            if(cols[0].count||s==0)parquet::write((o.out/"user_daily"/("part-"+std::to_string(s)+".parquet")).string(),cols);
            profiles[worker].parquet+=elapsed(before);
    }
    void campaigns_output() {
        CampaignTotals all;
        if(o.dense_totals) {
            for(size_t i=0;i<dense_totals[0].size();++i) {
                Aggregate a;
                for(auto& w:dense_totals){const auto& t=w[i];a.sessions+=t.sessions;a.purchasing+=t.purchasing;a.revenue+=t.revenue;a.assisted+=t.assisted;}
                if(a.sessions||a.assisted)all[{dayof(start)+i/campaign_names.size(),i%campaign_names.size()}]=a;
            }
        }
        for(auto& worker:totals)for(auto [key,t]:worker){auto& a=all[key];a.sessions+=t.sessions;a.purchasing+=t.purchasing;a.revenue+=t.revenue;a.assisted+=t.assisted;}
        using parquet::Column;
        std::vector<Column> cols={{"campaign_id",6},{"channel",6},{"day",1,false,true},{"sessions",2},{"purchasing_sessions",2},{"revenue_cents",2},{"assisted_sessions",2}};
        for(auto [key,t]:all){auto [day,cam]=key;cols[0].text(campaign_names[cam]);cols[1].text(channels[cam]);cols[2].number(day);cols[3].number(t.sessions);cols[4].number(t.purchasing);cols[5].number(t.revenue);cols[6].number(t.assisted);}
        parquet::write((o.out/"campaign_daily"/"part-0.parquet").string(),cols);
    }
    template<class R> void ingest_and_dedup(Timer& timer) {
        Workers<R> raw(o.threads,Buckets<R>(o.ingest_shards));
        parse_events(raw);
        timer.mark(o.overlap_links?"parse_events_and_links":"parse_events");
        dedup(raw);
        timer.mark("dedup");
    }
    void run(){
        Timer t(o.threads);
        load();t.mark(o.overlap_links?"load_inputs":"load_links");
        // Reserve one offset as an invalid-row marker and retain a wide path
        // for long windows or product dictionaries with more categories.
        if(o.compact_raw&&uint64_t(end-start)<=UINT32_MAX&&categories.size()<=256) {
            ingest_and_dedup<CompactRaw>(t);
        } else {
            ingest_and_dedup<Raw>(t);
        }
        parse_clicks();t.mark("parse_clicks");
        sessionize();t.mark("sessions_and_user_output");
        campaigns_output();t.mark("campaign_output");
    }
};
int main(int argc,char** argv){try{Pipeline(options(argc,argv)).run();return 0;}catch(const std::exception& e){std::cerr<<"shopping: "<<e.what()<<'\n';return 1;}}
