#include "core.hpp"
#include "parquet.hpp"
#include "flat_map.hpp"
#include "sort.hpp"
#include "id.hpp"
#include "short_id.hpp"
#include "event_id.hpp"
#include "csv_scan.hpp"
#include "ingest_order.hpp"
#include "ingest_arena.hpp"
#include "location_codec.hpp"
#include "sparse_state.hpp"
#include "fast_time.hpp"
#include "worker_pool.hpp"
#include "gap_sessions.hpp"
#include "../third_party/unordered_dense/ankerl/unordered_dense.h"
#ifdef SHOP_IPS_SORTS
#include "ips4o.hpp"
#include "ips2ra.hpp"
#endif
#include <cstdlib>
#include <functional>
#ifndef SHOP_VIEW_IDS
#define SHOP_PACKED_IDS
#endif
#if defined(__x86_64__) && !defined(SHOP_WIDE_IDS) && !defined(SHOP_VIEW_IDS) && !defined(SHOP_SHORT_IDS)
#define SHOP_SHORT_IDS
#endif

using namespace shop;
#ifdef SHOP_PACKED_IDS
#ifdef SHOP_SHORT_IDS
using Key=ShortId;
using KeyHash=ShortIdHash;
using PeopleType=PackedPeopleFor<Key,KeyHash,true>;
SV text(const Key& key,ShortId::Buffer& buffer){return key.view(buffer);}
#else
using Key=Id;using KeyHash=IdHash;using PeopleType=PackedPeople;
SV text(const Id& s,ShortId::Buffer&){return s.view();}
#endif
#else
using Key=SV;using KeyHash=Hash;using PeopleType=People;
SV text(SV s,ShortId::Buffer&){return s;}
#endif
#if defined(SHOP_SHORT_IDS) && !defined(SHOP_PLAIN_EVENT_HASH)
using EventId=ReversibleEventId;
using EventIdHash=ReversibleEventHash;
#else
using EventId=Key;
using EventIdHash=KeyHash;
#endif
template<class V>using EventIdDict=std::unordered_map<EventId,V,EventIdHash>;
template<class V>using EventIdFlatMap=FlatMap<V,EventId,EventIdHash>;
template<class V>using KeyFlatMap=FlatMap<V,Key,KeyHash>;
struct DenseKeyHash {
    using is_avalanching=void;
    size_t operator()(const EventId& key)const noexcept {auto h=EventIdHash{}(key);return h^(h>>32);}
};
struct Raw {EventId id;Key person;int64_t ts,ingest;uint32_t price,category;};
struct RawLocation {EventId id;int64_t ingest;uint64_t location;};
struct NarrowRawLocation {EventId id;NarrowLocation location;};
static_assert(sizeof(NarrowRawLocation)==sizeof(EventId)+8);
struct CachedEventKey {
    EventId original;
    size_t hash;
    bool empty()const{return original.empty();}
    bool operator==(const CachedEventKey& other)const{return hash==other.hash&&original==other.original;}
};
struct CachedEventHash {
    size_t operator()(const CachedEventKey& id)const noexcept{return id.hash;}
};
struct CachedRawLocation {CachedEventKey id;NarrowLocation location;};
template<class R>constexpr bool is_location_record=std::is_same_v<R,RawLocation>||std::is_same_v<R,NarrowRawLocation>;
struct CompactRaw {
    EventId id;
    Key person;
    int64_t ingest;
    uint32_t offset,purchase;
    static constexpr uint32_t invalid=UINT32_MAX;
    static constexpr uint32_t price_mask=(1u<<24)-1;
};
static_assert(sizeof(CompactRaw)==sizeof(EventId)+sizeof(Key)+16);
struct Event {
    Key person;int64_t ts;uint32_t price,category;
    bool operator<(const Event& b)const{return person!=b.person?person<b.person:ts<b.ts;}
};
struct Click {Key person,id;int64_t ts;uint32_t campaign;};
struct RoutedEvent {Key person;uint32_t offset,purchase;};
constexpr uint32_t route_hash_bit=uint32_t{1}<<31;
struct UserGroupingHash {
    size_t operator()(const Key& name)const noexcept{return uint64_t(KeyHash{}(name))>>32;}
};
struct RoutedClick {Key person,id;uint32_t offset,campaign;};
static_assert(sizeof(RoutedEvent)==sizeof(Key)+8);
static_assert(sizeof(RoutedClick)==2*sizeof(Key)+8);
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
struct LinkedEvent {
    uint32_t ts,purchase;
    bool operator<(const LinkedEvent& other)const{return ts<other.ts;}
};
struct LinkedTimestamp {
    uint32_t ts;
    bool operator<(const LinkedTimestamp& other)const{return ts<other.ts;}
};
struct LinkedClick {
    Key id;uint32_t ts,campaign;
    bool operator<(const LinkedClick& other)const{return ts!=other.ts?ts<other.ts:id>other.id;}
};
static_assert(sizeof(LinkedEvent)==8);
static_assert(sizeof(LinkedClick)==sizeof(Key)+8);
struct TimeSortKey {
    bool requires_next_sort=false;
    static constexpr size_t ska_sort_width=8;
    bool Interrupted()const{return false;}
    bool ByteIsSkippable(size_t)const{return false;}
    template<class E>uint64_t operator()(const E& e)const{return uint64_t(e.ts)^0x8000000000000000ULL;}
};
struct Aggregate {int64_t sessions=0,purchasing=0,revenue=0,assisted=0;};
struct WorkProfile {
    double gather=0,intern=0,sort=0,merge=0,local_sort=0,aggregate=0,parquet=0;
    uint64_t sessions=0,no_click_sessions=0;
};
using CampaignTotals=std::map<std::pair<int64_t,uint32_t>,Aggregate>;
template<class T> using Buckets=std::vector<std::vector<T>>;
template<class T> using Workers=std::vector<Buckets<T>>;
#ifndef SHOP_VECTOR_INDIRECT_EVENTS
using CachedEventStorage=IngestArena<CachedRawLocation>;
using CachedEventBuckets=decltype(CachedEventStorage::buckets);
#else
using CachedEventStorage=Workers<CachedRawLocation>;
using CachedEventBuckets=CachedEventStorage;
#endif

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
    bool avx2_csv=true,row_csv=true;
    bool tag_links=true,heavy_shards_first=false;
    bool sparse_duplicates=true;
    bool compact_raw=true;
    bool inline_short_links=true;
    bool packed_routing=true;
    bool fast_link_csv=true,reserve_links=true;
    bool batch_link_probes=true;
    bool narrow_count_columns=true;
    bool redirect_linked_groups=true;
    bool specialize_aggregation=true;
    bool raw_arena=true;
    bool mark_duplicate_events=true;
    bool narrow_duplicate_locations=true;
    bool cache_indirect_events=true;
    bool cache_routed_users=true;
    bool branchless_event_validation=true;
    bool reserve_routes=true;
    bool parallel_route_reserve=true;
    bool cache_utc_days=true;
    bool split_group_purchases=true;
    bool persistent_workers=true;
    bool compact_assist_counters=true;
    bool fuse_isolated_groups=true;
    bool worker_parquet=false;
    size_t gap_bin_min_events=256;
    size_t sparse_bin_min_events=16;
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
        else if(arg=="--row-csv")o.row_csv=true;
        else if(arg=="--no-row-csv")o.row_csv=false;
        else if(arg=="--no-avx2-csv")o.avx2_csv=false;
        else if(arg=="--tag-links")o.tag_links=true;
        else if(arg=="--no-tag-links")o.tag_links=false;
        else if(arg=="--heavy-shards-first")o.heavy_shards_first=true;
        else if(arg=="--sparse-duplicates")o.sparse_duplicates=true;
        else if(arg=="--no-sparse-duplicates")o.sparse_duplicates=false;
        else if(arg=="--compact-raw")o.compact_raw=true;
        else if(arg=="--wide-raw")o.compact_raw=false;
        else if(arg=="--inline-short-links")o.inline_short_links=true;
        else if(arg=="--no-inline-short-links")o.inline_short_links=false;
        else if(arg=="--packed-routing")o.packed_routing=true;
        else if(arg=="--wide-routing")o.packed_routing=false;
        else if(arg=="--fast-link-csv")o.fast_link_csv=true;
        else if(arg=="--reserve-links")o.reserve_links=true;
        else if(arg=="--legacy-link-input"){o.fast_link_csv=false;o.reserve_links=false;}
        else if(arg=="--batch-link-probes")o.batch_link_probes=true;
        else if(arg=="--unbatched-link-probes")o.batch_link_probes=false;
        else if(arg=="--narrow-count-columns")o.narrow_count_columns=true;
        else if(arg=="--wide-count-columns")o.narrow_count_columns=false;
        else if(arg=="--redirect-linked-groups")o.redirect_linked_groups=true;
        else if(arg=="--no-redirect-linked-groups")o.redirect_linked_groups=false;
        else if(arg=="--specialize-aggregation")o.specialize_aggregation=true;
        else if(arg=="--generic-aggregation")o.specialize_aggregation=false;
        else if(arg=="--raw-arena")o.raw_arena=true;
        else if(arg=="--vector-raw")o.raw_arena=false;
        else if(arg=="--mark-duplicate-events")o.mark_duplicate_events=true;
        else if(arg=="--copy-dedup-winners")o.mark_duplicate_events=false;
        else if(arg=="--narrow-duplicate-locations")o.narrow_duplicate_locations=true;
        else if(arg=="--wide-duplicate-locations")o.narrow_duplicate_locations=false;
        else if(arg=="--cached-indirect-events")o.cache_indirect_events=true;
        else if(arg=="--uncached-indirect-events")o.cache_indirect_events=false;
        else if(arg=="--cached-user-routes")o.cache_routed_users=true;
        else if(arg=="--plain-user-routes")o.cache_routed_users=false;
        else if(arg=="--branchless-event-validation")o.branchless_event_validation=true;
        else if(arg=="--conditional-event-validation")o.branchless_event_validation=false;
        else if(arg=="--reserve-routes")o.reserve_routes=true;
        else if(arg=="--grow-routes")o.reserve_routes=false;
        else if(arg=="--parallel-route-reserve")o.parallel_route_reserve=true;
        else if(arg=="--serial-route-reserve")o.parallel_route_reserve=false;
        else if(arg=="--cache-utc-days")o.cache_utc_days=true;
        else if(arg=="--no-cache-utc-days")o.cache_utc_days=false;
        else if(arg=="--split-group-purchases")o.split_group_purchases=true;
        else if(arg=="--inline-group-purchases")o.split_group_purchases=false;
        else if(arg=="--persistent-workers")o.persistent_workers=true;
        else if(arg=="--transient-workers")o.persistent_workers=false;
        else if(arg=="--compact-assist-counters")o.compact_assist_counters=true;
        else if(arg=="--wide-assist-counters")o.compact_assist_counters=false;
        else if(arg=="--fuse-isolated-groups")o.fuse_isolated_groups=true;
        else if(arg=="--defer-isolated-groups")o.fuse_isolated_groups=false;
        else if(arg=="--worker-parquet")o.worker_parquet=true;
        else if(arg=="--gap-bin-min-events"&&i+1<argc)o.gap_bin_min_events=std::stoull(argv[++i]);
        else if(arg=="--sparse-bin-min-events"&&i+1<argc)o.sparse_bin_min_events=std::stoull(argv[++i]);
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
    if(o.baseline)o.narrow_count_columns=false;
    if(o.baseline){o.fast_link_csv=false;o.reserve_links=false;}
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
    struct UserOutput {
        std::vector<parquet::Column> columns;
        std::unique_ptr<parquet::File> file;
    };
    Options o;
    Inputs in;
    PeopleType people;
    KeyFlatMap<uint32_t> person_lookup;
    FlatMap<uint32_t> products,campaigns;
    Dict<uint32_t> category_index;
    std::vector<SV> categories={"unknown"},campaign_names={"organic"},channels={"organic"};
    int64_t start,end;
    IngestOrder ingest_order;
    StrictTimeParser strict_time;
    Workers<Event> events;
    Workers<Click> clicks;
    Workers<RoutedEvent> routed_events;
    Workers<RoutedClick> routed_clicks;
    std::vector<CampaignTotals> totals;
    std::vector<std::vector<Aggregate>> dense_totals;
    std::vector<std::vector<uint32_t>> dense_assists;
    std::vector<WorkProfile> profiles;
    std::vector<UserOutput> user_outputs;
    std::unique_ptr<LocationCodec> location_codec;
    std::unique_ptr<WorkerPool> workers;
    bool cache_user_hashes=false;
    bool clicks_parsed=false;
    const bool profile_detail=std::getenv("SHOP_PROFILE_DETAIL")!=nullptr;
    explicit Pipeline(Options options):o(std::move(options)),
        events(o.threads,Buckets<Event>(o.shards)),clicks(o.threads,Buckets<Click>(o.shards)),totals(o.threads),profiles(o.threads){}
    using Clock=std::chrono::steady_clock;
    static double elapsed(Clock::time_point t){return std::chrono::duration<double>(Clock::now()-t).count();}
    template<class F>void parallel(size_t jobs,int threads,F work) {
        if(!o.persistent_workers||threads==1)shop::parallel(jobs,threads,std::move(work));
        else {
            if(!workers)workers=std::make_unique<WorkerPool>(threads);
            workers->run(jobs,std::move(work));
        }
    }
    size_t shard(Key s)const{return KeyHash{}(s)&(o.shards-1);}
    size_t event_shard(EventId s)const{return EventIdHash{}(s)&(o.dedup_shards-1);}
    template<class T>
    void reserve_routes(Workers<T>& routes,const std::vector<Chunk>& chunks,size_t bytes_per_row) {
        if(routes.empty())return;
        size_t bytes=0;
        for(auto chunk:chunks)bytes+=chunk.end-chunk.begin;
        size_t expected=bytes/(bytes_per_row*o.threads*o.shards);
        if(expected<32)return;
        auto reserve_worker=[&](size_t worker,int) {
            try {
                for(auto& shard:routes[worker])shard.reserve(expected);
            } catch(const std::bad_alloc&) {
                // Partially reserved vectors can still grow on demand.
            }
        };
        if(o.parallel_route_reserve&&o.persistent_workers)parallel(routes.size(),o.threads,reserve_worker);
        else for(size_t worker=0;worker<routes.size();++worker)reserve_worker(worker,0);
    }
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
        if(o.fast_time)strict_time.init(start,end,o.cache_utc_days);
        std::vector<Chunk> lookup;
        in.add(o.data/"products.csv",lookup);
        category_index.emplace("unknown",0);
        for(auto chunk:lookup)Inputs::lines(chunk,[&](SV line){
            std::array<SV,2> f;if(!csv(line,f))throw std::runtime_error("malformed products.csv");
            auto [it,added]=category_index.try_emplace(f[1],categories.size());
            if(added)categories.push_back(f[1]);
            products.try_emplace(f[0],it->second);
        });
        lookup.clear();in.add(o.data/"campaigns.csv",lookup);
        for(auto chunk:lookup)Inputs::lines(chunk,[&](SV line){
            std::array<SV,2> f;if(!csv(line,f))throw std::runtime_error("malformed campaigns.csv");
            campaigns.try_emplace(f[0],campaign_names.size());campaign_names.push_back(f[0]);channels.push_back(f[1]);
        });
        products.freeze();
        campaigns.freeze();
        if(o.packed_routing&&o.direct_encoding&&o.group_person) {
            if(uint64_t(end-start-1)<=UINT32_MAX&&categories.size()<=256)
                routed_events.assign(o.threads,Buckets<RoutedEvent>(o.shards));
            if(uint64_t(end-start-1)+WEEK<=UINT32_MAX)
                routed_clicks.assign(o.threads,Buckets<RoutedClick>(o.shards));
        }
        if(o.dense_totals) {
            auto days=dayof(end-1)-dayof(start)+1;
            dense_totals.resize(o.threads);
            for(auto& t:dense_totals)t.resize(size_t(days)*campaign_names.size());
        }
        for(auto p:files(o.data/"links"))in.add(p,in.links);
        o.redirect_linked_groups=o.redirect_linked_groups&&o.overlap_links&&
            !in.links.empty()&&!routed_events.empty()&&!routed_clicks.empty();
        if(!o.overlap_links)build_links();
        for(auto p:files(o.data/"events"))in.add(p,in.events);
        for(auto p:files(o.data/"clicks"))in.add(p,in.clicks);
        if(o.worker_parquet) {
            size_t bytes=0;
            for(auto chunk:in.events)bytes+=chunk.end-chunk.begin;
            // Each accepted event row contains at least six separators.
            const int count_type=o.narrow_count_columns&&bytes/6<=INT32_MAX?1:2;
            user_outputs.resize(o.threads);
            for(auto& output:user_outputs)output.columns=user_columns(count_type);
        }
        if(o.compact_assist_counters&&o.redirect_linked_groups&&o.specialize_aggregation&&
           o.array_active&&o.array_categories&&o.dense_totals) {
            size_t bytes=0;
            for(auto chunk:in.events)bytes+=chunk.end-chunk.begin;
            // Each accepted event row has six commas. A campaign assists
            // each session at most once, and every session has an event.
            if(bytes/6<=UINT32_MAX)
                dense_assists.assign(o.threads,std::vector<uint32_t>(dense_totals.front().size()));
        }
        if(o.reserve_routes) {
            reserve_routes(routed_events,in.events,64);
            reserve_routes(routed_clicks,in.clicks,40);
        }
        fs::create_directories(o.out/"user_daily");fs::create_directories(o.out/"campaign_daily");
        // Replace only our table outputs, so rerunning never leaves extra rows.
        for(auto table:{"user_daily","campaign_daily"})for(auto p:files_parquet(o.out/table))fs::remove(p);
    }
    void build_links() {
        if(o.reserve_links) {
            constexpr size_t reserve_limit=1u<<20;
            size_t edges=0;
            for(auto chunk:in.links) {
                edges+=std::count(chunk.begin,chunk.end,'\n')+(chunk.begin!=chunk.end&&chunk.end[-1]!='\n');
                if(edges>=reserve_limit){edges=reserve_limit;break;}
            }
            if(edges) {
                people.index.reserve(edges);
                people.parents.reserve(edges);
                people.sizes.reserve(edges);
                people.names.reserve(edges);
            }
        }
        auto join=[&](const std::array<SV,2>& fields) {
            if(!fields[0].empty()&&!fields[1].empty())people.join(Key(fields[0]),Key(fields[1]));
        };
        for(auto chunk:in.links) {
            if(o.fast_link_csv)records<2>(chunk,join);
            else Inputs::lines(chunk,[&](SV line) {
                std::array<SV,2> fields;
                if(csv(line,fields))join(fields);
            });
        }
#ifdef SHOP_SHORT_IDS
        people.finish(o.inline_short_links);
#else
        people.finish();
#endif
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
#ifdef SHOP_SHORT_IDS
    std::pair<Key,size_t> canonical_shard(Key person,size_t hash)const {
        auto* alias=people.index.find(person,hash);
        if(alias&&alias->second!=person.word) {
            person.word=alias->second;
            hash=KeyHash{}(person);
        }
        return {person,hash&(o.shards-1)};
    }
#endif
    Aggregate& total(int worker,int64_t day,uint32_t campaign) {
        if(o.dense_totals)return dense_totals[worker][size_t(day-dayof(start))*campaign_names.size()+campaign];
        return totals[worker][{day,campaign}];
    }
    template<size_t N,class F>void records(Chunk chunk,F consume)const {
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
        if(o.fused_csv&&o.avx2_csv&&avx2_csv_available()) {
            if(o.row_csv&&row_csv_available())scan_csv_rows<N>(chunk,consume);
            else scan_csv_avx2<N>(chunk,consume);
            return;
        }
#endif
        if(o.fused_csv)scan_csv<N>(chunk,consume);
        else Inputs::lines(chunk,[&](SV line){std::array<SV,N> fields;if(csv(line,fields))consume(fields);});
    }
    int64_t parse_time(SV s)const{return o.fast_time?strict_time.parse(s,o.cache_utc_days):timestamp(s);}
    static unsigned classify_event(SV type) {
        if(type.size()==4) {
            uint32_t word;
            std::memcpy(&word,type.data(),4);
            constexpr auto view=std::bit_cast<uint32_t>(std::array<char,4>{'v','i','e','w'});
            constexpr auto cart=std::bit_cast<uint32_t>(std::array<char,4>{'c','a','r','t'});
            return (word==view)|(word==cart);
        }
        return type=="purchase"?2:0;
    }
    template<class R,class Buffer>void parse_event_chunk(std::vector<std::vector<Buffer>>& raw,
                                                        size_t i,int worker,[[maybe_unused]] CachedEventBuckets* indirect) {
        records<7>(in.events[i],[&](const std::array<SV,7>& f){
            if(f[0].empty())return;
            // Invalid winners remain in the dedup input. A later valid copy
            // must never rescue an invalid earliest copy.
            int64_t ts=parse_time(f[2]),ingest=o.ordered_ingest?IngestOrder::pack(f[3]):parse_time(f[3]);
            uint64_t price=0;uint32_t category=0;
            bool purchase,valid;
            if(o.branchless_event_validation&&!o.ordered_ingest) {
                unsigned type=classify_event(f[4]);
                purchase=type==2;
                bool late=(ingest>ts)&(uint64_t(ingest)-uint64_t(ts)>uint64_t(DAY));
                valid=!f[1].empty()&!f[5].empty()&(type!=0)&(ts>=start)&(ts<end)&!late;
            } else {
                purchase=f[4]=="purchase";
                valid=!f[1].empty()&&!f[5].empty()&&(purchase||f[4]=="view"||f[4]=="cart")&&
                    ts>=start&&ts<end&&(o.ordered_ingest?ingest_order.on_time(ingest,ts):ingest-ts<=DAY);
            }
            if(purchase) {
                valid=valid&&digits(f[6],price,10000000)&&price>0;
                if(valid){auto it=products.find(f[5]);if(it!=products.end())category=it->second;}
            }
            if(!valid)ts=-1;
            EventId event_id(f[0]);
            Key user_id(f[1]);
            Key person=o.overlap_links?user_id:canonical(user_id);
            const size_t hash=EventIdHash{}(event_id);
            size_t bucket=hash&(o.ingest_shards-1);
            if constexpr(is_location_record<R>) {
                size_t destination=0,index=0;
                if(valid) {
                    const size_t person_hash=KeyHash{}(person);
                    destination=person_hash&(o.shards-1);
                    uint32_t offset=uint32_t(ts-start),payload=uint32_t(price)|(category<<24);
                    if(cache_user_hashes&&!price) {
                        // The purchase payload is free on views and carts.
                        offset|=route_hash_bit;
                        payload=uint64_t(person_hash)>>32;
                    }
                    auto& target=routed_events[worker][destination];
                    if(target.size()>=UINT32_MAX)throw std::runtime_error("too many events in one worker partition");
                    index=target.size()+1;
                    target.push_back({person,offset,payload});
                }
                if constexpr(std::is_same_v<R,NarrowRawLocation>) {
                    auto location=location_codec->encode(ingest,worker,destination,index);
#ifdef SHOP_SHORT_IDS
                    if(indirect&&event_id.indirect())(*indirect)[worker][bucket].push_back({{event_id,hash},location});
                    else
#endif
                    raw[worker][bucket].push_back({event_id,location});
                }
                else raw[worker][bucket].push_back({event_id,ingest,LocationCodec::wide_position(worker,destination,index)});
            } else if constexpr(std::is_same_v<R,CompactRaw>) {
                uint32_t offset=valid?uint32_t(ts-start):CompactRaw::invalid;
                uint32_t purchase_info=(uint32_t(price)&CompactRaw::price_mask)|(category<<24);
                raw[worker][bucket].push_back({event_id,person,ingest,offset,purchase_info});
            } else {
                raw[worker][bucket].push_back({event_id,person,ts,ingest,uint32_t(price),category});
            }
        });
    }
    template<class R,class Buffer> void parse_events(std::vector<std::vector<Buffer>>& raw,
                                                     [[maybe_unused]] CachedEventBuckets* indirect) {
        struct alignas(64) ParseWorkerTiming {
            Clock::time_point last_chunk;
            size_t chunks=0;
        };
        const bool measure=profile_detail&&o.overlap_links&&!in.events.empty();
        std::vector<ParseWorkerTiming> parse_times(measure?o.threads:0);
        const auto phase_start=measure?Clock::now():Clock::time_point{};
        Clock::time_point link_start,link_finish;
        int link_worker=-1;
        parallel(in.events.size()+size_t(o.overlap_links),o.threads,[&](size_t i,int worker){
            if(o.overlap_links) {
                // This job owns the link graph. The other jobs carry raw user
                // IDs until the barrier, and therefore never read it early.
                if(!i) {
                    if(measure){link_start=Clock::now();link_worker=worker;}
                    build_links();
                    if(measure)link_finish=Clock::now();
                    return;
                }
                --i;
            }
            parse_event_chunk<R>(raw,i,worker,indirect);
            if(measure) {
                parse_times[worker].last_chunk=Clock::now();
                ++parse_times[worker].chunks;
            }
        });
        if(measure) {
            auto csv_finish=phase_start;
            size_t early_workers=0;
            for(const auto& timing:parse_times) {
                if(!timing.chunks)continue;
                csv_finish=std::max(csv_finish,timing.last_chunk);
                early_workers+=timing.last_chunk<link_finish;
            }
            const auto seconds=[](auto duration){return std::chrono::duration<double>(duration).count();};
            std::cerr<<"event_link_overlap_seconds links="<<seconds(link_finish-link_start)
                <<" links_finish="<<seconds(link_finish-phase_start)
                <<" csv_finish="<<seconds(csv_finish-phase_start)
                <<" links_tail="<<std::max(0.0,seconds(link_finish-csv_finish))
                <<" early_csv_workers="<<early_workers
                <<" link_worker_csv_chunks="<<parse_times[link_worker].chunks
                <<" csv_chunks="<<in.events.size()<<'\n';
        }
    }
    template<class Buffer>static void release_raw(Buffer& buffer) {
        if constexpr(requires {buffer.release();})buffer.release();
        else Buffer().swap(buffer);
    }
    // Records already reside in their user partitions. Only losing copies
    // need a write after the earliest-ingest comparison.
    template<class R,class Buffer>void mark_duplicate_shard(std::vector<std::vector<Buffer>>& raw,size_t s) {
        constexpr bool cached=std::is_same_v<R,CachedRawLocation>;
        using DedupKey=std::conditional_t<cached,CachedEventKey,EventId>;
        using DedupHash=std::conditional_t<cached,CachedEventHash,EventIdHash>;
        auto drop=[&](const R& row) {
            uint64_t location;
            if constexpr(cached||std::is_same_v<R,NarrowRawLocation>)location=location_codec->decode(row.location);
            else location=row.location;
            if(location)routed_events[location>>48][(location>>32)&65535][uint32_t(location)-1].offset=CompactRaw::invalid;
        };
        auto earlier=[&](const R& a,const R& b) {
            if constexpr(cached||std::is_same_v<R,NarrowRawLocation>)return location_codec->earlier(a.location,b.location);
            else return a.ingest<b.ingest;
        };
        size_t count=0;
        for(const auto& worker:raw)count+=worker[s].size();
        if(!count)return;
        size_t partitions=o.dedup_shards/o.ingest_shards;
        unsigned shift=std::countr_zero(unsigned(o.ingest_shards));
        std::vector<std::vector<R>> rows(partitions);
        for(auto& part:rows)part.reserve(count/partitions+count/partitions/8+16);
        for(auto& worker:raw) {
            for(const auto& row:worker[s])rows[(DedupHash{}(row.id)&(o.dedup_shards-1))>>shift].push_back(row);
            release_raw(worker[s]);
        }
        for(const auto& part:rows) {
            if(part.size()<2)continue;
            std::array<uint64_t,1024> seen{},repeated{};
            std::vector<uint16_t> tags;
            tags.reserve(part.size());
            for(const auto& row:part) {
                uint16_t tag=uint64_t(DedupHash{}(row.id))>>32;
                tags.push_back(tag);
                uint64_t bit=uint64_t{1}<<(tag&63);
                repeated[tag>>6]|=seen[tag>>6]&bit;
                seen[tag>>6]|=bit;
            }
            FlatMap<size_t,DedupKey,DedupHash> winners;
            winners.reserve(part.size()/8);
            for(size_t i=0;i<part.size();++i) {
                uint16_t tag=tags[i];
                if(!(repeated[tag>>6]&(uint64_t{1}<<(tag&63))))continue;
                const auto& row=part[i];
                auto [entry,added]=winners.try_emplace(row.id,i);
                if(added)continue;
                const auto& current=part[entry->second];
                if(earlier(row,current)){drop(current);entry->second=i;}
                else drop(row);
            }
        }
    }
    template<class R,class Buffer>void mark_duplicate_locations(std::vector<std::vector<Buffer>>& raw) {
        parallel(o.ingest_shards,o.threads,[&](size_t s,int){mark_duplicate_shard<R>(raw,s);});
    }
    template<class R,class Buffer> void dedup(std::vector<std::vector<Buffer>>& raw) {
        parallel(o.ingest_shards,o.threads,[&](size_t s,int worker){
            size_t count=0;for(auto& w:raw)count+=w[s].size();
            if(!count)return;
            auto append=[&](Event event,size_t destination) {
                if(routed_events.empty())events[worker][destination].push_back(event);
                else routed_events[worker][destination].push_back({event.person,uint32_t(event.ts-start),
                                                                   event.price|(event.category<<24)});
            };
#ifdef SHOP_SHORT_IDS
            struct Pending {Event event;size_t hash,destination;};
            std::array<Pending,16> pending;
            size_t used=0;
            bool batch=!o.redirect_linked_groups&&o.batch_link_probes&&o.overlap_links&&o.inline_short_links&&people.index.size()!=0;
            auto flush_links=[&] {
                for(size_t i=0;i<used;++i) {
                    auto& entry=pending[i];
                    auto [person,destination]=canonical_shard(entry.event.person,entry.hash);
                    entry.event.person=person;
                    entry.destination=destination;
                }
                for(size_t i=0;i<used;++i)append(pending[i].event,pending[i].destination);
                used=0;
            };
#endif
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
#ifdef SHOP_SHORT_IDS
                if(batch) {
                    size_t hash=KeyHash{}(r.person);
                    people.index.prefetch(hash);
                    pending[used++]={{r.person,ts,price,category},hash,0};
                    if(used==pending.size())flush_links();
                    return;
                }
#endif
                Key person=o.overlap_links&&!o.redirect_linked_groups?canonical(r.person):r.person;
                append({person,ts,price,category},shard(person));
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
                    release_raw(buckets[s]);
                }
                for(auto& rows:subpartitions)if(!rows.empty())dedup_rows(rows,emit);
            } else if(o.direct_dedup&&o.dedup=="flat") {
                EventIdFlatMap<const R*> winners;
                winners.reserve(count);
                for(const auto& buckets:raw) {
                    for(const auto& event:buckets[s]) {
                        auto [entry,inserted]=winners.try_emplace(event.id,&event);
                        if(!inserted&&event.ingest<entry->second->ingest)entry->second=&event;
                    }
                }
                winners.each([&](const EventId&,const R* event){emit(*event);});
                for(auto& buckets:raw)release_raw(buckets[s]);
            } else {
                std::vector<R> rows;rows.reserve(count);
                for(auto& w:raw){rows.insert(rows.end(),w[s].begin(),w[s].end());release_raw(w[s]);}
                dedup_rows(rows,emit);
            }
#ifdef SHOP_SHORT_IDS
            flush_links();
#endif
        });
    }
    template<class R,class Emit> void dedup_rows(std::vector<R>& rows,Emit emit) {
        if(o.sparse_duplicates&&o.dedup=="flat"&&rows.size()>=1024) {
            dedup_sparse(rows,emit);
            return;
        }
        if(o.dedup=="dense") {
            ankerl::unordered_dense::map<EventId,size_t,DenseKeyHash> winners;winners.reserve(rows.size());
            for(size_t i=0;i<rows.size();++i){auto [it,added]=winners.try_emplace(rows[i].id,i);if(!added&&rows[i].ingest<rows[it->second].ingest)it->second=i;}
            for(auto [id,i]:winners)emit(rows[i]);
        } else if(o.dedup=="flat") {
            EventIdFlatMap<size_t> winners;winners.reserve(rows.size());
            for(size_t i=0;i<rows.size();++i){auto [it,added]=winners.try_emplace(rows[i].id,i);if(!added&&rows[i].ingest<rows[it->second].ingest)it->second=i;}
            winners.each([&](EventId,size_t i){emit(rows[i]);});
        } else if(o.dedup=="hash") {
            EventIdDict<size_t> winners;winners.reserve(rows.size());
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
            uint16_t tag=uint64_t(EventIdHash{}(event.id))>>32;
            tags.push_back(tag);
            const uint64_t bit=uint64_t{1}<<(tag&63);
            shared[tag>>6]|=seen[tag>>6]&bit;
            seen[tag>>6]|=bit;
        }
        EventIdFlatMap<size_t> winners;
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
        winners.each([&](const EventId&,size_t row){emit(rows[row]);});
    }
    void parse_click_chunk(size_t i,int worker) {
        records<4>(in.clicks[i],[&](const std::array<SV,4>& f){
            if(f[0].empty()||f[1].empty())return;
            auto it=campaigns.find(f[2]);if(it==campaigns.end())return;
            auto ts=parse_time(f[3]);if(ts==-1||ts<start-WEEK||ts>=end)return;
            auto person=Key(f[1]);
            if(!o.redirect_linked_groups)person=canonical(person);
            size_t destination=shard(person);
            Key id(f[0]);
            if(routed_clicks.empty())clicks[worker][destination].push_back({person,id,ts,it->second});
            else routed_clicks[worker][destination].push_back({person,id,uint32_t(ts-(start-WEEK)),it->second});
        });
    }
    void parse_clicks() {
        parallel(in.clicks.size(),o.threads,[&](size_t i,int worker){parse_click_chunk(i,worker);});
        // Identical duplicate clicks need no materialized dedup: they cannot
        // change the winning (timestamp,id), nor campaign membership in a window.
    }
    template<class R,class Buffer>void ingest_dag(std::vector<std::vector<Buffer>>& raw,
                                                  CachedEventBuckets* indirect,bool click_tail) {
        enum class InputKind {links,event,click};
        struct InputJob {InputKind kind;size_t index;};
        const size_t event_chunks=in.events.size(),click_chunks=in.clicks.size();
        std::vector<InputJob> input;
        input.reserve(event_chunks+click_chunks+size_t(o.overlap_links));
        if(o.overlap_links)input.push_back({InputKind::links,0});
        size_t click=0;
        for(size_t event=0;event<event_chunks;++event) {
            input.push_back({InputKind::event,event});
            if(!click_tail) {
                const size_t through=click_chunks*(event+1)/event_chunks;
                while(click<through)input.push_back({InputKind::click,click++});
            }
        }
        while(click<click_chunks)input.push_back({InputKind::click,click++});

        constexpr unsigned events_ready=1,cancelled=2;
        std::atomic<size_t> pending_events{event_chunks};
        std::atomic<unsigned> state{event_chunks?0u:events_ready};
        std::atomic<size_t> next_input{0},next_dedup{0};
        const size_t raw_jobs=o.ingest_shards,mapped_jobs=indirect?raw_jobs:0;
        parallel(o.threads,o.threads,[&](size_t,int worker) {
            try {
                for(;;) {
                    const unsigned ready=state.load(std::memory_order_acquire);
                    if(ready&cancelled)return;
                    if(ready&events_ready) {
                        const size_t job=next_dedup.fetch_add(1,std::memory_order_relaxed);
                        if(job<raw_jobs) {
                            mark_duplicate_shard<R>(raw,job);
                            continue;
                        }
                        if(job<raw_jobs+mapped_jobs) {
                            mark_duplicate_shard<CachedRawLocation>(*indirect,job-raw_jobs);
                            continue;
                        }
                    }
                    const size_t job=next_input.fetch_add(1,std::memory_order_relaxed);
                    if(job>=input.size()) {
                        if(ready&events_ready)return;
                        state.wait(ready,std::memory_order_acquire);
                        continue;
                    }
                    const auto task=input[job];
                    switch(task.kind) {
                        case InputKind::links:build_links();break;
                        case InputKind::click:parse_click_chunk(task.index,worker);break;
                        case InputKind::event:
                            parse_event_chunk<R>(raw,task.index,worker,indirect);
                            // Publish every event buffer before dedup can mutate routed offsets.
                            if(pending_events.fetch_sub(1,std::memory_order_acq_rel)==1) {
                                state.fetch_or(events_ready,std::memory_order_release);
                                state.notify_all();
                            }
                            break;
                    }
                }
            } catch(...) {
                state.fetch_or(cancelled,std::memory_order_release);
                state.notify_all();
                throw;
            }
        });
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
    template<class T,class F> static void consume_bucket(Workers<T>& workers,size_t shard,F consume,
                                                         [[maybe_unused]] bool prefetch_users=false) {
        for(auto& buckets:workers) {
            auto& records=buckets[shard];
#ifdef SHOP_SHORT_IDS
            if constexpr(std::is_same_v<T,RoutedEvent>||std::is_same_v<T,RoutedClick>) {
                constexpr size_t batch=64;
                const auto sample=std::span(records).first(std::min(batch,records.size()));
                if(prefetch_users&&std::any_of(sample.begin(),sample.end(),[](const auto& row){return row.person.indirect();})) {
                    for(size_t begin=0;begin<records.size();begin+=batch) {
                        const size_t end=std::min(begin+batch,records.size());
                        const size_t ahead=std::min(end+batch,records.size());
                        for(size_t i=end;i<ahead;++i) {
                            const auto& person=records[i].person;
                            if(person.indirect()&&!(person.word&ShortId::extra_bit))
                                __builtin_prefetch(reinterpret_cast<const char*>(person.word&ShortId::pointer_mask),0,1);
                        }
                        for(size_t i=begin;i<end;++i)consume(records[i]);
                    }
                    std::vector<T>().swap(records);
                    continue;
                }
            }
#endif
            for(const auto& record:records)consume(record);
            std::vector<T>().swap(records);
        }
    }
    size_t event_bucket_size(size_t s)const {
        return routed_events.empty()?bucket_size(events,s):bucket_size(routed_events,s);
    }
    size_t click_bucket_size(size_t s)const {
        return routed_clicks.empty()?bucket_size(clicks,s):bucket_size(routed_clicks,s);
    }
    template<class E> void encode_shard(size_t shard,int worker,int64_t time_base) {
        auto before=Clock::now();
        if(o.direct_encoding) {
            auto consume_events=[&](auto f) {
                if(routed_events.empty())consume_bucket(events,shard,f);
                else consume_bucket(routed_events,shard,[&](const RoutedEvent& e) {
                    f(Event{e.person,start+e.offset,e.purchase&CompactRaw::price_mask,e.purchase>>24});
                });
            };
            auto consume_clicks=[&](auto f) {
                if(routed_clicks.empty())consume_bucket(clicks,shard,f);
                else consume_bucket(routed_clicks,shard,[&](const RoutedClick& c) {
                    f(Click{c.person,c.id,start-WEEK+c.offset,c.campaign});
                });
            };
            encode_partition<E>(shard,worker,event_bucket_size(shard),click_bucket_size(shard),
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
                weights[shard]=event_bucket_size(shard)+click_bucket_size(shard);
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
    }
    void report_session_profile()const {
        WorkProfile sum;
        for(const auto& p:profiles) {
            sum.gather+=p.gather;sum.intern+=p.intern;sum.sort+=p.sort;sum.merge+=p.merge;
            sum.local_sort+=p.local_sort;sum.aggregate+=p.aggregate;sum.parquet+=p.parquet;
            sum.sessions+=p.sessions;sum.no_click_sessions+=p.no_click_sessions;
        }
        std::cerr<<"session_work_seconds gather="<<sum.gather<<" intern="<<sum.intern<<" sort="<<sum.sort<<" aggregate="<<sum.aggregate<<" parquet="<<sum.parquet<<'\n';
        if(profile_detail) {
            std::cerr<<"session_detail_worker_seconds grouping="<<sum.gather+sum.intern+sum.merge
                <<" sorting_and_binning="<<sum.sort+sum.local_sort<<" aggregation="<<sum.aggregate-sum.local_sort-sum.merge
                <<" user_parquet="<<sum.parquet<<'\n';
            std::cerr<<"session_click_counts sessions="<<sum.sessions<<" without_clicks="<<sum.no_click_sessions<<'\n';
        }
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
    template<class T,class U=T,class Project=std::identity>
    static Grouped<U> group_records(std::vector<T>& input,std::vector<size_t>& counts,Project project={}) {
        size_t sum=0;
        for(auto& count:counts){auto n=count;count=sum;sum+=n;}
        counts.push_back(sum);
        auto positions=counts;
        // Default-initialize trivial records: avoid an extra zero-fill pass.
        std::unique_ptr<U[]> output(new U[sum]);
        for(auto& row:input)output[positions[row.person]++]=project(row);
        std::vector<T>().swap(input);
        return {std::move(output),sum};
    }
    template<bool SplitPurchases=false>void sessionize_linked_groups() {
        using GroupEvent=std::conditional_t<SplitPurchases,LinkedTimestamp,LinkedEvent>;
        struct NoPurchases {};
        struct EventInput {GroupEvent event;uint32_t person;};
        struct PurchaseInput {LinkedEvent event;uint32_t person;};
        struct ClickInput {LinkedClick click;uint32_t person;};
        struct Source {
            Grouped<GroupEvent> events;
            Grouped<LinkedClick> clicks;
            [[no_unique_address]] std::conditional_t<SplitPurchases,Grouped<LinkedEvent>,NoPurchases> purchases;
        };
        struct Group {
            Key name;
            std::span<GroupEvent> events;
            std::span<LinkedClick> clicks;
            [[no_unique_address]] std::conditional_t<SplitPurchases,std::span<LinkedEvent>,NoPurchases> purchases;
        };
        const bool specialized=o.specialize_aggregation&&o.array_active&&o.array_categories&&o.dense_totals;
        auto consume_group=[&](const Group& group,auto& consume,int worker) {
            auto ev=group.events;
            auto cl=group.clicks;
            if(ev.empty())return;
            const auto before=profile_detail?Clock::now():Clock::time_point{};
            auto sorted=[&](auto records,auto... args) {
                if(profile_detail)profiles[worker].local_sort+=elapsed(before);
                consume(group.name,records,cl,start,start-WEEK,args...);
            };
            if(cl.size()>1)sort_group(cl.data(),cl.data()+cl.size(),[](auto& a,auto& b){return a.ts!=b.ts?a.ts<b.ts:a.id>b.id;});
            if constexpr(SplitPurchases) {
                auto purchases=group.purchases;
                if(purchases.size()>1)sort_group(purchases.data(),purchases.data()+purchases.size(),
                    [](auto& a,auto& b){return a.ts<b.ts;});
                if(o.gap_bin_min_events&&o.sparse_bin_min_events&&ev.size()>=o.sparse_bin_min_events&&ev.size()<1024) {
                    thread_local GapSessions<true> sparse_bins;
                    sorted(sparse_bins.summarize(ev,uint32_t(end-start-1)),purchases);
                    return;
                }
                if(o.gap_bin_min_events&&ev.size()>=o.gap_bin_min_events) {
                    thread_local GapSessions<> bins;
                    sorted(bins.summarize(ev,uint32_t(end-start-1)),purchases);
                    return;
                }
                sort_group(ev.data(),ev.data()+ev.size(),[](auto& a,auto& b){return a.ts<b.ts;});
                sorted(ev,purchases);
            } else {
                sort_group(ev.data(),ev.data()+ev.size(),[](auto& a,auto& b){return a.ts<b.ts;});
                sorted(ev);
            }
        };
        std::vector<Source> sources(o.shards);
        Workers<Group> grouped(o.threads,Buckets<Group>(o.shards));
        parallel(o.shards,o.threads,[&](size_t s,int worker) {
            auto before=Clock::now();
            FlatMap<uint32_t,Key,UserGroupingHash> codes;
            std::vector<Key> names;
            std::vector<size_t> event_counts,click_counts,purchase_counts;
            auto person_code=[&](Key name,bool cached=false,uint32_t hash=0) {
                auto [entry,added]=cached?codes.try_emplace<true>(name,names.size(),hash):codes.try_emplace(name,names.size());
                if(added) {
                    names.push_back(name);event_counts.push_back(0);click_counts.push_back(0);
                    if constexpr(SplitPurchases)purchase_counts.push_back(0);
                }
                return entry->second;
            };
            std::vector<EventInput> ev;ev.reserve(event_bucket_size(s));
            std::vector<PurchaseInput> purchases;
            consume_bucket(routed_events,s,[&](const RoutedEvent& event) {
                if(o.mark_duplicate_events&&event.offset==CompactRaw::invalid)return;
                uint32_t offset=event.offset,purchase=event.purchase;
                const bool cached=cache_user_hashes&&(offset&route_hash_bit);
                auto code=person_code(event.person,cached,purchase);
                if(cached){offset&=~route_hash_bit;purchase=0;}
                ++event_counts[code];
                if constexpr(SplitPurchases) {
                    ev.push_back({{offset},code});
                    if(purchase) {
                        purchases.push_back({{offset,purchase},code});
                        ++purchase_counts[code];
                    }
                } else ev.push_back({{offset,purchase},code});
            },o.cache_routed_users);
            std::vector<ClickInput> cl;cl.reserve(click_bucket_size(s));
            consume_bucket(routed_clicks,s,[&](const RoutedClick& click) {
                auto code=person_code(click.person);
                ++click_counts[code];
                cl.push_back({{click.id,click.offset,click.campaign},code});
            },o.cache_routed_users);
            auto& source=sources[s];
            source.events=group_records<EventInput,GroupEvent>(ev,event_counts,[](const EventInput& row){return row.event;});
            source.clicks=group_records<ClickInput,LinkedClick>(cl,click_counts,[](const ClickInput& row){return row.click;});
            if constexpr(SplitPurchases)source.purchases=group_records<PurchaseInput,LinkedEvent>(purchases,purchase_counts,
                [](const PurchaseInput& row){return row.event;});
            std::vector<Group> isolated;
            size_t isolated_events=0;
            for(size_t i=0;i<names.size();++i) {
                Key person=names[i];
                bool independent=false;
#ifdef SHOP_SHORT_IDS
                if(o.fuse_isolated_groups&&people.inlined) {
                    if(auto* entry=people.index.find(person))person.word=entry->second;
                    else independent=true;
                } else
#endif
                person=canonical(person);
                size_t destination=person==names[i]?s:shard(person);
                Group group{person,
                    {source.events.data.get()+event_counts[i],event_counts[i+1]-event_counts[i]},
                    {source.clicks.data.get()+click_counts[i],click_counts[i+1]-click_counts[i]}};
                if constexpr(SplitPurchases)group.purchases={source.purchases.data.get()+purchase_counts[i],
                    purchase_counts[i+1]-purchase_counts[i]};
                if(independent) {
                    // An ID absent from the full link graph cannot receive
                    // events or clicks from any other source partition.
                    if(!group.events.empty()) {
                        isolated_events+=group.events.size();
                        isolated.push_back(group);
                    }
                } else grouped[worker][destination].push_back(group);
            }
            profiles[worker].intern+=elapsed(before);
            if(!isolated.empty()) {
                auto enumerate=[&](auto consume){for(const auto& group:isolated)consume_group(group,consume,worker);};
                if(specialized)aggregate_users<true>(s,worker,isolated_events,enumerate,"part-independent-");
                else aggregate_users(s,worker,isolated_events,enumerate,"part-independent-");
            }
        });
        parallel(o.shards,o.threads,[&](size_t s,int worker) {
            auto before=Clock::now();
            auto groups=gather(grouped,s);
            size_t event_count=0;
            for(const auto& group:groups)event_count+=group.events.size();
            if(groups.size()>1)duckdb_pdqsort::pdqsort(groups.begin(),groups.end(),[](auto& a,auto& b){return a.name<b.name;});
            profiles[worker].sort+=elapsed(before);
            auto enumerate=[&](auto consume) {
                std::vector<GroupEvent> merged_events;
                std::vector<LinkedClick> merged_clicks;
                std::vector<LinkedEvent> merged_purchases;
                for(size_t i=0;i<groups.size();) {
                    const auto group_start=profile_detail?Clock::now():Clock::time_point{};
                    size_t finish=i+1;
                    while(finish<groups.size()&&groups[finish].name==groups[i].name)++finish;
                    Group group=groups[i];
                    if(finish>i+1) {
                        merged_events.clear();merged_clicks.clear();
                        if constexpr(SplitPurchases)merged_purchases.clear();
                        for(size_t j=i;j<finish;++j) {
                            merged_events.insert(merged_events.end(),groups[j].events.begin(),groups[j].events.end());
                            merged_clicks.insert(merged_clicks.end(),groups[j].clicks.begin(),groups[j].clicks.end());
                            if constexpr(SplitPurchases)merged_purchases.insert(merged_purchases.end(),
                                groups[j].purchases.begin(),groups[j].purchases.end());
                        }
                        group.events=std::span(merged_events);group.clicks=std::span(merged_clicks);
                        if constexpr(SplitPurchases)group.purchases=std::span(merged_purchases);
                    }
                    if(profile_detail)profiles[worker].merge+=elapsed(group_start);
                    consume_group(group,consume,worker);
                    i=finish;
                }
            };
            if(specialized)
                aggregate_users<true>(s,worker,event_count,enumerate);
            else aggregate_users(s,worker,event_count,enumerate);
        });
    }
    template<class T,class Less>void sort_group(T* begin,T* end,Less less) {
        constexpr bool is_click=std::is_same_v<T,CompactClick>||std::is_same_v<T,LinkedClick>;
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
                if constexpr(is_click) {
                    for(T* b=begin;b<end;){T* e=b+1;while(e<end&&e->ts==b->ts)++e;if(e-b>1)duckdb_pdqsort::pdqsort(b,e,less);b=e;}
                }
            } else
#endif
            duckdb_ska_sort::ska_sort(begin,end,TimeSortKey{is_click});
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
            profiles[worker].sort+=elapsed(before);
            aggregate_users(s,worker,ev.size(),[&](auto consume) {
                size_t cb=0;
                for(size_t begin=0;begin<ev.size();) {
                    size_t finish=begin+1;
                    auto person=ev[begin].person;
                    while(finish<ev.size()&&ev[finish].person==person)++finish;
                    while(cb<cl.size()&&cl[cb].person<person)++cb;
                    size_t ce=cb;
                    while(ce<cl.size()&&cl[ce].person==person)++ce;
                    consume(name(person),ev.subspan(begin,finish-begin),cl.subspan(cb,ce-cb),time_base,0);
                    cb=ce;
                    begin=finish;
                }
            });
    }
    static std::vector<parquet::Column> user_columns(int count_type) {
        return {{"user_id",6},{"day",1,false,true},{"sessions",count_type},{"events",count_type},
                {"revenue_cents",2},{"top_category",6,true}};
    }
    void flush_user_output(size_t index,int worker,bool finish=false) {
        auto before=Clock::now();
        auto& [columns,file]=user_outputs[index];
        if(columns[0].count||(!file&&index==0&&finish)) {
            if(!file)file=std::make_unique<parquet::File>(
                (o.out/"user_daily"/("part-"+std::to_string(index)+".parquet")).string());
            file->append(columns);
            for(auto& column:columns) {
                column.values.clear();
                column.levels.clear();
                column.count=0;
            }
        }
        if(finish&&file)file->finish(columns);
        profiles[worker].parquet+=elapsed(before);
    }
    template<bool Arrays=false,class Enumerate>void aggregate_users(size_t s,int worker,size_t event_count,Enumerate enumerate,
                                                                    const char* output_prefix="part-") {
            auto before=Clock::now();
            const double previous_parquet=profiles[worker].parquet;
            const int count_type=o.narrow_count_columns&&event_count<=INT32_MAX?1:2;
            const bool batch=o.worker_parquet;
            auto shard_columns=batch?std::vector<parquet::Column>{}:user_columns(count_type);
            auto& cols=batch?user_outputs[worker].columns:shard_columns;
            CampaignWindow campaign_window((Arrays||o.array_active)?campaign_names.size():0);
            CategoryRevenue category_revenue((Arrays||o.array_categories)?categories.size():0);
            enumerate([&](Key person_name,auto ev,auto cl,int64_t time_base,int64_t click_base,
                          std::span<LinkedEvent> purchases={}) {
                using EventType=std::remove_cv_t<typename decltype(ev)::element_type>;
                constexpr bool summarized=std::is_same_v<EventType,GapSession>;
                ShortId::Buffer decoded_name;
                SV person=text(person_name,decoded_name);
                size_t hi=0,lo=0,purchase_index=0;
                struct Unused {};
                std::conditional_t<Arrays,Unused,std::unordered_map<uint32_t,size_t>> active;
                campaign_window.clear();
                int64_t day=-1,ns=0,ne=0,nr=0;
                std::conditional_t<Arrays,Unused,std::unordered_map<uint32_t,int64_t>> cat_revenue;
                Aggregate* day_totals=nullptr;
                uint32_t* day_assists=nullptr;
                auto totals_for=[&](uint32_t campaign)->Aggregate& {
                    if constexpr(Arrays)return day_totals[campaign];
                    else return total(worker,day,campaign);
                };
                auto flush=[&]{
                    if(!ns)return;
                    if(profile_detail) {
                        profiles[worker].sessions+=ns;
                        if(cl.empty())profiles[worker].no_click_sessions+=ns;
                    }
                    cols[0].text(person);cols[1].number(day);cols[2].number(ns);cols[3].number(ne);cols[4].number(nr);
                    if(Arrays||o.array_categories) {
                        if(category_revenue.present.empty())cols[5].null();
                        else cols[5].text(categories[category_revenue.top(categories)]);
                    } else if constexpr(!Arrays) {
                        if(cat_revenue.empty())cols[5].null();
                        else {uint32_t top=0;int64_t amount=-1;for(auto [c,r]:cat_revenue)if(r>amount||(r==amount&&categories[c]<categories[top])){top=c;amount=r;}cols[5].text(categories[top]);}
                    }
                    ns=ne=nr=0;
                    if constexpr(!Arrays)cat_revenue.clear();
                    category_revenue.clear();
                    constexpr size_t column_target=8u<<20;
                    if(batch&&(cols[0].values.size()>=column_target||cols[5].values.size()>=column_target))
                        flush_user_output(worker,worker);
                };
                for(size_t i=0;i<ev.size();) {
                    const int64_t st=time_base+ev[i].ts,sd=dayof(st);
                    if(sd!=day) {
                        flush();day=sd;
                        if constexpr(Arrays) {
                            size_t offset=size_t(day-dayof(start))*campaign_names.size();
                            day_totals=dense_totals[worker].data()+offset;
                            if(!dense_assists.empty())day_assists=dense_assists[worker].data()+offset;
                        }
                    }
                    size_t j=i+1,session_events;
                    if constexpr(summarized)session_events=ev[i].count;
                    else {
                        while(j<ev.size()&&ev[j].ts-ev[j-1].ts<=GAP)++j;
                        session_events=j-i;
                    }
                    int64_t revenue=0;
                    auto add_purchase=[&](uint32_t price,uint32_t category) {
                        if(!price)return;
                        revenue+=price;
                        if(Arrays||o.array_categories)category_revenue.add(category,price);
                        else if constexpr(!Arrays)cat_revenue[category]+=price;
                    };
                    if constexpr(summarized||std::is_same_v<EventType,LinkedTimestamp>) {
                        // Every purchase still has a timestamp in ev. The
                        // session's last event bounds its purchase stream.
                        uint32_t last;
                        if constexpr(summarized)last=ev[i].last;
                        else last=ev[j-1].ts;
                        while(purchase_index<purchases.size()&&purchases[purchase_index].ts<=last) {
                            uint32_t packed=purchases[purchase_index++].purchase;
                            add_purchase(packed&CompactRaw::price_mask,packed>>24);
                        }
                    } else for(size_t k=i;k<j;++k) {
                        uint32_t price,category;
                        if constexpr(std::is_same_v<EventType,LinkedEvent>) {
                            price=ev[k].purchase&CompactRaw::price_mask;
                            category=ev[k].purchase>>24;
                        } else {
                            price=ev[k].price;
                            category=ev[k].category;
                        }
                        add_purchase(price,category);
                    }
                    ++ns;ne+=session_events;nr+=revenue;
                    uint32_t credited=0;
                    const int64_t click_at=st-click_base;
                    if(!Arrays&&o.baseline) {
                        // Intentionally simple oracle: rescan clicks per session.
                        std::unordered_map<uint32_t,bool> assisted;
                        for(size_t k=0;k<cl.size();++k)if(cl[k].ts<=click_at&&cl[k].ts>=click_at-WEEK){credited=cl[k].campaign;assisted[credited]=true;}
                        for(auto [cam,unused]:assisted)++totals_for(cam).assisted;
                    } else if(Arrays||o.array_active) {
                        while(hi<cl.size()&&cl[hi].ts<=click_at){campaign_window.add(cl[hi].campaign);++hi;}
                        while(lo<hi&&cl[lo].ts<click_at-WEEK){campaign_window.remove(cl[lo].campaign);++lo;}
                        if(hi>lo)credited=cl[hi-1].campaign;
                        if(day_assists)for(auto cam:campaign_window.present)++day_assists[cam];
                        else for(auto cam:campaign_window.present)++totals_for(cam).assisted;
                    } else if constexpr(!Arrays) {
                        while(hi<cl.size()&&cl[hi].ts<=click_at){++active[cl[hi].campaign];++hi;}
                        while(lo<hi&&cl[lo].ts<click_at-WEEK){auto it=active.find(cl[lo].campaign);if(!--it->second)active.erase(it);++lo;}
                        if(hi>lo)credited=cl[hi-1].campaign;
                        for(auto [cam,count]:active)++totals_for(cam).assisted;
                    }
                    auto& credited_total=totals_for(credited);++credited_total.sessions;credited_total.purchasing+=revenue>0;credited_total.revenue+=revenue;
                    i=j;
                }
                flush();
            });
            profiles[worker].aggregate+=elapsed(before)-(profiles[worker].parquet-previous_parquet);
            if(!batch) {
                before=Clock::now();
                if(cols[0].count||s==0)parquet::write((o.out/"user_daily"/(output_prefix+std::to_string(s)+".parquet")).string(),cols);
                profiles[worker].parquet+=elapsed(before);
            }
    }
    void campaigns_output() {
        CampaignTotals all;
        if(o.dense_totals) {
            for(size_t i=0;i<dense_totals[0].size();++i) {
                Aggregate a;
                for(size_t worker=0;worker<dense_totals.size();++worker) {
                    const auto& t=dense_totals[worker][i];
                    a.sessions+=t.sessions;a.purchasing+=t.purchasing;a.revenue+=t.revenue;a.assisted+=t.assisted;
                    if(!dense_assists.empty())a.assisted+=dense_assists[worker][i];
                }
                if(a.sessions||a.assisted)all[{dayof(start)+i/campaign_names.size(),i%campaign_names.size()}]=a;
            }
        }
        for(auto& worker:totals)for(auto [key,t]:worker){auto& a=all[key];a.sessions+=t.sessions;a.purchasing+=t.purchasing;a.revenue+=t.revenue;a.assisted+=t.assisted;}
        using parquet::Column;
        const bool counts_fit=o.narrow_count_columns&&std::all_of(all.begin(),all.end(),[](const auto& entry) {
            const auto& t=entry.second;
            return t.sessions<=INT32_MAX&&t.purchasing<=INT32_MAX&&t.assisted<=INT32_MAX;
        });
        const int count_type=counts_fit?1:2;
        std::vector<Column> cols={{"campaign_id",6},{"channel",6},{"day",1,false,true},{"sessions",count_type},
                                  {"purchasing_sessions",count_type},{"revenue_cents",2},{"assisted_sessions",count_type}};
        for(auto [key,t]:all){auto [day,cam]=key;cols[0].text(campaign_names[cam]);cols[1].text(channels[cam]);cols[2].number(day);cols[3].number(t.sessions);cols[4].number(t.purchasing);cols[5].number(t.revenue);cols[6].number(t.assisted);}
        parquet::write((o.out/"campaign_daily"/"part-0.parquet").string(),cols);
    }
    template<class R> void ingest_and_dedup(Timer& timer) {
        std::unique_ptr<CachedEventStorage> indirect_owner;
        CachedEventBuckets* indirect=nullptr;
#ifdef SHOP_SHORT_IDS
        if constexpr(std::is_same_v<R,NarrowRawLocation>) {
            if(o.cache_indirect_events) {
#ifndef SHOP_VECTOR_INDIRECT_EVENTS
                if(o.raw_arena) {
                    size_t bytes=0;
                    for(auto chunk:in.events)bytes+=chunk.end-chunk.begin;
                    try {
                        indirect_owner=std::make_unique<CachedEventStorage>(o.threads,o.ingest_shards,bytes/8);
                        indirect=&indirect_owner->buckets;
                    } catch(const std::bad_alloc&) {
                        // The original exact path remains available.
                    }
                }
#else
                indirect_owner=std::make_unique<CachedEventStorage>(o.threads,Buckets<CachedRawLocation>(o.ingest_shards));
                indirect=indirect_owner.get();
#endif
            }
        }
#endif
        auto process_raw=[&](auto& raw) {
            if constexpr(is_location_record<R>) {
                if(const auto* mode=std::getenv("SHOP_INGEST_DAG");mode&&o.redirect_linked_groups&&o.threads>1) {
                    ingest_dag<R>(raw,indirect,SV(mode)=="tail");
                    clicks_parsed=true;
                    timer.mark("ingest_dag");
                    return;
                }
            }
            parse_events<R>(raw,indirect);
            timer.mark(o.overlap_links?"parse_events_and_links":"parse_events");
            if constexpr(is_location_record<R>) {
                if(std::getenv("SHOP_FUSE_DEDUP_CLICKS")) {
                    const size_t raw_jobs=o.ingest_shards;
                    const size_t mapped_jobs=indirect?raw_jobs:0;
                    parallel(raw_jobs+mapped_jobs+in.clicks.size(),o.threads,[&](size_t job,int worker) {
                        if(job<raw_jobs)mark_duplicate_shard<R>(raw,job);
                        else if(job<raw_jobs+mapped_jobs)mark_duplicate_shard<CachedRawLocation>(*indirect,job-raw_jobs);
                        else parse_click_chunk(job-raw_jobs-mapped_jobs,worker);
                    });
                    clicks_parsed=true;
                    timer.mark("dedup_and_clicks");
                    return;
                }
            }
            if constexpr(is_location_record<R>)mark_duplicate_locations<R>(raw);
            else dedup<R>(raw);
#ifdef SHOP_SHORT_IDS
            if constexpr(std::is_same_v<R,NarrowRawLocation>) {
                if(indirect)mark_duplicate_locations<CachedRawLocation>(*indirect);
            }
#endif
            timer.mark("dedup");
        };
        if(o.raw_arena&&!o.baseline&&!in.events.empty()) {
            size_t bytes=0;
            for(auto chunk:in.events)bytes+=chunk.end-chunk.begin;
            std::unique_ptr<IngestArena<R>> arena;
            try {arena=std::make_unique<IngestArena<R>>(o.threads,o.ingest_shards,bytes);}
            catch(const std::bad_alloc&) {} // Keep the ordinary allocation path available.
            if(arena){process_raw(arena->buckets);return;}
        }
        Workers<R> raw(o.threads,Buckets<R>(o.ingest_shards));
        process_raw(raw);
    }
    void run(){
        Timer t(o.threads);
        load();t.mark(o.overlap_links?"load_inputs":"load_links");
        // Reserve one offset as an invalid-row marker and retain a wide path
        // for long windows or product dictionaries with more categories.
        bool narrow_raw=o.compact_raw&&uint64_t(end-start)<=UINT32_MAX&&categories.size()<=256;
        o.mark_duplicate_events=o.mark_duplicate_events&&narrow_raw&&o.redirect_linked_groups&&
            o.sparse_duplicates&&o.dedup=="flat";
        // The tagged offset must leave UINT32_MAX available for duplicate losers.
        cache_user_hashes=o.cache_routed_users&&o.mark_duplicate_events&&uint64_t(end-start)<route_hash_bit;
        if(narrow_raw) {
            if(o.mark_duplicate_events&&o.narrow_duplicate_locations&&!o.ordered_ingest&&o.threads<=8) {
                location_codec=std::make_unique<LocationCodec>(start-WEEK,o.threads,o.shards);
                ingest_and_dedup<NarrowRawLocation>(t);
                location_codec.reset();
            } else if(o.mark_duplicate_events)ingest_and_dedup<RawLocation>(t);
            else ingest_and_dedup<CompactRaw>(t);
        } else {
            ingest_and_dedup<Raw>(t);
        }
        if(!clicks_parsed){parse_clicks();t.mark("parse_clicks");}
        if(o.redirect_linked_groups) {
            if(o.split_group_purchases)sessionize_linked_groups<true>();
            else sessionize_linked_groups();
        }
        else sessionize();
        if(o.worker_parquet)parallel(user_outputs.size(),o.threads,[&](size_t index,int worker){
            flush_user_output(index,worker,true);
        });
        report_session_profile();
        t.mark("sessions_and_user_output");
        campaigns_output();t.mark("campaign_output");
    }
};
int main(int argc,char** argv){try{Pipeline(options(argc,argv)).run();return 0;}catch(const std::exception& e){std::cerr<<"shopping: "<<e.what()<<'\n';return 1;}}
