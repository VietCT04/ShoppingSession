#include "core.hpp"
#include "parquet.hpp"
#include "flat_map.hpp"
#include "sort.hpp"
#include <cstdlib>

using namespace shop;
struct Raw {SV id,person;int64_t ts,ingest;uint32_t price,category;};
struct Event {SV person;int64_t ts;uint32_t price,category;};
struct Click {SV person,id;int64_t ts;uint32_t campaign;};
struct CompactClick {SV id;int64_t ts;uint32_t campaign,person;};
struct Aggregate {int64_t sessions=0,purchasing=0,revenue=0,assisted=0;};
struct WorkProfile {double gather=0,intern=0,sort=0,aggregate=0,parquet=0;};
using CampaignTotals=std::map<std::pair<int64_t,uint32_t>,Aggregate>;
template<class T> using Buckets=std::vector<std::vector<T>>;
template<class T> using Workers=std::vector<Buckets<T>>;

struct Options {
    fs::path data="/data",out="/output";
    int threads=8,shards=256;
    bool baseline=false;
    bool compact=true;
    bool flat_links=false;
    bool dense_totals=true;
    std::string dedup="flat";
    std::string sorter="duckdb";
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
        else if(arg=="--threads"&&i+1<argc)o.threads=std::stoi(argv[++i]);
        else if(arg=="--shards"&&i+1<argc)o.shards=std::stoi(argv[++i]);
        else if(arg=="--dedup"&&i+1<argc)o.dedup=argv[++i];
        else if(arg=="--sorter"&&i+1<argc)o.sorter=argv[++i];
        else if(arg=="--help") {std::cout<<"shopping [DATA OUTPUT] [--baseline] [--threads N] [--shards N] [--dedup sort|hash|flat] [--compact|--no-compact] [--sorter std|duckdb]\n";std::exit(0);}
        else if(arg.starts_with("--"))throw std::runtime_error("unknown option "+arg);
        else if(positional++==0)o.data=arg;
        else if(positional==2)o.out=arg;
        else throw std::runtime_error("too many paths");
    }
    if(o.baseline){o.threads=1;o.shards=1;o.dedup="sort";o.compact=false;o.flat_links=false;o.dense_totals=false;o.sorter="std";}
    if(o.threads<1||o.threads>256||o.shards<1||o.shards>4096||(o.shards&(o.shards-1)))throw std::runtime_error("threads must be 1..256; shards a power of two in 1..4096");
    if(o.dedup!="sort"&&o.dedup!="hash"&&o.dedup!="flat")throw std::runtime_error("dedup must be sort, hash, or flat");
    if(o.sorter!="std"&&o.sorter!="duckdb")throw std::runtime_error("sorter must be std or duckdb");
    return o;
}
struct Pipeline {
    Options o;
    Inputs in;
    People people;
    FlatMap<uint32_t> person_lookup;
    Dict<uint32_t> products,campaigns,category_index;
    std::vector<SV> categories={"unknown"},campaign_names={"organic"},channels={"organic"};
    int64_t start,end;
    Workers<Raw> raw;
    Workers<Event> events;
    Workers<Click> clicks;
    std::vector<CampaignTotals> totals;
    std::vector<std::vector<Aggregate>> dense_totals;
    std::vector<WorkProfile> profiles;
    explicit Pipeline(Options options):o(std::move(options)),raw(o.threads,Buckets<Raw>(o.shards)),
        events(o.threads,Buckets<Event>(o.shards)),clicks(o.threads,Buckets<Click>(o.shards)),totals(o.threads),profiles(o.threads){}
    using Clock=std::chrono::steady_clock;
    static double elapsed(Clock::time_point t){return std::chrono::duration<double>(Clock::now()-t).count();}
    size_t shard(SV s)const{return Hash{}(s)&(o.shards-1);}
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
        for(auto chunk:in.links)Inputs::lines(chunk,[&](SV line){
            std::array<SV,2> f;if(csv(line,f)&&!f[0].empty()&&!f[1].empty())people.join(f[0],f[1]);
        });
        people.finish();
        if(o.flat_links) {
            person_lookup.reserve(people.index.size());
            for(auto [name,root]:people.index)person_lookup.try_emplace(name,root);
            Dict<uint32_t>().swap(people.index);
            std::vector<uint32_t>().swap(people.parents);std::vector<uint32_t>().swap(people.sizes);
        }
        for(auto p:files(o.data/"events"))in.add(p,in.events);
        for(auto p:files(o.data/"clicks"))in.add(p,in.clicks);
        fs::create_directories(o.out/"user_daily");fs::create_directories(o.out/"campaign_daily");
        // Replace only our table outputs, so rerunning never leaves extra rows.
        for(auto table:{"user_daily","campaign_daily"})for(auto p:files_parquet(o.out/table))fs::remove(p);
    }
    static std::vector<fs::path> files_parquet(const fs::path& dir) {
        std::vector<fs::path> r;for(auto& p:fs::directory_iterator(dir))if(p.is_regular_file()&&p.path().extension()==".parquet")r.push_back(p.path());return r;
    }
    SV canonical(SV s)const {
        if(!o.flat_links)return people.person(s);
        auto it=person_lookup.find(s);return it==person_lookup.end()?s:people.names[it->second];
    }
    Aggregate& total(int worker,int64_t day,uint32_t campaign) {
        if(o.dense_totals)return dense_totals[worker][size_t(day-dayof(start))*campaign_names.size()+campaign];
        return totals[worker][{day,campaign}];
    }
    void parse_events() {
        parallel(in.events.size(),o.threads,[&](size_t i,int worker){
            Inputs::lines(in.events[i],[&](SV line){
                std::array<SV,7> f;if(!csv(line,f)||f[0].empty())return;
                // Invalid winners remain in the dedup input. A later valid copy
                // must never rescue an invalid earliest copy.
                int64_t ts=timestamp(f[2]),ingest=timestamp(f[3]);
                uint64_t price=0;uint32_t category=0;
                bool purchase=f[4]=="purchase";
                bool valid=!f[1].empty()&&!f[5].empty()&&(purchase||f[4]=="view"||f[4]=="cart")&&
                    ts>=start&&ts<end&&ingest-ts<=DAY;
                if(purchase) {
                    valid=valid&&digits(f[6],price,10000000)&&price>0;
                    if(valid){auto it=products.find(f[5]);if(it!=products.end())category=it->second;}
                }
                if(!valid)ts=-1;
                raw[worker][shard(f[0])].push_back({f[0],canonical(f[1]),ts,ingest,uint32_t(price),category});
            });
        });
    }
    void dedup() {
        parallel(o.shards,o.threads,[&](size_t s,int worker){
            size_t count=0;for(auto& w:raw)count+=w[s].size();
            std::vector<Raw> rows;rows.reserve(count);
            for(auto& w:raw){rows.insert(rows.end(),w[s].begin(),w[s].end());std::vector<Raw>().swap(w[s]);}
            auto emit=[&](const Raw& r){if(r.ts>=start)events[worker][shard(r.person)].push_back({r.person,r.ts,r.price,r.category});};
            if(o.dedup=="flat") {
                FlatMap<size_t> winners;winners.reserve(rows.size());
                for(size_t i=0;i<rows.size();++i){auto [it,added]=winners.try_emplace(rows[i].id,i);if(!added&&rows[i].ingest<rows[it->second].ingest)it->second=i;}
                winners.each([&](SV,size_t i){emit(rows[i]);});
            } else if(o.dedup=="hash") {
                Dict<size_t> winners;winners.reserve(rows.size());
                for(size_t i=0;i<rows.size();++i){auto [it,added]=winners.try_emplace(rows[i].id,i);if(!added&&rows[i].ingest<rows[it->second].ingest)it->second=i;}
                for(auto [id,i]:winners)emit(rows[i]);
            } else {
                std::sort(rows.begin(),rows.end(),[](const Raw& a,const Raw& b){return a.id!=b.id?a.id<b.id:a.ingest<b.ingest;});
                for(size_t i=0;i<rows.size();){emit(rows[i]);size_t j=i+1;while(j<rows.size()&&rows[j].id==rows[i].id)++j;i=j;}
            }
        });
    }
    void parse_clicks() {
        parallel(in.clicks.size(),o.threads,[&](size_t i,int worker){
            Inputs::lines(in.clicks[i],[&](SV line){
                std::array<SV,4> f;if(!csv(line,f)||f[0].empty()||f[1].empty())return;
                auto it=campaigns.find(f[2]);if(it==campaigns.end())return;
                auto ts=timestamp(f[3]);if(ts<start-WEEK||ts>=end)return;
                auto p=canonical(f[1]);clicks[worker][shard(p)].push_back({p,f[0],ts,it->second});
            });
        });
        // Identical duplicate clicks need no materialized dedup: they cannot
        // change the winning (timestamp,id), nor campaign membership in a window.
    }
    template<class T> static std::vector<T> gather(Workers<T>& workers,size_t s) {
        size_t n=0;for(auto& w:workers)n+=w[s].size();std::vector<T> r;r.reserve(n);
        for(auto& w:workers){r.insert(r.end(),w[s].begin(),w[s].end());std::vector<T>().swap(w[s]);}return r;
    }
    void sessionize() {
        parallel(o.shards,o.threads,[&](size_t s,int worker){
            auto before=Clock::now();
            auto ev=gather(events,s);auto cl=gather(clicks,s);
            profiles[worker].gather+=elapsed(before);before=Clock::now();
            if(o.compact) {
                FlatMap<uint32_t> codes;
                std::vector<SV> names;
                std::vector<CompactEvent> compact_events;compact_events.reserve(ev.size());
                for(auto& e:ev) {
                    auto [it,added]=codes.try_emplace(e.person,names.size());
                    if(added)names.push_back(e.person);
                    compact_events.push_back({e.ts,e.price,e.category,it->second});
                }
                std::vector<Event>().swap(ev);
                std::vector<CompactClick> compact_clicks;compact_clicks.reserve(cl.size());
                for(auto& c:cl) {
                    auto it=codes.find(c.person);
                    if(it!=codes.end())compact_clicks.push_back({c.id,c.ts,c.campaign,it->second});
                }
                std::vector<Click>().swap(cl);
                profiles[worker].intern+=elapsed(before);
                process(s,worker,compact_events,compact_clicks,[&](uint32_t code){return names[code];});
            } else process(s,worker,ev,cl,[](SV person){return person;});
        });
        WorkProfile sum;
        for(auto p:profiles){sum.gather+=p.gather;sum.intern+=p.intern;sum.sort+=p.sort;sum.aggregate+=p.aggregate;sum.parquet+=p.parquet;}
        std::cerr<<"session_work_seconds gather="<<sum.gather<<" intern="<<sum.intern<<" sort="<<sum.sort<<" aggregate="<<sum.aggregate<<" parquet="<<sum.parquet<<'\n';
    }
    template<class E,class C,class Name> void process(size_t s,int worker,std::vector<E>& ev,std::vector<C>& cl,Name name) {
            auto before=Clock::now();
            auto event_less=[](auto& a,auto& b){return a.person!=b.person?a.person<b.person:a.ts<b.ts;};
            if(o.sorter=="duckdb") {
                if constexpr(std::is_same_v<E,CompactEvent>) {
                    duckdb_sort_events(ev,start,end);
                } else duckdb_pdqsort::pdqsort(ev.begin(),ev.end(),event_less);
            } else std::sort(ev.begin(),ev.end(),event_less);
            // Within a timestamp, reverse click_id order puts the winning ID last.
            auto click_less=[](auto& a,auto& b){return a.person!=b.person?a.person<b.person:(a.ts!=b.ts?a.ts<b.ts:a.id>b.id);};
            if(o.sorter=="duckdb")duckdb_pdqsort::pdqsort(cl.begin(),cl.end(),click_less);
            else std::sort(cl.begin(),cl.end(),click_less);
            profiles[worker].sort+=elapsed(before);before=Clock::now();
            using parquet::Column;
            std::vector<Column> cols={{"user_id",6},{"day",1,false,true},{"sessions",2},{"events",2},{"revenue_cents",2},{"top_category",6,true}};
            size_t cb=0;
            for(size_t begin=0;begin<ev.size();) {
                size_t finish=begin+1;auto person_key=ev[begin].person;SV person=name(person_key);
                while(finish<ev.size()&&ev[finish].person==person_key)++finish;
                while(cb<cl.size()&&cl[cb].person<person_key)++cb;
                size_t ce=cb;while(ce<cl.size()&&cl[ce].person==person_key)++ce;
                size_t hi=cb,lo=cb;
                std::unordered_map<uint32_t,size_t> active;
                int64_t day=-1,ns=0,ne=0,nr=0;
                std::unordered_map<uint32_t,int64_t> cat_revenue;
                auto flush=[&]{
                    if(!ns)return;
                    cols[0].text(person);cols[1].number(day);cols[2].number(ns);cols[3].number(ne);cols[4].number(nr);
                    if(cat_revenue.empty())cols[5].null();
                    else {uint32_t top=0;int64_t amount=-1;for(auto [c,r]:cat_revenue)if(r>amount||(r==amount&&categories[c]<categories[top])){top=c;amount=r;}cols[5].text(categories[top]);}
                    ns=ne=nr=0;cat_revenue.clear();
                };
                for(size_t i=begin;i<finish;) {
                    const int64_t st=ev[i].ts,sd=dayof(st);
                    if(sd!=day){flush();day=sd;}
                    size_t j=i+1;while(j<finish&&ev[j].ts-ev[j-1].ts<=GAP)++j;
                    int64_t revenue=0;
                    for(size_t k=i;k<j;++k)if(ev[k].price){revenue+=ev[k].price;cat_revenue[ev[k].category]+=ev[k].price;}
                    ++ns;ne+=j-i;nr+=revenue;
                    uint32_t credited=0;
                    if(o.baseline) {
                        // Intentionally simple oracle: rescan clicks per session.
                        std::unordered_map<uint32_t,bool> assisted;
                        for(size_t k=cb;k<ce;++k)if(cl[k].ts<=st&&cl[k].ts>=st-WEEK){credited=cl[k].campaign;assisted[credited]=true;}
                        for(auto [cam,unused]:assisted)++total(worker,sd,cam).assisted;
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
    void run(){Timer t;load();t.mark("load_links");parse_events();t.mark("parse_events");dedup();t.mark("dedup");parse_clicks();t.mark("parse_clicks");sessionize();t.mark("sessions_and_user_output");campaigns_output();t.mark("campaign_output");}
};
int main(int argc,char** argv){try{Pipeline(options(argc,argv)).run();return 0;}catch(const std::exception& e){std::cerr<<"shopping: "<<e.what()<<'\n';return 1;}}
