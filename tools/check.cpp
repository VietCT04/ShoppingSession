#include "duckdb.h"
#include "../native/core.hpp"
#include <cstdlib>
#include <iomanip>
#include <sys/resource.h>
#include <sys/wait.h>
using namespace shop;
struct Database {
    duckdb_database db=nullptr;
    duckdb_connection con=nullptr;
    Database(){if(duckdb_open(nullptr,&db)||duckdb_connect(db,&con))throw std::runtime_error("cannot open validator database");query("SET threads=4");query("SET memory_limit='1GB'");}
    ~Database(){duckdb_disconnect(&con);duckdb_close(&db);}
    int64_t query(const std::string& sql) {
        duckdb_result r{};
        if(duckdb_query(con,sql.c_str(),&r)) {std::string error=duckdb_result_error(&r);duckdb_destroy_result(&r);throw std::runtime_error(error+"\nSQL: "+sql);}
        int64_t v=duckdb_row_count(&r)?duckdb_value_int64(&r,0,0):0;duckdb_destroy_result(&r);return v;
    }
};
std::string quote(std::string s){std::string r="'";for(char c:s){r+=c;if(c=='\'')r+=c;}return r+"'";}
std::string json_string(SV s) {
    std::string r="\"";
    const char* hex="0123456789abcdef";
    for(unsigned char c:s) {
        if(c=='"'||c=='\\'){r+='\\';r+=c;}
        else if(c<32){r+="\\u00";r+=hex[c>>4];r+=hex[c&15];}
        else r+=c;
    }
    return r+'"';
}
std::string source(fs::path root,std::string table) {
    auto csv=root/(table+".csv");
    if(fs::exists(csv)) {
        std::string types=table=="user_daily"?
            "{'user_id':'VARCHAR','day':'DATE','sessions':'BIGINT','events':'BIGINT','revenue_cents':'BIGINT','top_category':'VARCHAR'}":
            "{'campaign_id':'VARCHAR','channel':'VARCHAR','day':'DATE','sessions':'BIGINT','purchasing_sessions':'BIGINT','revenue_cents':'BIGINT','assisted_sessions':'BIGINT'}";
        return "read_csv("+quote(csv.string())+", header=true, columns="+types+")";
    }
    return "read_parquet("+quote((root/table/"*.parquet").string())+")";
}
void compare(Database& db,const fs::path& a,const fs::path& b) {
    for(std::string table:{"user_daily","campaign_daily"}) {
        auto left=source(a,table),right=source(b,table);
        std::string cols=table=="user_daily"?"user_id,day,sessions,events,revenue_cents,top_category":"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions";
        // BY NAME and explicit projection reject missing columns; schema checks
        // below also reject extras and a string or timestamp masquerading as DATE.
        int64_t count=db.query("SELECT count(*) FROM (DESCRIBE SELECT * FROM "+left+")");
        if(count!=(table=="user_daily"?6:7))throw std::runtime_error(table+": wrong column count");
        int64_t date=db.query("SELECT count(*) FROM (DESCRIBE SELECT * FROM "+left+") WHERE column_name='day' AND column_type='DATE'");
        if(date!=1)throw std::runtime_error(table+": day must be Parquet DATE");
        int64_t different=db.query("SELECT count(*) FROM ((SELECT "+cols+" FROM "+left+" EXCEPT ALL SELECT "+cols+" FROM "+right+") UNION ALL (SELECT "+cols+" FROM "+right+" EXCEPT ALL SELECT "+cols+" FROM "+left+"))");
        if(different)throw std::runtime_error(table+": "+std::to_string(different)+" differing rows");
        auto key=table=="user_daily"?"user_id,day":"campaign_id,day";
        if(db.query("SELECT count(*) FROM (SELECT "+std::string(key)+",count(*) n FROM "+left+" GROUP BY ALL HAVING n<>1)"))throw std::runtime_error(table+": duplicate keys");
    }
}
struct Run {double seconds;long rss;};
Run run(std::vector<std::string> args,const fs::path& log) {
    auto start=std::chrono::steady_clock::now();
    pid_t pid=fork();if(pid<0)throw std::runtime_error("fork failed");
    if(pid==0) {
        int fd=open(log.c_str(),O_WRONLY|O_CREAT|O_TRUNC,0644);if(fd<0)_exit(126);
        dup2(fd,STDERR_FILENO);close(fd);
        std::vector<char*> argv;for(auto& a:args)argv.push_back(a.data());argv.push_back(nullptr);
        execvp(argv[0],argv.data());_exit(127);
    }
    int status=0;struct rusage usage{};
    if(wait4(pid,&status,0,&usage)<0)throw std::runtime_error("wait4 failed");
    double seconds=std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count();
    if(!WIFEXITED(status)||WEXITSTATUS(status)) {
        std::ifstream file(log);std::string output((std::istreambuf_iterator<char>(file)),{});
        if(output.size()>4000)output.erase(0,output.size()-4000);
        throw std::runtime_error("pipeline failed; see "+log.string()+"\n"+output);
    }
    return {seconds,usage.ru_maxrss};
}
int main(int argc,char** argv) {
    try {
        if(argc<2)throw std::runtime_error("usage: check compare OUT EXPECTED | fixtures [BINARY] | bench DATA EXPECTED [REPEATS=3]");
        std::string mode=argv[1];Database db;
        if(mode=="compare"&&argc==4){compare(db,argv[2],argv[3]);std::cout<<"MATCH\n";}
        else if(mode=="fixtures") {
            fs::path fixture_output=std::getenv("FIXTURE_SINGLE")?"build/production-fixture-output":"build/fixture-output";
            fs::create_directories(fixture_output);int cases=0;
            std::vector<fs::path> paths;for(auto& p:fs::directory_iterator("fixtures"))if(p.is_directory())paths.push_back(p.path());std::sort(paths.begin(),paths.end());
            std::vector<std::string> fixture_configs={"baseline","sort","hash","flat","hash-compact","flat-compact","hash-duckdb","hash-compact-duckdb","hash-compact-duckdb-frozen","hash-compact-duckdb-dense","hash-person-dense","dense-person-dense","flat-person-dense-fused","flat-person-dense-ordered","flat-person-local","flat-person-local-active","flat-person-local-categories","flat-person-local-arrays","flat-person-local-arrays-time"};
            if(std::getenv("FIXTURE_SINGLE"))fixture_configs={"production"};
            for(const auto& p:paths)for(std::string config:fixture_configs) {
                std::vector<std::string> args={argc>2?argv[2]:"build/shopping",(p/"input").string(),fixture_output.string()};
                if(config=="baseline")args.push_back("--baseline");else if(config!="production") {
                    args.insert(args.end(),{"--wide-events","--no-fast-time","--no-compact","--no-array-active","--no-array-categories","--dedup-shards","0","--no-fused-csv","--no-group-person","--no-dense-totals","--sorter","std"});
                    args.push_back("--dedup");args.push_back(config.substr(0,config.find('-')));
                    if(config.find("compact")!=std::string::npos)args.push_back("--compact");
                    if(config.find("duckdb")!=std::string::npos){args.push_back("--sorter");args.push_back("duckdb");}
                    if(config.ends_with("-frozen"))args.push_back("--flat-links");
                    if(config.ends_with("-dense"))args.push_back("--dense-totals");
                    if(config.find("person")!=std::string::npos)args.push_back("--group-person");
                    if(config.ends_with("-fused")){args.push_back("--fused-csv");args.push_back("--dense-totals");}
                    if(config.ends_with("-ordered")){args.push_back("--fused-csv");args.push_back("--ordered-ingest");args.push_back("--dense-totals");}
                    if(config.find("-local")!=std::string::npos){args.push_back("--fused-csv");args.push_back("--dense-totals");args.push_back("--dedup-shards");args.push_back("4096");}
                    if(config.ends_with("-active")||config.find("-arrays")!=std::string::npos)args.push_back("--array-active");
                    if(config.ends_with("-categories")||config.find("-arrays")!=std::string::npos)args.push_back("--array-categories");
                    if(config.ends_with("-time"))args.push_back("--fast-time");
                }
                run(args,"build/fixture.log");compare(db,fixture_output,p/"expected");
                std::cout<<"PASS "<<p.filename().string()<<" "<<config<<'\n';++cases;
            }
            std::cout<<cases<<" fixture/config checks passed\n";
        } else if(mode=="bench"&&argc>=4) {
            int repeats=argc>4?std::stoi(argv[4]):3;
            if(repeats<1)throw std::runtime_error("repeats must be positive");
            fs::create_directories("benchmarks");
            fs::path out="build/bench-output";
            struct Config{std::string name;std::vector<std::string> flags;};
            std::vector<Config> configs={
                {"baseline",{"--baseline"}},
                {"sort-8-64",{"--threads","8","--shards","64","--dedup","sort"}},
                {"hash-8-64",{"--threads","8","--shards","64","--dedup","hash"}},
                {"hash-8-256",{"--threads","8","--shards","256","--dedup","hash"}},
                {"hash-4-64",{"--threads","4","--shards","64","--dedup","hash"}},
                {"flat-8-64",{"--threads","8","--shards","64","--dedup","flat"}},
                {"flat-compact-8-64",{"--threads","8","--shards","64","--dedup","flat","--compact"}},
                {"flat-compact-8-256",{"--threads","8","--shards","256","--dedup","flat","--compact"}}
            };
            configs.push_back({"duckdb-8-256",{"--threads","8","--shards","256","--dedup","hash","--sorter","duckdb"}});
            configs.push_back({"duckdb-compact-8-256",{"--threads","8","--shards","256","--dedup","hash","--sorter","duckdb","--compact"}});
            configs.push_back({"duckdb-frozen-8-256",{"--threads","8","--shards","256","--dedup","hash","--sorter","duckdb","--compact","--flat-links"}});
            configs.push_back({"duckdb-flat-8-256",{"--threads","8","--shards","256","--dedup","flat","--sorter","duckdb","--compact"}});
            configs.push_back({"duckdb-flat-8-1024",{"--threads","8","--shards","1024","--dedup","flat","--sorter","duckdb","--compact"}});
            configs.push_back({"duckdb-dense-8-256",{"--threads","8","--shards","256","--dedup","hash","--sorter","duckdb","--compact","--dense-totals"}});
            configs.push_back({"duckdb-flat-dense-8-256",{"--threads","8","--shards","256","--dedup","flat","--sorter","duckdb","--compact","--dense-totals"}});
            configs.push_back({"packed-8-256",{"--threads","8","--shards","256","--dedup","flat","--sorter","duckdb","--no-compact","--dense-totals"}});
            configs.push_back({"person-8-256",{"--threads","8","--shards","256","--dedup","flat","--sorter","duckdb","--group-person","--dense-totals"}});
            configs.push_back({"person-dense-8-256",{"--threads","8","--shards","256","--dedup","dense","--sorter","duckdb","--group-person","--dense-totals"}});
            configs.push_back({"person-ips4o-8-256",{"--threads","8","--shards","256","--dedup","flat","--group-person","--dense-totals","--group-sort","ips4o"}});
            configs.push_back({"person-ips2ra-8-256",{"--threads","8","--shards","256","--dedup","flat","--group-person","--dense-totals","--group-sort","ips2ra"}});
            configs.push_back({"person-fused-8-256",{"--threads","8","--shards","256","--dedup","flat","--group-person","--dense-totals","--fused-csv"}});
            configs.push_back({"person-ingest-8-256",{"--threads","8","--shards","256","--dedup","flat","--group-person","--dense-totals","--fused-csv","--ordered-ingest"}});
            configs.push_back({"dedup-local-1024",{"--threads","8","--shards","256","--dedup-shards","1024","--dedup","flat","--group-person","--dense-totals","--fused-csv"}});
            configs.push_back({"dedup-local-4096",{"--threads","8","--shards","256","--dedup-shards","4096","--dedup","flat","--group-person","--dense-totals","--fused-csv"}});
            configs.push_back({"dedup-local-16384",{"--threads","8","--shards","256","--dedup-shards","16384","--dedup","flat","--group-person","--dense-totals","--fused-csv"}});
            configs.push_back({"array-active",{"--threads","8","--shards","256","--dedup-shards","4096","--dedup","flat","--group-person","--dense-totals","--fused-csv","--array-active"}});
            configs.push_back({"array-categories",{"--threads","8","--shards","256","--dedup-shards","4096","--dedup","flat","--group-person","--dense-totals","--fused-csv","--array-categories"}});
            configs.push_back({"array-both",{"--threads","8","--shards","256","--dedup-shards","4096","--dedup","flat","--group-person","--dense-totals","--fused-csv","--array-active","--array-categories"}});
            configs.push_back({"strict-time",{"--threads","8","--shards","256","--dedup-shards","4096","--dedup","flat","--group-person","--dense-totals","--fused-csv","--array-active","--array-categories","--fast-time"}});
            configs.push_back({"narrow-events",{"--threads","8","--shards","256","--dedup-shards","4096","--dedup","flat","--group-person","--dense-totals","--fused-csv","--array-active","--array-categories","--fast-time","--narrow-events"}});
            const auto narrow_control=configs.back().flags;
            configs.push_back({"direct-encoding",narrow_control});
            configs.back().flags.push_back("--direct-encoding");
            configs.push_back({"overlap-links",configs.back().flags});
            configs.back().flags.push_back("--overlap-links");
            const auto ingest_control=configs.back().flags;
            configs.push_back({"ingest-256",ingest_control});
            configs.back().flags.insert(configs.back().flags.end(),{"--ingest-shards","256"});
            const auto tier_control=configs.back().flags;
            configs.push_back({"tag-links",tier_control});
            configs.back().flags.push_back("--tag-links");
            configs.push_back({"avx2-csv",configs.back().flags});
            configs.back().flags.push_back("--avx2-csv");
            configs.push_back({"heavy-shards",tier_control});
            configs.back().flags.push_back("--heavy-shards-first");
            configs.push_back({"ingest-1024",ingest_control});
            configs.back().flags.insert(configs.back().flags.end(),{"--ingest-shards","1024"});
            configs.push_back({"direct-dedup",narrow_control});
            configs.back().flags.push_back("--direct-dedup");
            configs.push_back({"direct-both",configs.back().flags});
            configs.back().flags.push_back("--direct-encoding");
            auto requested_binary=std::getenv("BINARY");
            if(!requested_binary||std::string(requested_binary).find("-ips")==std::string::npos)
                configs.erase(std::remove_if(configs.begin(),configs.end(),[](const auto& c){return c.name.find("-ips")!=std::string::npos;}),configs.end());
            // SKIP_BASELINE is useful when timing large datasets against a
            // previously computed oracle. Correctness remains mandatory.
            if(std::getenv("SKIP_BASELINE"))configs.erase(configs.begin());
            if(auto filter=std::getenv("CONFIG")) {
                auto wanted=","+std::string(filter)+",";
                configs.erase(std::remove_if(configs.begin(),configs.end(),[&](const auto& c){return wanted.find(","+c.name+",")==std::string::npos;}),configs.end());
            }
            if(configs.empty())throw std::runtime_error("CONFIG did not match a benchmark configuration");
            std::ofstream results("benchmarks/runs.jsonl",std::ios::app);
            std::vector<std::vector<double>> times(configs.size());
            auto measure=[&](size_t index,int i) {
                    auto& config=configs[index];
                    const char* binary=std::getenv("BINARY");
                    std::vector<std::string> args={binary?binary:"build/shopping",argv[2],out.string()};args.insert(args.end(),config.flags.begin(),config.flags.end());
                    // Matrix entries start from fixed defaults rather than the
                    // tuned production defaults. Explicit flags override these.
                    args.insert(args.begin()+3,{"--no-tag-links","--ingest-shards","0","--no-overlap-links","--no-direct-encoding","--wide-events","--no-fast-time","--no-compact","--no-array-active","--no-array-categories","--dedup-shards","0","--no-fused-csv","--no-group-person","--no-dense-totals","--sorter","std"});
                    auto log=fs::path("benchmarks")/(config.name+"-"+std::to_string(i)+".log");
                    auto r=run(args,log);compare(db,out,argv[3]);times[index].push_back(r.seconds);
                    auto timestamp=std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                    results<<"{\"config\":"<<json_string(config.name)<<",\"binary\":"<<json_string(args[0])<<",\"dataset\":"<<json_string(argv[2])<<",\"timestamp\":"<<timestamp<<",\"repeat\":"<<i<<",\"seconds\":"<<std::setprecision(9)<<r.seconds<<",\"peak_rss_kib\":"<<r.rss<<",\"correct\":true}\n";results.flush();
                    std::cout<<config.name<<" "<<i<<" "<<r.seconds<<"s "<<r.rss/1024.0<<" MiB MATCH\n"<<std::flush;
            };
            if(std::getenv("ROUND_ROBIN")) {
                for(int i=0;i<repeats;++i)for(size_t c=0;c<configs.size();++c)measure(i%2?configs.size()-1-c:c,i);
            } else {
                for(size_t c=0;c<configs.size();++c)for(int i=0;i<repeats;++i)measure(c,i);
            }
            for(size_t c=0;c<configs.size();++c) {
                auto& t=times[c];std::sort(t.begin(),t.end());std::cout<<"MEDIAN "<<configs[c].name<<" "<<t[t.size()/2]<<"s\n";
            }
        } else throw std::runtime_error("invalid arguments");
    }catch(const std::exception& e){std::cerr<<"check: "<<e.what()<<'\n';return 1;}
}
