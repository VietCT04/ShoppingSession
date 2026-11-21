#pragma once
#include "core.hpp"
namespace shop {
// Strict format parser specialized by length, with calendar days cached around
// the runtime manifest window. Arbitrary dates and uncommon numeric lengths
// retain the original general parser. No dataset IDs or seeds are assumed.
struct StrictTimeParser {
    static constexpr int64_t BAD=INT64_MIN;
    int first_year=0,last_year=-1;
    std::vector<int64_t> calendar;
    void init(int64_t start,int64_t end) {
        auto year=[](int64_t ts){return int(std::chrono::year_month_day{std::chrono::sys_days{std::chrono::days{dayof(ts)}}}.year());};
        first_year=year(start)-1;last_year=year(end-1)+1;
        calendar.assign(size_t(last_year-first_year+1)*512,BAD);
        for(int y=first_year;y<=last_year;++y)for(unsigned m=1;m<=12;++m)for(unsigned d=1;d<=31;++d) {
            std::chrono::year_month_day date{std::chrono::year{y},std::chrono::month{m},std::chrono::day{d}};
            if(date.ok())calendar[size_t(y-first_year)*512+m*32+d]=civil(y,m,d)*DAY;
        }
    }
    static int pair(const char* p){return (p[0]-'0')*10+p[1]-'0';}
    static int checked_pair(const char* p) {
        unsigned a=static_cast<unsigned char>(p[0])-'0',b=static_cast<unsigned char>(p[1])-'0';
        return a<=9&&b<=9?int(a*10+b):-1;
    }
    static bool first_date_digits(const char* p) {
#ifdef __SSE2__
        auto bytes=_mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
        auto bad=_mm_or_si128(_mm_cmplt_epi8(bytes,_mm_set1_epi8('0')),_mm_cmpgt_epi8(bytes,_mm_set1_epi8('9')));
        return (_mm_movemask_epi8(bad)&0xdb6f)==0;
#else
        for(int i:{0,1,2,3,5,6,8,9,11,12,14,15})if(unsigned(static_cast<unsigned char>(p[i])-'0')>9)return false;
        return true;
#endif
    }
    static bool eight_digits(const char* p) {
#ifdef __SSE2__
        auto bytes=_mm_loadl_epi64(reinterpret_cast<const __m128i*>(p));
        auto bad=_mm_or_si128(_mm_cmplt_epi8(bytes,_mm_set1_epi8('0')),_mm_cmpgt_epi8(bytes,_mm_set1_epi8('9')));
        return (_mm_movemask_epi8(bad)&255)==0;
#else
        for(int i=0;i<8;++i)if(unsigned(static_cast<unsigned char>(p[i])-'0')>9)return false;
        return true;
#endif
    }
    static uint32_t decimal8(const char* p) {
        uint64_t v;std::memcpy(&v,p,8);
        if constexpr(std::endian::native!=std::endian::little)v=__builtin_bswap64(v);
        v=(v&0x0f0f0f0f0f0f0f0fULL)*2561>>8;
        v=(v&0x00ff00ff00ff00ffULL)*6553601>>16;
        return uint32_t((v&0x0000ffff0000ffffULL)*42949672960001ULL>>32);
    }
    int64_t parse(SV s)const {
        const char* p=s.data();
        if((s.size()==20||s.size()==25)&&p[4]=='-') {
            if(p[7]!='-'||p[10]!='T'||p[13]!=':'||p[16]!=':'||!first_date_digits(p))return -1;
            int y=pair(p)*100+pair(p+2),m=pair(p+5),d=pair(p+8),h=pair(p+11),mi=pair(p+14),sec=checked_pair(p+17);
            if(m<1||m>12||d<1||d>31||h>23||mi>59||sec<0||sec>59)return -1;
            int64_t offset=0;
            if(s.size()==20){if(p[19]!='Z')return -1;}
            else {
                if((p[19]!='+'&&p[19]!='-')||p[22]!=':')return -1;
                int oh=checked_pair(p+20),om=checked_pair(p+23);
                if(oh<0||oh>23||om<0||om>59)return -1;
                offset=int64_t(oh*60+om)*60000*(p[19]=='+'?1:-1);
            }
            if(y<first_year||y>last_year)return timestamp(s);
            auto day=calendar[size_t(y-first_year)*512+m*32+d];
            if(day==BAD)return -1;
            return day+int64_t(h*3600+mi*60+sec)*1000-offset;
        }
        if(s.size()>=8&&s.size()<=18) {
            if(!eight_digits(p))return -1;
            uint64_t n=decimal8(p);
            for(size_t i=8;i<s.size();++i){unsigned d=static_cast<unsigned char>(p[i])-'0';if(d>9)return -1;n=n*10+d;}
            return int64_t(n);
        }
        return timestamp(s);
    }
};
}
