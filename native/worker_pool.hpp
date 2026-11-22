#pragma once
#include "core.hpp"
#include <barrier>
#include <functional>

namespace shop {
class WorkerPool {
    std::barrier<> phase;
    std::vector<std::thread> workers;
    std::function<void(size_t,int)> task;
    std::atomic<size_t> next{0};
    size_t count=0;
    bool stopping=false;
    std::mutex failure_mutex;
    std::exception_ptr failure;

    void serve(int worker) {
        for(;;) {
            phase.arrive_and_wait();
            if(stopping)return;
            try {
                for(;;) {
                    size_t i=next.fetch_add(1,std::memory_order_relaxed);
                    if(i>=count)break;
                    task(i,worker);
                }
            } catch(...) {
                std::lock_guard lock(failure_mutex);
                if(!failure)failure=std::current_exception();
                next.store(count,std::memory_order_relaxed);
            }
            phase.arrive_and_wait();
        }
    }
    void stop() {
        stopping=true;
        phase.arrive_and_wait();
        for(auto& worker:workers)worker.join();
    }
public:
    explicit WorkerPool(int threads):phase(threads+1) {
        workers.reserve(threads);
        try {
            for(int i=0;i<threads;++i)workers.emplace_back([this,i]{serve(i);});
        } catch(...) {
            for(size_t i=workers.size();i<size_t(threads);++i)phase.arrive_and_drop();
            stop();
            throw;
        }
    }
    ~WorkerPool(){stop();}

    template<class F>void run(size_t jobs,F work) {
        task=std::move(work);
        next.store(0,std::memory_order_relaxed);
        count=jobs;
        failure=nullptr;
        phase.arrive_and_wait();
        phase.arrive_and_wait();
        task={};
        if(failure)std::rethrow_exception(failure);
    }
};
}
