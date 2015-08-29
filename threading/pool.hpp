#ifndef MEMGRAPH_THREADING_POOL_HPP
#define MEMGRAPH_THREADING_POOL_HPP

#include <mutex>
#include <atomic>
#include <condition_variable>
#include <future>

#include "data_structures/queue/slqueue.hpp"
#include "threading/sync/lockable.hpp"
#include "worker.hpp"
#include "data_structures/queue/slqueue.hpp"

class Pool : Lockable<std::mutex>
{
    using task_t = std::function<void()>;

public:
    Pool(size_t n = std::thread::hardware_concurrency())
        : alive(true)
    {
        start(n);
    }
    
    ~Pool()
    {
        alive.store(false, std::memory_order_release);
        cond.notify_all();

        for(auto& worker : workers)
            worker.join();
    }

    size_t size()
    {
        return workers.size();
    }

    template <class F, class... Args>
    void execute(F&& f, Args&&... args)
    {
        {
            auto guard = acquire();
            tasks.emplace([f, args...]() { f(args...);  });
        }

        cond.notify_one();
    }

    template <class F, class... Args>
    auto execute_with_result(F&& f, Args&&... args)
        -> std::future<typename std::result_of<F(Args...)>::type>
    {
        using ret_t = typename std::result_of<F(Args...)>::type;

        auto task = std::make_shared<std::packaged_task<ret_t()>>
            (std::bind(f, args...));

        auto result = task->get_future();

        {
            auto guard = acquire();
            tasks.emplace([task]() { (*task)();  });
        }

        cond.notify_one();
        return result;
    }

private:
    Pool(const Pool&) = delete;
    Pool(Pool&&) = delete;

    std::vector<Worker<task_t>> workers;
    spinlock::Queue<task_t> tasks;

    std::atomic<bool> alive;

    std::mutex mutex;
    std::condition_variable cond;

    void loop()
    {
        task_t task;

        while(true)
        {
            while(tasks.pop(task))
               task(); 

            auto guard = acquire();

            cond.wait(guard, [this] {
                return !this->alive || !this->tasks.empty();
            });

            if(!alive && tasks.empty())
                return;
        }
    }

    void start(size_t n)
    {
        for(size_t i = 0; i < n; ++i)
            workers.emplace_back([this]()->void { this->loop(); });
    }
};

#endif
