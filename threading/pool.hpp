#ifndef MEMGRAPH_THREADING_POOL_HPP
#define MEMGRAPH_THREADING_POOL_HPP

#include <mutex>
#include <atomic>
#include <future>
#include <queue>
#include <condition_variable>

#include "sync/lockable.hpp"

class Pool : Lockable<std::mutex>
{
    using task_t = std::function<void()>;
public:
    Pool(size_t n = std::thread::hardware_concurrency()) : alive(true)
    {
        threads.reserve(n);

        for(size_t i = 0; i < n; ++i)
            threads.emplace_back([this]()->void { loop(); });
    }

    Pool(Pool&) = delete;
    Pool(Pool&&) = delete;
    
    ~Pool()
    {
        alive.store(false, std::memory_order_release);
        cond.notify_all();

        for(auto& thread : threads)
            thread.join();
    }

    template <class F, class... Args>
    void run(F&& f, Args&&... args)
    {
        {
            auto lock = acquire();

            tasks.emplace([&f, &args...]() {
                f(std::forward<Args>(args)...);
            });
        }

        cond.notify_one();
    }

private:
    std::vector<std::thread> threads;
    std::queue<task_t> tasks;
    std::atomic<bool> alive;

    std::mutex mutex;
    std::condition_variable cond;

    void loop()
    {
        while(true)
        {
            task_t task;

            {
                auto lock = acquire();

                cond.wait(lock, [this] {
                    return !this->alive || !this->tasks.empty();
                });

                if(!alive && tasks.empty())
                    return;

                task = std::move(tasks.front());
                tasks.pop();
            }

            task();
        }
    }
};

#endif
