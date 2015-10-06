#include <iostream>
#include <vector>
#include <thread>
#include <atomic>

#include "debug/log.hpp"

static constexpr int N = 1000;

using q_t = lockfree::MpscQueue<std::string>;

void produce()
{
    std::hash<std::thread::id> hasher;

    for(int i = 0; i < N; ++i)
    {
        auto str = "Hello number " + std::to_string(i) + " from thread " +
            std::to_string(hasher(std::this_thread::get_id()));

        LOG_DEBUG(str);
    }
}

int main(void)
{
    constexpr int THREADS = 256;
    
    std::vector<std::thread> threads;

    for(int i = 0; i < THREADS; ++i)
        threads.push_back(std::thread([]() { produce(); }));

    for(auto& thread : threads)
        thread.join();

    return 0;
}
