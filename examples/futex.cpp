#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <vector>
#include <cassert>
#include <random>

#include "threading/sync/futex.hpp"
#include "debug/log.hpp"

Futex futex;
int x = 0;

void test_lock(int id)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 1000);

    for(int i = 0; i < 100000; ++i)
    {
        // uncomment sleeps and LOG_DEBUGs to test high contention

        LOG_DEBUG("Acquiring Futex (" << id << ")");

        {
            std::unique_lock<Futex> guard(futex);
            x++;

            std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));

            LOG_DEBUG("Critical section no. " << i << " (" << id << ")");
            assert(x == 1);

            x--;
        }

        LOG_DEBUG("Non Critical section... (" << id << ")");
        std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));
    }
}

int main(void)
{
    constexpr int N = 128;

    std::vector<std::thread> threads;

    for(int i = 0; i < N; ++i)
        threads.push_back(std::thread(test_lock, i));

    for(auto& thread : threads){
        thread.join();
    }

    return 0;
}
