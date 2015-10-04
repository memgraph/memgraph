#include <thread>
#include <chrono>
#include <vector>
#include <mutex>
#include <atomic>

#include "catch.hpp"
#include "sync/spinlock.hpp"

TEST_CASE("a thread can acquire and release the lock", "[spinlock]")
{
    {
        std::unique_lock<SpinLock> lock;
        // I HAS A LOCK!
    }

    REQUIRE(true);
}

int x = 0;

SpinLock lock;

void test_lock()
{
    using namespace std::literals;

    {
        std::unique_lock<SpinLock> guard(lock);
        x++;

        std::this_thread::sleep_for(25ms);
    
        REQUIRE(x < 2);
        x--;
    }
}

TEST_CASE("only one thread at a time can own the lock", "[spinlock]")
{
    constexpr int N = 64;

    std::vector<std::thread> threads;

    for(int i = 0; i < N; ++i)
        threads.push_back(std::thread(test_lock));

    for(auto& thread : threads){
        thread.join();
    }
}
