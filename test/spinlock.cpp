#include <thread>
#include <chrono>
#include <vector>

#include "catch.hpp"
#include "utils/sync/spinlock.hpp"

TEST_CASE("a thread can acquire and release the lock", "[spinlock]")
{
    SpinLock lock;

    lock.acquire();
    // i have a lock
    lock.release();

    REQUIRE(true);
}

int x = 0;

SpinLock lock;

void test_lock()
{
    using namespace std::literals;

    lock.acquire();
    x++;

    REQUIRE(x < 2);
    std::this_thread::sleep_for(25ms);
    
    x--;
    lock.release();
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
