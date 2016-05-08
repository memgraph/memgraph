#include <thread>
#include <vector>
#include <unistd.h>

#include "catch.hpp"

#include "transaction/transactionengine.hpp"
#include "sync/spinlock.hpp"

TEST_CASE("(try to) test correctness of the transaction life cycle")
{
    constexpr int THREADS = 16;
    constexpr int TRANSACTIONS = 10;

    TransactionEngine<uint64_t, SpinLock> engine(0);
    std::vector<uint64_t> sums;

    sums.resize(THREADS);

    auto f = [&engine, &sums](int idx, int n)
    {
        uint64_t sum = 0;
    
        for(int i = 0; i < n; ++i)
        {
            auto t = engine.begin();
            sum += t.id;
            engine.commit(t);
        }

        sums[idx] = sum;
    };
    
    std::vector<std::thread> threads;

    for(int i = 0; i < THREADS; ++i)
        threads.push_back(std::thread(f, i, TRANSACTIONS));

    for(auto& thread : threads)
        thread.join();

    uint64_t sum_computed = 0;

    for(int i = 0; i < THREADS; ++i)
        sum_computed += sums[i];

    uint64_t sum_actual = 0;

    for(uint64_t i = 0; i <= THREADS * TRANSACTIONS; ++i)
        sum_actual += i;

    REQUIRE(sum_computed == sum_actual);
}
