#include <iostream>
#include <thread>

#include "threading/sync/spinlock.hpp"

#include "transaction/transaction_engine.hpp"
#include "memory/memory_engine.hpp"
#include "storage/storage_engine.hpp"

constexpr int K = 10000000;
constexpr int N = 64;

void insert(TransactionEngine* tx, StorageEngine* storage)
{
    for(int i = 0; i < (K / N); ++i)
    {
        auto t = tx->begin();
    
        Vertex* v;
        storage->insert(&v, t);

        tx->commit(t);
    }
}

int main(void)
{
    auto tx = new TransactionEngine(0);
    auto mem = new MemoryEngine();
    auto storage = new StorageEngine(*mem);

//    auto t1 = tx->begin();
//
//    Vertex* v1 = nullptr;
//    storage->insert(&v1, t1);
//
//    tx->commit(t1);
//
//    auto t2 = tx->begin();
//
//    Vertex* v2 = nullptr;
//    storage->insert(&v2, t2);
//
//    tx->commit(t2);

    std::vector<std::thread> threads;

    for(int i = 0; i < N; ++i)
        threads.push_back(std::thread(insert, tx, storage));

    for(auto& thread : threads)
        thread.join();

    return 0;
}
