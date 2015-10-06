#include <iostream>
#include <vector>
#include <thread>
#include <atomic>

#include "data_structures/queue/mpsc_queue.hpp"

static constexpr int N = 1000;

std::atomic<bool> alive { true };

using q_t = lockfree::MpscQueue<std::string>;

void produce(q_t& q)
{
    std::hash<std::thread::id> hasher;

    for(int i = 0; i < N; ++i)
    {
        auto str = std::to_string(i) + " " +
            std::to_string(hasher(std::this_thread::get_id()));

        q.push(std::make_unique<std::string>(str));
    }
}

void consume(q_t& q)
{
    using namespace std::chrono_literals;

    int i = 0;

    while(true)
    {
        auto message = q.pop();

        if(message != nullptr)
        {
            std::cerr << (i++) << ": " << *message << std::endl;
            continue;
        }

        if(!alive)
            return;

        std::this_thread::sleep_for(10ms);
    }
}

int main(void)
{
    constexpr int THREADS = 256;
    q_t q;

    auto consumer = std::thread([&q]() { consume(q); });
    
    std::vector<std::thread> threads;

    for(int i = 0; i < THREADS; ++i)
        threads.push_back(std::thread([&q]() { produce(q); }));

    for(auto& thread : threads)
        thread.join();

    std::cout << "FINISHED" << std::endl;
    alive.store(false);
    consumer.join();

    return 0;
}
