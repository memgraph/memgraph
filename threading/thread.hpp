#pragma once

#include <atomic>
#include <thread>
#include <cassert>

#include "utils/underlying_cast.hpp"
#include "id.hpp"

class Thread
{
    static std::atomic<unsigned> thread_counter;

public:
    static size_t count(std::memory_order order = std::memory_order_seq_cst)
    {
        return thread_counter.load(order);
    }

    static constexpr unsigned UNINITIALIZED = -1;
    static constexpr unsigned MAIN_THREAD = 0;

    template <class F>
    Thread(F f)
    {
        thread_id = thread_counter.fetch_add(1, std::memory_order_acq_rel);
        thread = std::thread([this, f]() { start_thread(f); });
    }

    Thread() = default;
    Thread(const Thread&) = delete;

    Thread(Thread&& other)
    {
        assert(thread_id == UNINITIALIZED);
        thread_id = other.thread_id;
        thread = std::move(other.thread);
    }

    void join() { return thread.join(); }

private:
    unsigned thread_id = UNINITIALIZED;
    std::thread thread;

    template <class F, class... Args>
    void start_thread(F&& f)
    {
        this_thread::id = thread_id;
        f();
    }
};

std::atomic<unsigned> Thread::thread_counter {1};
