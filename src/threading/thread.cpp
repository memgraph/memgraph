#include "threading/thread.hpp"

Thread::Thread(Thread &&other)
{
    assert(thread_id == UNINITIALIZED);
    thread_id = other.thread_id;
    thread = std::move(other.thread);
}

void Thread::join() { return thread.join(); }

std::atomic<unsigned> Thread::thread_counter{1};
