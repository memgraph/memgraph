#include "thread.hpp"
#include "utils/assert.hpp"

Thread::Thread(Thread &&other) {
  debug_assert(thread_id == UNINITIALIZED, "Thread was initialized before.");
  thread_id = other.thread_id;
  thread = std::move(other.thread);
}

void Thread::join() { return thread.join(); }

std::atomic<unsigned> Thread::thread_counter{1};
