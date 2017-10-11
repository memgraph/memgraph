#include "glog/logging.h"
#include "thread.hpp"

Thread::Thread(Thread &&other) {
  DCHECK(thread_id == UNINITIALIZED) << "Thread was initialized before.";
  thread_id = other.thread_id;
  thread = std::move(other.thread);
}

void Thread::join() { return thread.join(); }

std::atomic<unsigned> Thread::thread_counter{1};
