#include "utils/thread.hpp"

#include <sys/prctl.h>

#include <glog/logging.h>

namespace utils {

void ThreadSetName(const std::string &name) {
  CHECK(name.size() <= 16) << "Thread name '" << name << "'too long";
  LOG_IF(WARNING, prctl(PR_SET_NAME, name.c_str()) != 0)
      << "Couldn't set thread name: " << name << "!";
}

Thread::Thread(Thread &&other) {
  DCHECK(thread_id == UNINITIALIZED) << "Thread was initialized before.";
  thread_id = other.thread_id;
  thread = std::move(other.thread);
}

void Thread::join() { return thread.join(); }

std::atomic<unsigned> Thread::thread_counter{1};

ThreadPool::ThreadPool(size_t threads) {
  for (size_t i = 0; i < threads; ++i)
    workers_.emplace_back([this] {
      while (true) {
        std::function<void()> task;
        {
          std::unique_lock<std::mutex> lock(mutex_);
          cvar_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
          if (stop_ && tasks_.empty()) return;
          task = std::move(tasks_.front());
          tasks_.pop();
        }
        task();
      }
    });
}

ThreadPool::~ThreadPool() {
  std::unique_lock<std::mutex> lock(mutex_);
  stop_ = true;
  lock.unlock();
  cvar_.notify_all();
  for (std::thread &worker : workers_) {
    if (worker.joinable()) worker.join();
  }
}

}  // namespace utils
