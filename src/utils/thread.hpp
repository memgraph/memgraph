/// @file
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>

#include "utils/future.hpp"

namespace utils {

/// This function sets the thread name of the calling thread.
/// Beware, the name length limit is 16 characters!
void ThreadSetName(const std::string &name);

class Thread {
  static std::atomic<unsigned> thread_counter;

 public:
  static size_t count(std::memory_order order = std::memory_order_seq_cst) {
    return thread_counter.load(order);
  }

  static constexpr unsigned UNINITIALIZED = -1;
  static constexpr unsigned MAIN_THREAD = 0;

  template <class F>
  explicit Thread(F f) {
    thread_id = thread_counter.fetch_add(1, std::memory_order_acq_rel);
    thread = std::thread([this, f]() { start_thread(f); });
  }

  Thread() = default;
  Thread(const Thread &) = delete;

  Thread(Thread &&other);

  void join();

 private:
  unsigned thread_id = UNINITIALIZED;
  std::thread thread;

  template <class F, class... Args>
  void start_thread(F &&f) {
    // this_thread::id = thread_id;
    f();
  }
};

/// A thread pool for asynchronous task execution. Supports tasks that produce
/// return values by returning `utils::Future` objects.
class ThreadPool final {
 public:
  /// Creates a thread pool with the given number of threads.
  explicit ThreadPool(size_t threads);
  ~ThreadPool();

  ThreadPool(const ThreadPool &) = delete;
  ThreadPool(ThreadPool &&) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;
  ThreadPool &operator=(ThreadPool &&) = delete;

  /// Runs the given callable with the given args, asynchronously. This function
  /// immediately returns an `utils::Future` with the result, to be
  /// consumed when ready.
  template <class TCallable, class... TArgs>
  auto Run(TCallable &&callable, TArgs &&... args) {
    auto task = std::make_shared<
        std::packaged_task<std::result_of_t<TCallable(TArgs...)>()>>(std::bind(
        std::forward<TCallable>(callable), std::forward<TArgs>(args)...));

    auto res = utils::make_future(task->get_future());

    std::unique_lock<std::mutex> lock(mutex_);
    CHECK(!stop_) << "ThreadPool::Run called on stopped ThreadPool.";
    tasks_.emplace([task]() { (*task)(); });
    lock.unlock();
    cvar_.notify_one();
    return res;
  }

 private:
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  std::mutex mutex_;
  std::condition_variable cvar_;
  bool stop_{false};
};

};  // namespace utils
