#pragma once
/// @file

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

#include "glog/logging.h"

#include "utils/future.hpp"

namespace threading {

/// A thread pool for asynchronous task execution. Supports tasks that produce
/// return values by returning `utils::Future` objects.
class ThreadPool {
 public:
  /// Creates a thread pool with the given number of threads.
  explicit ThreadPool(size_t threads) {
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

  ~ThreadPool() {
    std::unique_lock<std::mutex> lock(mutex_);
    stop_ = true;
    lock.unlock();
    cvar_.notify_all();
    for (std::thread &worker : workers_) {
      if (worker.joinable()) worker.join();
    }
  }

 private:
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  std::mutex mutex_;
  std::condition_variable cvar_;
  bool stop_{false};
};
}  // namespace threading
