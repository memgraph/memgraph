#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

#include "glog/logging.h"

// Thread safe queue. Probably doesn't perform very well, but it works.
template <typename T>
class Queue {
 public:
  Queue() = default;
  Queue(const Queue &) = delete;
  Queue &operator=(const Queue &) = delete;
  Queue(Queue &&) = delete;
  Queue &operator=(Queue &&) = delete;

  void Push(T x) {
    std::unique_lock<std::mutex> guard(mutex_);
    queue_.emplace(std::move(x));
    guard.unlock();
    cvar_.notify_one();
  }

  template <typename... Args>
  void Emplace(Args &&... args) {
    std::unique_lock<std::mutex> guard(mutex_);
    queue_.emplace(std::forward<Args>(args)...);
    guard.unlock();
    cvar_.notify_one();
  }

  int64_t size() const {
    std::unique_lock<std::mutex> guard(mutex_);
    return queue_.size();
  }

  bool empty() const {
    std::unique_lock<std::mutex> guard(mutex_);
    return queue_.empty();
  }

  // Block until there is an element in the queue and then pop it from the queue
  // and return it. Function can return nullopt if Queue is signaled via
  // Shutdown function or if there is no element to pop after timeout elapses.
  std::optional<T> AwaitPop(std::chrono::system_clock::duration timeout =
                                std::chrono::system_clock::duration::max()) {
    std::unique_lock<std::mutex> guard(mutex_);
    auto now = std::chrono::system_clock::now();
    auto until = std::chrono::system_clock::time_point::max() - timeout > now
                     ? now + timeout
                     : std::chrono::system_clock::time_point::max();
    cvar_.wait_until(guard, until,
                     [this] { return !queue_.empty() || !alive_; });
    if (queue_.empty() || !alive_) return std::nullopt;
    std::optional<T> x(std::move(queue_.front()));
    queue_.pop();
    return x;
  }

  // Nonblocking version of above function.
  std::optional<T> MaybePop() {
    std::unique_lock<std::mutex> guard(mutex_);
    if (queue_.empty()) return std::nullopt;
    std::optional<T> x(std::move(queue_.front()));
    queue_.pop();
    return x;
  }

  // Notify all threads waiting on conditional variable to stop waiting. New
  // threads that try to Await will not block.
  void Shutdown() {
    std::unique_lock<std::mutex> guard(mutex_);
    alive_ = false;
    guard.unlock();
    cvar_.notify_all();
  }

 private:
  bool alive_ = true;
  std::queue<T> queue_;
  std::condition_variable cvar_;
  mutable std::mutex mutex_;
};
