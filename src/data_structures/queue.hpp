#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <experimental/optional>
#include <iostream>
#include <mutex>
#include <queue>

// Thread safe queue. Probably doesn't perform very well, but it works.
template <typename T>
class Queue {
 public:
  Queue() = default;
  Queue(const Queue &) = delete;
  Queue &operator=(const Queue &) = delete;
  Queue(Queue &&) = delete;
  Queue &operator=(Queue &&) = delete;

  ~Queue() { Signal(); }

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
  // and return it. Function can return nullopt only if Queue is signaled via
  // Signal function.
  std::experimental::optional<T> AwaitPop() {
    std::unique_lock<std::mutex> guard(mutex_);
    cvar_.wait(guard, [this] { return !queue_.empty() || signaled_; });
    if (queue_.empty()) return std::experimental::nullopt;
    std::experimental::optional<T> x(std::move(queue_.front()));
    queue_.pop();
    return x;
  }

  // Nonblocking version of above function.
  std::experimental::optional<T> MaybePop() {
    std::unique_lock<std::mutex> guard(mutex_);
    if (queue_.empty()) return std::experimental::nullopt;
    std::experimental::optional<T> x(std::move(queue_.front()));
    queue_.pop();
    return x;
  }

  // Notify all threads waiting on conditional variable to stop waiting.
  void Signal() {
    signaled_ = true;
    cvar_.notify_all();
  }

 private:
  std::atomic<bool> signaled_{false};
  std::queue<T> queue_;
  std::condition_variable cvar_;
  mutable std::mutex mutex_;
};
