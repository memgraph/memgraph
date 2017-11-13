#pragma once

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

  T AwaitPop() {
    std::unique_lock<std::mutex> guard(mutex_);
    cvar_.wait(guard, [this]() { return !queue_.empty(); });
    auto x = std::move(queue_.front());
    queue_.pop();
    return x;
  }

  std::experimental::optional<T> MaybePop() {
    std::unique_lock<std::mutex> guard(mutex_);
    if (queue_.empty()) return std::experimental::nullopt;
    auto x = std::move(queue_.front());
    queue_.pop();
    return x;
  }

 private:
  std::queue<T> queue_;
  std::condition_variable cvar_;
  mutable std::mutex mutex_;
};
