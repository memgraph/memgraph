// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/timer_backend.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#endif

namespace memgraph::utils {

#ifdef __linux__

/// Linux timerfd-based timer backend
/// Uses kernel-driven wake-ups for minimal non-determinism events
class TimerFdBackend : public ITimerBackend {
 public:
  TimerFdBackend() {
    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
    cancel_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);

    // Add both fds to epoll
    struct epoll_event ev {};
    ev.events = EPOLLIN;
    ev.data.fd = timer_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timer_fd_, &ev);

    ev.data.fd = cancel_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, cancel_fd_, &ev);
  }

  ~TimerFdBackend() override {
    if (epoll_fd_ >= 0) close(epoll_fd_);
    if (cancel_fd_ >= 0) close(cancel_fd_);
    if (timer_fd_ >= 0) close(timer_fd_);
  }

  TimerFdBackend(const TimerFdBackend &) = delete;
  TimerFdBackend &operator=(const TimerFdBackend &) = delete;
  TimerFdBackend(TimerFdBackend &&) = delete;
  TimerFdBackend &operator=(TimerFdBackend &&) = delete;

  bool WaitUntil(time_point deadline) override {
    if (cancelled_.load(std::memory_order_acquire)) return false;

    SetTimerDeadline(deadline);

    // Wait for either timer or cancel event
    struct epoll_event events[2];
    int nfds = epoll_wait(epoll_fd_, events, 2, -1);

    if (cancelled_.load(std::memory_order_acquire)) return false;

    for (int i = 0; i < nfds; ++i) {
      if (events[i].data.fd == cancel_fd_) {
        // Drain the eventfd
        uint64_t val;
        [[maybe_unused]] auto _ = read(cancel_fd_, &val, sizeof(val));
        // Check if this was a cancel or just a deadline update
        if (cancelled_.load(std::memory_order_acquire)) return false;
        // Continue waiting with potentially new deadline
      } else if (events[i].data.fd == timer_fd_) {
        // Timer fired - drain it
        uint64_t expirations;
        [[maybe_unused]] auto _ = read(timer_fd_, &expirations, sizeof(expirations));
        return true;
      }
    }
    return false;
  }

  void UpdateDeadline(time_point new_deadline) override {
    SetTimerDeadline(new_deadline);
    // Signal the epoll to wake up and re-check
    uint64_t val = 1;
    [[maybe_unused]] auto _ = write(cancel_fd_, &val, sizeof(val));
  }

  void Cancel() override {
    cancelled_.store(true, std::memory_order_release);
    uint64_t val = 1;
    [[maybe_unused]] auto _ = write(cancel_fd_, &val, sizeof(val));
  }

  bool IsCancelled() const override { return cancelled_.load(std::memory_order_acquire); }

 private:
  void SetTimerDeadline(time_point deadline) {
    auto now = std::chrono::steady_clock::now();
    auto duration = deadline - now;

    struct itimerspec its {};
    if (duration.count() > 0) {
      auto secs = std::chrono::duration_cast<std::chrono::seconds>(duration);
      auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(duration - secs);
      its.it_value.tv_sec = secs.count();
      its.it_value.tv_nsec = nsecs.count();
    } else {
      // Already past deadline - fire immediately
      its.it_value.tv_nsec = 1;
    }
    // One-shot timer (it_interval stays zero)
    timerfd_settime(timer_fd_, 0, &its, nullptr);
  }

  int timer_fd_{-1};
  int cancel_fd_{-1};
  int epoll_fd_{-1};
  std::atomic<bool> cancelled_{false};
};

#endif  // __linux__

/// Portable condition_variable-based timer backend
/// Works on all platforms but generates more non-determinism events
class ConditionVariableBackend : public ITimerBackend {
 public:
  bool WaitUntil(time_point deadline) override {
    std::unique_lock lock(mutex_);
    cv_.wait_until(lock, deadline, [this] { return cancelled_ || deadline_updated_; });

    if (cancelled_) return false;
    if (deadline_updated_) {
      deadline_updated_ = false;
      return false;  // Caller should re-check new deadline
    }
    return true;
  }

  void UpdateDeadline(time_point /*new_deadline*/) override {
    {
      std::lock_guard lock(mutex_);
      deadline_updated_ = true;
    }
    cv_.notify_one();
  }

  void Cancel() override {
    {
      std::lock_guard lock(mutex_);
      cancelled_ = true;
    }
    cv_.notify_one();
  }

  bool IsCancelled() const override {
    std::lock_guard lock(mutex_);
    return cancelled_;
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool cancelled_{false};
  bool deadline_updated_{false};
};

std::unique_ptr<ITimerBackend> CreateTimerBackend(TimerBackendType type) {
  switch (type) {
    case TimerBackendType::AUTO:
#ifdef __linux__
      return std::make_unique<TimerFdBackend>();
#else
      return std::make_unique<ConditionVariableBackend>();
#endif
    case TimerBackendType::TIMERFD:
#ifdef __linux__
      return std::make_unique<TimerFdBackend>();
#else
      // Fallback on non-Linux
      return std::make_unique<ConditionVariableBackend>();
#endif
    case TimerBackendType::CONDITION_VARIABLE:
      return std::make_unique<ConditionVariableBackend>();
  }
  return std::make_unique<ConditionVariableBackend>();
}

}  // namespace memgraph::utils
