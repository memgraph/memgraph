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

namespace memgraph::utils {

#ifdef __linux__

// Linux implementation using timerfd for minimal non-determinism events

TimerBackend::TimerBackend() {
  timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
  cancel_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);

  struct epoll_event ev {};
  ev.events = EPOLLIN;
  ev.data.fd = timer_fd_;
  epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timer_fd_, &ev);

  ev.data.fd = cancel_fd_;
  epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, cancel_fd_, &ev);
}

TimerBackend::~TimerBackend() {
  if (epoll_fd_ >= 0) close(epoll_fd_);
  if (cancel_fd_ >= 0) close(cancel_fd_);
  if (timer_fd_ >= 0) close(timer_fd_);
}

bool TimerBackend::WaitUntil(time_point deadline) {
  if (cancelled_.load(std::memory_order_acquire)) return false;

  SetTimerDeadline(deadline);

  struct epoll_event events[2];
  int nfds = epoll_wait(epoll_fd_, events, 2, -1);

  if (cancelled_.load(std::memory_order_acquire)) return false;

  for (int i = 0; i < nfds; ++i) {
    if (events[i].data.fd == cancel_fd_) {
      uint64_t val;
      [[maybe_unused]] auto _ = read(cancel_fd_, &val, sizeof(val));
      if (cancelled_.load(std::memory_order_acquire)) return false;
    } else if (events[i].data.fd == timer_fd_) {
      uint64_t expirations;
      [[maybe_unused]] auto _ = read(timer_fd_, &expirations, sizeof(expirations));
      return true;
    }
  }
  return false;
}

void TimerBackend::UpdateDeadline(time_point new_deadline) {
  SetTimerDeadline(new_deadline);
  uint64_t val = 1;
  [[maybe_unused]] auto _ = write(cancel_fd_, &val, sizeof(val));
}

void TimerBackend::Cancel() {
  cancelled_.store(true, std::memory_order_release);
  uint64_t val = 1;
  [[maybe_unused]] auto _ = write(cancel_fd_, &val, sizeof(val));
}

void TimerBackend::SetTimerDeadline(time_point deadline) {
  // Convert steady_clock::time_point directly to timespec for absolute time
  // steady_clock uses CLOCK_MONOTONIC on Linux, same clock as our timerfd
  // This avoids calling steady_clock::now() which would trigger clock_gettime
  auto ns = deadline.time_since_epoch();
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(ns);
  auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(ns - secs);

  struct itimerspec its {};
  its.it_value.tv_sec = secs.count();
  its.it_value.tv_nsec = nsecs.count();

  // IMPORTANT: With timerfd, setting it_value = {0, 0} DISARMS the timer!
  // If deadline is epoch (0), use 1 nanosecond instead to trigger immediate fire
  if (its.it_value.tv_sec == 0 && its.it_value.tv_nsec == 0) {
    its.it_value.tv_nsec = 1;
  }

  // Use TFD_TIMER_ABSTIME - deadline is interpreted as absolute CLOCK_MONOTONIC time
  // If deadline is in the past, timer fires immediately
  timerfd_settime(timer_fd_, TFD_TIMER_ABSTIME, &its, nullptr);
}

#else

// Non-Linux fallback using condition_variable

TimerBackend::TimerBackend() = default;

TimerBackend::~TimerBackend() = default;

bool TimerBackend::WaitUntil(time_point deadline) {
  std::unique_lock lock(mutex_);
  cv_.wait_until(lock, deadline, [this] { return cancelled_ || deadline_updated_; });

  if (cancelled_) return false;
  if (deadline_updated_) {
    deadline_updated_ = false;
    return false;
  }
  return true;
}

void TimerBackend::UpdateDeadline(time_point /*new_deadline*/) {
  {
    std::lock_guard lock(mutex_);
    deadline_updated_ = true;
  }
  cv_.notify_one();
}

void TimerBackend::Cancel() {
  {
    std::lock_guard lock(mutex_);
    cancelled_ = true;
  }
  cv_.notify_one();
}

#endif

}  // namespace memgraph::utils
