// Copyright 2026 Memgraph Ltd.
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

#include <array>
#include <cerrno>
#include <utility>

namespace memgraph::utils {

std::expected<TimerBackend, TimerBackendError> TimerBackend::Create() {
  TimerBackend backend;

  // Create timerfd with CLOCK_MONOTONIC for reliable timing unaffected by system clock changes
  // TFD_CLOEXEC: prevent fd leak to child processes on fork+exec
  // TFD_NONBLOCK: defensive - not strictly needed with epoll, but prevents blocking on read()
  //               if called outside epoll_wait context
  backend.timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
  if (backend.timer_fd_ < 0) {
    return std::unexpected(TimerBackendError{.message = "timerfd_create failed", .errno_value = errno});
  }

  // Create eventfd for cancel/wake signaling
  // EFD_CLOEXEC: prevent fd leak to child processes
  // EFD_NONBLOCK: allow non-blocking reads after epoll signals readiness
  // NOTE: No EFD_SEMAPHORE - multiple writes consolidate into one wake, which is desired
  //       for UpdateDeadline() where we just need to wake the waiter once
  backend.cancel_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (backend.cancel_fd_ < 0) {
    const int saved_errno = errno;
    close(std::exchange(backend.timer_fd_, -1));
    return std::unexpected(TimerBackendError{.message = "eventfd failed", .errno_value = saved_errno});
  }

  // Create epoll instance for multiplexing timer and cancel events
  // EPOLL_CLOEXEC: prevent fd leak to child processes
  backend.epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
  if (backend.epoll_fd_ < 0) {
    const int saved_errno = errno;
    close(std::exchange(backend.cancel_fd_, -1));
    close(std::exchange(backend.timer_fd_, -1));
    return std::unexpected(TimerBackendError{.message = "epoll_create1 failed", .errno_value = saved_errno});
  }

  // Register timer_fd with epoll
  // EPOLLIN: notify when timer expires (readable)
  // Using level-triggered (default) - safer than edge-triggered (EPOLLET)
  // With level-triggered, we won't miss events even if we don't drain the fd completely
  struct epoll_event ev {};
  ev.events = EPOLLIN;
  ev.data.fd = backend.timer_fd_;
  if (epoll_ctl(backend.epoll_fd_, EPOLL_CTL_ADD, backend.timer_fd_, &ev) < 0) {
    const int saved_errno = errno;
    close(std::exchange(backend.epoll_fd_, -1));
    close(std::exchange(backend.cancel_fd_, -1));
    close(std::exchange(backend.timer_fd_, -1));
    return std::unexpected(TimerBackendError{.message = "epoll_ctl(timer_fd) failed", .errno_value = saved_errno});
  }

  // Register cancel_fd with epoll
  ev.data.fd = backend.cancel_fd_;
  if (epoll_ctl(backend.epoll_fd_, EPOLL_CTL_ADD, backend.cancel_fd_, &ev) < 0) {
    const int saved_errno = errno;
    close(std::exchange(backend.epoll_fd_, -1));
    close(std::exchange(backend.cancel_fd_, -1));
    close(std::exchange(backend.timer_fd_, -1));
    return std::unexpected(TimerBackendError{.message = "epoll_ctl(cancel_fd) failed", .errno_value = saved_errno});
  }

  return backend;
}

TimerBackend::~TimerBackend() {
  if (auto fd = std::exchange(epoll_fd_, -1); fd >= 0) close(fd);
  if (auto fd = std::exchange(cancel_fd_, -1); fd >= 0) close(fd);
  if (auto fd = std::exchange(timer_fd_, -1); fd >= 0) close(fd);
}

TimerBackend::TimerBackend(TimerBackend &&other) noexcept
    : timer_fd_(std::exchange(other.timer_fd_, -1)),
      cancel_fd_(std::exchange(other.cancel_fd_, -1)),
      epoll_fd_(std::exchange(other.epoll_fd_, -1)),
      cancelled_(other.cancelled_.load(std::memory_order_acquire)) {}

TimerBackend &TimerBackend::operator=(TimerBackend &&other) noexcept {
  if (this != &other) {
    // Close existing fds
    if (auto fd = std::exchange(epoll_fd_, -1); fd >= 0) close(fd);
    if (auto fd = std::exchange(cancel_fd_, -1); fd >= 0) close(fd);
    if (auto fd = std::exchange(timer_fd_, -1); fd >= 0) close(fd);

    // Take ownership from other
    timer_fd_ = std::exchange(other.timer_fd_, -1);
    cancel_fd_ = std::exchange(other.cancel_fd_, -1);
    epoll_fd_ = std::exchange(other.epoll_fd_, -1);
    cancelled_.store(other.cancelled_.load(std::memory_order_acquire), std::memory_order_release);
  }
  return *this;
}

bool TimerBackend::WaitUntil(time_point deadline) {
  if (cancelled_.load(std::memory_order_acquire)) return false;

  SetTimerDeadline(deadline);

  std::array<struct epoll_event, 2> events{};
  int nfds = 0;

  // Retry epoll_wait if interrupted by signal (EINTR)
  // This is important for robustness - signals like SIGUSR1 can interrupt syscalls
  do {
    nfds = epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
  } while (nfds < 0 && errno == EINTR);

  // Check for unexpected errors
  if (nfds < 0) {
    // Unexpected error - treat as cancellation
    return false;
  }

  if (cancelled_.load(std::memory_order_acquire)) return false;

  for (int i = 0; i < nfds; ++i) {
    if (events[static_cast<size_t>(i)].data.fd == cancel_fd_) {
      // Drain the eventfd counter (without EFD_SEMAPHORE, this reads the entire value)
      uint64_t val = 0;
      [[maybe_unused]] auto _ = read(cancel_fd_, &val, sizeof(val));
      if (cancelled_.load(std::memory_order_acquire)) return false;
    } else if (events[static_cast<size_t>(i)].data.fd == timer_fd_) {
      // Timer expired - read expiration count to clear the event
      uint64_t expirations = 0;
      [[maybe_unused]] auto _ = read(timer_fd_, &expirations, sizeof(expirations));
      return true;
    }
  }

  // No matching fd - shouldn't happen, but treat as wake-up to re-evaluate
  return false;
}

void TimerBackend::UpdateDeadline(time_point new_deadline) {
  SetTimerDeadline(new_deadline);
  // Write to cancel_fd to wake up any thread blocked in epoll_wait
  // The value doesn't matter - we just need to trigger EPOLLIN
  uint64_t val = 1;
  [[maybe_unused]] auto _ = write(cancel_fd_, &val, sizeof(val));
}

void TimerBackend::Cancel() {
  cancelled_.store(true, std::memory_order_release);
  // Write to cancel_fd to wake up any thread blocked in epoll_wait
  uint64_t val = 1;
  [[maybe_unused]] auto _ = write(cancel_fd_, &val, sizeof(val));
}

// NOLINTNEXTLINE(readability-make-member-function-const) - modifies timer state via timerfd_settime
void TimerBackend::SetTimerDeadline(time_point deadline) {
  // Convert steady_clock::time_point directly to timespec for absolute time
  // steady_clock uses CLOCK_MONOTONIC on Linux, same clock as our timerfd
  // This avoids calling steady_clock::now() which would trigger clock_gettime syscall
  const auto ns = deadline.time_since_epoch();
  const auto secs = std::chrono::duration_cast<std::chrono::seconds>(ns);
  const auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(ns - secs);

  struct itimerspec its {};
  its.it_value.tv_sec = secs.count();
  its.it_value.tv_nsec = nsecs.count();

  // IMPORTANT: With timerfd, setting it_value = {0, 0} DISARMS the timer!
  // If deadline is epoch (0), use 1 nanosecond instead to trigger immediate fire
  if (its.it_value.tv_sec == 0 && its.it_value.tv_nsec == 0) {
    its.it_value.tv_nsec = 1;
  }

  // Use TFD_TIMER_ABSTIME - deadline is interpreted as absolute CLOCK_MONOTONIC time
  // If deadline is in the past, timer fires immediately (no need for clock_gettime)
  (void)timerfd_settime(timer_fd_, TFD_TIMER_ABSTIME, &its, nullptr);
}

}  // namespace memgraph::utils
