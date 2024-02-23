// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/async_timer.hpp"

#include <csignal>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <limits>

#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace {

inline constexpr uint64_t kInvalidFlagId = 0U;
// std::numeric_limits<time_t>::max() cannot be represented precisely as a double, so the next smallest value is the
// maximum number of seconds the timer can be used with
const double max_seconds_as_double = std::nexttoward(std::numeric_limits<time_t>::max(), 0.0);

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> expiration_flag_counter{kInvalidFlagId + 1U};

struct ExpirationFlagInfo {
  uint64_t id{0U};
  std::weak_ptr<std::atomic<bool>> flag{};
};

bool operator==(const ExpirationFlagInfo &lhs, const ExpirationFlagInfo &rhs) { return lhs.id == rhs.id; }
bool operator<(const ExpirationFlagInfo &lhs, const ExpirationFlagInfo &rhs) { return lhs.id < rhs.id; }
bool operator==(const ExpirationFlagInfo &flag_info, const uint64_t id) { return flag_info.id == id; }
bool operator<(const ExpirationFlagInfo &flag_info, const uint64_t id) { return flag_info.id < id; }

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
memgraph::utils::SkipList<ExpirationFlagInfo> expiration_flags{};

uint64_t AddFlag(std::weak_ptr<std::atomic<bool>> flag) {
  const auto id = expiration_flag_counter.fetch_add(1, std::memory_order_relaxed);
  expiration_flags.access().insert({id, std::move(flag)});
  return id;
}

void EraseFlag(uint64_t flag_id) { expiration_flags.access().remove(flag_id); }

std::weak_ptr<std::atomic<bool>> GetFlag(uint64_t flag_id) {
  const auto flag_accessor = expiration_flags.access();
  const auto iter = flag_accessor.find(flag_id);
  if (iter == flag_accessor.end()) {
    return {};
  }
  return iter->flag;
}

void MarkDone(const uint64_t flag_id) {
  const auto weak_flag = GetFlag(flag_id);
  if (weak_flag.expired()) {
    return;
  }
  auto flag = weak_flag.lock();
  if (flag != nullptr) {
    flag->store(true, std::memory_order_relaxed);
  }
}
}  // namespace

namespace memgraph::utils {

namespace {
struct ThreadInfo {
  pid_t thread_id;
  std::atomic<bool> setup_done{false};
};

void *TimerBackgroundWorker(void *args) {
  auto *thread_info = static_cast<ThreadInfo *>(args);
  thread_info->thread_id = syscall(SYS_gettid);
  thread_info->setup_done.store(true, std::memory_order_release);

  sigset_t ss;
  sigemptyset(&ss);
  sigaddset(&ss, SIGTIMER);
  sigprocmask(SIG_BLOCK, &ss, nullptr);

  while (true) {
    siginfo_t si;
    int result = sigwaitinfo(&ss, &si);

    if (result <= 0) {
      continue;
    }

    if (si.si_code == SI_TIMER) {
      auto flag_id = kInvalidFlagId;
      std::memcpy(&flag_id, &si.si_value.sival_ptr, sizeof(flag_id));
      MarkDone(flag_id);
    } else if (si.si_code == SI_TKILL) {
      pthread_exit(nullptr);
    }
  }
}
}  // namespace

AsyncTimer::AsyncTimer() : flag_id_{kInvalidFlagId} {};

AsyncTimer::AsyncTimer(double seconds)
    : expiration_flag_{std::make_shared<std::atomic<bool>>(false)}, flag_id_{kInvalidFlagId}, timer_id_{} {
  MG_ASSERT(seconds <= max_seconds_as_double,
            "The AsyncTimer cannot handle larger time values than {:f}, the specified value: {:f}",
            max_seconds_as_double, seconds);
  MG_ASSERT(seconds >= 0.0, "The AsyncTimer cannot handle negative time values: {:f}", seconds);

  static pthread_t background_timer_thread;
  static ThreadInfo thread_info;
  static std::once_flag timer_thread_setup_flag;

  std::call_once(timer_thread_setup_flag, [] {
    pthread_create(&background_timer_thread, nullptr, TimerBackgroundWorker, &thread_info);
    while (!thread_info.setup_done.load(std::memory_order_acquire))
      ;
  });

  flag_id_ = AddFlag(std::weak_ptr<std::atomic<bool>>{expiration_flag_});

  sigevent notification_settings{};
  notification_settings.sigev_notify = SIGEV_THREAD_ID;
  notification_settings.sigev_signo = SIGTIMER;
  notification_settings._sigev_un._tid = thread_info.thread_id;
  static_assert(sizeof(void *) == sizeof(flag_id_), "ID size must be equal to pointer size!");
  std::memcpy(&notification_settings.sigev_value.sival_ptr, &flag_id_, sizeof(flag_id_));
  MG_ASSERT(timer_create(CLOCK_MONOTONIC, &notification_settings, &timer_id_) == 0, "Couldn't create timer: ({}) {}",
            errno, strerror(errno));

  static constexpr auto kSecondsToNanos = 1000 * 1000 * 1000;
  // Casting will truncate down, but that's exactly what we want.
  const auto second_as_time_t = static_cast<time_t>(seconds);
  const auto remaining_nano_seconds = static_cast<time_t>((seconds - second_as_time_t) * kSecondsToNanos);

  struct itimerspec spec;
  spec.it_interval.tv_sec = 0;
  spec.it_interval.tv_nsec = 0;
  spec.it_value.tv_sec = second_as_time_t;
  spec.it_value.tv_nsec = remaining_nano_seconds;

  MG_ASSERT(timer_settime(timer_id_, 0, &spec, nullptr) == 0, "Couldn't set timer: ({}) {}", errno, strerror(errno));
}

AsyncTimer::~AsyncTimer() { ReleaseResources(); }

AsyncTimer::AsyncTimer(AsyncTimer &&other) noexcept
    : expiration_flag_{std::move(other.expiration_flag_)}, flag_id_{other.flag_id_}, timer_id_{other.timer_id_} {
  other.flag_id_ = kInvalidFlagId;
}

// NOLINTNEXTLINE (hicpp-noexcept-move)
AsyncTimer &AsyncTimer::operator=(AsyncTimer &&other) {
  if (this == &other) {
    return *this;
  }

  ReleaseResources();

  expiration_flag_ = std::move(other.expiration_flag_);
  flag_id_ = std::exchange(other.flag_id_, kInvalidFlagId);
  timer_id_ = other.timer_id_;

  return *this;
};

bool AsyncTimer::IsExpired() const noexcept {
  if (expiration_flag_ != nullptr) {
    return expiration_flag_->load(std::memory_order_relaxed);
  }
  return false;
}

void AsyncTimer::ReleaseResources() {
  if (expiration_flag_ != nullptr) {
    timer_delete(timer_id_);
    EraseFlag(flag_id_);
    flag_id_ = kInvalidFlagId;
    expiration_flag_ = std::shared_ptr<std::atomic<bool>>{};
  }
}

}  // namespace memgraph::utils
