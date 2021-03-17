#include "utils/async_timer.hpp"

#include <signal.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace {

constexpr uint64_t kInvalidFlagId = 0U;

std::atomic<uint64_t> expiration_flag_counter{kInvalidFlagId + 1U};

struct ExpirationFlagInfo {
  uint64_t id{0U};
  std::weak_ptr<std::atomic<bool>> flag{};
};

utils::Synchronized<std::vector<ExpirationFlagInfo>, utils::SpinLock> expiration_flags{};

struct IdMatcher {
  uint64_t id;
  bool operator()(const ExpirationFlagInfo &flag_info) { return flag_info.id == id; };
};

uint64_t AddFlag(std::weak_ptr<std::atomic<bool>> flag) {
  const auto id = expiration_flag_counter.fetch_add(1, std::memory_order_relaxed);
  expiration_flags.WithLock([&](auto &locked_expiration_flags) mutable {
    auto it = std::find_if(locked_expiration_flags.begin(), locked_expiration_flags.end(), IdMatcher{kInvalidFlagId});
    if (locked_expiration_flags.end() != it) {
      *it = ExpirationFlagInfo{id, std::move(flag)};
    } else {
      locked_expiration_flags.push_back(ExpirationFlagInfo{id, std::move(flag)});
    }
  });
  return id;
}

void EraseFlag(uint64_t flag_id) {
  expiration_flags.WithLock([&](auto &locked_expiration_flags) mutable {
    auto it = std::find_if(locked_expiration_flags.begin(), locked_expiration_flags.end(), IdMatcher{flag_id});
    if (locked_expiration_flags.end() != it) {
      it->id = kInvalidFlagId;
      it->flag.reset();
    }
  });
}

std::weak_ptr<std::atomic<bool>> GetFlag(uint64_t flag_id) {
  return expiration_flags.WithLock([&](auto &locked_expiration_flags) mutable -> std::weak_ptr<std::atomic<bool>> {
    auto it = std::find_if(locked_expiration_flags.begin(), locked_expiration_flags.end(), IdMatcher{flag_id});
    if (locked_expiration_flags.end() != it) {
      return it->flag;
    } else {
      return {};
    }
  });
}

void NotifyFunction(sigval arg) {
  const auto flag_id = reinterpret_cast<uint64_t>(arg.sival_ptr);
  auto weak_flag = GetFlag(flag_id);
  if (!weak_flag.expired()) {
    auto flag = weak_flag.lock();
    if (nullptr != flag) {
      flag->store(true, std::memory_order_relaxed);
    }
  }
}
}  // namespace

namespace utils {

AsyncTimer::AsyncTimer() : expiration_flag_{}, flag_id_{kInvalidFlagId}, timer_id_{} {};

AsyncTimer::AsyncTimer(double seconds)
    : expiration_flag_{std::make_shared<std::atomic<bool>>(false)}, flag_id_{kInvalidFlagId}, timer_id_{} {
  flag_id_ = AddFlag(std::weak_ptr<std::atomic<bool>>{expiration_flag_});
  MG_ASSERT(seconds >= 0.0, "The AsyncTimer cannot handle negative time values: {}", seconds);
  sigevent notification_settings{};
  notification_settings.sigev_notify = SIGEV_THREAD;
  notification_settings.sigev_notify_function = &NotifyFunction;
  static_assert(sizeof(void *) == sizeof(decltype(flag_id_)), "ID size must be equal to pointer size!");
  notification_settings.sigev_value.sival_ptr = reinterpret_cast<void *>(flag_id_);
  MG_ASSERT(timer_create(CLOCK_MONOTONIC, &notification_settings, &timer_id_) == 0, "Couldn't create timer: ({}) {}",
            errno, strerror(errno));

  constexpr auto kSecondsToNanos = 1000 * 1000 * 1000;
  const auto inNanoSeconds = seconds * kSecondsToNanos;
  struct itimerspec spec;
  spec.it_interval.tv_sec = 0;
  spec.it_interval.tv_nsec = 0;
  // Casting will truncate down, but that's exactly what we want.
  spec.it_value.tv_sec = static_cast<time_t>(seconds);
  spec.it_value.tv_nsec = static_cast<time_t>(inNanoSeconds) % kSecondsToNanos;

  MG_ASSERT(timer_settime(timer_id_, 0, &spec, nullptr) == 0, "Couldn't set timer: ({}) {}", errno, strerror(errno));
}

AsyncTimer::~AsyncTimer() { ReleaseResources(); }

AsyncTimer::AsyncTimer(AsyncTimer &&other) noexcept
    : expiration_flag_{std::move(other.expiration_flag_)}, flag_id_{other.flag_id_}, timer_id_{other.timer_id_} {
  other.flag_id_ = kInvalidFlagId;
}

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

bool AsyncTimer::IsExpired() const {
  if (nullptr != expiration_flag_) {
    return expiration_flag_->load(std::memory_order_relaxed);
  }
  return false;
}

void AsyncTimer::ReleaseResources() {
  if (nullptr != expiration_flag_) {
    timer_delete(timer_id_);
    EraseFlag(flag_id_);
    flag_id_ = kInvalidFlagId;
    expiration_flag_ = std::shared_ptr<std::atomic<bool>>{};
  }
}

}  // namespace utils
