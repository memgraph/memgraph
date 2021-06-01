#include "utils/async_timer.hpp"

#include <signal.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>

#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace {

constexpr uint64_t kInvalidFlagId = 0U;
// std::numeric_limits<time_t>::max() cannot be represented precisely as a double, so the next smallest value is the
// maximum number of seconds the timer can be used with
const double max_seconds_as_double = std::nexttoward(std::numeric_limits<time_t>::max(), 0.0);

std::atomic<uint64_t> expiration_flag_counter{kInvalidFlagId + 1U};

struct ExpirationFlagInfo {
  uint64_t id{0U};
  std::weak_ptr<std::atomic<bool>> flag{};
};

bool operator==(const ExpirationFlagInfo &lhs, const ExpirationFlagInfo &rhs) { return lhs.id == rhs.id; }
bool operator<(const ExpirationFlagInfo &lhs, const ExpirationFlagInfo &rhs) { return lhs.id < rhs.id; }
bool operator==(const ExpirationFlagInfo &flag_info, const uint64_t id) { return flag_info.id == id; }
bool operator<(const ExpirationFlagInfo &flag_info, const uint64_t id) { return flag_info.id < id; }

utils::SkipList<ExpirationFlagInfo> expiration_flags{};

uint64_t AddFlag(std::weak_ptr<std::atomic<bool>> flag) {
  const auto id = expiration_flag_counter.fetch_add(1, std::memory_order_relaxed);
  expiration_flags.access().insert({id, std::move(flag)});
  return id;
}

void EraseFlag(uint64_t flag_id) { expiration_flags.access().remove(flag_id); }

std::weak_ptr<std::atomic<bool>> GetFlag(uint64_t flag_id) {
  const auto flag_accessor = expiration_flags.access();
  const auto it = flag_accessor.find(flag_id);
  if (it == flag_accessor.end()) {
    return {};
  } else {
    return it->flag;
  }
}

void NotifyFunction(uint64_t flag_id) {
  auto weak_flag = GetFlag(flag_id);
  if (weak_flag.expired()) {
    return;
  }
  auto flag = weak_flag.lock();
  if (flag != nullptr) {
    flag->store(true, std::memory_order_relaxed);
  }
}
}  // namespace

namespace utils {

namespace {
void TimerBackgroundWorker(int signal, siginfo_t *si, void * /*unused*/) {
  if (si->si_code != SI_TIMER) {
    return;
  }

  auto flag_id = reinterpret_cast<uint64_t>(si->si_value.sival_ptr);
  // we can use ThreadPool for this
  std::thread signal_handler{[flag_id]() { NotifyFunction(flag_id); }};

  signal_handler.detach();
}
}  // namespace

AsyncTimer::AsyncTimer() : expiration_flag_{}, flag_id_{kInvalidFlagId}, timer_id_{} {};

AsyncTimer::AsyncTimer(double seconds)
    : expiration_flag_{std::make_shared<std::atomic<bool>>(false)}, flag_id_{kInvalidFlagId}, timer_id_{} {
  MG_ASSERT(seconds <= max_seconds_as_double,
            "The AsyncTimer cannot handle larger time values than {:f}, the specified value: {:f}",
            max_seconds_as_double, seconds);
  MG_ASSERT(seconds >= 0.0, "The AsyncTimer cannot handle negative time values: {:f}", seconds);

  [[maybe_unused]] static auto setup = []() {
    struct sigaction action;
    action.sa_handler = nullptr;
    action.sa_sigaction = TimerBackgroundWorker;
    action.sa_flags = SA_RESTART | SA_SIGINFO;
    MG_ASSERT(sigaction(SIGTIMER, &action, nullptr) != -1);
    return true;
  }();

  flag_id_ = AddFlag(std::weak_ptr<std::atomic<bool>>{expiration_flag_});

  sigevent notification_settings{};
  notification_settings.sigev_notify = SIGEV_SIGNAL;
  notification_settings.sigev_signo = SIGTIMER;
  static_assert(sizeof(void *) == sizeof(flag_id_), "ID size must be equal to pointer size!");
  notification_settings.sigev_value.sival_ptr = reinterpret_cast<void *>(flag_id_);
  MG_ASSERT(timer_create(CLOCK_MONOTONIC, &notification_settings, &timer_id_) == 0, "Couldn't create timer: ({}) {}",
            errno, strerror(errno));

  constexpr auto kSecondsToNanos = 1000 * 1000 * 1000;
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

}  // namespace utils
