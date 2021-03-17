#include "utils/async_timer.hpp"

#include <signal.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include "utils/spin_lock.hpp"

namespace {

constexpr uint64_t kInvalidId = 0U;
std::atomic<uint64_t> expiration_flag_counter{kInvalidId + 1U};
utils::SpinLock expiration_flags_lock{};

struct ExpirationFlagInfo {
  uint64_t id{0U};
  std::weak_ptr<std::atomic<bool>> flag{};
};

std::vector<ExpirationFlagInfo> expiration_flags{};

struct IdMatcher {
  uint64_t id;
  bool operator()(const ExpirationFlagInfo &flag_info) { return flag_info.id == id; };
};

uint64_t AddFlag(std::weak_ptr<std::atomic<bool>> flag) {
  const auto id = expiration_flag_counter.fetch_add(1, std::memory_order_relaxed);
  {
    std::lock_guard<utils::SpinLock> guard{expiration_flags_lock};
    auto it = std::find_if(expiration_flags.begin(), expiration_flags.end(), IdMatcher{kInvalidId});
    if (expiration_flags.end() != it) {
      *it = ExpirationFlagInfo{id, std::move(flag)};
    } else {
      expiration_flags.push_back(ExpirationFlagInfo{id, std::move(flag)});
    }
  }
  return id;
}

std::weak_ptr<std::atomic<bool>> EraseFlag(uint64_t flag_id) {
  std::weak_ptr<std::atomic<bool>> tmp;
  {
    std::lock_guard<utils::SpinLock> guard{expiration_flags_lock};
    auto it = std::find_if(expiration_flags.begin(), expiration_flags.end(), IdMatcher{flag_id});
    if (expiration_flags.end() != it) {
      it->id = kInvalidId;
      it->flag.swap(tmp);
    }
  }
  return tmp;
}

void NotifyFunction(sigval arg) {
  const auto flag_id = reinterpret_cast<uint64_t>(arg.sival_ptr);
  auto weak_flag = EraseFlag(flag_id);
  if (!weak_flag.expired()) {
    auto flag = weak_flag.lock();
    if (nullptr != flag) {
      flag->store(true, std::memory_order_relaxed);
    }
  }
}
}  // namespace

namespace utils {

AsyncTimer::AsyncTimer(int seconds) : expiration_flag_{std::make_shared<std::atomic<bool>>(false)} {
  expiration_flag_id_ = AddFlag(std::weak_ptr{expiration_flag_});
  sigevent notification_settings{};
  notification_settings.sigev_notify = SIGEV_THREAD;
  notification_settings.sigev_notify_function = &NotifyFunction;
  static_assert(sizeof(void *) == sizeof(decltype(expiration_flag_id_)), "ID size must be equal to pointer size!");
  notification_settings.sigev_value.sival_ptr = reinterpret_cast<void *>(expiration_flag_id_);
  MG_ASSERT(timer_create(CLOCK_MONOTONIC, &notification_settings, &timer_id_) == 0, "Couldn't create timer: ({}) {}",
            errno, strerror(errno));

  struct itimerspec spec;
  spec.it_interval.tv_sec = 0;
  spec.it_interval.tv_nsec = 0;
  spec.it_value.tv_sec = static_cast<time_t>(seconds);
  spec.it_value.tv_nsec = 0;
  MG_ASSERT(timer_settime(timer_id_, 0, &spec, nullptr) == 0, "Couldn't set timer: ({}) {}", errno, strerror(errno));
}

AsyncTimer::~AsyncTimer() {
  timer_delete(timer_id_);
  EraseFlag(expiration_flag_id_);
}

bool AsyncTimer::IsExpired() const {
  auto value = expiration_flag_->load(std::memory_order_relaxed);
  return value;
}

}  // namespace utils
