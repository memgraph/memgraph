#pragma once
#include <time.h>

#include <memory>

#include "utils/logging.hpp"

namespace utils {

#define SIGTIMER (SIGRTMAX - 2)

class AsyncTimer {
 public:
  AsyncTimer();
  explicit AsyncTimer(double seconds);
  ~AsyncTimer();
  AsyncTimer(AsyncTimer &&other) noexcept;
  // NOLINTNEXTLINE (hicpp-noexcept-move)
  AsyncTimer &operator=(AsyncTimer &&other);

  AsyncTimer(const AsyncTimer &) = delete;
  AsyncTimer &operator=(const AsyncTimer &) = delete;

  // Returns false if the object isn't associated with any timer.
  bool IsExpired() const noexcept;

 private:
  void ReleaseResources();

  // If the expiration_flag_ is nullptr, then the object is not associated with any timer, therefore no clean up
  // is necessary. Furthermore, the the POSIX API doesn't specify any value as "invalid" for timer_t, so the timer_id_
  // cannot be used to determine whether the object is associated with any timer or not.
  std::shared_ptr<std::atomic<bool>> expiration_flag_;
  uint64_t flag_id_;
  timer_t timer_id_;
};
}  // namespace utils
