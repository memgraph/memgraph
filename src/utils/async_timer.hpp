#pragma once

#include <time.h>

#include <memory>

#include "utils/logging.hpp"

namespace utils {

class AsyncTimer {
 public:
  AsyncTimer(int seconds);
  ~AsyncTimer();

  AsyncTimer(const AsyncTimer &) = delete;
  AsyncTimer(AsyncTimer &&) = delete;
  AsyncTimer &operator==(const AsyncTimer &) = delete;
  AsyncTimer &operator==(AsyncTimer &&) = delete;

  bool IsExpired() const;

 private:
  std::shared_ptr<std::atomic<bool>> expiration_flag_;
  uint64_t expiration_flag_id_{0U};
  timer_t timer_id_;
};
}  // namespace utils
