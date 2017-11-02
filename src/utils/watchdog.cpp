#include "watchdog.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <random>
#include <thread>

#include "glog/logging.h"

using std::chrono::milliseconds;
using std::chrono::steady_clock;

Watchdog::Watchdog(const milliseconds &min_timeout,
                   const milliseconds &max_timeout,
                   const std::function<void()> &callback)
    : min_timeout_(min_timeout),
      max_timeout_(max_timeout),
      generator_(std::random_device{}()),
      distribution_(min_timeout.count(), max_timeout_.count()),
      callback_(callback),
      draining_(false),
      blocked_(false) {
  DCHECK(min_timeout_ <= max_timeout_)
      << "Min timeout should be less than max timeout";
  Notify();
  thread_ = std::thread([this]() { Run(); });
}

Watchdog::~Watchdog() {
  draining_ = true;
  if (thread_.joinable()) {
    thread_.join();
  }
}

void Watchdog::Notify() {
  std::lock_guard<std::mutex> guard(mutex_);
  callback_threshold_ =
      steady_clock::now() + milliseconds(distribution_(generator_));
}

void Watchdog::Block() { blocked_ = true; }

void Watchdog::Unblock() {
  if (blocked_) {
    Notify();
  }
  blocked_ = false;
}

void Watchdog::Run() {
  while (!draining_) {
    steady_clock::time_point t;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      t = callback_threshold_;
    }
    if (steady_clock::now() > t) {
      if (!blocked_) {
        callback_();
      }
      Notify();
    }
    std::this_thread::sleep_until(t);
  }
}
