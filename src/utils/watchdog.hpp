#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <mutex>
#include <random>
#include <thread>

/**
 * @brief - Keeps track of how long it's been since `Notify` method was
 * called. If it wasn't called for a sufficiently long time interval (randomly
 * chosen between `min_timeout` and `max_timeout`), the watchdog will
 * periodically call `callback` until it is notified or destroyed.
 */
class Watchdog {
 public:
  Watchdog(const std::chrono::milliseconds &min_timeout,
           const std::chrono::milliseconds &max_timeout,
           const std::function<void()> &callback);
  ~Watchdog();
  Watchdog(Watchdog &&) = delete;
  Watchdog(const Watchdog &) = delete;
  Watchdog &operator=(Watchdog &&) = delete;
  Watchdog &operator=(const Watchdog &) = delete;

  void Notify();

  /** Calling `Block` is equivalent to continuously calling `Notify`
   * until `Unblock` is called.
   */
  void Block();
  void Unblock();

 private:
  void Run();

  std::chrono::milliseconds min_timeout_;
  std::chrono::milliseconds max_timeout_;

  std::mutex mutex_;

  // Used to generate callback timeouts.
  std::mt19937 generator_;
  std::uniform_int_distribution<int> distribution_;
  std::chrono::steady_clock::time_point callback_threshold_;

  std::function<void()> callback_;

  // Used to notify the watchdog loop it should stop.
  std::atomic<bool> draining_;
  std::atomic<bool> blocked_;
  std::thread thread_;
};
