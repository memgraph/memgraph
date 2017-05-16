#pragma once

#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>

#include "threading/pool.hpp"

/**
 * @brief - Singleton class which implements thread pool.
 */
class GlobalPool {
 public:
  // Guaranteed by the C++11 standard to be thread-safe.
  static GlobalPool *getSingletonInstance() {
    static GlobalPool instance;
    return &instance;
  }

  void run(Pool::task_t f) { thread_pool_.run(f); }

  GlobalPool(const GlobalPool &) = delete;
  GlobalPool(const GlobalPool &&) = delete;
  GlobalPool operator=(const GlobalPool &) = delete;
  GlobalPool operator=(const GlobalPool &&) = delete;

 private:
  GlobalPool() {}
  Pool thread_pool_;
};
