#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <thread>

#include "utils/assert.hpp"

/**
 * Class used to run scheduled function execution. Class is templated with
 * mutex class TMutex which is used to synchronize threads. Default template
 * value is std::mutex.
 */
template <typename TMutex = std::mutex>
class Scheduler {
 public:
  Scheduler() {}
  /**
   * @param pause - Duration between two function executions. If function is
   * still running when it should be ran again, it will not be ran and next
   * start time will be increased to current time plus pause.
   * @param f - Function
   * @Tparam TRep underlying arithmetic type in duration
   * @Tparam TPeriod duration in seconds between two ticks
   */
  template <typename TRep, typename TPeriod>
  void Run(const std::chrono::duration<TRep, TPeriod> &pause,
           const std::function<void()> &f) {
    debug_assert(is_working_ == false, "Thread already running.");
    debug_assert(pause > std::chrono::seconds(0), "Pause is invalid.");
    is_working_ = true;
    thread_ = std::thread([this, pause, f]() {
      auto start_time = std::chrono::system_clock::now();
      for (;;) {
        if (!is_working_.load()) break;

        f();

        std::unique_lock<std::mutex> lk(mutex_);

        auto now = std::chrono::system_clock::now();
        while (now >= start_time) start_time += pause;

        condition_variable_.wait_for(
            lk, start_time - now, [&] { return is_working_.load() == false; });
        lk.unlock();
      }
    });
  }

  /**
   * @brief Stops the thread execution. This is a blocking call and may take as
   * much time as one call to the function given previously to Run takes.
   */
  void Stop() {
    is_working_.store(false);
    {
      std::unique_lock<std::mutex> lk(mutex_);
      condition_variable_.notify_one();
    }
    if (thread_.joinable()) thread_.join();
  }

  ~Scheduler() { Stop(); }

 private:
  /**
   * Variable is true when thread is running.
   */
  std::atomic<bool> is_working_{false};

  /**
   * Mutex used to synchronize threads using condition variable.
   */
  TMutex mutex_;

  /**
   * Condition variable is used to stop waiting until the end of the
   * time interval if destructor is called.
   */
  std::condition_variable condition_variable_;

  /**
   * Thread which runs function.
   */
  std::thread thread_;
};
