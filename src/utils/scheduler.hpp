#include <atomic>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <thread>

/**
 * Class used to run given std::function<void()>. Function is ran and then sleep
 * is called with parameter given in class constructor. Class is templated with
 * mutex class TMutex which is used to synchronize threads. Default template
 * value is std::mutex.
 */
template <typename TMutex = std::mutex>
class Scheduler {
 public:
  /**
   * @param t - Time between executing function. If function is still running
   *            when it should be ran again, it will not be ran and next
   *            start time will be increased current time plus t. If t equals
   *            -1, scheduler will not run.
   * @param f - Function which will be executed.
   */
  Scheduler(const std::chrono::seconds &t, const std::function<void()> &f)
      : pause_(t), func_(f) {
    thread_ = std::thread([this]() {
      // if pause_ equals -1, scheduler will not run
      if (pause_ == std::chrono::seconds(-1)) return;
      auto start_time = std::chrono::system_clock::now();
      for (;;) {
        if (!is_working_.load()) break;

        func_();

        std::unique_lock<std::mutex> lk(mutex_);

        auto now = std::chrono::system_clock::now();
        while (now >= start_time) start_time += pause_;

        condition_variable_.wait_for(
            lk, start_time - now, [&] { return is_working_.load() == false; });
        lk.unlock();
      }
    });
  }

  ~Scheduler() {
    is_working_.store(false);
    {
      std::unique_lock<std::mutex> lk(mutex_);
      condition_variable_.notify_one();
    }
    if (thread_.joinable()) thread_.join();
  }

 private:
  /**
   * Variable is true when thread is running.
   */
  std::atomic<bool> is_working_{true};
  /**
   * Time interval between function execution.
   */
  const std::chrono::seconds pause_;
  /**
   * Function which scheduler executes.
   */
  const std::function<void()> func_;
  /**
   * Mutex used to synchronize threads using condition variable.
   */
  TMutex mutex_;

  /**
   * Condition variable is used to stop waiting until the end of the
   * time interval if destructor is  called.
   */
  std::condition_variable condition_variable_;
  /**
   * Thread which runs function.
   */
  std::thread thread_;
};
