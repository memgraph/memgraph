#pragma once

#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>

#include "threading/sync/lockable.hpp"

/**
 * ThreadPool which will invoke maximum concurrent number of threads for the
 * used hardware and will schedule tasks on thread as they are added to the
 * pool.
 */
class Pool : Lockable<std::mutex> {
 public:
  using task_t = std::function<void()>;
  using sptr = std::shared_ptr<Pool>;

  explicit Pool(size_t n = std::thread::hardware_concurrency()) : alive(true) {
    threads.reserve(n);

    for (size_t i = 0; i < n; ++i)
      threads.emplace_back([this]() -> void { loop(); });
  }

  Pool(Pool &) = delete;
  Pool(Pool &&) = delete;

  ~Pool() {
    {
      // We need to hold the lock before we notify threads because condition
      // variable wait for could read the value of alive as true, then receive a
      // notification and then continue waiting since it read the value as true,
      // that's why we have to force notification to occur while the condition
      // variable doesn't have a lock.
      auto lock = acquire_unique();
      alive.store(false, std::memory_order_seq_cst);
      cond.notify_all();
    }

    for (auto &thread : threads) thread.join();
  }

  /**
   * Runs an asynchronous task.
   * @param f - task to run.
   */
  void run(task_t f) {
    {
      auto lock = acquire_unique();
      tasks.push(f);
    }

    cond.notify_one();
  }

 private:
  std::vector<std::thread> threads;
  std::queue<task_t> tasks;
  std::atomic<bool> alive;

  std::mutex mutex;
  std::condition_variable cond;

  void loop() {
    while (true) {
      task_t task;

      {
        auto lock = acquire_unique();

        cond.wait(lock,
                  [this] { return !this->alive || !this->tasks.empty(); });

        if (!alive && tasks.empty()) return;

        task = std::move(tasks.front());
        tasks.pop();
      }

      // Start the execution of task.
      task();
    }
  }
};
