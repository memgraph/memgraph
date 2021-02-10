#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>

#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/thread.hpp"

namespace utils {

class ThreadPool {
  using TaskSignature = std::function<void()>;

 public:
  explicit ThreadPool(size_t pool_size);

  void AddTask(std::function<void()> new_task);

  void Shutdown();

  ~ThreadPool();

  ThreadPool(const ThreadPool &) = delete;
  ThreadPool(ThreadPool &&) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;
  ThreadPool &operator=(ThreadPool &&) = delete;

  size_t UnfinishedTasksNum() const;

 private:
  std::unique_ptr<TaskSignature> PopTask();

  void ThreadLoop();

  std::vector<std::thread> thread_pool_;

  std::atomic<size_t> unfinished_tasks_num_{0};
  std::atomic<bool> terminate_pool_{false};
  std::atomic<bool> stopped_{false};
  utils::Synchronized<std::queue<std::unique_ptr<TaskSignature>>,
                      utils::SpinLock>
      task_queue_;
  std::mutex pool_lock_;
  std::condition_variable queue_cv_;
};

}  // namespace utils
