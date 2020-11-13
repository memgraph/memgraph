#include "utils/thread_pool.hpp"

namespace utils {

ThreadPool::ThreadPool(const size_t pool_size) {
  for (size_t i = 0; i < pool_size; ++i) {
    thread_pool_.emplace_back(([this] { this->ThreadLoop(); }));
  }
}

void ThreadPool::AddTask(std::function<void()> new_task) {
  task_queue_.WithLock([&](auto &queue) {
    queue.emplace(std::make_unique<TaskSignature>(std::move(new_task)));
  });
  queue_cv_.notify_one();
}

void ThreadPool::Shutdown() {
  terminate_pool_.store(true);
  queue_cv_.notify_all();

  for (auto &thread : thread_pool_) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  thread_pool_.clear();
  stopped_.store(true);
}

ThreadPool::~ThreadPool() {
  if (!stopped_.load()) {
    Shutdown();
  }
}

std::unique_ptr<ThreadPool::TaskSignature> ThreadPool::PopTask() {
  return task_queue_.WithLock(
      [](auto &queue) -> std::unique_ptr<TaskSignature> {
        if (queue.empty()) {
          return nullptr;
        }
        auto front = std::move(queue.front());
        queue.pop();
        return front;
      });
}

void ThreadPool::ThreadLoop() {
  std::unique_ptr<TaskSignature> task = PopTask();
  while (true) {
    while (task) {
      if (terminate_pool_.load()) {
        return;
      }
      (*task)();
      task = PopTask();
    }

    std::unique_lock guard(pool_lock_);
    idle_thread_num_.fetch_add(1);
    queue_cv_.wait(guard, [&] {
      task = PopTask();
      return task || terminate_pool_.load();
    });
    idle_thread_num_.fetch_sub(1);
    if (terminate_pool_.load()) {
      return;
    }
  }
}

size_t ThreadPool::IdleThreadNum() const { return idle_thread_num_.load(); }

}  // namespace utils
