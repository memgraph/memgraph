// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/priority_thread_pool.hpp"
#include <atomic>
#include <chrono>
#include <limits>
#include <mutex>
#include <random>
#include <thread>

#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/priorities.hpp"

namespace {
// std::barrier seems to have a bug which leads to missed notifications, so some threads block forever
class SimpleBarrier {
 public:
  explicit SimpleBarrier(size_t n) : phase1_{n}, phase2_{0}, final_{n} {}

  ~SimpleBarrier() { wait(); }

  void arrive_and_wait() {
    --phase1_;
    while (phase1_ > 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ++phase2_;
  }

  void wait() {
    while (phase2_ < final_) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

 private:
  std::atomic<size_t> phase1_;
  std::atomic<size_t> phase2_;
  size_t final_;
};

auto get_rand_delay() {
  static thread_local std::random_device r;
  static thread_local std::default_random_engine e1(r());
  static thread_local std::uniform_int_distribution<int> uniform_dist(50, 200);
  return std::chrono::milliseconds(uniform_dist(e1));
}
}  // namespace

namespace memgraph::utils {

PriorityThreadPool::PriorityThreadPool(size_t mixed_work_threads_count, size_t high_priority_threads_count) {
  pool_.reserve(mixed_work_threads_count + high_priority_threads_count);
  work_buckets_.resize(mixed_work_threads_count);
  hp_work_buckets_.resize(high_priority_threads_count);

  const size_t nthreads = mixed_work_threads_count + high_priority_threads_count;
  SimpleBarrier barrier{nthreads};

  for (size_t i = 0; i < mixed_work_threads_count; ++i) {
    pool_.emplace_back([this, i, &barrier]() {
      Worker worker(*this);
      // Divide work by each thread
      work_buckets_[i] = &worker;
      barrier.arrive_and_wait();
      worker.operator()<Priority::LOW>();
    });
  }

  for (size_t i = 0; i < high_priority_threads_count; ++i) {
    pool_.emplace_back([this, i, &barrier]() {
      Worker worker(*this);
      hp_work_buckets_[i] = &worker;
      barrier.arrive_and_wait();
      worker.operator()<Priority::HIGH>();
    });
  }

  barrier.wait();

  // Under heavy load a task can get stuck, monitor and move to different thread
  // TODO only if has more than one thread
  monitoring_.SetInterval(std::chrono::seconds(1));
  monitoring_.Run("sched_mon", [this, last_task = std::invoke([&] {
                                        auto vec = std::vector<uint64_t>{};
                                        vec.resize(work_buckets_.size());
                                        return vec;
                                      })]() mutable {
    // TODO range
    size_t i = 0;
    for (auto *worker : work_buckets_) {
      const auto worker_id = i++;
      auto update = utils::OnScopeExit{[&]() mutable { last_task[worker_id] = worker->last_task_; }};
      if (/*worker->working_ && */ worker->has_pending_work_ && last_task[worker_id] == worker->last_task_) {
        // worker stuck on a task; move task to a different queue

        auto l = std::unique_lock{worker->mtx_, std::defer_lock};
        if (!l.try_lock()) continue;  // Thread is busy...
        // Recheck under lock
        if (worker->work_.empty() || last_task[worker_id] != worker->work_.top().id) continue;
        Worker::Work work{worker->work_.top().id, std::move(worker->work_.top().work)};
        worker->work_.pop();
        l.unlock();

        // TODO how to find the correct worker?
        // Just move to the next queue for now
        const auto next_worker_id = (worker_id + 1) % work_buckets_.size();
        work_buckets_[next_worker_id]->push(std::move(work.work), work.id);
      }
    }
  });
}

PriorityThreadPool::~PriorityThreadPool() {
  if (!pool_stop_source_.stop_requested()) {
    ShutDown();
  }
}

void PriorityThreadPool::ShutDown() {
  monitoring_.Stop();
  {
    auto guard = std::unique_lock{pool_lock_};
    pool_stop_source_.request_stop();

    // Clear the task queue
    high_priority_queue_ = {};
    low_priority_queue_ = {};

    // Stop threads waiting for work
    while (!high_priority_threads_.empty()) {
      high_priority_threads_.top()->stop();
      high_priority_threads_.pop();
    }
    while (!mixed_threads_.empty()) {
      mixed_threads_.top()->stop();
      mixed_threads_.pop();
    }
    // Other threads are working and will stop when they check for next scheduled work

    // WIP
    for (auto *worker : work_buckets_) {
      worker->stop();
    }
    for (auto *worker : hp_work_buckets_) {
      worker->stop();
    }
  }
  pool_.clear();
}

void PriorityThreadPool::ScheduledAddTask(TaskSignature new_task, const Priority priority) {
  // auto l = std::unique_lock{pool_lock_};
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    return;
  }  // Not sure about this

  // const auto this_cpu = sched_getcpu();
  auto tid = tid_++ % work_buckets_.size();

  const auto id =
      (uint64_t(priority == Priority::HIGH) << 63U) + id_--;  // Way to priorities hp tasks (overflow concerns)

  // Add task to current CPU's thread (cheap)
  auto *this_bucket = work_buckets_[tid];
  this_bucket->push(std::move(new_task), id);

  // High priority tasks are marked and given to mixed priority threads (at front of the queue)
  // HP threads are going to steal this work if not executed in time
}

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
thread_local Priority PriorityThreadPool::Worker::priority = Priority::HIGH;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
// thread_local std::priority_queue<PriorityThreadPool::Worker::Work> PriorityThreadPool::Worker::work_{};

void PriorityThreadPool::Worker::push(TaskSignature new_task, uint64_t id) {
  {
    auto l = std::unique_lock{mtx_};
    // std::cout << "Worker " << (void *)this << " got a task " << id << " in " << (void *)&work_ << std::endl;
    // DMG_ASSERT(!task_, "Thread already has a task");
    // task_.emplace(std::move(new_task));
    work_.emplace(id, std::move(new_task));
    has_pending_work_ = true;
  }
  cv_.notify_one();
}

void PriorityThreadPool::Worker::stop() {
  {
    auto l = std::unique_lock{mtx_};
    run_ = false;
  }
  cv_.notify_one();
}

template <Priority ThreadPriority>
void PriorityThreadPool::Worker::operator()() {
  utils::ThreadSetName(ThreadPriority == Priority::HIGH ? "high prior." : "low prior.");
  priority = ThreadPriority;  // Update the visible thread's priority

  if (std::thread::hardware_concurrency() > 1 && pinned_core_ >= 0) {
    pthread_t self = pthread_self();

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(pinned_core_ % std::thread::hardware_concurrency(), &cpuset);
    MG_ASSERT(pthread_setaffinity_np(self, sizeof(cpu_set_t), &cpuset) == 0, "Failed to pin worker core");

    // sched_param param;
    // param.sched_pri::arrive_and_waitority = 75;
    // int rc = pthread_setschedparam(self, SCHED_RR, &param);
    // MG_ASSERT(rc == 0, "Failed to set scheduler priority {}", rc);
  }

  const auto delay = get_rand_delay();

  std::optional<TaskSignature> task;
  std::optional<Work> tmp_work;
  while (run_) {
    // std::cout << "Worker " << (void *)this << " loop run..." << std::endl;
    {
      auto l = std::unique_lock{mtx_};
      if (work_.empty()) {
        // Try to steal work before going to wait
        {
          l.unlock();
          for (auto *worker : scheduler_.work_buckets_) {
            if (has_pending_work_.load(std::memory_order_acquire)) break;  // This worker received work

            if (worker->has_pending_work_.load(std::memory_order_acquire)) {
              auto l2 = std::unique_lock{worker->mtx_, std::defer_lock};
              if (!l2.try_lock()) continue;  // Busy, skip
              // Re-check under lock
              if (worker->work_.empty()) continue;
              // HP threads can only steal HP work
              if constexpr (ThreadPriority == Priority::HIGH) {
                // If LP work, skip
                if (worker->work_.top().id <= std::numeric_limits<int64_t>::max()) continue;
              }
              tmp_work.emplace(worker->work_.top().id, std::move(worker->work_.top().work));
              worker->has_pending_work_.store(worker->work_.size() > 1, std::memory_order_release);
              worker->work_.pop();
              l2.unlock();
              // Move work to current thread
              l.lock();
              work_.emplace(tmp_work->id, std::move(tmp_work->work));
              has_pending_work_.store(work_.size() > 1,
                                      std::memory_order_release);  // > 1 because one will be immediately executed
              break;
            }
          }
          if (!l.owns_lock()) l.lock();
        }

        // Couldn't steal
        if (work_.empty()) {
          // std::cout << "Worker " << (void *)this << " no work in " << (void *)&work_ << ", wait..." << std::endl;
          // Wait for a new task
          cv_.wait_for(l, delay, [this] { return !work_.empty() || !run_; });

          // std::cout << "Worker " << (void *)this << " loop check... " << work_.empty() << std::endl;

          // Just looping
          if (work_.empty()) {
            continue;
          }
        }
      }

      // Stop requested
      if (!run_) [[unlikely]] {
        return;
      }

      // std::cout << "Worker " << (void *)this << " taking task " << work_.top().id << std::endl;
      has_pending_work_.store(work_.size() > 1, std::memory_order::release);
      last_task_.store(work_.top().id, std::memory_order_release);
      task = std::move(work_.top().work);
      work_.pop();
    }
    // std::cout << "Worker " << (void *)this << " executing task..." << std::endl;
    // Execute the task
    working_.store(true, std::memory_order::release);
    task.value()();
    task.reset();
    working_.store(false, std::memory_order::release);
  }
}

}  // namespace memgraph::utils

template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::LOW>();
template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::HIGH>();
