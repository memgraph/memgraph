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
#include <functional>
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

  SimpleBarrier(const SimpleBarrier &) = delete;
  SimpleBarrier &operator=(const SimpleBarrier &) = delete;
  SimpleBarrier(SimpleBarrier &&) = delete;
  SimpleBarrier &operator=(SimpleBarrier &&) = delete;

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

struct yielder {
  void operator()() noexcept {
#if defined(__i386__) || defined(__x86_64__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("YIELD");
#else
#error("no PAUSE/YIELD instructions for unknown architecture");
#endif
    ++count;
    if (count > 1024U) [[unlikely]] {
      count = 0;
      std::this_thread::yield();
    }
  }

 private:
  uint_fast32_t count{0};
};
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
      Worker worker(*this, i);
      // Divide work by each thread
      work_buckets_[i] = &worker;
      barrier.arrive_and_wait();
      worker.operator()<Priority::LOW>();
    });
  }

  for (size_t i = 0; i < high_priority_threads_count; ++i) {
    pool_.emplace_back([this, i, &barrier]() {
      Worker worker(*this, i);
      hp_work_buckets_[i] = &worker;
      barrier.arrive_and_wait();
      worker.operator()<Priority::HIGH>();
    });
  }

  barrier.wait();

  // Under heavy load a task can get stuck, monitor and move to different thread
  // TODO only if has more than one thread
  monitoring_.SetInterval(std::chrono::milliseconds(100));
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
      if (last_task[worker_id] == worker->last_task_ && worker->working_ && worker->has_pending_work_) {
        // worker stuck on a task; move task to a different queue
        auto l = std::unique_lock{worker->mtx_, std::defer_lock};
        if (!l.try_lock()) continue;  // Thread is busy...
        // Recheck under lock
        if (worker->work_.empty() || last_task[worker_id] != worker->last_task_) continue;
        // Update flag as soon as possible
        worker->has_pending_work_.store(worker->work_.size() > 1, std::memory_order_release);
        Worker::Work work{worker->work_.top().id, std::move(worker->work_.top().work)};
        worker->work_.pop();
        l.unlock();

        uint64_t tid = get_hot_thread();
        if (tid >= work_buckets_.size()) {
          // No LP threads available; schedule HP work to HP thread
          if (work.id > std::numeric_limits<int64_t>::max()) {
            static size_t last_hp_thread = 0;
            auto &hp_worker =
                hp_work_buckets_[hp_work_buckets_.size() > 1 ? last_hp_thread++ % hp_work_buckets_.size() : 0];
            if (!hp_worker->has_pending_work_) {
              hp_worker->push(std::move(work.work), work.id);
              continue;
            }
          }
          tid = (worker_id + 1) % work_buckets_.size();
        }
        work_buckets_[tid]->push(std::move(work.work), work.id);
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
  {
    pool_stop_source_.request_stop();

    // Stop monitoring thread before workers
    monitoring_.Stop();

    // Mixed work workers
    for (auto *worker : work_buckets_) {
      worker->stop();
    }

    // High priority workers
    for (auto *worker : hp_work_buckets_) {
      worker->stop();
    }
  }
  pool_.clear();
}

void PriorityThreadPool::ScheduledAddTask(TaskSignature new_task, const Priority priority) {
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    return;
  }
  const auto id = (uint64_t(priority == Priority::HIGH) * std::numeric_limits<int64_t>::max()) +
                  --id_;  // Way to priorities hp tasks (overflow concerns)
  uint64_t tid = get_hot_thread();
  if (tid >= work_buckets_.size()) {
    tid = tid_++ % work_buckets_.size();
  }
  work_buckets_[tid]->push(std::move(new_task), id);
  // High priority tasks are marked and given to mixed priority threads (at front of the queue)
  // HP threads are going to steal this work if not executed in time
}

void PriorityThreadPool::Worker::push(TaskSignature new_task, uint64_t id) {
  {
    auto l = std::unique_lock{mtx_};
    work_.emplace(id, std::move(new_task));
  }
  has_pending_work_ = true;
  cv_.notify_one();
}

void PriorityThreadPool::Worker::stop() {
  {
    auto l = std::unique_lock{mtx_};
    run_ = false;
  }
  cv_.notify_all();
}

template <Priority ThreadPriority>
void PriorityThreadPool::Worker::operator()() {
  utils::ThreadSetName(ThreadPriority == Priority::HIGH ? "high prior." : "low prior.");

  const auto workers = std::invoke([&, ptr = this] {
    auto workers = scheduler_.work_buckets_;
    workers.erase(std::find(workers.begin(), workers.end(), ptr));
    return workers;
  });

  std::optional<TaskSignature> task;

  while (run_.load(std::memory_order_acquire)) {
    // Phase 1 get scheduled work <- cold thread???
    // Phase 2 try to steal and loop <- hot thread
    // Phase 3 spin wait <- hot thread
    // Phase 4 go to sleep <- cold thread

    // Phase 1A - already picked a task, needs to be executed
    if (task) {
      working_.store(true, std::memory_order_release);
      task.value()(ThreadPriority);
      task.reset();
    }
    // Phase 1B - check if there is other scheduled work
    {
      auto l = std::unique_lock{mtx_};
      if (!work_.empty()) {
        has_pending_work_.store(work_.size() > 1, std::memory_order::release);
        last_task_.store(work_.top().id, std::memory_order_release);
        task = std::move(work_.top().work);
        work_.pop();
        continue;  // Spin to phase 1A
      }
    }

    working_.store(false, std::memory_order_release);
    if constexpr (ThreadPriority != Priority::HIGH) {
      scheduler_.set_hot_thread(id_);
    }

    // Phase 2A - try to steal work
    for (auto *worker : workers) {
      if (has_pending_work_.load(std::memory_order_acquire)) break;  // This worker received work

      if (worker->has_pending_work_.load(std::memory_order_acquire) &&
          worker->working_.load(std::memory_order_acquire)) {
        auto l2 = std::unique_lock{worker->mtx_, std::defer_lock};
        if (!l2.try_lock()) continue;  // Busy, skip
        // Re-check under lock
        if (worker->work_.empty()) continue;
        // HP threads can only steal HP work
        if constexpr (ThreadPriority == Priority::HIGH) {
          // If LP work, skip
          if (worker->work_.top().id <= std::numeric_limits<int64_t>::max()) continue;
        }

        // Update flag as soon as possible
        worker->has_pending_work_.store(worker->work_.size() > 1, std::memory_order_release);

        // Move work to current thread
        last_task_.store(worker->work_.top().id, std::memory_order_release);
        task = std::move(worker->work_.top().work);

        worker->work_.pop();

        l2.unlock();
        break;
      }
    }
    // Phase 2B - check results and spin to execute
    if (task) {
      if constexpr (ThreadPriority != Priority::HIGH) {
        scheduler_.reset_hot_thread(id_);
      }
      continue;
    }

    // Phase 3 - spin for a while waiting on work
    {
      const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
      yielder y;
      while (!has_pending_work_.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < end) {
        y();
      }
    }

    // Phase 4 - go to sleep
    if constexpr (ThreadPriority != Priority::HIGH) {
      scheduler_.reset_hot_thread(id_);
    }
    {
      auto l = std::unique_lock{mtx_};
      cv_.wait(l, [this] { return !work_.empty() || !run_; });
    }
  }
}

void PriorityThreadPool::set_hot_thread(const uint64_t id) {
  hot_threads_.fetch_or(1UL << id, std::memory_order::acq_rel);
}
void PriorityThreadPool::reset_hot_thread(const uint64_t id) {
  hot_threads_.fetch_and(~(1UL << id), std::memory_order::acq_rel);
}
int PriorityThreadPool::get_hot_thread() {
  auto hot_threads = hot_threads_.load(std::memory_order::acquire);
  if (hot_threads == 0) return 64;  // Max
  auto id = std::countr_zero(hot_threads);
  auto next_ht = hot_threads & ~(1UL << id);
  while (!hot_threads_.compare_exchange_weak(hot_threads, next_ht, std::memory_order::acq_rel)) {
    if (hot_threads == 0) return 64;
    id = std::countr_zero(hot_threads);
    next_ht = hot_threads & ~(1UL << id);
  }
  reset_hot_thread(id);
  return id;  // TODO This needs to be atomic get/reset
}

}  // namespace memgraph::utils

template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::LOW>();
template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::HIGH>();
