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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <mutex>
#include <thread>

#include "utils/barrier.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/priorities.hpp"
#include "utils/thread.hpp"
#include "utils/yielder.hpp"

namespace {
constexpr memgraph::utils::PriorityThreadPool::TaskID kMaxLowPriorityId = std::numeric_limits<int64_t>::max();
constexpr memgraph::utils::PriorityThreadPool::TaskID kMinHighPriorityId = kMaxLowPriorityId;
constexpr uint16_t kMaxWorkers = memgraph::utils::HotMask::kMaxElements;
}  // namespace

namespace memgraph::utils {

struct TmpHotElement {
  uint16_t id;
  uint64_t new_mask;

  static inline TmpHotElement Get(uint64_t state) {
    uint16_t hot_id = std::countr_zero(state);      // Get first hot thread in group
    uint64_t new_state = state & ~(1UL << hot_id);  // Update group to reflect thread reservation
    return {hot_id, new_state};
  }
};

std::optional<uint16_t> HotMask::GetHotElement() {
  // Go through all groups and check
  for (size_t group_i = 0; group_i < n_groups_; ++group_i) {
    // Get group and check if there are any hot elements
    auto &group = hot_masks_[group_i];
    auto group_mask = group.load(std::memory_order::acquire);
    // No hot thread in this group
    if (group_mask == 0) continue;
    auto res = TmpHotElement::Get(group_mask);
    while (!group.compare_exchange_weak(group_mask, res.new_mask, std::memory_order::acq_rel)) {
      // Failed to update state; either cew failed or state changed | re-read group info
      if (group_mask == 0) break;  // No hot thread in this group
      res = TmpHotElement::Get(group_mask);
    }
    // Successfully updated the state | check if any hot element was available
    if (group_mask != 0) return res.id + (group_i * kGroupSize);
  }
  // None found
  return {};
}

PriorityThreadPool::PriorityThreadPool(uint16_t mixed_work_threads_count, uint16_t high_priority_threads_count)
    : hot_threads_{mixed_work_threads_count}, task_id_{kMaxLowPriorityId}, last_wid_{0} {
  MG_ASSERT(mixed_work_threads_count > 0, "PriorityThreadPool requires at least one mixed work thread");
  MG_ASSERT(mixed_work_threads_count <= kMaxWorkers,
            "PriorityThreadPool supports a maximum of 1024 mixed work threads");
  MG_ASSERT(high_priority_threads_count > 0, "PriorityThreadPool requires at least one high priority work thread");

  pool_.reserve(mixed_work_threads_count + high_priority_threads_count);
  work_buckets_.resize(mixed_work_threads_count);
  hp_work_buckets_.resize(high_priority_threads_count);

  const size_t nthreads = mixed_work_threads_count + high_priority_threads_count;
  SimpleBarrier barrier{nthreads};

  for (size_t i = 0; i < mixed_work_threads_count; ++i) {
    pool_.emplace_back([this, i, &barrier]() {
      Worker worker;
      // Divide work by each thread
      work_buckets_[i] = &worker;
      barrier.arrive_and_wait();
      worker.operator()<Priority::LOW>(i, work_buckets_, hot_threads_);
    });
  }

  for (size_t i = 0; i < high_priority_threads_count; ++i) {
    pool_.emplace_back([this, i, &barrier]() {
      Worker worker;
      hp_work_buckets_[i] = &worker;
      barrier.arrive_and_wait();
      worker.operator()<Priority::HIGH>(i, work_buckets_, hot_threads_);
    });
  }

  barrier.wait();

  // Under heavy load a task can get stuck, monitor and move to different thread
  // TODO only if has more than one thread
  monitoring_.SetInterval(std::chrono::milliseconds(100));
  monitoring_.Run(
      "sched_mon", [this, workers_num = work_buckets_.size(), hp_workers_num = hp_work_buckets_.size()]() mutable {
        // TODO range
        size_t i = 0;
        std::array<TaskID, kMaxWorkers> last_task{};
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

            auto tid = hot_threads_.GetHotElement();
            if (!tid) {
              // No LP threads available; schedule HP work to HP thread
              if (work.id > kMinHighPriorityId) {
                static size_t last_hp_thread = 0;
                auto &hp_worker = hp_work_buckets_[hp_workers_num > 1 ? last_hp_thread++ % hp_workers_num : 0];
                if (!hp_worker->has_pending_work_) {
                  hp_worker->push(std::move(work.work), work.id);
                  continue;
                }
              }
              // No hot thread and no low priority work, schedule to the next lp worker
              tid = (worker_id + 1) % workers_num;
            }
            work_buckets_[*tid]->push(std::move(work.work), work.id);
          }
        }
      });
}

PriorityThreadPool::~PriorityThreadPool() {
  if (!pool_stop_source_.stop_requested()) {
    ShutDown();
  }
}

void PriorityThreadPool::AwaitShutdown() { pool_.clear(); }

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
}

void PriorityThreadPool::ScheduledAddTask(TaskSignature new_task, const Priority priority) {
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    return;
  }
  const auto id = (TaskID(priority == Priority::HIGH) * kMinHighPriorityId) +
                  --task_id_;  // Way to priorities hp tasks (overflow concerns)
  auto tid = hot_threads_.GetHotElement();
  if (!tid) {
    // Limit the number of directly used threads when there are more workers than hw threads.
    // Gives better overall performance.
    static const auto max_wakeup_thread =
        std::min(static_cast<TaskID>(std::thread::hardware_concurrency()), work_buckets_.size());
    // If no hot thread found, give it to the next thread
    tid = last_wid_++ % max_wakeup_thread;
  }
  work_buckets_[*tid]->push(std::move(new_task), id);
  // High priority tasks are marked and given to mixed priority threads (at front of the queue)
  // HP threads are going to steal this work if not executed in time
}

void PriorityThreadPool::Worker::push(TaskSignature new_task, TaskID id) {
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
  cv_.notify_one();
}

template <Priority ThreadPriority>
void PriorityThreadPool::Worker::operator()(const uint16_t worker_id, std::vector<Worker *> workers_pool,
                                            HotMask &hot_threads) {
  utils::ThreadSetName(ThreadPriority == Priority::HIGH ? "high prior." : "low prior.");

  // Both mixed and high priority worker only steal from mixed worker
  const auto other_workers = std::invoke([workers_pool = std::move(workers_pool), ptr = this]() mutable {
    workers_pool.erase(std::find(workers_pool.begin(), workers_pool.end(), ptr));
    return workers_pool;
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
      hot_threads.Set(worker_id);
    }

    // Phase 2A - try to steal work
    for (auto *worker : other_workers) {
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
          if (worker->work_.top().id <= kMaxLowPriorityId) continue;
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
        hot_threads.Reset(worker_id);
      }
      continue;
    }

    // Phase 3 - spin for a while waiting on work
    {
      const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(1);
      yielder y;  // NOLINT (misc-const-correctness)
      while (std::chrono::steady_clock::now() < end) {
        if (y([this] { return has_pending_work_.load(std::memory_order_acquire); }, 1024U, 0U)) break;
      }
    }

    // Phase 4 - go to sleep
    if constexpr (ThreadPriority != Priority::HIGH) {
      hot_threads.Reset(worker_id);
    }
    {
      auto l = std::unique_lock{mtx_};
      cv_.wait(l, [this] { return !work_.empty() || !run_; });
    }
  }
}

}  // namespace memgraph::utils

template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::LOW>(
    uint16_t worker_id, std::vector<memgraph::utils::PriorityThreadPool::Worker *>,
    memgraph::utils::HotMask &hot_threads);
template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::HIGH>(
    uint16_t worker_id, std::vector<memgraph::utils::PriorityThreadPool::Worker *>,
    memgraph::utils::HotMask &hot_threads);
