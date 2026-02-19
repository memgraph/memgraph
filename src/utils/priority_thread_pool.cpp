// Copyright 2026 Memgraph Ltd.
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
#include <memory>
#include <mutex>
#include <thread>

#include "utils/barrier.hpp"
#include "utils/logging.hpp"
#include "utils/priorities.hpp"
#include "utils/thread.hpp"
#include "utils/tsc.hpp"
#include "utils/yielder.hpp"

namespace {
constexpr memgraph::utils::PriorityThreadPool::TaskID kMaxLowPriorityId = std::numeric_limits<int64_t>::max();
constexpr memgraph::utils::PriorityThreadPool::TaskID kMinHighPriorityId = kMaxLowPriorityId;
constexpr uint16_t kMaxWorkers = memgraph::utils::HotMask::kMaxElements;
}  // namespace

namespace memgraph::utils {

struct TmpHotElement {
  uint8_t id;
  uint64_t new_mask;

  static TmpHotElement Get(uint64_t state) {
    uint8_t hot_id = std::countr_zero(state);       // Get first hot thread in group
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

PriorityThreadPool::PriorityThreadPool(uint16_t work_threads_count, ThreadInitCallback thread_init_callback,
                                       WorkerYieldRegistry *yield_registry)
    : hot_threads_{work_threads_count}, task_id_{kMaxLowPriorityId}, yield_registry_{yield_registry} {
  MG_ASSERT(work_threads_count > 0, "PriorityThreadPool requires at least one mixed work thread");
  MG_ASSERT(work_threads_count <= kMaxWorkers, "PriorityThreadPool supports a maximum of 1024 mixed work threads");

  pool_.reserve(work_threads_count);
  workers_.resize(work_threads_count);

  SimpleBarrier barrier{work_threads_count};

  for (size_t i = 0; i < work_threads_count; ++i) {
    pool_.emplace_back([this, i, &barrier, thread_init_callback]() {
      // Divide work by each thread
      workers_[i] = std::make_unique<Worker>();
      barrier.arrive_and_wait();
      // Call user-defined thread initialization callback (e.g., to register with Python interpreter)
      if (thread_init_callback) {
        thread_init_callback();
      }
      workers_[i]->operator()<Priority::LOW>(i, workers_, hot_threads_, yield_registry_);
    });
  }

  barrier.wait();

  // // Under heavy load a task can get stuck, monitor and move to different thread
  // monitoring_.SetInterval(std::chrono::milliseconds(100));
  // monitoring_.Run("sched_mon",
  //                 [this,
  //                  workers_num = workers_.size(),
  //                  hp_workers_num = hp_workers_.size(),
  //                  last_task = std::array<TaskID, kMaxWorkers>{}]() mutable {
  //                   size_t i = 0;
  //                   for (auto &worker : workers_) {
  //                     const auto worker_id = i++;
  //                     auto &worker_last_task = last_task[worker_id];
  //                     auto update = utils::OnScopeExit{[&]() mutable { worker_last_task = worker->last_task_; }};
  //                     if (worker_last_task == worker->last_task_ && worker->working_ && worker->has_pending_work_) {
  //                       // worker stuck on a task; move task to a different queue
  //                       auto l = std::unique_lock{worker->mtx_, std::defer_lock};
  //                       if (!l.try_lock()) continue;  // Thread is busy...
  //                       // Recheck under lock
  //                       if (worker->work_.empty() || worker_last_task != worker->last_task_) continue;
  //                       // Update flag as soon as possible
  //                       worker->has_pending_work_.store(worker->work_.size() > 1, std::memory_order_release);
  //                       Worker::Work work{.id = worker->work_.top().id, .work = std::move(worker->work_.top().work)};
  //                       worker->work_.pop();
  //                       l.unlock();

  //                       auto tid = hot_threads_.GetHotElement();
  //                       if (!tid) {
  //                         // No hot LP threads available; schedule HP work to HP thread
  //                         if (work.id > kMinHighPriorityId) {
  //                           static size_t last_hp_thread = 0;
  //                           auto &hp_worker = hp_workers_[hp_workers_num > 1 ? last_hp_thread++ % hp_workers_num :
  //                           0]; if (!hp_worker->has_pending_work_) {
  //                             hp_worker->push(std::move(work.work), work.id);
  //                             continue;
  //                           }
  //                         }
  //                         // No hot thread and low priority work, schedule to the next lp worker
  //                         tid = (worker_id + 1) % workers_num;
  //                       }
  //                       workers_[*tid]->push(std::move(work.work), work.id);
  //                     }
  //                   }
  //                 });
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
    // Workers
    for (auto &worker : workers_) {
      worker->stop();
    }
  }
}

void PriorityThreadPool::ScheduledAddTask(TaskSignature new_task, const Priority priority) {
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    return;
  }
  const auto id = (TaskID(priority == Priority::HIGH) * kMinHighPriorityId) +
                  task_id_.fetch_sub(1, std::memory_order_acq_rel);  // Way to priorities hp tasks and older tasks
  auto tid = hot_threads_.GetHotElement();
  if (!tid) {
    static thread_local size_t last_wid = 0;
    // Limit the number of directly used threads when there are more workers than hw threads.
    // Gives better overall performance.
    static const auto max_wakeup_thread =
        std::max(1UL, std::min(static_cast<TaskID>(std::thread::hardware_concurrency()), workers_.size()));
    // If no hot thread found, give it to the next thread
    tid = last_wid++ % max_wakeup_thread;
  }
  workers_[*tid]->push(std::move(new_task), id);
  // High priority tasks are marked and given to mixed priority threads (at front of the queue)
  // HP threads are going to steal this work if not executed in time
}

// Rescheduling: use the target worker's current task ID (last_task_) when set, so the
// continuation is the same logical task (e.g. after yield). Pin the task to this worker
// so it is not stealable by others.
void PriorityThreadPool::RescheduleTaskOnWorker(uint16_t worker_id, TaskSignature task, const Priority priority) {
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    return;
  }
  DMG_ASSERT(
      worker_id < workers_.size(), "worker_id {} out of range (num mixed workers {})", worker_id, workers_.size());
  Worker *const w = workers_[worker_id].get();
  const TaskID current_task = w->last_task_.load(std::memory_order_acquire);
  const TaskID id = (current_task != 0) ? current_task
                                        : (TaskID(priority == Priority::HIGH) * kMinHighPriorityId) +
                                              task_id_.fetch_sub(1, std::memory_order_acq_rel);
  w->push(std::move(task), id, /*pinned=*/true);
}

void PriorityThreadPool::Worker::push(TaskSignature new_task, TaskID id, bool pinned) {
  {
    auto l = std::unique_lock{mtx_};
    Work w{id, std::move(new_task), pinned};
    (pinned ? work_pinned_ : work_).push(std::move(w));
    // TODO thing about atomic ordering and if this can be missed or requested when not needed
    if (id > kMaxLowPriorityId && yield_registry_) {
      DMG_ASSERT(worker_id_ < yield_registry_->MaxWorkers(),
                 "worker_id {} out of range (max {})",
                 worker_id_,
                 yield_registry_->MaxWorkers());
      yield_registry_->RequestYieldForWorker(worker_id_);
    }
  }
  has_pending_work_.store(true, std::memory_order_release);
  cv_.notify_one();
}

void PriorityThreadPool::Worker::stop() {
  {
    auto l = std::unique_lock{mtx_};
    run_.store(false, std::memory_order_release);
  }
  cv_.notify_one();
}

template <Priority ThreadPriority>
void PriorityThreadPool::Worker::operator()(const uint16_t worker_id,
                                            const std::vector<std::unique_ptr<Worker>> &workers_pool,
                                            HotMask &hot_threads, WorkerYieldRegistry *yield_registry) {
  utils::ThreadSetName("worker");

  yield_registry_ = yield_registry;
  worker_id_ = worker_id;
  if (yield_registry) {
    DMG_ASSERT(worker_id < yield_registry->MaxWorkers(),
               "worker_id {} out of range (max {})",
               worker_id,
               yield_registry->MaxWorkers());
    // This is all TLS, so no need to update or lock
    yield_registry->SetCurrentWorker(worker_id);
  }

  // Both mixed and high priority worker only steal from mixed worker
  const auto other_workers = std::invoke([&workers_pool, self = this, worker_id]() -> std::vector<Worker *> {
    // Only mixed work threads can have work stolen, workers_pool does not contain hp threads (skip self)
    const auto other_workers_size = workers_pool.size() - 1;
    if (other_workers_size == 0) return {};
    std::vector<Worker *> other_workers(other_workers_size, nullptr);
    size_t i = other_workers_size - worker_id;  // Optimization to mix thread stealing between workers
    for (const auto &worker : workers_pool) {
      if (worker.get() == self) continue;
      other_workers[i % other_workers_size] = worker.get();
      ++i;
    }
    return other_workers;
  });

  std::optional<TaskSignature> task;
  auto pop_task = [&] {
    // Prefer higher-priority task (larger id) from either queue
    const bool use_pinned = !work_pinned_.empty() && (work_.empty() || work_pinned_.top().id > work_.top().id);
    auto &q = use_pinned ? work_pinned_ : work_;
    has_pending_work_.store(work_pinned_.size() + work_.size() > 1, std::memory_order::release);
    last_task_.store(q.top().id, std::memory_order_release);
    task = std::move(const_cast<Work &>(q.top()).work);
    q.pop();
  };

  while (run_.load(std::memory_order_acquire)) {
    // Phase 1 get scheduled work <- cold thread???
    // Phase 2 try to steal and loop <- hot thread
    // Phase 3 spin wait <- hot thread
    // Phase 4 go to sleep <- cold thread

    // Phase 1A - already picked a task, needs to be executed
    if (task) {
      // TODO Think about the ordereding of this and the yield request
      working_.store(true, std::memory_order_release);
      if (yield_registry) {
        memgraph::utils::WorkerYieldRegistry::ClearYieldForCurrentWorker();
      }
      task.value()(Priority::LOW);  // TODO Remove thread priority from task
      task.reset();
      working_.store(false, std::memory_order_release);
    }
    // Phase 1B - check if there is other scheduled work
    {
      auto l = std::unique_lock{mtx_};
      if (!work_.empty() || !work_pinned_.empty()) {
        pop_task();
        continue;  // Spin to phase 1A
      }
    }

    hot_threads.Set(worker_id);

    // Phase 2A - try to steal work
    for (auto *worker : other_workers) {
      if (has_pending_work_.load(std::memory_order_acquire)) break;  // This worker received work

      if (worker->has_pending_work_.load(std::memory_order_acquire) &&
          worker->working_.load(std::memory_order_acquire)) {
        auto l2 = std::unique_lock{worker->mtx_, std::defer_lock};
        if (!l2.try_lock()) continue;  // Busy, skip
        // Steal only from stealable queue (work_); work_pinned_ is never stolen
        if (worker->work_.empty()) continue;

        worker->has_pending_work_.store(worker->work_pinned_.size() + worker->work_.size() > 1,
                                        std::memory_order_release);
        last_task_.store(worker->work_.top().id, std::memory_order_release);
        task = std::move(const_cast<Worker::Work &>(worker->work_.top()).work);
        worker->work_.pop();

        l2.unlock();
        break;
      }
    }
    if (task) {
      hot_threads.Reset(worker_id);
      continue;
    }

    // Phase 3 - spin for a while waiting on work (available only if TSC is available)
    const auto freq = utils::GetTSCFrequency();
    if (freq) {
      const utils::TSCTimer timer{freq};
      yielder y;                         // NOLINT (misc-const-correctness)
      while (timer.Elapsed() < 0.001) {  // 1ms
        if (y([this] { return has_pending_work_.load(std::memory_order_acquire); }, 1024U, 0U)) break;
      }
    }

    hot_threads.Reset(worker_id);
    // Phase 4 - check if work available (sleep or spin)
    {
      auto l = std::unique_lock{mtx_};
      cv_.wait(l, [this, &pop_task] {
        // Under lock, check if there is work waiting
        if (!work_.empty() || !work_pinned_.empty()) {
          pop_task();
          return true;  // Spin to phase 1A and execute task
        }
        return !run_;  // Return and shutdown
      });
    }
  }
}

// Prepares task for safe scheduling
TaskSignature TaskCollection::WrapTask(size_t index) {
  auto &task = tasks_[index];
  return [&task = task.task_, state = task.state_](utils::Priority priority) {
    auto expected = Task::State::IDLE;
    if (!state->compare_exchange_strong(expected, Task::State::SCHEDULED, std::memory_order_acq_rel)) {
      return;  // Task already scheduled
    }

    try {
      task(priority);
      state->store(Task::State::FINISHED, std::memory_order_release);
      state->notify_one();  // Notify waiting threads
    } catch (...) {
      state->store(Task::State::FINISHED, std::memory_order_release);
      state->notify_one();  // Notify even on exception
      throw;
    }
  };
}

void TaskCollection::Wait() {
  for (auto &task : tasks_) {
    auto expected = task.state_->load(std::memory_order_acquire);
    while (expected != Task::State::FINISHED) {
      task.state_->wait(expected, std::memory_order_acquire);
      expected = task.state_->load(std::memory_order_acquire);
    }
  }
}

void TaskCollection::WaitOrSteal() {
  // Phase 1 - steal tasks that are not scheduled
  for (auto &task : tasks_) {
    auto expected = Task::State::IDLE;
    if (task.state_->compare_exchange_strong(expected, Task::State::SCHEDULED, std::memory_order_acq_rel)) {
      try {
        task.task_(Priority::LOW);
        task.state_->store(Task::State::FINISHED, std::memory_order_release);
        task.state_->notify_one();  // Notify waiting threads
      } catch (...) {
        task.state_->store(Task::State::FINISHED, std::memory_order_release);
        task.state_->notify_one();  // Notify even on exception
        throw;
      }
    }
  }
  // Phase 2 - wait for tasks to finish
  Wait();
}

}  // namespace memgraph::utils

template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::LOW>(
    uint16_t worker_id, const std::vector<std::unique_ptr<memgraph::utils::PriorityThreadPool::Worker>> &,
    memgraph::utils::HotMask &hot_threads, memgraph::utils::WorkerYieldRegistry *yield_registry);
template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::HIGH>(
    uint16_t worker_id, const std::vector<std::unique_ptr<memgraph::utils::PriorityThreadPool::Worker>> &,
    memgraph::utils::HotMask &hot_threads, memgraph::utils::WorkerYieldRegistry *yield_registry);
