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
#include "utils/on_scope_exit.hpp"
#include "utils/priorities.hpp"
#include "utils/system_info.hpp"
#include "utils/thread.hpp"
#include "utils/tsc.hpp"
#include "utils/yielder.hpp"

namespace {
constexpr memgraph::utils::PriorityThreadPool::TaskID kMaxLowPriorityId = std::numeric_limits<int64_t>::max();
constexpr memgraph::utils::PriorityThreadPool::TaskID kMinHighPriorityId = kMaxLowPriorityId;
constexpr uint16_t kMaxWorkers = memgraph::utils::HotMask::kMaxElements;
}  // namespace

namespace memgraph::utils {

namespace {
// LP-worker-only TLS (contract in the header); never populated for HP workers.
thread_local std::optional<size_t> tls_current_worker_id;
}  // namespace

void SetCurrentWorker(size_t worker_id) { tls_current_worker_id = worker_id; }

std::optional<size_t> GetCurrentWorkerId() { return tls_current_worker_id; }

void ClearCurrentWorker() { tls_current_worker_id = std::nullopt; }

struct TmpHotElement {
  uint8_t id;
  uint64_t new_mask;

  static inline TmpHotElement Get(uint64_t state) {
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

PriorityThreadPool::PriorityThreadPool(uint16_t mixed_work_threads_count, uint16_t high_priority_threads_count,
                                       ThreadInitCallback thread_init_callback)
    : hot_threads_{mixed_work_threads_count}, task_id_{kMaxLowPriorityId}, last_wid_{0} {
  MG_ASSERT(mixed_work_threads_count > 0, "PriorityThreadPool requires at least one mixed work thread");
  MG_ASSERT(mixed_work_threads_count <= kMaxWorkers,
            "PriorityThreadPool supports a maximum of 1024 mixed work threads");
  MG_ASSERT(high_priority_threads_count > 0, "PriorityThreadPool requires at least one high priority work thread");

  pool_.reserve(mixed_work_threads_count + high_priority_threads_count);
  workers_.resize(mixed_work_threads_count);
  hp_workers_.resize(high_priority_threads_count);

  const size_t nthreads = mixed_work_threads_count + high_priority_threads_count;
  SimpleBarrier barrier{nthreads};

  for (size_t i = 0; i < mixed_work_threads_count; ++i) {
    pool_.emplace_back([this, i, &barrier, thread_init_callback]() {
      // Divide work by each thread
      workers_[i] = std::make_unique<Worker>();
      barrier.arrive_and_wait();
      // Call user-defined thread initialization callback (e.g., to register with Python interpreter)
      if (thread_init_callback) {
        thread_init_callback();
      }
      workers_[i]->operator()<Priority::LOW>(i, workers_, hot_threads_);
    });
  }

  for (size_t i = 0; i < high_priority_threads_count; ++i) {
    pool_.emplace_back([this, i, &barrier, thread_init_callback]() {
      hp_workers_[i] = std::make_unique<Worker>();
      barrier.arrive_and_wait();
      // Call user-defined thread initialization callback (e.g., to register with Python interpreter)
      if (thread_init_callback) {
        thread_init_callback();
      }
      hp_workers_[i]->operator()<Priority::HIGH>(i, workers_, hot_threads_);
    });
  }

  barrier.wait();

  // Under heavy load a task can get stuck, monitor and move to different thread
  monitoring_.SetInterval(std::chrono::milliseconds(100));
  monitoring_.Run("sched_mon",
                  [this,
                   workers_num = workers_.size(),
                   hp_workers_num = hp_workers_.size(),
                   last_task = std::array<TaskID, kMaxWorkers>{}]() mutable {
                    size_t i = 0;
                    for (auto &worker : workers_) {
                      const auto worker_id = i++;
                      auto &worker_last_task = last_task[worker_id];
                      auto update = utils::OnScopeExit{[&]() mutable { worker_last_task = worker->last_task_; }};
                      if (worker_last_task == worker->last_task_ && worker->working_ && worker->has_pending_work_) {
                        // worker stuck on a task; move task to a different queue
                        auto l = std::unique_lock{worker->mtx_, std::defer_lock};
                        if (!l.try_lock()) continue;  // Thread is busy...
                        // Recheck under lock — only work_; work_pinned_ is invisible to sched_mon.
                        if (worker->work_.empty() || worker_last_task != worker->last_task_) continue;
                        // Update flag as soon as possible (account for both queues)
                        worker->has_pending_work_.store(worker->work_.size() + worker->work_pinned_.size() > 1,
                                                        std::memory_order_release);
                        Worker::Work work{.id = worker->work_.top().id, .work = std::move(worker->work_.top().work)};
                        worker->work_.pop();
                        l.unlock();

                        auto tid = hot_threads_.GetHotElement();
                        if (!tid) {
                          // No hot LP threads available; schedule HP work to HP thread
                          if (work.id > kMinHighPriorityId) {
                            static size_t last_hp_thread = 0;
                            auto &hp_worker = hp_workers_[hp_workers_num > 1 ? last_hp_thread++ % hp_workers_num : 0];
                            if (!hp_worker->has_pending_work_) {
                              hp_worker->push(std::move(work.work), work.id);
                              continue;
                            }
                          }
                          // No hot thread and low priority work, schedule to the next lp worker
                          tid = (worker_id + 1) % workers_num;
                        }
                        workers_[*tid]->push(std::move(work.work), work.id);
                      }
                    }
                    // Deadline sweep for parked waiters; a cheap no-op when nothing is registered.
                    // The registry invokes each claimed waiter's own on_resume (the pool stays
                    // coroutine-agnostic).
                    park_registry_.Sweep(std::chrono::steady_clock::now());
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
    // Mark shutting down first: ScheduledAddTask refuses new work and IsShuttingDown() starts
    // reading true from here.
    pool_stop_source_.request_stop();

    // Drain parked waiters WHILE THE WORKERS ARE STILL RUNNING (load-bearing, not just ordering):
    // each ParkState's on_resume pins a resume back onto its owning worker, which is still looping
    // (no worker stopped yet) and services it like any other pinned task. The resumed chain
    // observes IsShuttingDown() and bails cleanly. Sequence: mark -> drain (here) -> stop monitor
    // -> stop workers (each finishes its pinned queue first, see the drain loop in operator()).
    park_registry_.Drain();

    // Stop monitoring thread before workers
    monitoring_.Stop();
    // Mixed work workers
    for (auto &worker : workers_) {
      worker->stop();
    }
    // High priority workers
    for (auto &worker : hp_workers_) {
      worker->stop();
    }
  }
}

void PriorityThreadPool::ScheduledAddTask(TaskSignature new_task, const Priority priority) {
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    return;
  }
  const auto id = (TaskID(priority == Priority::HIGH) * kMinHighPriorityId) +
                  --task_id_;  // Way to priorities hp tasks and older tasks
  auto tid = hot_threads_.GetHotElement();
  if (!tid) {
    // Limit the number of directly used threads when there are more workers than hw threads.
    // Gives better overall performance.
    static const auto max_wakeup_thread =
        std::max(1UL, std::min(static_cast<TaskID>(GetSafeHardwareConcurrency()), workers_.size()));
    // If no hot thread found, give it to the next thread
    tid = last_wid_++ % max_wakeup_thread;
  }
  workers_[*tid]->push(std::move(new_task), id);
  // High priority tasks are marked and given to mixed priority threads (at front of the queue)
  // HP threads are going to steal this work if not executed in time
}

// Reschedule a closure on a specific LP worker, PINNED so the steal loop and sched_mon (which look
// only at work_) never touch it. Reuses that worker's current task id so the pinned item keeps the
// priority position of an in-place continuation; falls back to a fresh LOW id if it hasn't run yet.
void PriorityThreadPool::RescheduleTaskOnWorker(size_t worker_id, std::function<void()> closure) {
  DMG_ASSERT(
      worker_id < workers_.size(), "worker_id {} out of range (num mixed workers {})", worker_id, workers_.size());
  // F1 fix (shutdown-window UAF): NEVER resume inline, shutdown or not. A coroutine parking on THIS
  // worker keeps touching frame state after publishing its ParkState (the re-probe, the shutdown
  // self-claim), which is only safe because a same-worker pinned resume can't run until worker_id
  // returns from await_suspend. Always posting pinned is safe at shutdown too: Drain() runs before
  // any worker stops, and operator() drains work_pinned_ before exiting as a backstop.
  Worker *const w = workers_[worker_id].get();
  TaskID id = w->last_task_.load(std::memory_order_acquire);
  if (id == 0) {
    // Worker has not executed anything yet; treat as a new LOW priority task.
    id = --task_id_;
  }
  w->push([c = std::move(closure)](Priority /*priority*/) mutable { c(); }, id, /*pinned=*/true);
}

void PriorityThreadPool::Worker::push(TaskSignature new_task, TaskID id, bool pinned) {
  {
    auto l = std::unique_lock{mtx_};
    Work w{.id = id, .work = std::move(new_task), .pinned = pinned};
    (pinned ? work_pinned_ : work_).push(std::move(w));
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
void PriorityThreadPool::Worker::operator()(const uint16_t worker_id,
                                            const std::vector<std::unique_ptr<Worker>> &workers_pool,
                                            HotMask &hot_threads) {
  utils::ThreadSetName(ThreadPriority == Priority::HIGH ? "high prior." : "low prior.");

  // Publish this worker's id for the run loop (LP workers only -- see header for why HP must not).
  if constexpr (ThreadPriority != Priority::HIGH) {
    SetCurrentWorker(worker_id);
  }

  // Both mixed and high priority worker only steal from mixed worker
  const auto other_workers = std::invoke([&workers_pool, self = this, worker_id]() -> std::vector<Worker *> {
    if constexpr (ThreadPriority != Priority::HIGH) {
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
    } else {
      // Hp threads steal from any mixed work thread (workers_pool contains only mixed work threads)
      (void)self;
      (void)worker_id;
      return workers_pool | std::views::transform([](auto &o) { return o.get(); }) | std::ranges::to<std::vector>();
    }
  });

  std::optional<TaskSignature> task;
  // Drains BOTH queues, pinned first (deliberate, not a correctness requirement). Only this
  // worker's dequeue and push() touch work_pinned_, so a pinned task only runs on its worker.
  auto pop_task = [&] {
    const bool use_pinned = !work_pinned_.empty();
    auto &q = use_pinned ? work_pinned_ : work_;
    has_pending_work_.store(work_.size() + work_pinned_.size() > 1, std::memory_order::release);
    last_task_.store(q.top().id, std::memory_order_release);
    task = std::move(q.top().work);
    q.pop();
  };

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
    // Phase 1B - check if there is other scheduled work (both queues)
    {
      auto l = std::unique_lock{mtx_};
      if (!work_.empty() || !work_pinned_.empty()) {
        pop_task();
        continue;  // Spin to phase 1A
      }
    }

    working_.store(false, std::memory_order_release);
    if constexpr (ThreadPriority != Priority::HIGH) {
      hot_threads.Set(worker_id);
    }

    // Phase 2A - try to steal work (steal only from work_; work_pinned_ is never stolen)
    for (auto *worker : other_workers) {
      if (has_pending_work_.load(std::memory_order_acquire)) break;  // This worker received work

      if (worker->has_pending_work_.load(std::memory_order_acquire) &&
          worker->working_.load(std::memory_order_acquire)) {
        auto l2 = std::unique_lock{worker->mtx_, std::defer_lock};
        if (!l2.try_lock()) continue;  // Busy, skip
        // Re-check under lock — only work_ (stealable); work_pinned_ is invisible to the steal loop.
        if (worker->work_.empty()) continue;
        // HP threads can only steal HP work
        if constexpr (ThreadPriority == Priority::HIGH) {
          // If LP work, skip
          if (worker->work_.top().id <= kMaxLowPriorityId) continue;
        }

        // Update flag as soon as possible (account for both queues)
        worker->has_pending_work_.store(worker->work_.size() + worker->work_pinned_.size() > 1,
                                        std::memory_order_release);

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

    // Phase 3 - spin for a while waiting on work (available only if TSC is available)
    const auto freq = utils::GetTSCFrequency();
    if (freq) {
      const utils::TSCTimer timer{freq};
      yielder y;                         // NOLINT (misc-const-correctness)
      while (timer.Elapsed() < 0.001) {  // 1ms
        if (y([this] { return has_pending_work_.load(std::memory_order_acquire); }, 1024U, 0U)) break;
      }
    }

    // Phase 4A - reset hot mask
    if constexpr (ThreadPriority != Priority::HIGH) {
      hot_threads.Reset(worker_id);
    }
    // Phase 4B - check if work available (sleep or spin) — predicate checks both queues
    {
      auto l = std::unique_lock{mtx_};
      cv_.wait(l, [this, &pop_task] {
        // Under lock, check if there is work waiting in either queue
        if (!work_.empty() || !work_pinned_.empty()) {
          pop_task();
          return true;  // Spin to phase 1A and execute task
        }
        return !run_;  // Return and shutdown
      });
    }
  }

  // F1 fix: the loop can exit with `task` already popped by the cv_.wait predicate (pop and the
  // run_ check are not atomic), so run it rather than drop it. Then drain any remaining PINNED work
  // -- a bail-resume can still be posted here in the window between stop() and this thread
  // returning. Drain ONLY work_pinned_ (never work_, which may hold app work that must not start on
  // a tearing-down worker) so no parked frame is left registered-but-never-resumed.
  if (task) {
    task.value()(ThreadPriority);
    task.reset();
  }
  for (;;) {
    TaskSignature drained_task;
    {
      auto l = std::unique_lock{mtx_};
      if (work_pinned_.empty()) break;
      drained_task = std::move(work_pinned_.top().work);
      work_pinned_.pop();
    }
    drained_task(ThreadPriority);
  }

  // Teardown: drop the published identity so GetCurrentWorkerId() returns nullopt off-role.
  if constexpr (ThreadPriority != Priority::HIGH) {
    ClearCurrentWorker();
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
    memgraph::utils::HotMask &hot_threads);
template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::HIGH>(
    uint16_t worker_id, const std::vector<std::unique_ptr<memgraph::utils::PriorityThreadPool::Worker>> &,
    memgraph::utils::HotMask &hot_threads);
