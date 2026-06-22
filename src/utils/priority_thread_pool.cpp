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

#include <algorithm>
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
struct CurrentResumableTaskState {
  PriorityThreadPool *pool{nullptr};
  std::optional<uint16_t> worker_id;
  std::function<void()> resume_task{};
  bool parked{false};
  std::shared_ptr<std::atomic<TaskCollection::Task::State>> task_state;  // For atomic PARKED state store
};

thread_local std::vector<CurrentResumableTaskState> current_resumable_task_stack;

class CurrentResumableTaskScope {
 public:
  CurrentResumableTaskScope(PriorityThreadPool *pool, std::optional<uint16_t> worker_id, std::function<void()> resume,
                            std::shared_ptr<std::atomic<TaskCollection::Task::State>> task_state = nullptr)
      : active_(true) {
    current_resumable_task_stack.push_back(CurrentResumableTaskState{.pool = pool,
                                                                     .worker_id = worker_id,
                                                                     .resume_task = std::move(resume),
                                                                     .parked = false,
                                                                     .task_state = std::move(task_state)});
  }

  CurrentResumableTaskScope(const CurrentResumableTaskScope &) = delete;
  CurrentResumableTaskScope &operator=(const CurrentResumableTaskScope &) = delete;

  CurrentResumableTaskScope(CurrentResumableTaskScope &&other) noexcept
      : active_(std::exchange(other.active_, false)) {}

  ~CurrentResumableTaskScope() {
    if (active_) {
      DMG_ASSERT(!current_resumable_task_stack.empty(), "Missing current resumable task state");
      current_resumable_task_stack.pop_back();
    }
  }

  [[nodiscard]] bool WasParked() {
    DMG_ASSERT(active_ && !current_resumable_task_stack.empty(), "Missing current resumable task state");
    auto &state = current_resumable_task_stack.back();
    bool was_parked = state.parked;
    state.parked = false;  // Reset after reading - prevents stale flag from affecting subsequent yields
    return was_parked;
  }

 private:
  bool active_{false};
};
}  // namespace

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
      // Publish worker_id_ / yield_registry_ BEFORE the barrier: push() (called from the main thread
      // right after the ctor returns, once the barrier releases) reads them, so the barrier must provide
      // the happens-before edge. Setting them inside operator() (after the barrier) is a data race.
      workers_[i]->worker_id_ = static_cast<uint16_t>(i);
      workers_[i]->yield_registry_ = yield_registry_;
      barrier.arrive_and_wait();
      // Call user-defined thread initialization callback (e.g., to register with Python interpreter)
      if (thread_init_callback) {
        thread_init_callback();
      }
      workers_[i]->operator()<Priority::LOW>(i, workers_, hot_threads_, yield_registry_);
    });
  }

  barrier.wait();
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
    // Workers
    for (auto &worker : workers_) {
      worker->stop();
    }
  }
}

void PriorityThreadPool::ScheduledAddTask(TaskSignature new_task, const Priority priority) {
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    new_task();  // Execute synchronously instead of dropping - prevents deadlock (P9)
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

bool CurrentResumableTask::RegisterWaiter(WorkerResumeEvent &event, uint64_t observed_epoch) {
  if (current_resumable_task_stack.empty()) return false;
  auto &state = current_resumable_task_stack.back();
  if (!state.resume_task) return false;
  // EC-2 fix: A task without a pinned worker_id was stolen (executed inline on a non-pool thread
  // via TryExecuteOneIdleTask). If we allowed it to park, NotifyAll would resume the closure on
  // whatever thread calls NotifyAll (the producer), violating TLS pinning invariants.
  // Return false → await_suspend busy-spins instead of parking.
  if (!state.worker_id) return false;

  TaskSignature resume_task = [resume = state.resume_task]() mutable { resume(); };
  if (!event.RegisterTaskWaiter(
          std::move(resume_task), state.pool, state.worker_id, observed_epoch, state.task_state)) {
    return false;
  }

  state.parked = true;
  return true;
}

std::shared_ptr<std::atomic<TaskCollection::Task::State>> CurrentResumableTask::GetCurrentTaskState() {
  if (current_resumable_task_stack.empty()) return nullptr;
  return current_resumable_task_stack.back().task_state;
}

void CurrentResumableTask::ClearParked() {
  if (current_resumable_task_stack.empty()) return;
  current_resumable_task_stack.back().parked = false;
}

const std::function<void()> *CurrentResumableTask::GetCurrentResumeTask() {
  if (current_resumable_task_stack.empty()) return nullptr;
  auto &state = current_resumable_task_stack.back();
  if (!state.resume_task) return nullptr;
  return &state.resume_task;
}

void CurrentResumableTask::SetParked() {
  if (current_resumable_task_stack.empty()) return;
  current_resumable_task_stack.back().parked = true;
}

void PriorityThreadPool::ScheduleResumableTask(ResumableTaskSignature task, Priority priority) {
  struct ResumableWrapper {
    std::shared_ptr<ResumableTaskSignature> task;
    PriorityThreadPool *pool;
    Priority priority;

    void Run() {
      const auto worker_id = WorkerYieldRegistry::GetCurrentWorkerId();
      CurrentResumableTaskScope current_task_scope{pool, worker_id, [w = *this]() mutable { w.Run(); }};
      const bool yielded = (*task)();
      const bool was_parked = current_task_scope.WasParked();
      if (was_parked) {
        return;
      }
      if (!yielded) {
        return;
      }
      auto *sig = WorkerYieldRegistry::GetCurrentYieldSignal();
      if (sig && sig->load(std::memory_order_acquire)) {
        // Clear the yield signal before rescheduling to prevent infinite rescheduling loops (P10 fix)
        sig->store(false, std::memory_order_release);
      }
      if (pool->IsShuttingDown()) {
        // Drain the continuation inline rather than rescheduling (workers are stopping; EC-3: iterative,
        // not recursive, to avoid unbounded stack growth from recursive Run() calls). A cooperative task
        // observes its abort condition and returns false within a few iterations; the bound caps an
        // ill-behaved task that keeps yielding with no way to observe shutdown (there is no abort channel
        // into the user task body), so the drain cannot wait for cooperation. The task has already run
        // once above, so call-once progress is kept.
        constexpr int kMaxShutdownDrainIterations = 64;
        for (int drain_i = 0; drain_i < kMaxShutdownDrainIterations; ++drain_i) {
          const auto wid = WorkerYieldRegistry::GetCurrentWorkerId();
          CurrentResumableTaskScope scope{pool, wid, [w = *this]() mutable { w.Run(); }};
          const bool inner_yielded = (*task)();
          if (scope.WasParked() || !inner_yielded) break;
          auto *inner_sig = WorkerYieldRegistry::GetCurrentYieldSignal();
          if (inner_sig && inner_sig->load(std::memory_order_acquire)) {
            inner_sig->store(false, std::memory_order_release);
          }
        }
        return;
      }
      // P10: Yield signal has been cleared above
      // Task must be rescheduled on the same worker to preserve thread-local state
      if (auto wid = WorkerYieldRegistry::GetCurrentWorkerId()) {
        pool->RescheduleTaskOnWorker(*wid, [w = *this]() mutable { w.Run(); });
      } else {
        // No worker ID - use ScheduledAddTask as fallback
        pool->ScheduledAddTask([w = *this]() mutable { w.Run(); }, priority);
      }
    }
  };

  ResumableWrapper w{
      .task = std::make_shared<ResumableTaskSignature>(std::move(task)), .pool = this, .priority = priority};
  ScheduledAddTask([w]() mutable { w.Run(); }, priority);
}

// Reschedule a continuation on a specific worker, preserving its original task ID.
// last_task_ holds the ID of the task currently executing on that worker. Reusing it
// ensures the continuation sits at exactly the same position in the priority queue as
// the original task: LP continuations (ID ≤ INT64_MAX) naturally run after any pending
// HP tasks (ID > INT64_MAX) without needing any special priority boost.
void PriorityThreadPool::RescheduleTaskOnWorker(uint16_t worker_id, TaskSignature task) {
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    task();  // Execute synchronously instead of dropping - prevents deadlock (P4)
    return;
  }
  DMG_ASSERT(
      worker_id < workers_.size(), "worker_id {} out of range (num mixed workers {})", worker_id, workers_.size());
  Worker *const w = workers_[worker_id].get();
  TaskID id = w->last_task_.load(std::memory_order_acquire);
  if (id == 0) {
    // If no task is currently active, treat it as a new LOW priority task
    id = task_id_.fetch_sub(1, std::memory_order_acq_rel);
  }
  w->push(std::move(task), id, /*pinned=*/true);
}

void PriorityThreadPool::Worker::push(TaskSignature new_task, TaskID id, bool pinned) {
  {
    auto l = std::unique_lock{mtx_};
    Work w{.id = id, .work = std::move(new_task), .pinned = pinned};
    (pinned ? work_pinned_ : work_).push(std::move(w));
    // TODO thing about atomic ordering and if this can be missed or requested when not needed
    // Only request a yield when the worker is actively running a task. If it is idle it will
    // pick up the HP task immediately from the priority queue without needing an interrupt.
    // Pinned tasks are always internal continuations (yielded/parked resumptions) — they must never
    // trigger preemption of the currently-running task even when they carry an HP-inherited ID.
    if (!pinned && id > kMaxLowPriorityId && yield_registry_ && working_.load(std::memory_order_acquire)) {
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

  // worker_id_ / yield_registry_ are already published by the ctor before the start barrier (so cross-
  // thread push() reads are happens-before ordered); do NOT re-assign them here (that write would race
  // a concurrent push()). Use the by-value params below.
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
      working_.store(true, std::memory_order_release);
      if (yield_registry) {
        // Clear any stale yield signal from the previous task, then re-arm under
        // the worker mutex if an HP task arrived in the window between pop_task()
        // and this clear. push() holds mtx_ when it sets the signal, so the
        // check below is linearizable with any concurrent push():
        //   - HP pushed before we lock   → task is in queue   → re-arm ✓
        //   - HP pushed after we unlock  → push() sees working_=true → sets signal ✓
        //   - HP pushed while we hold lock → impossible (same mutex)
        memgraph::utils::WorkerYieldRegistry::ClearYieldForCurrentWorker();
        {
          auto l = std::unique_lock{mtx_};
          // Only non-pinned (externally-submitted) HP tasks should re-arm the yield signal.
          // Pinned tasks are always internal continuations and never warrant preemption.
          const bool has_hp = !work_.empty() && work_.top().id > kMaxLowPriorityId;
          if (has_hp) {
            yield_registry->RequestYieldForWorker(worker_id_);
          }
        }
      }
      task.value()();
      task.reset();
      // Do NOT set working_=false yet; check Phase 1B first.
      // Keeping working_=true through the queue check ensures that a HP task arriving
      // in push() between here and pop_task() correctly sees the worker as busy and
      // sends a yield signal (rather than treating it as idle).
    }
    // Phase 1B - check if there is other scheduled work
    {
      auto l = std::unique_lock{mtx_};
      if (!work_.empty() || !work_pinned_.empty()) {
        pop_task();
        continue;  // Spin to phase 1A (working_ still true)
      }
    }
    // No more queued work — worker is genuinely going idle now.
    working_.store(false, std::memory_order_release);

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

  // Teardown: drop this thread's published worker identity so GetCurrentWorkerId()/
  // GetCurrentYieldSignal() return nullopt/nullptr once the thread leaves its pool role. Pool
  // threads are not reused today (the jthread joins on pool destruction), so this is defensive
  // hygiene that makes the TLS lifetime contract explicit rather than relying on thread death.
  if (yield_registry_) {
    WorkerYieldRegistry::ClearCurrentWorker();
  }
}

// Prepares task for safe scheduling
ResumableTaskSignature TaskCollection::WrapTask(size_t index, PriorityThreadPool *pool) {
  auto &task = tasks_[index];
  return [this, self = weak_from_this().lock(), index, &task = task.task_, state = task.state_, pool]() -> bool {
    // Lifetime barrier (see in_flight_ docs): count this WrapTask invocation BEFORE touching any task
    // state, so once Wait() observes a task FINISHED (release/acquire on state_) it also sees this
    // increment and will not return until we fully exit (the OnScopeExit decrements after NotifyProgress,
    // on every path including the early CAS-fail returns and exceptions).
    in_flight_.fetch_add(1, std::memory_order_relaxed);
    const auto in_flight_guard = utils::OnScopeExit{[this, self]() noexcept {
      in_flight_.fetch_sub(1, std::memory_order_acq_rel);
      in_flight_.notify_all();
    }};
    // NOTE: every Task::State is a valid entry state and is handled below — IDLE (run), SCHEDULED/PARKED
    // (resume), STOLEN (claimed by WaitOrSteal -> skip), FINISHED (stale queued closure -> skip). Do NOT
    // assert a subset here: a stolen (STOLEN) or already-finished (FINISHED) closure reaching this point
    // is expected, and the CAS-and-skip below is the real guard.

    auto expected = Task::State::IDLE;
    if (!state->compare_exchange_strong(expected, Task::State::SCHEDULED, std::memory_order_acq_rel)) {
      // SCHEDULED means this is a resume after a yield.
      // PARKED means an external event woke the task and requeued it.
      // STOLEN means WaitOrSteal claimed it for direct execution — don't double-run.
      // FINISHED means already done — skip.
      if (expected != Task::State::SCHEDULED && expected != Task::State::PARKED) {
        return false;
      }
      // If resuming from PARKED, transition to SCHEDULED so Wait() can track progress correctly.
      // Without this transition, the task remains in PARKED state indefinitely,
      // causing TaskCollection::Wait() to timeout waiting for FINISHED.
      if (expected == Task::State::PARKED) {
        // EC-6 fix: use CAS instead of plain store.  A plain store would silently
        // overwrite any state written by a concurrent actor between our failed CAS
        // (which read PARKED) and this store.  If a future writer (e.g. a timeout
        // → FINISHED transition) races here, the CAS detects it and skips.
        auto parked = Task::State::PARKED;
        if (!state->compare_exchange_strong(parked, Task::State::SCHEDULED, std::memory_order_acq_rel)) {
          // State changed while we weren't looking — honour the new state.
          return false;
        }
      }
    }

    try {
      CurrentResumableTaskScope current_task_scope{pool,
                                                   WorkerYieldRegistry::GetCurrentWorkerId(),
                                                   [this, self, index, pool]() mutable {
                                                     auto wrapped_task = WrapTask(index, pool);
                                                     wrapped_task();
                                                   },
                                                   state};  // Pass task_state for atomic PARKED store
      const bool yielded = task();
      const bool was_parked = current_task_scope.WasParked();
      if (was_parked) {
        // State already stored as PARKED by RegisterTaskWaiter under mutex_
        NotifyProgress();  // Notify that task parked (progress in the sense of "state changed")
        return false;
      }
      if (yielded) {
        NotifyProgress();
        return true;
      }
      state->store(Task::State::FINISHED, std::memory_order_release);
      state->notify_one();  // Notify waiting threads
      NotifyProgress();
      return false;
    } catch (...) {
      state->store(Task::State::FINISHED, std::memory_order_release);
      state->notify_one();  // Notify even on exception
      NotifyProgress();
      throw;
    }
  };
}

bool TaskCollection::Wait(std::chrono::milliseconds timeout) {
  const auto start = std::chrono::steady_clock::now();
  const auto deadline = start + timeout;
  for (size_t i = 0; i < tasks_.size(); ++i) {
    auto &task = tasks_[i];
    auto expected = task.state_->load(std::memory_order_acquire);
    while (expected != Task::State::FINISHED) {
      // EC-4 fix: use atomic::wait() instead of sleep_for(10ms) polling.
      // notify_one() is already called by WrapTask/TryExecuteOneIdleTask when storing FINISHED.
      // Spurious wakeups are handled by re-checking expected after each wait.
      auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        spdlog::error("TaskCollection::Wait() timeout after {}ms - task[{}] stuck in state={}",
                      timeout.count(),
                      i,
                      static_cast<int>(expected));
        return false;
      }
      // Block until notify_one() wakes us (or spurious wakeup); re-read state after.
      task.state_->wait(expected, std::memory_order_acquire);
      expected = task.state_->load(std::memory_order_acquire);
    }
  }
  // Lifetime barrier: a worker that stored the last FINISHED (which woke us above) is still executing
  // WrapTask afterwards (e.g. NotifyProgress() touching progress_cv_). Do NOT return — the caller may
  // destroy the collection right after — until every in-flight WrapTask invocation has fully exited.
  // (TryExecuteOneIdleTask runs inline on this same thread, sequenced-before here, so it is not counted.)
  auto inflight = in_flight_.load(std::memory_order_acquire);
  while (inflight != 0) {
    if (std::chrono::steady_clock::now() >= deadline) {
      spdlog::error("TaskCollection::Wait() timeout: {} in-flight WrapTask invocation(s) did not drain", inflight);
      return false;
    }
    in_flight_.wait(inflight, std::memory_order_acquire);
    inflight = in_flight_.load(std::memory_order_acquire);
  }
  return true;
}

bool TaskCollection::WaitOrSteal(std::chrono::milliseconds timeout) {
  // Phase 1 - steal tasks that have not been picked up by a pool worker yet.
  // Use IDLE→STOLEN (not IDLE→SCHEDULED): WrapTask closures still in the pool
  // queue interpret SCHEDULED as "resume after yield" and would fall through to
  // double-execute. STOLEN is a distinct state that WrapTask recognises as
  // "already claimed by the calling thread — skip".
  while (TryExecuteOneIdleTask()) {
  }
  // Phase 2 - wait for tasks to finish
  return Wait(timeout);
}

bool TaskCollection::TryExecuteOneIdleTask(PriorityThreadPool *pool) {
  // EC-5 fix: claim the task under the lock, then release before executing.
  // Previously tasks_mutex_ was held for the entire task body on non-yielding tasks,
  // which would deadlock any concurrent caller that also needs tasks_mutex_.
  //
  // IMPORTANT: Do NOT move task.task_ out of tasks_[index].  WrapTask closures
  // capture tasks_[index].task_ by reference (&task = task.task_); moving it out
  // leaves a dangling reference and causes a heap-use-after-free when the WrapTask
  // closure later calls task().  STOLEN state provides the required exclusivity —
  // any WrapTask closure in the pool queue sees STOLEN and returns false (skips
  // execution), so accessing tasks_[captured_index].task_() directly is safe.
  std::shared_ptr<std::atomic<Task::State>> captured_state;
  size_t captured_index = 0;

  {
    std::unique_lock lock(tasks_mutex_);
    for (size_t index = 0; index < tasks_.size(); ++index) {
      auto &task = tasks_[index];
      auto expected = Task::State::IDLE;
      if (!task.state_->compare_exchange_strong(expected, Task::State::STOLEN, std::memory_order_acq_rel)) {
        continue;
      }
      captured_state = task.state_;
      captured_index = index;
      break;
    }
  }  // lock released before execution

  if (!captured_state) return false;

  try {
    // Call tasks_[captured_index].task_() directly — STOLEN state ensures no concurrent
    // access (any WrapTask closure in the pool queue sees STOLEN and skips execution).
    const bool yielded = tasks_[captured_index].task_();
    if (yielded) {
      if (pool != nullptr) {
        captured_state->store(Task::State::SCHEDULED, std::memory_order_release);
        pool->ScheduleResumableTask(WrapTask(captured_index, pool), Priority::LOW);
        NotifyProgress();
      } else {
        throw std::runtime_error("WaitOrSteal cannot handle yielding tasks. Use co_await Finished() instead.");
      }
    } else {
      captured_state->store(Task::State::FINISHED, std::memory_order_release);
      captured_state->notify_one();
      NotifyProgress();
    }
  } catch (...) {
    captured_state->store(Task::State::FINISHED, std::memory_order_release);
    captured_state->notify_one();
    NotifyProgress();
    throw;
  }
  return true;
}

bool TaskCollection::WaitForProgress(std::chrono::milliseconds timeout) {
  std::unique_lock lock(progress_mutex_);
  const auto observed_epoch = progress_epoch_;
  return progress_cv_.wait_for(
      lock, timeout, [this, observed_epoch] { return progress_epoch_ != observed_epoch || Finished(); });
}

uint64_t TaskCollection::ProgressEpoch() const {
  const auto lock = std::lock_guard(progress_mutex_);
  return progress_epoch_;
}

bool TaskCollection::Finished() const {
  return std::ranges::all_of(
      tasks_, [](const auto &task) { return task.state_->load(std::memory_order_acquire) == Task::State::FINISHED; });
}

bool TaskCollection::AllTerminal() const {
  // EC-7 fix: delegate to Finished() — both check only FINISHED today.
  // STOLEN is never a resting state; no other terminal state exists yet.
  // If a second terminal state (e.g. DESTROYED) is introduced, update both here.
  return Finished();
}

bool TaskCollection::HasNonTerminalTasks() const {
  return std::ranges::any_of(tasks_, [](const auto &task) {
    const auto s = task.state_->load(std::memory_order_acquire);
    return s != Task::State::FINISHED;
  });
}

#ifndef NDEBUG
void TaskCollection::DbgVerifyState() const {
  for (size_t i = 0; i < tasks_.size(); ++i) {
    const auto &task = tasks_[i];
    const auto s = task.state_->load(std::memory_order_acquire);
    // State should always be valid
    DMG_ASSERT(s == Task::State::IDLE || s == Task::State::SCHEDULED || s == Task::State::PARKED ||
                   s == Task::State::STOLEN || s == Task::State::FINISHED,
               "Task {} has invalid state {}",
               i,
               static_cast<uint8_t>(s));
  }
}
#endif

bool TaskCollection::RegisterProgressWaiter(TaskSignature resume_task, PriorityThreadPool *pool, uint16_t worker_id,
                                            uint64_t observed_epoch,
                                            std::shared_ptr<std::atomic<Task::State>> task_state) {
  const auto lock = std::lock_guard(progress_mutex_);
  // shutdown-park guard: never park into a tearing-down pool (nothing would fire NotifyProgress ->
  // the task would stay PARKED until Wait() times out). Returning false makes the caller busy-spin and
  // observe shutdown, exactly like the EC-2 stolen-task fallback. (Full interrupt->wake-all-parked is a
  // P3.2 concern with the parallel-join consumer; this guard covers shutdown-already-in-progress.)
  if (pool && pool->IsShuttingDown()) return false;
  if (progress_epoch_ != observed_epoch || Finished()) {
    return false;  // lost-wakeup guard: progress already happened, caller must not suspend
  }
  // MG_ASSERT (not DMG_): the single-waiter invariant is load-bearing — it is what makes the
  // generation-counter unnecessary (one resume closure per park). A release-build double-registration
  // would silently drop the first waiter's closure, leaving its task PARKED forever (Wait() timeout).
  MG_ASSERT(!progress_waiter_task_, "Only one progress waiter can be registered at a time");
  // RELEASE: Finished()/HasNonTerminalTasks()/AllTerminal() read task_state OUTSIDE this mutex with
  // acquire, so a parked task must be visible to them as PARKED (matches RegisterTaskWaiter / :854).
  if (task_state) {
    task_state->store(Task::State::PARKED, std::memory_order_release);
  }
  progress_waiter_task_ = std::move(resume_task);
  progress_waiter_pool_ = pool;
  progress_waiter_worker_id_ = worker_id;
  progress_waiter_task_state_ = std::move(task_state);
  return true;
}

void TaskCollection::NotifyProgress() {
  TaskSignature waiter_task;
  PriorityThreadPool *waiter_pool{nullptr};
  std::optional<uint16_t> waiter_worker_id;
  {
    auto lock = std::lock_guard(progress_mutex_);
    ++progress_epoch_;
    waiter_task = std::move(progress_waiter_task_);
    waiter_pool = std::exchange(progress_waiter_pool_, nullptr);
    waiter_worker_id = std::exchange(progress_waiter_worker_id_, std::nullopt);
    progress_waiter_task_state_ = {};  // task_state ownership released; WrapTask CAS guards the resume
  }
  progress_cv_.notify_all();
  if (!waiter_task) return;
  // The closure IS the WrapTask re-entry: RescheduleTaskOnWorker enqueues it pinned, and WrapTask's
  // PARKED->SCHEDULED CAS is the double-resume + UAF guard (a FINISHED/aborted task fails the CAS and
  // is never touched). This NotifyProgress path does NO raw handle.resume() — it dispatches via the
  // task-closure (unlike WorkerResumeEvent::NotifyAll's raw-handle overload, which is a separate,
  // currently-unused API path). (No generation guard needed: ResumableWrapper::Run never reschedules a
  // parked task, so no stale WrapTask closure can race this one; the CAS is the backstop. A future
  // change that reschedules a parked task would need a per-park generation guard.)
  if (waiter_pool && waiter_worker_id) {
    waiter_pool->RescheduleTaskOnWorker(*waiter_worker_id, std::move(waiter_task));
  } else {
    waiter_task();  // unreachable today (EC-2 blocks stolen tasks from registering); safe via CAS anyway
  }
}

bool CollectionScheduler::RegisterProgressWaiter(uint64_t observed_epoch) const {
  if (!collection_ || !pool_) return false;
  const auto worker_id = WorkerYieldRegistry::GetCurrentWorkerId();
  if (!worker_id) return false;  // EC-2: stolen task -> busy-spin
  const auto task_state = CurrentResumableTask::GetCurrentTaskState();
  if (!task_state) return false;  // not inside a WrapTask frame -> busy-spin
  const auto *resume_fn = CurrentResumableTask::GetCurrentResumeTask();
  if (!resume_fn || !(*resume_fn)) return false;
  TaskSignature resume_task = [fn = *resume_fn]() mutable { fn(); };
  const bool registered =
      collection_->RegisterProgressWaiter(std::move(resume_task), pool_, *worker_id, observed_epoch, task_state);
  if (registered) CurrentResumableTask::SetParked();  // WrapTask's WasParked() -> true -> suspends
  return registered;
}

uint64_t CollectionScheduler::ProgressEpoch() const {
  if (!collection_) return 0;
  return collection_->ProgressEpoch();
}

uint64_t WorkerResumeEvent::Epoch() const {
  const auto lock = std::lock_guard(mutex_);
  return epoch_;
}

bool WorkerResumeEvent::RegisterWaiter(std::coroutine_handle<> handle, PriorityThreadPool *pool,
                                       std::optional<uint16_t> worker_id, uint64_t observed_epoch) {
  const auto lock = std::lock_guard(mutex_);
  if (epoch_ != observed_epoch) {
    return false;
  }
  waiters_.push_back(Waiter{.handle = handle, .pool = pool, .worker_id = worker_id});
  return true;
}

bool WorkerResumeEvent::RegisterTaskWaiter(TaskSignature task, PriorityThreadPool *pool,
                                           std::optional<uint16_t> worker_id, uint64_t observed_epoch,
                                           std::shared_ptr<std::atomic<TaskCollection::Task::State>> task_state) {
  const auto lock = std::lock_guard(mutex_);
  if (epoch_ != observed_epoch) {
    return false;
  }
  // CRITICAL FIX: Store PARKED state BEFORE adding to waiters list
  // This ensures atomicity - either both happen, or neither.
  // RELEASE (not relaxed): although this store is under WorkerResumeEvent::mutex_, the PARKED state is
  // also read OUTSIDE that mutex by Finished()/HasNonTerminalTasks()/AllTerminal() (acquire loads). On a
  // weakly-ordered arch a relaxed store could be invisible to those out-of-mutex observers; release pairs
  // with their acquire so a parked task is never mistaken for non-existent/terminal.
  if (task_state) {
    task_state->store(TaskCollection::Task::State::PARKED, std::memory_order_release);
  }
  waiters_.push_back(Waiter{.task = std::move(task), .pool = pool, .worker_id = worker_id, .task_state = task_state});
  return true;
}

bool WorkerResumeEvent::RemoveWaiter(std::coroutine_handle<> handle, uint64_t observed_epoch,
                                     std::shared_ptr<std::atomic<TaskCollection::Task::State>> task_state) {
  const auto lock = std::lock_guard(mutex_);
  if (epoch_ != observed_epoch) {
    return false;  // NotifyAll already ran, too late to remove
  }
  // Match handle-based waiters by handle, task-based waiters by task_state identity.
  auto it = std::remove_if(waiters_.begin(), waiters_.end(), [&](const auto &w) {
    if (w.handle) return w.handle == handle;
    return task_state && w.task_state == task_state;
  });
  const bool found = it != waiters_.end();
  waiters_.erase(it, waiters_.end());
  if (found && task_state) {
    task_state->store(TaskCollection::Task::State::SCHEDULED, std::memory_order_release);
  }
  return found;
}

void WorkerResumeEvent::NotifyAll() {
  std::vector<Waiter> waiters;
  {
    auto lock = std::lock_guard(mutex_);
    ++epoch_;
    waiters.swap(waiters_);
  }
  for (auto &waiter : waiters) {
    if (waiter.handle) {
      if (waiter.handle.done()) {
        continue;
      }
      if (waiter.pool && waiter.worker_id) {
        waiter.pool->RescheduleTaskOnWorker(*waiter.worker_id, [handle = waiter.handle]() mutable {
          if (handle && !handle.done()) handle.resume();
        });
        continue;
      }
      waiter.handle.resume();
      continue;
    }
    if (!waiter.task) {
      continue;
    }
    if (waiter.pool && waiter.worker_id) {
      waiter.pool->RescheduleTaskOnWorker(*waiter.worker_id, std::move(waiter.task));
      continue;
    }
    waiter.task();
  }
}

}  // namespace memgraph::utils

template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::LOW>(
    uint16_t worker_id, const std::vector<std::unique_ptr<memgraph::utils::PriorityThreadPool::Worker>> &,
    memgraph::utils::HotMask &hot_threads, memgraph::utils::WorkerYieldRegistry *yield_registry);
template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::HIGH>(
    uint16_t worker_id, const std::vector<std::unique_ptr<memgraph::utils::PriorityThreadPool::Worker>> &,
    memgraph::utils::HotMask &hot_threads, memgraph::utils::WorkerYieldRegistry *yield_registry);
