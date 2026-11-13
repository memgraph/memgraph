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
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <latch>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

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

bool HotMask::AnySet() const noexcept {
  for (size_t g = 0; g < n_groups_; ++g) {
    if (hot_masks_[g].load(std::memory_order_acquire) != 0) return true;
  }
  return false;
}

uint16_t HotMask::Count() const noexcept {
  // Relaxed: soft pressure signal; stale bits are tolerable.
  uint32_t total = 0;
  for (size_t g = 0; g < n_groups_; ++g) {
    total += std::popcount(hot_masks_[g].load(std::memory_order_relaxed));
  }
  return static_cast<uint16_t>(total);
}

PriorityThreadPool::PriorityThreadPool(uint16_t mixed_work_threads_count, uint16_t high_priority_threads_count,
                                       ThreadInitCallback thread_init_callback,
                                       std::chrono::milliseconds monitor_interval)
    : hot_threads_{mixed_work_threads_count},
      task_id_{kMaxLowPriorityId},
      last_wid_{0},
      max_wakeup_thread_{
          std::max<TaskID>(1, std::min<TaskID>(GetSafeHardwareConcurrency(), mixed_work_threads_count))} {
  MG_ASSERT(mixed_work_threads_count > 0, "PriorityThreadPool requires at least one mixed work thread");
  MG_ASSERT(mixed_work_threads_count <= kMaxWorkers,
            "PriorityThreadPool supports a maximum of 1024 mixed work threads");
  MG_ASSERT(high_priority_threads_count > 0, "PriorityThreadPool requires at least one high priority work thread");

  pool_.reserve(mixed_work_threads_count + high_priority_threads_count);
  workers_.resize(mixed_work_threads_count);
  hp_workers_.resize(high_priority_threads_count);

  const size_t nthreads = mixed_work_threads_count + high_priority_threads_count;
  // Every worker and this constructor wait for all slots to be published: workers steal from each
  // other, and the monitor and every scheduled task dereference the slots.
  // Shared, rather than a local, because arriving is not the same as being
  // finished with the latch: the last thread will notify after arriving, by
  // which time this constructor may have completed and destructed all locals.
  // The last thread to exit its lambda drops the final reference cleanly and
  // safely.
  auto const published = std::make_shared<std::latch>(static_cast<std::ptrdiff_t>(nthreads));

  // Both priorities steal from workers_, whichever vector holds their own slot.
  auto const spawn = [&]<Priority ThreadPriority>(std::vector<std::unique_ptr<Worker>> &slots, size_t count) {
    for (size_t i = 0; i < count; ++i) {
      pool_.emplace_back([this, i, &slots, published, thread_init_callback]() {
        slots[i] = std::make_unique<Worker>();
        slots[i]->productive_pending_ = &productive_pending_;
        published->arrive_and_wait();
        // Call user-defined thread initialization callback (e.g., to register with Python interpreter)
        if (thread_init_callback) {
          thread_init_callback();
        }
        slots[i]->operator()<ThreadPriority>(i, workers_, hot_threads_);
      });
    }
  };

  spawn.operator()<Priority::LOW>(workers_, mixed_work_threads_count);
  spawn.operator()<Priority::HIGH>(hp_workers_, high_priority_threads_count);

  published->wait();

  // Under heavy load a task can get stuck, monitor and move to different thread
  monitoring_.SetInterval(monitor_interval);
  monitoring_.Run(
      "sched_mon",
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
            // Recheck under lock
            if (worker->work_.empty() || worker_last_task != worker->last_task_) continue;
            // Update flag as soon as possible
            worker->has_pending_work_.store(worker->work_.size() > 1, std::memory_order_release);
            const bool prod = worker->work_.top().productive;
            Worker::Work work{
                .id = worker->work_.top().id, .work = std::move(worker->work_.top().work), .productive = prod};
            if (prod) productive_pending_.fetch_sub(1, std::memory_order_relaxed);
            worker->work_.pop();
            l.unlock();

            auto tid = hot_threads_.GetHotElement();
            if (!tid) {
              // No hot LP threads available; schedule HP work to HP thread
              if (work.id > kMinHighPriorityId) {
                static size_t last_hp_thread = 0;
                auto &hp_worker = hp_workers_[hp_workers_num > 1 ? last_hp_thread++ % hp_workers_num : 0];
                if (!hp_worker->has_pending_work_) {
                  hp_worker->push(std::move(work.work), work.id, work.productive);
                  continue;
                }
              }
              // No hot thread and low priority work, schedule to the next lp worker
              tid = (worker_id + 1) % workers_num;
            }
            workers_[*tid]->push(std::move(work.work), work.id, work.productive);
          }
        }

        // Re-inject expired parked admissions, plus the oldest remaining one to prevent starvation.
        {
          const auto now = std::chrono::steady_clock::now();
          using WakeEntry = std::pair<TaskID, TaskSignature>;
          std::vector<WakeEntry> wakeups;
          {
            const std::unique_lock lk{parked_mtx_};
            if (!parked_admissions_.empty()) {
              wakeups.reserve(parked_admissions_.size());
              for (auto it = parked_admissions_.begin(); it != parked_admissions_.end();) {
                if (it->deadline <= now) {
                  wakeups.emplace_back(it->id, std::move(it->task));
                  it = parked_admissions_.erase(it);
                } else {
                  ++it;
                }
              }
              if (!parked_admissions_.empty()) {
                wakeups.emplace_back(parked_admissions_.front().id, std::move(parked_admissions_.front().task));
                parked_admissions_.pop_front();
              }
              has_parked_.store(!parked_admissions_.empty(), std::memory_order_release);
            }
          }
          for (auto &[id, task] : wakeups) {
            ScheduledReAddTask(std::move(task), id);
          }
        }
      });
}

PriorityThreadPool::~PriorityThreadPool() {
  if (!pool_stop_source_.stop_requested()) {
    ShutDown();
  }
}

void PriorityThreadPool::AwaitShutdown() {
  pool_.clear();

  // A non-pool waker (lock-release hook on another thread) may have extracted parked entries and not yet re-posted
  // them. Wait it out so its re-post (a worker queue push, or ReparkOnStop) lands before the takes below.
  {
    std::unique_lock lk{parked_mtx_};
    parked_cv_.wait(lk, [&] { return extracting_ == 0; });
  }

  // Workers exit on !run_ without draining, so queued tasks remain. They own DB accessors: release them here while
  // the DBMS is alive. Moved out under the mutex, destroyed unlocked, so a destructor re-entering the pool is a no-op.
  std::vector<TaskSignature> orphans;
  const auto take_queue = [&](Worker &worker) {
    int64_t productive = 0;
    {
      const std::unique_lock lk{worker.mtx_};
      while (!worker.work_.empty()) {
        productive += worker.work_.top().productive ? 1 : 0;
        orphans.push_back(std::move(worker.work_.top().work));
        worker.work_.pop();
      }
      worker.has_pending_work_.store(false, std::memory_order_release);
    }
    productive_pending_.fetch_sub(productive, std::memory_order_relaxed);
  };
  for (auto &worker : workers_) take_queue(*worker);
  for (auto &worker : hp_workers_) take_queue(*worker);

  std::deque<ParkedAdmission> parked;
  {
    const std::unique_lock lk{parked_mtx_};
    parked.swap(parked_admissions_);
    has_parked_.store(false, std::memory_order_release);
    parked_closed_ = true;
  }
  orphans.clear();
  parked.clear();
}

void PriorityThreadPool::ShutDown() {
  // Monitor first: a sweep tick must not race the stop below.
  monitoring_.Stop();
  // Parked continuations are NOT run here: a drained COMMIT could block this thread in WAL/replication before the
  // caller reaches replication shutdown. They stay parked (WakeMatching/ParkAdmission refuse while draining) and
  // AwaitShutdown destroys them; teardown errors the clients.
  draining_admissions_.store(true, std::memory_order_release);
  // From here ScheduledReAddTask re-parks instead of posting.
  pool_stop_source_.request_stop();
  for (auto &worker : workers_) {
    worker->stop();
  }
  for (auto &worker : hp_workers_) {
    worker->stop();
  }
}

PriorityThreadPool::TaskID PriorityThreadPool::ScheduledAddTask(TaskSignature new_task, Priority priority,
                                                                bool productive) {
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    return 0;
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
  workers_[*tid]->push(std::move(new_task), id, productive);
  // High priority tasks are marked and given to mixed priority threads (at front of the queue)
  // HP threads are going to steal this work if not executed in time
  return id;
}

void PriorityThreadPool::ScheduledReAddTask(TaskSignature task, TaskID id) {
  if (pool_stop_source_.stop_requested()) [[unlikely]] {
    ReparkOnStop(std::move(task), id);
    return;
  }
  auto tid = hot_threads_.GetHotElement();
  if (!tid) {
    tid = last_wid_++ % max_wakeup_thread_;
  }
  workers_[*tid]->push(std::move(task), id, /*productive=*/false);
}

void PriorityThreadPool::ReparkOnStop(TaskSignature task, TaskID id) {
  const std::unique_lock lk{parked_mtx_};
  // AwaitShutdown waits for in-flight wakers before closing, so every re-post lands before parked_closed_ is set.
  // This relies on ParkAdmission/ScheduledReAddTask callers being pool workers (joined first) or WakeMatching wakers
  // (counted by extracting_).
  DMG_ASSERT(!parked_closed_, "task re-parked after AwaitShutdown closed the parked set");
  if (parked_closed_) [[unlikely]] {
    // Release-build fallback: the by-value `task` is destroyed after `lk` is released on return; never leak it
    // (a leaked DatabaseAccess hangs ~Gatekeeper).
    spdlog::error("Task re-parked after AwaitShutdown closed the parked set; destroying it");
    return;
  }
  // Inert after stop: nothing consults the tag, AwaitShutdown destroys the entry regardless of resource.
  parked_admissions_.push_back({.id = id,
                                .task = std::move(task),
                                .deadline = std::chrono::steady_clock::time_point::max(),
                                .resource = WaitResource::CommitLock});
  has_parked_.store(true, std::memory_order_seq_cst);
}

std::chrono::microseconds PriorityThreadPool::AdmissionTryBudget() const noexcept {
  if (IsDrainingAdmissions()) return kTryBudgetMin;

  const uint64_t pool = GetNumWorkers();
  const uint64_t free = FreeWorkers();
  const uint64_t backlog = QueuedProductiveTasks();

  if (free * 10 >= pool && backlog == 0) return kTryBudgetMax;

  const uint64_t safe_pool = pool > 0 ? pool : 1;
  const uint64_t clamped_backlog = backlog < safe_pool ? backlog : safe_pool;
  const auto range = static_cast<uint64_t>(kTryBudgetMax.count() - kTryBudgetMin.count());
  const int64_t budget_us =
      static_cast<int64_t>(kTryBudgetMax.count()) - static_cast<int64_t>((clamped_backlog * range) / safe_pool);

  if (free * 10 < pool) return kTryBudgetMin;

  int64_t clamped = budget_us;
  if (budget_us < kTryBudgetMin.count())
    clamped = kTryBudgetMin.count();
  else if (budget_us > kTryBudgetMax.count())
    clamped = kTryBudgetMax.count();
  return std::chrono::microseconds{clamped};
}

uint32_t PriorityThreadPool::AdmissionRescheduleCap() const noexcept {
  const uint64_t pool = GetNumWorkers();
  const uint64_t free = FreeWorkers();
  const uint64_t backlog = QueuedProductiveTasks();

  if (free * 10 < pool) return kRescheduleCapMin;

  if (backlog == 0) return kRescheduleCapMax;

  const uint64_t safe_pool = pool > 0 ? pool : 1;
  const uint64_t clamped_backlog = backlog < safe_pool ? backlog : safe_pool;
  const uint32_t range = kRescheduleCapMax - kRescheduleCapMin;
  const auto reduction = static_cast<uint32_t>((clamped_backlog * range) / safe_pool);
  const uint32_t cap = kRescheduleCapMax > reduction ? kRescheduleCapMax - reduction : kRescheduleCapMin;

  if (cap < kRescheduleCapMin) return kRescheduleCapMin;
  if (cap > kRescheduleCapMax) return kRescheduleCapMax;
  return cap;
}

void PriorityThreadPool::ParkAdmission(TaskSignature task, TaskID id, std::chrono::steady_clock::time_point deadline,
                                       WaitResource resource, std::optional<uint64_t> observed_epoch) {
  {
    const std::unique_lock lk{parked_mtx_};
    if (draining_admissions_.load(std::memory_order_acquire)) return;  // shutting down: drop; teardown errors client
    // Dekker pairing, all seq_cst: this has_parked_ store then epoch load vs WakeMatching's epoch bump then
    // has_parked_ load. Either the waker sees has_parked_ (and blocks on parked_mtx_) or we see the bump.
    has_parked_.store(true, std::memory_order_seq_cst);
    if (!observed_epoch || WakeEpoch(resource) == *observed_epoch) {
      parked_admissions_.push_back({.id = id, .task = std::move(task), .deadline = deadline, .resource = resource});
      return;
    }
    has_parked_.store(!parked_admissions_.empty(), std::memory_order_release);
  }
  ScheduledReAddTask(std::move(task), id);
}

bool PriorityThreadPool::ShouldParkAdmission() const noexcept {
  return IsDrainingAdmissions() || (productive_pending_.load(std::memory_order_acquire) > 0 && !hot_threads_.AnySet());
}

void PriorityThreadPool::WakeMatching(WaitResource resource) {
  wake_epochs_[static_cast<size_t>(resource)].value.fetch_add(1, std::memory_order_seq_cst);

  // Stale-true costs one lock acquisition; ParkAdmission's seq_cst pairing rules out a miss.
  if (!has_parked_.load(std::memory_order_seq_cst)) return;

  using WakeEntry = std::pair<TaskID, TaskSignature>;
  std::vector<WakeEntry> wakeups;
  {
    const std::unique_lock lk{parked_mtx_};
    // Shutting down: ShutDown's swap owns the parked set (ParkAdmission refuses new parks while draining).
    if (draining_admissions_.load(std::memory_order_acquire)) return;
    for (auto it = parked_admissions_.begin(); it != parked_admissions_.end();) {
      if (it->resource == resource) {
        wakeups.emplace_back(it->id, std::move(it->task));
        it = parked_admissions_.erase(it);
      } else {
        ++it;
      }
    }
    has_parked_.store(!parked_admissions_.empty(), std::memory_order_release);
    if (wakeups.empty()) return;
    ++extracting_;  // same critical section as the extraction: AwaitShutdown cannot miss these entries
  }  // parked_mtx_ released before ScheduledReAddTask takes Worker::mtx_
  // Runs on the exception path too (Worker::push can throw OOM); otherwise AwaitShutdown would wait forever.
  const utils::OnScopeExit release_extracting{[&] {
    wakeups.clear();  // unmoved tasks own DB accessors: destroy before AwaitShutdown can proceed
    const std::unique_lock lk{parked_mtx_};
    --extracting_;
    parked_cv_.notify_all();  // under the lock: the waker never touches parked_cv_ after AwaitShutdown can return
  }};
  if (wake_extracted_hook_) [[unlikely]]
    wake_extracted_hook_();
  for (auto &[id, task] : wakeups) {
    ScheduledReAddTask(std::move(task), id);
  }
}

void PriorityThreadPool::Worker::push(TaskSignature new_task, TaskID id, bool productive) {
  {
    auto l = std::unique_lock{mtx_};
    if (productive && productive_pending_) productive_pending_->fetch_add(1, std::memory_order_relaxed);
    work_.emplace(id, std::move(new_task), productive);
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
  auto pop_task = [&] {
    has_pending_work_.store(work_.size() > 1, std::memory_order::release);
    last_task_.store(work_.top().id, std::memory_order_release);
    if (work_.top().productive && productive_pending_) productive_pending_->fetch_sub(1, std::memory_order_relaxed);
    task = std::move(work_.top().work);
    work_.pop();
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
    // Phase 1B - check if there is other scheduled work
    {
      auto l = std::unique_lock{mtx_};
      if (!work_.empty()) {
        pop_task();
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
        if (worker->work_.top().productive && productive_pending_)
          productive_pending_->fetch_sub(1, std::memory_order_relaxed);
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
    // Phase 4B - check if work available (sleep or spin)
    {
      auto l = std::unique_lock{mtx_};
      cv_.wait(l, [this, &pop_task] {
        // Under lock, check if there is work waiting
        if (!work_.empty()) {
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
    memgraph::utils::HotMask &hot_threads);
template void memgraph::utils::PriorityThreadPool::Worker::operator()<memgraph::utils::Priority::HIGH>(
    uint16_t worker_id, const std::vector<std::unique_ptr<memgraph::utils::PriorityThreadPool::Worker>> &,
    memgraph::utils::HotMask &hot_threads);
