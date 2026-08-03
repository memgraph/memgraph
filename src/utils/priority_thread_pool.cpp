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
#include "utils/park_state.hpp"
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
// LP-worker-only TLS (see the free-function declarations in priority_thread_pool.hpp for the
// full lifetime/scope contract). Deliberately NOT populated for HP workers.
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
                        // Recheck under lock — only ever considers work_ (stealable/migratable);
                        // work_must_run_ is invisible to sched_mon so a resume is never migrated.
                        if (worker->work_.empty() || worker_last_task != worker->last_task_) continue;
                        // Update flag as soon as possible (account for both queues)
                        worker->has_pending_work_.store(worker->work_.size() + worker->work_must_run_.size() > 1,
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
                    // Deadline sweep for parked waiters. One atomic load per tick when nothing is
                    // registered (Sweep's empty fast path), which is every tick with the
                    // coro-prepare flag off -- cheaper than a flag check would be, so this is
                    // deliberately not gated. The registry invokes each claimed waiter's on_resume
                    // itself, so the pool needs no coroutine knowledge.
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
    // Mark shutting down first: ScheduledAddTask refuses new work once set, and IsShuttingDown() (read
    // by AcquireAccessorCoro's post-resume check and AcquireAwaitable's shutdown self-claim) starts
    // observing true. PostResumeTask deliberately does NOT branch on this flag -- a resume is posted
    // shutdown or not, because dropping one leaks the parked session and stalls this very shutdown.
    pool_stop_source_.request_stop();

    // Drain parked waiters WHILE THE WORKERS ARE STILL RUNNING; this is load-bearing, not an ordering
    // nicety. Draining claims each ParkState and requests its resume through the gate, which posts onto
    // a still-running worker -- stop() below has not run for ANY of them yet, so the post has somewhere
    // to land. A waiter whose parking thread has not reached its task boundary is delivered by that
    // arming side instead, from a worker that is by definition still alive. Each resumed chain then
    // observes IsShuttingDown() and bails cleanly (throw -> unwind -> release accessor -> the session's
    // parked_prepare_ is cleared) instead of proceeding into per-database work.
    //
    // Sequence: mark shutting down -> drain parked (here, workers still looping) -> stop monitor -> stop
    // workers (each finishes its must-run queue, including a resume posted late in this window, before
    // exiting) -> AwaitShutdown() joins -> destroy pool.
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

// Post a parked coroutine's resume to any mixed-work (LP) worker, preferring an idle one (the same
// hot-thread-first selection as ScheduledAddTask). An arbitrary worker is safe because the ParkState
// delivery gate -- not the choice of worker -- is what guarantees a resume cannot start while the
// parking thread is still inside its own await_suspend/driver.
//
// POSTS rather than resuming inline for as long as any worker can still accept the task: the caller is
// whichever thread claimed the park (a lock releaser, the deadline sweep, the shutdown drain), and none
// of them may drive -- let alone destroy -- a coroutine frame. Posting during shutdown is both allowed
// and required: ShutDown() drains parked waiters BEFORE stopping any worker, so the target is still
// looping, and Worker::operator()'s tail drains work_must_run_ as a backstop for a resume posted after
// stop() was requested but before the thread returned.
//
// ONE exception: if every worker refuses the push (all stopped), the fallback at the bottom of this
// function runs the closure inline on the caller's thread. That is reachable only once no thread is left
// that could run the task, where the choice is between resuming inline and stranding the query forever.
void PriorityThreadPool::PostResumeTask(std::function<void()> closure) {
  DMG_ASSERT(!workers_.empty(), "PostResumeTask on a pool with no mixed-work workers.");
  // Deliberately no `pool_stop_source_.stop_requested()` bail (unlike ScheduledAddTask): a dropped
  // resume leaks the parked session and stalls the shutdown that dropped it.
  const auto id = --task_id_;  // ordinary LOW-priority id; resumes are ordered among themselves

  // Target selection matters more for a resume than for ordinary work, because must-run items are not
  // migrated by sched_mon: handing one to a mid-task worker means waiting out that task. Prefer a hot
  // (spinning) worker; failing that ANY worker not currently executing -- a sleeping one is not "hot"
  // but has working_ == false, and push() notifies its cv. Round-robin is the last resort.
  //
  // Migration is not needed on top of that, though its absence reads like a gap. If a worker is idle or
  // asleep at post time the scan below finds it (working_ == false covers both), so a resume never lands
  // on a busy worker while a free one exists. If one frees up AFTER the post it reaches Phase 2A and
  // steals the resume, since a victim with a queued must-run item satisfies the steal precondition
  // exactly. What remains is every LP worker being and staying busy, where the resume runs on whichever
  // reaches a task boundary first -- the pool's own scheduling granularity, with nowhere to migrate to.
  auto tid = hot_threads_.GetHotElement();
  if (!tid) {
    for (size_t i = 0; i < workers_.size(); ++i) {
      const auto candidate = (last_wid_ + i) % workers_.size();
      if (!workers_[candidate]->working_.load(std::memory_order_acquire)) {
        tid = static_cast<uint16_t>(candidate);
        break;
      }
    }
  }
  if (!tid) {
    static const auto max_wakeup_thread =
        std::max(1UL, std::min(static_cast<TaskID>(GetSafeHardwareConcurrency()), workers_.size()));
    tid = last_wid_++ % max_wakeup_thread;
  }

  auto task = TaskSignature{[c = std::move(closure)](Priority /*priority*/) mutable { c(); }};

  // A resume must land on a worker that will still RUN it. Pinning used to make that automatic: the
  // target was the parking worker, which is mid-task (hence alive) whenever a resume is posted for
  // it. An arbitrary target has no such guarantee during teardown -- workers are stopped one by one,
  // and a worker that already finished its tail drain would silently swallow the resume, leaking the
  // parked session. try_push refuses a worker whose run_ is already false; the converse is safe by
  // construction, since a worker that accepts an item drains work_must_run_ before it returns.
  const auto n = workers_.size();
  for (size_t i = 0; i < n; ++i) {
    auto &worker = workers_[(*tid + i) % n];
    if (worker->try_push(std::move(task), id, /*must_run=*/true)) return;
  }

  // Every worker is already stopped. Run it here rather than drop it: dropping leaks the parked session
  // and stalls the very shutdown that dropped it. Driving the frame is safe because of the delivery
  // gate -- `on_resume` fires only AFTER the parking thread's task ended, so the frame is quiescent and
  // nobody else is driving it. (Pre-gate, an inline resume could race a parking thread still inside
  // await_suspend, which is why the old pinned path never resumed inline at all.)
  //
  // Three real claimants reach here:
  //   1. an LP worker arming from its tail, every worker having been stopped -- note its OWN try_push
  //      refuses too, since stop() has already cleared run_; that is precisely how this case arises;
  //   2. the main shutdown thread, via Storage::StopAllBackgroundTasks() -> the wake event's Drain();
  //   3. any thread releasing main_lock_ in that window (~Accessor -> release -> the admit observer).
  //
  // For 2 and 3 this runs a full session chain on a thread that is not a pool worker, and naming the
  // thread understates it: the chain reaches Session::RunLoop -> session_.Execute(), which does a
  // SYNCHRONOUS, UN-TIMED socket send before arming a fresh async read. A client with a full receive
  // window can therefore block a GC/TTL/replication or main-shutdown thread here indefinitely. Master
  // never did session I/O off a pool or io thread; this is a genuine delta, accepted not unnoticed.
  //
  // Neither obvious mitigation works. Gating on IsShuttingDown() is a no-op, since reaching here already
  // implies it (try_push refuses only on !run_, set only by stop(), called only by ShutDown()). Dropping
  // the resume is strictly worse: the frame stays parked holding its campaign PendingHandle, and the
  // eventual destruction chain would call unregister_pending() on an already-destroyed main_lock_. The
  // real fix, if this delta ever has to go, is to suppress connection I/O in the resumed hook while
  // shutting down -- letting the frame unwind and release its handle without touching the socket.
  //
  // ParkArmGuard is REQUIRED, not defensive: the resumed chain can park again, and such a park has no
  // other arming site (the run loop's guards belong to worker task bodies, and this is not one).
  // Without it that park sits at gate == kParking forever, blocking every later acquisition on its
  // storage. catch(...) for the same class of reason: this runs inside ResumeAll's loop over claimed
  // waiters, so one escaping exception -- std or not -- would leave the rest permanently parked.
  spdlog::trace("PostResumeTask: all workers stopped, running parked resume inline during teardown");
  ParkArmGuard const arm_guard;
  try {
    task(Priority::LOW);
  } catch (...) {
    spdlog::critical("Parked query's inline teardown resume threw. That query will not make progress.");
  }
}

// Like push, but refuses once this worker has been asked to stop -- see PostResumeTask for why a
// resume must never be handed to a worker that will not run it. `run_` is written under mtx_ by
// stop(), so this check cannot miss a concurrent stop; and a worker that has NOT yet observed the stop
// still drains work_must_run_ in its tail before returning.
//
// Takes `new_task` by rvalue REFERENCE, not by value, and moves from it only after committing: the
// caller loops over candidate workers with the same task, so a refusal must leave it intact. A
// by-value parameter would consume it on every attempt -- including refused ones -- and leave the
// caller holding an empty std::move_only_function to hand to the next worker (or to invoke).
bool PriorityThreadPool::Worker::try_push(TaskSignature &&new_task, TaskID id, bool must_run) {
  {
    auto l = std::unique_lock{mtx_};
    if (!run_.load(std::memory_order_relaxed)) return false;
    Work w{.id = id, .work = std::move(new_task)};
    (must_run ? work_must_run_ : work_).push(std::move(w));
  }
  has_pending_work_ = true;
  cv_.notify_one();
  return true;
}

void PriorityThreadPool::Worker::push(TaskSignature new_task, TaskID id, bool must_run) {
  {
    auto l = std::unique_lock{mtx_};
    Work w{.id = id, .work = std::move(new_task)};
    (must_run ? work_must_run_ : work_).push(std::move(w));
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

  // Publish this worker's id for the duration of the run loop (LP workers only — see the
  // free-function doc comment in the header for why HP workers must not publish).
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
  // Drains BOTH queues; must-run tasks (parked-coroutine resumes) take precedence, so a resume is
  // serviced at the next task boundary rather than behind this worker's whole backlog. Only this
  // work_must_run_ is touched by this worker's own dequeue path (here), by push/try_push, and by an
  // LP thief's Phase 2A steal -- never by sched_mon (which only migrates work_) and never by an HP
  // thief. All of those hold this worker's mtx_.
  // True when work_'s head is a HIGH-priority item. Ids are handed out decreasing from
  // kMaxLowPriorityId for LP and from kMinHighPriorityId upward for HP, and Work orders by id in a
  // max-heap, so work_.top() is HP exactly when its id exceeds the LP ceiling.
  auto work_head_is_high_priority = [this] { return !work_.empty() && work_.top().id > kMaxLowPriorityId; };

  // Set by pop_task alongside `task`: whether the item it pulled came from work_must_run_. Read only
  // by the post-loop tail, which must run a leftover resume but must NOT start ordinary work.
  bool task_is_must_run = false;

  auto pop_task = [&] {
    // Must-run (a parked coroutine's resume) beats ordinary work: a query is blocked on it with its
    // storage-access timeout running. It must NOT beat a queued HIGH-priority item, though -- before
    // this queue existed there was one max-heap and HP was strictly first, and silently demoting HP
    // below every resume would be a priority inversion introduced by this feature. Note the queue's
    // precedence rules are NOT flag-gated -- they run for every installation regardless of whether
    // parking is enabled -- so the inversion would be everyone's, not an opt-in minority's.
    const bool use_must_run = !work_must_run_.empty() && !work_head_is_high_priority();
    task_is_must_run = use_must_run;
    auto &q = use_must_run ? work_must_run_ : work_;
    has_pending_work_.store(work_.size() + work_must_run_.size() > 1, std::memory_order::release);
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
      {
        // Arming a park published by this task is the pool's job, not any individual driver's:
        // publishing happens deep inside query::detail::AcquireAwaitable::await_suspend, while the
        // exclusion the gate provides must last until the whole task (and therefore the driver's
        // post-Resume() bookkeeping) is done. Doing it here, unconditionally and via a scope guard
        // so a throwing task still arms, is what makes "forgot to arm" -- a permanent hang --
        // unrepresentable. Harmless no-op for the overwhelming majority of tasks, which never park.
        // See utils/park_state.hpp.
        ParkArmGuard const arm_guard;
        task.value()(ThreadPriority);
      }
      task.reset();
    }
    // Phase 1B - check if there is other scheduled work (both queues)
    {
      auto l = std::unique_lock{mtx_};
      if (!work_.empty() || !work_must_run_.empty()) {
        pop_task();
        continue;  // Spin to phase 1A
      }
    }

    working_.store(false, std::memory_order_release);
    if constexpr (ThreadPriority != Priority::HIGH) {
      hot_threads.Set(worker_id);
    }

    // Phase 2A - try to steal work (LP thieves prefer work_must_run_, i.e. parked-coroutine
    // resumes; HP thieves only ever take HP items out of work_)
    for (auto *worker : other_workers) {
      if (has_pending_work_.load(std::memory_order_acquire)) break;  // This worker received work

      if (worker->has_pending_work_.load(std::memory_order_acquire) &&
          worker->working_.load(std::memory_order_acquire)) {
        auto l2 = std::unique_lock{worker->mtx_, std::defer_lock};
        if (!l2.try_lock()) continue;  // Busy, skip

        // Re-check under lock, and pick which queue to steal from. An LP thief prefers a parked
        // coroutine's resume (work_must_run_): it is latency-critical (a query is waiting on it, with
        // its storage-access timeout running) and, unlike ordinary work, sched_mon will never migrate
        // it off a wedged worker. Stealing is what keeps a resume from waiting out an unrelated long
        // task when every worker happened to be busy at post time. HP thieves never take it: a
        // resumed park continues Prepare, which must run as LP work (see the worker-id TLS contract).
        std::priority_queue<Work> *victim_q = nullptr;
        if constexpr (ThreadPriority != Priority::HIGH) {
          // Same precedence rule as the victim's own pop_task: prefer its resume, unless it has a
          // HIGH-priority item queued, which stays first.
          // Ordered so the must-run test short-circuits: with the flag off work_must_run_ is always
          // empty, and this runs under the victim's mtx_ on the steal hot path for every LP thief.
          if (!worker->work_must_run_.empty() &&
              !(!worker->work_.empty() && worker->work_.top().id > kMaxLowPriorityId)) {
            victim_q = &worker->work_must_run_;
          }
        }
        if (!victim_q) {
          if (worker->work_.empty()) continue;
          // HP threads can only steal HP work
          if constexpr (ThreadPriority == Priority::HIGH) {
            // If LP work, skip
            if (worker->work_.top().id <= kMaxLowPriorityId) continue;
          }
          victim_q = &worker->work_;
        }

        // Update flag as soon as possible (account for both queues so a remaining must-run task
        // is not mistakenly reported as "no pending work")
        worker->has_pending_work_.store(worker->work_.size() + worker->work_must_run_.size() > 1,
                                        std::memory_order_release);

        // Move work to current thread
        last_task_.store(victim_q->top().id, std::memory_order_release);
        task_is_must_run = (victim_q == &worker->work_must_run_);
        task = std::move(victim_q->top().work);

        victim_q->pop();

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
        if (!work_.empty() || !work_must_run_.empty()) {
          pop_task();
          return true;  // Spin to phase 1A and execute task
        }
        return !run_;  // Return and shutdown
      });
    }
  }

  // IP-1 F1 fix (opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md REVISION 5):
  // the loop above can exit (run_ observed false at the top of `while`) in the very same instant the
  // cv_.wait predicate already popped a task into `task` -- popping and the run_ check are not atomic
  // with each other. If that item was a RESUME it must still run: dropping it strands a parked query
  // and leaks the session it pins, which is what try_push's "accepted => eventually run" promise
  // rests on.
  //
  // If it was ordinary work, it is dropped, exactly as master does. Master's loop ends right here, so
  // a task popped in that same instant was destroyed unrun -- and running it instead would be a
  // flag-independent change to shutdown behaviour (query work starting after ShutDown() was
  // requested, concurrently with the rest of the teardown handler). The argument does not depend on the
  // flag's default: this tail runs for every installation whether or not parking is enabled.
  // `task_is_must_run` is what keeps the fix to the case that needs it.
  if (task && task_is_must_run) {
    ParkArmGuard const arm_guard;  // same task-boundary arming as Phase 1A above
    // catch(...) for the same reason as PostResumeTask's inline fallback: an exception escaping here
    // would propagate out of the worker's thread function, which terminates the process, and would
    // also abandon whatever else is still queued in the drain below. One stranded query beats that.
    try {
      task.value()(ThreadPriority);
    } catch (...) {
      spdlog::critical("Parked query's resume threw during worker teardown. That query will not make progress.");
    }
  }
  task.reset();

  // Then drain any remaining must-run work. PostResumeTask (see its doc comment) posts rather than
  // resuming inline while any worker lives, so a bail-resume can still be accepted here, in the window
  // between stop() being requested and this thread actually returning (e.g. a lock-release NotifyAll
  // racing the tail of PriorityThreadPool::ShutDown()). Draining ONLY work_must_run_ -- never the
  // stealable work_ queue, which may hold ordinary application work that must NOT start once this
  // worker is torn down -- guarantees no parked coroutine frame is left registered-but-never-resumed.
  for (;;) {
    TaskSignature drained_task;
    {
      auto l = std::unique_lock{mtx_};
      if (work_must_run_.empty()) break;
      drained_task = std::move(work_must_run_.top().work);
      work_must_run_.pop();
    }
    ParkArmGuard const arm_guard;  // a drained resume can itself re-park; arm it like any other task
    try {
      drained_task(ThreadPriority);
    } catch (...) {
      // Must not abandon the rest of the queue, and must not escape the thread function.
      spdlog::critical("Parked query's resume threw during the worker's must-run drain. It will not make progress.");
    }
  }

  // Teardown: drop this worker's published identity so GetCurrentWorkerId() returns nullopt
  // once the thread leaves its pool role.
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
