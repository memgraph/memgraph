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
#include "utils/numa.hpp"
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

PriorityThreadPool::PriorityThreadPool(uint16_t mixed_work_threads_count, uint16_t high_priority_threads_count)
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
    pool_.emplace_back([this, i, &barrier]() {
      // Divide work by each thread
      workers_[i] = std::make_unique<Worker>();
      barrier.arrive_and_wait();
      workers_[i]->operator()<Priority::LOW>(i, workers_, hot_threads_);
    });
  }

  for (size_t i = 0; i < high_priority_threads_count; ++i) {
    pool_.emplace_back([this, i, &barrier]() {
      hp_workers_[i] = std::make_unique<Worker>();
      barrier.arrive_and_wait();
      hp_workers_[i]->operator()<Priority::HIGH>(i, workers_, hot_threads_);
    });
  }

  barrier.wait();
}

PriorityThreadPool::PriorityThreadPool(uint16_t mixed_work_threads_count, uint16_t high_priority_threads_count,
                                       const numa::NUMATopology &topology)
    : hot_threads_{mixed_work_threads_count}, task_id_{kMaxLowPriorityId}, last_wid_{0}, numa_topology_{topology} {
  MG_ASSERT(mixed_work_threads_count > 0, "PriorityThreadPool requires at least one mixed work thread");
  MG_ASSERT(mixed_work_threads_count <= kMaxWorkers,
            "PriorityThreadPool supports a maximum of 1024 mixed work threads");
  MG_ASSERT(high_priority_threads_count > 0, "PriorityThreadPool requires at least one high priority work thread");

  // Build NUMA node assignment: fill one NUMA node at a time
  // We pin to NUMA groups (not specific CPUs) to allow OS scheduling flexibility
  std::vector<int> numa_assignments;  // NUMA node IDs

  // First pass: assign NUMA nodes based on primary cores (one NUMA node at a time)
  for (const auto &node : topology.nodes) {
    auto primary_cores = node.GetPrimaryCores();
    // Assign one worker per primary core in this NUMA node
    for (size_t i = 0; i < primary_cores.size() && numa_assignments.size() < mixed_work_threads_count; ++i) {
      numa_assignments.push_back(node.node_id);
    }
  }

  // Second pass: if we need more workers, use hyperthreads (still one NUMA node at a time)
  if (numa_assignments.size() < mixed_work_threads_count) {
    for (const auto &node : topology.nodes) {
      auto hyperthreads = node.GetHyperthreads();
      // Assign one worker per hyperthread in this NUMA node
      for (size_t i = 0; i < hyperthreads.size() && numa_assignments.size() < mixed_work_threads_count; ++i) {
        numa_assignments.push_back(node.node_id);
      }
    }
  }

  // Assign HP threads: one per NUMA node
  std::vector<int> hp_numa_assignments;
  for (size_t i = 0; i < topology.nodes.size() && i < static_cast<size_t>(high_priority_threads_count); ++i) {
    hp_numa_assignments.push_back(topology.nodes[i].node_id);
  }

  // If we don't have enough NUMA assignments, extend by repeating
  if (numa_assignments.size() < mixed_work_threads_count) {
    spdlog::warn(
        "Not enough NUMA capacity: requested {} workers, but only {} NUMA slots found. "
        "Some workers will share NUMA nodes.",
        mixed_work_threads_count, numa_assignments.size());
    // Extend by repeating assignments cyclically
    size_t original_len = numa_assignments.size();
    if (original_len > 0) {
      while (numa_assignments.size() < mixed_work_threads_count) {
        numa_assignments.push_back(numa_assignments[numa_assignments.size() % original_len]);
      }
    }
  }

  pool_.reserve(mixed_work_threads_count + high_priority_threads_count);
  workers_.resize(mixed_work_threads_count);
  hp_workers_.resize(high_priority_threads_count);

  const size_t nthreads = mixed_work_threads_count + high_priority_threads_count;
  SimpleBarrier barrier{nthreads};

  // Create mixed priority workers with NUMA node pinning
  for (size_t i = 0; i < mixed_work_threads_count; ++i) {
    int numa_node = numa_assignments.empty() ? 0 : numa_assignments[i % numa_assignments.size()];

    pool_.emplace_back([this, i, numa_node, &barrier]() {
      // Pin thread to NUMA node (allows OS to schedule on any CPU in that NUMA node)
      if (!numa::PinThreadToNUMANode(numa_node)) {
        spdlog::warn("Failed to pin worker {} to NUMA node {}", i, numa_node);
      } else {
        spdlog::trace("Pinned worker {} to NUMA node {}", i, numa_node);
      }

      workers_[i] = std::make_unique<Worker>();
      workers_[i]->numa_node_ = numa_node;
      workers_[i]->cpu_id_ = -1;  // Not pinned to specific CPU, just NUMA node

      barrier.arrive_and_wait();
      workers_[i]->operator()<Priority::LOW>(i, workers_, hot_threads_);
    });
  }

  // Create high priority workers with NUMA node pinning (one per NUMA node)
  for (size_t i = 0; i < high_priority_threads_count; ++i) {
    int numa_node = topology.nodes[0].node_id;  // Default to first NUMA node
    if (i < hp_numa_assignments.size()) {
      numa_node = hp_numa_assignments[i];
    }

    pool_.emplace_back([this, i, numa_node, &barrier]() {
      // Pin thread to NUMA node (allows OS to schedule on any CPU in that NUMA node)
      if (!numa::PinThreadToNUMANode(numa_node)) {
        spdlog::warn("Failed to pin HP worker {} to NUMA node {}", i, numa_node);
      } else {
        spdlog::trace("Pinned HP worker {} to NUMA node {}", i, numa_node);
      }

      hp_workers_[i] = std::make_unique<Worker>();
      hp_workers_[i]->numa_node_ = numa_node;
      hp_workers_[i]->cpu_id_ = -1;  // Not pinned to specific CPU, just NUMA node

      barrier.arrive_and_wait();
      hp_workers_[i]->operator()<Priority::HIGH>(i, workers_, hot_threads_);
    });
  }

  barrier.wait();

  // Under heavy load a task can get stuck, monitor and move to different thread
  monitoring_.SetInterval(std::chrono::milliseconds(100));
  monitoring_.Run("sched_mon", [this, workers_num = workers_.size(), hp_workers_num = hp_workers_.size(),
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
        Worker::Work work{worker->work_.top().id, std::move(worker->work_.top().work)};
        worker->work_.pop();
        l.unlock();

        // Try to find a worker in the same NUMA group first
        Worker *target_worker = nullptr;
        if (numa_topology_.has_value() && worker->numa_node_ >= 0) {
          // First try same NUMA group
          auto same_numa_workers = GetWorkersInNUMAGroup(worker->numa_node_);
          for (auto *w : same_numa_workers) {
            if (w != worker.get() && !w->has_pending_work_.load(std::memory_order_acquire)) {
              target_worker = w;
              break;
            }
          }

          // If no worker in same NUMA group, allow cross-NUMA migration for stuck tasks
          if (!target_worker) {
            target_worker = GetWorkerFromDifferentNUMA(worker->numa_node_);
          }
        }

        // Fallback to original logic if not NUMA-aware or no NUMA worker found
        if (!target_worker) {
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
          target_worker = workers_[*tid].get();
        }

        if (target_worker) {
          target_worker->push(std::move(work.work), work.id);
        }
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

  // If NUMA-aware, try to find a hot thread in the same NUMA group as the current thread
  uint16_t target_worker_id = 0;
  if (numa_topology_.has_value()) {
    int current_numa = numa::GetCurrentNUMANode();
    if (current_numa >= 0) {
      auto same_numa_workers = GetWorkersInNUMAGroup(current_numa);
      // Try to find a hot thread in same NUMA group
      auto tid = hot_threads_.GetHotElement();
      if (tid && *tid < workers_.size() && workers_[*tid]->numa_node_ == current_numa) {
        target_worker_id = *tid;
      } else {
        // Find first available worker in same NUMA group
        for (auto *w : same_numa_workers) {
          size_t worker_idx = 0;
          for (size_t i = 0; i < workers_.size(); ++i) {
            if (workers_[i].get() == w) {
              worker_idx = i;
              break;
            }
          }
          if (worker_idx < workers_.size()) {
            target_worker_id = static_cast<uint16_t>(worker_idx);
            break;
          }
        }
      }
    }
  }

  // Fallback to original logic
  if (!numa_topology_.has_value() || target_worker_id == 0) {
    auto tid = hot_threads_.GetHotElement();
    if (!tid) {
      // Limit the number of directly used threads when there are more workers than hw threads.
      // Gives better overall performance.
      static const auto max_wakeup_thread =
          std::min(static_cast<TaskID>(std::thread::hardware_concurrency()), workers_.size());
      // If no hot thread found, give it to the next thread
      tid = last_wid_++ % max_wakeup_thread;
    }
    target_worker_id = *tid;
  }

  workers_[target_worker_id]->push(std::move(new_task), id);
  // High priority tasks are marked and given to mixed priority threads (at front of the queue)
  // HP threads are going to steal this work if not executed in time
}

std::vector<PriorityThreadPool::Worker *> PriorityThreadPool::GetWorkersInNUMAGroup(int numa_node) const {
  std::vector<Worker *> result;
  for (const auto &worker : workers_) {
    if (worker && worker->numa_node_ == numa_node) {
      result.push_back(worker.get());
    }
  }
  return result;
}

PriorityThreadPool::Worker *PriorityThreadPool::GetWorkerFromDifferentNUMA(int exclude_numa_node) const {
  // Find first worker from a different NUMA node
  for (const auto &worker : workers_) {
    if (worker && worker->numa_node_ >= 0 && worker->numa_node_ != exclude_numa_node) {
      return worker.get();
    }
  }
  // Fallback: return first worker if no different NUMA node found
  return workers_.empty() ? nullptr : workers_[0].get();
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
void PriorityThreadPool::Worker::operator()(const uint16_t worker_id,
                                            const std::vector<std::unique_ptr<Worker>> &workers_pool,
                                            HotMask &hot_threads) {
  utils::ThreadSetName(ThreadPriority == Priority::HIGH ? "high prior." : "low prior.");

  // Both mixed and high priority worker only steal from mixed worker
  // NUMA-aware: only steal from workers in the same NUMA group
  const auto other_workers = std::invoke([&workers_pool, self = this, worker_id]() -> std::vector<Worker *> {
    if constexpr (ThreadPriority != Priority::HIGH) {
      // Only mixed work threads can have work stolen, workers_pool does not contain hp threads (skip self)
      // Filter to only workers in the same NUMA group
      std::vector<Worker *> other_workers;
      for (const auto &worker : workers_pool) {
        if (worker.get() == self) continue;
        // Only steal from workers in the same NUMA group
        if (self->numa_node_ >= 0 && worker->numa_node_ == self->numa_node_) {
          other_workers.push_back(worker.get());
        } else if (self->numa_node_ < 0) {
          // If NUMA info not available, allow stealing from any worker (fallback)
          other_workers.push_back(worker.get());
        }
      }
      // Shuffle order for better load distribution
      if (other_workers.size() > 1) {
        const size_t rotate_amount = other_workers.size() - (worker_id % other_workers.size());
        std::rotate(other_workers.begin(), other_workers.begin() + static_cast<ptrdiff_t>(rotate_amount),
                    other_workers.end());
      }
      return other_workers;
    } else {
      // Hp threads steal from any mixed work thread in the same NUMA group
      std::vector<Worker *> other_workers;
      for (const auto &worker : workers_pool) {
        // Only steal from workers in the same NUMA group
        if (self->numa_node_ < 0 || worker->numa_node_ == self->numa_node_) {
          other_workers.push_back(worker.get());
        }
      }
      return other_workers;
    }
  });

  std::optional<TaskSignature> task;
  auto pop_task = [&] {
    has_pending_work_.store(work_.size() > 1, std::memory_order::release);
    last_task_.store(work_.top().id, std::memory_order_release);
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
