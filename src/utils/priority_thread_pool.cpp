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
#include <bit>
#include <limits>

#include <spdlog/spdlog.h>

#include "utils/barrier.hpp"
#include "utils/thread.hpp"
#include "utils/tsc.hpp"
#include "utils/yielder.hpp"

namespace {
constexpr memgraph::utils::PriorityThreadPool::TaskID kMaxLowPriorityId = std::numeric_limits<int64_t>::max();
constexpr memgraph::utils::PriorityThreadPool::TaskID kMinHighPriorityId = kMaxLowPriorityId;
}  // namespace

namespace memgraph::utils {

// --- HotMask Implementation ---

HotMask::HotMask(uint16_t n_elements, const std::optional<numa::NUMATopology> &topo) : n_elements_(n_elements) {
  worker_to_node_.fill(0);
  if (topo && !topo->nodes.empty()) {
    // Explicit template argument to std::min solves uint16_t vs unsigned int conflict
    n_nodes_ = std::min<uint16_t>(static_cast<uint16_t>(topo->nodes.size()), kMaxNodes);
  }
}

void HotMask::RegisterWorker(uint16_t worker_id, int numa_node) {
  int node = (numa_node < 0) ? 0 : (numa_node % static_cast<int>(n_nodes_));
  worker_to_node_[worker_id] = node;
  node_slices_[node].n_groups = ((n_elements_ - 1) / kGroupSize) + 1;
}

void HotMask::Set(const uint16_t id) {
  int node = worker_to_node_[id];
  node_slices_[node].masks[id / kGroupSize].mask.fetch_or(1UL << (id % kGroupSize), std::memory_order_release);
}

void HotMask::Reset(const uint16_t id) {
  int node = worker_to_node_[id];
  node_slices_[node].masks[id / kGroupSize].mask.fetch_and(~(1UL << (id % kGroupSize)), std::memory_order_release);
}

std::optional<uint16_t> HotMask::SearchSlice(NodeSlice &slice) {
  for (uint16_t i = 0; i < slice.n_groups; ++i) {
    auto &atomic_mask = slice.masks[i].mask;
    uint64_t mask = atomic_mask.load(std::memory_order_acquire);
    while (mask > 0) {
      int bit = std::countr_zero(mask);
      uint64_t next = mask & ~(1UL << bit);
      if (atomic_mask.compare_exchange_weak(mask, next, std::memory_order::acq_rel)) {
        return static_cast<uint16_t>(bit + (i * kGroupSize));
      }
    }
  }
  return std::nullopt;
}

std::optional<uint16_t> HotMask::GetHotElementInNode(int numa_node) {
  int node = (numa_node < 0) ? 0 : (numa_node % static_cast<int>(n_nodes_));
  return SearchSlice(node_slices_[node]);
}

std::optional<uint16_t> HotMask::GetHotElementAny() {
  for (uint16_t i = 0; i < n_nodes_; ++i) {
    if (auto res = SearchSlice(node_slices_[i])) return res;
  }
  return std::nullopt;
}

// --- PriorityThreadPool Implementation ---

struct ThreadLocalDispatch {
  uint16_t local_rr_index = 0;
};

uint16_t PriorityThreadPool::GetNextWorkerID(int current_numa_node) {
  static thread_local ThreadLocalDispatch dispatch_state;

  // Use pre-cached NUMA slices to avoid $O(N)$ scanning and allocations
  if (current_numa_node >= 0 && current_numa_node < static_cast<int>(node_to_workers_.size())) {
    const auto &local_workers = node_to_workers_[current_numa_node];
    if (!local_workers.empty()) {
      uint16_t idx = dispatch_state.local_rr_index++ % static_cast<uint16_t>(local_workers.size());
      return local_workers[idx];
    }
  }

  // Fallback to global round-robin if node is empty or invalid
  return dispatch_state.local_rr_index++ % static_cast<uint16_t>(workers_.size());
}

PriorityThreadPool::PriorityThreadPool(uint16_t mixed_work_threads_count, uint16_t high_priority_threads_count)
    : hot_threads_{mixed_work_threads_count, std::nullopt}, task_id_{kMaxLowPriorityId} {
  spdlog::trace("PriorityThreadPool constructor: mixed_work_threads_count={}, high_priority_threads_count={}",
                mixed_work_threads_count, high_priority_threads_count);

  pool_.reserve(mixed_work_threads_count + high_priority_threads_count);
  workers_.resize(mixed_work_threads_count);
  hp_workers_.resize(high_priority_threads_count);
  spdlog::trace("PriorityThreadPool: reserved pool for {} threads, resized workers to {}, hp_workers to {}",
                mixed_work_threads_count + high_priority_threads_count, mixed_work_threads_count,
                high_priority_threads_count);

  SimpleBarrier barrier{static_cast<size_t>(mixed_work_threads_count + high_priority_threads_count)};
  spdlog::trace("PriorityThreadPool: created barrier for {} threads",
                mixed_work_threads_count + high_priority_threads_count);

  for (size_t i = 0; i < mixed_work_threads_count; ++i) {
    hot_threads_.RegisterWorker(static_cast<uint16_t>(i), 0);
    spdlog::trace("PriorityThreadPool: registering mixed worker {} to NUMA node 0", i);
    pool_.emplace_back([this, i, &barrier]() {
      workers_[i] = std::make_unique<Worker>();
      barrier.arrive_and_wait();
      spdlog::trace("PriorityThreadPool: mixed worker {} started and passed barrier", i);
      workers_[i]->operator()<Priority::LOW>(static_cast<uint16_t>(i), workers_, hot_threads_);
    });
    spdlog::trace("PriorityThreadPool: created mixed worker thread {}", i);
  }

  for (size_t i = 0; i < high_priority_threads_count; ++i) {
    pool_.emplace_back([this, i, &barrier]() {
      hp_workers_[i] = std::make_unique<Worker>();
      barrier.arrive_and_wait();
      spdlog::trace("PriorityThreadPool: high priority worker {} started and passed barrier", i);
      hp_workers_[i]->operator()<Priority::HIGH>(static_cast<uint16_t>(i), workers_, hot_threads_);
    });
    spdlog::trace("PriorityThreadPool: created high priority worker thread {}", i);
  }

  spdlog::trace("PriorityThreadPool constructor completed: {} total threads created",
                mixed_work_threads_count + high_priority_threads_count);
}

PriorityThreadPool::PriorityThreadPool(uint16_t mixed_work_threads_count, uint16_t high_priority_threads_count,
                                       const numa::NUMATopology &topology)
    : hot_threads_{mixed_work_threads_count, topology}, task_id_{kMaxLowPriorityId}, numa_topology_{topology} {
  spdlog::trace(
      "PriorityThreadPool constructor (NUMA-aware): mixed_work_threads_count={}, high_priority_threads_count={}, "
      "numa_nodes={}",
      mixed_work_threads_count, high_priority_threads_count, topology.nodes.size());

  workers_.resize(mixed_work_threads_count);
  hp_workers_.resize(high_priority_threads_count);
  node_to_workers_.resize(topology.nodes.size());
  spdlog::trace("PriorityThreadPool: resized workers to {}, hp_workers to {}, node_to_workers to {} nodes",
                mixed_work_threads_count, high_priority_threads_count, topology.nodes.size());

  SimpleBarrier barrier{static_cast<size_t>(mixed_work_threads_count + high_priority_threads_count)};
  spdlog::trace("PriorityThreadPool: created barrier for {} threads",
                mixed_work_threads_count + high_priority_threads_count);

  int assigned = 0;
  for (const auto &node : topology.nodes) {
    auto cores = node.GetPrimaryCores();
    spdlog::trace("PriorityThreadPool: processing NUMA node {} with {} primary cores", node.node_id, cores.size());
    for ([[maybe_unused]] int cpu : cores) {
      if (assigned >= mixed_work_threads_count) break;
      int idx = assigned++;
      hot_threads_.RegisterWorker(static_cast<uint16_t>(idx), node.node_id);
      node_to_workers_[node.node_id].push_back(static_cast<uint16_t>(idx));
      spdlog::trace("PriorityThreadPool: registering mixed worker {} to NUMA node {} (primary core)", idx,
                    node.node_id);

      pool_.emplace_back([this, idx, node_id = node.node_id, &barrier, &topology]() {
        numa::PinThreadToNUMANode(node_id, topology);
        workers_[idx] = std::make_unique<Worker>();
        workers_[idx]->numa_node_ = node_id;
        barrier.arrive_and_wait();
        spdlog::trace("PriorityThreadPool: mixed worker {} (NUMA node {}) started and passed barrier", idx, node_id);
        workers_[idx]->operator()<Priority::LOW>(static_cast<uint16_t>(idx), workers_, hot_threads_);
      });
      spdlog::trace("PriorityThreadPool: created mixed worker thread {} on NUMA node {}", idx, node.node_id);
    }
  }

  for (const auto &node : topology.nodes) {
    auto cores = node.GetHyperthreads();
    spdlog::trace("PriorityThreadPool: processing NUMA node {} with {} hyperthread cores", node.node_id, cores.size());
    for ([[maybe_unused]] int cpu : cores) {
      if (assigned >= mixed_work_threads_count) break;
      int idx = assigned++;
      hot_threads_.RegisterWorker(static_cast<uint16_t>(idx), node.node_id);
      node_to_workers_[node.node_id].push_back(static_cast<uint16_t>(idx));
      spdlog::trace("PriorityThreadPool: registering mixed worker {} to NUMA node {} (hyperthread)", idx, node.node_id);

      pool_.emplace_back([this, idx, node_id = node.node_id, &barrier, &topology]() {
        numa::PinThreadToNUMANode(node_id, topology);
        workers_[idx] = std::make_unique<Worker>();
        workers_[idx]->numa_node_ = node_id;
        barrier.arrive_and_wait();
        spdlog::trace("PriorityThreadPool: mixed worker {} (NUMA node {}) started and passed barrier", idx, node_id);
        workers_[idx]->operator()<Priority::LOW>(static_cast<uint16_t>(idx), workers_, hot_threads_);
      });
      spdlog::trace("PriorityThreadPool: created mixed worker thread {} on NUMA node {} (hyperthread)", idx,
                    node.node_id);
    }
  }

  // TODO Fix this logic (handle any number of threads)

  spdlog::trace("PriorityThreadPool: creating {} high priority workers", high_priority_threads_count);
  for (size_t i = 0; i < high_priority_threads_count; ++i) {
    int node_id = topology.nodes[i % topology.nodes.size()].node_id;
    spdlog::trace("PriorityThreadPool: assigning high priority worker {} to NUMA node {}", i, node_id);
    pool_.emplace_back([this, i, node_id, &barrier, &topology]() {
      numa::PinThreadToNUMANode(node_id, topology);
      hp_workers_[i] = std::make_unique<Worker>();
      hp_workers_[i]->numa_node_ = node_id;
      barrier.arrive_and_wait();
      spdlog::trace("PriorityThreadPool: high priority worker {} (NUMA node {}) started and passed barrier", i,
                    node_id);
      hp_workers_[i]->operator()<Priority::HIGH>(static_cast<uint16_t>(i), workers_, hot_threads_);
    });
    spdlog::trace("PriorityThreadPool: created high priority worker thread {} on NUMA node {}", i, node_id);
  }

  spdlog::trace(
      "PriorityThreadPool constructor (NUMA-aware) completed: {} mixed workers, {} high priority workers, "
      "total {} threads",
      assigned, high_priority_threads_count, assigned + high_priority_threads_count);
}

void PriorityThreadPool::ScheduledAddTask(TaskSignature new_task, Priority priority) {
  if (pool_stop_source_.stop_requested()) [[unlikely]]
    return;

  const auto id = (TaskID(priority == Priority::HIGH) * kMinHighPriorityId) + --task_id_;
  int cur_node = numa::GetCurrentNUMANode();

  std::optional<uint16_t> target = hot_threads_.GetHotElementInNode(cur_node);
  if (!target) target = hot_threads_.GetHotElementAny();
  if (!target) target = GetNextWorkerID(cur_node);

  workers_[*target]->push(std::move(new_task), id);
}

template <Priority ThreadPriority>
void PriorityThreadPool::Worker::operator()(uint16_t worker_id,
                                            const std::vector<std::unique_ptr<Worker>> &workers_pool,
                                            HotMask &hot_threads) {
  utils::ThreadSetName(ThreadPriority == Priority::HIGH ? "pool_hp" : "pool_mixed");

  std::vector<Worker *> neighbors;
  for (const auto &w : workers_pool) {
    if (w && w.get() != this && w->numa_node_ == this->numa_node_) {
      neighbors.push_back(w.get());
    }
  }

  std::optional<TaskSignature> task;
  while (run_.load(std::memory_order_acquire)) {
    if (task) {
      working_.store(true, std::memory_order_relaxed);
      (*task)(ThreadPriority);
      working_.store(false, std::memory_order_relaxed);
      task.reset();
    }

    {
      std::unique_lock l{mtx_};
      if (!work_.empty()) {
        last_task_.store(work_.top().id, std::memory_order_release);
        task = std::move(work_.top().work);
        work_.pop();
        has_pending_work_.store(!work_.empty(), std::memory_order_release);
        continue;
      }
    }

    if constexpr (ThreadPriority != Priority::HIGH) hot_threads.Set(worker_id);

    // Phase 2: Local NUMA stealing
    for (auto *neighbor : neighbors) {
      if (neighbor->has_pending_work_.load(std::memory_order_acquire)) {
        std::unique_lock l2{neighbor->mtx_, std::defer_lock};
        if (l2.try_lock() && !neighbor->work_.empty()) {
          if constexpr (ThreadPriority == Priority::HIGH) {
            if (neighbor->work_.top().id <= kMaxLowPriorityId) continue;
          }
          task = std::move(neighbor->work_.top().work);
          neighbor->work_.pop();
          neighbor->has_pending_work_.store(!neighbor->work_.empty(), std::memory_order_release);
          break;
        }
      }
    }

    if (task) {
      if constexpr (ThreadPriority != Priority::HIGH) hot_threads.Reset(worker_id);
      continue;
    }

    // Phase 3: Spin
    const auto freq = utils::GetTSCFrequency();
    if (freq) {
      const utils::TSCTimer timer{freq};
      yielder y;
      while (timer.Elapsed() < 0.001) {
        if (y([this] { return has_pending_work_.load(std::memory_order_acquire); }, 1024U, 0U)) break;
      }
    }

    if constexpr (ThreadPriority != Priority::HIGH) hot_threads.Reset(worker_id);

    std::unique_lock l{mtx_};
    cv_.wait(l, [this] { return !work_.empty() || !run_; });
  }
}

void PriorityThreadPool::Worker::push(TaskSignature new_task, TaskID id) {
  {
    std::lock_guard l{mtx_};
    work_.emplace(id, std::move(new_task));
  }
  has_pending_work_.store(true, std::memory_order_release);
  cv_.notify_one();
}

void PriorityThreadPool::Worker::stop() {
  {
    std::lock_guard l{mtx_};
    run_ = false;
  }
  cv_.notify_one();
}

void PriorityThreadPool::ShutDown() {
  pool_stop_source_.request_stop();
  monitoring_.Stop();
  for (auto &w : workers_)
    if (w) w->stop();
  for (auto &w : hp_workers_)
    if (w) w->stop();
}

PriorityThreadPool::~PriorityThreadPool() { ShutDown(); }

void PriorityThreadPool::AwaitShutdown() { pool_.clear(); }

void PriorityThreadPool::ScheduledCollection(TaskCollection &collection) {
  for (size_t i = 0; i < collection.Size(); ++i) {
    ScheduledAddTask(collection.WrapTask(i), Priority::LOW);
  }
}

TaskSignature TaskCollection::WrapTask(size_t index) {
  auto &task = tasks_[index];
  return [&task = task.task_, state = task.state_](utils::Priority priority) {
    auto expected = Task::State::IDLE;
    if (!state->compare_exchange_strong(expected, Task::State::SCHEDULED, std::memory_order_acq_rel)) return;

    try {
      task(priority);
      state->store(Task::State::FINISHED, std::memory_order_release);
      state->notify_one();
    } catch (...) {
      state->store(Task::State::FINISHED, std::memory_order_release);
      state->notify_one();
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
  for (auto &task : tasks_) {
    auto expected = Task::State::IDLE;
    if (task.state_->compare_exchange_strong(expected, Task::State::SCHEDULED, std::memory_order_acq_rel)) {
      task.task_(Priority::LOW);
      task.state_->store(Task::State::FINISHED, std::memory_order_release);
      task.state_->notify_one();
    }
  }
  Wait();
}

std::vector<PriorityThreadPool::Worker *> PriorityThreadPool::GetWorkersInNUMAGroup(int numa_node) const {
  std::vector<Worker *> result;
  for (const auto &worker : workers_) {
    if (worker && worker->numa_node_ == numa_node) result.push_back(worker.get());
  }
  return result;
}

PriorityThreadPool::Worker *PriorityThreadPool::GetWorkerFromDifferentNUMA(int exclude_numa_node) const {
  for (const auto &worker : workers_) {
    if (worker && worker->numa_node_ >= 0 && worker->numa_node_ != exclude_numa_node) return worker.get();
  }
  return workers_.empty() ? nullptr : workers_[0].get();
}

}  // namespace memgraph::utils
