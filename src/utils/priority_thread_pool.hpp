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

#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

#include "utils/numa.hpp"
#include "utils/priorities.hpp"
#include "utils/scheduler.hpp"

namespace memgraph::utils {

/**
 * NUMA-aware thread-safe mask that slices workers into NUMA groups.
 * This prevents cross-socket cache-line contention by isolating bitmask
 * operations to the specific NUMA node where the worker resides.
 */
class HotMask {
 public:
  static constexpr auto kMaxElements = 1024U;
  static constexpr auto kMaxNodes = 16U;
  static constexpr auto kCacheLineSize = 64U;

  explicit HotMask(uint16_t n_elements, const std::optional<numa::NUMATopology> &topo);

  // Register which NUMA node a worker belongs to during initialization
  void RegisterWorker(uint16_t worker_id, int numa_node);

  void Set(const uint16_t id);
  void Reset(const uint16_t id);

  // Tries to find a hot worker specifically within the given NUMA node
  std::optional<uint16_t> GetHotElementInNode(int numa_node);

  // Tries to find any hot worker across all NUMA nodes (global fallback)
  std::optional<uint16_t> GetHotElementAny();

 private:
  static constexpr auto kGroupSize = 64U;

  struct alignas(kCacheLineSize) PaddedAtomic {
    std::atomic<uint64_t> mask{0};
    std::array<uint8_t, kCacheLineSize - sizeof(std::atomic<uint64_t>)> padding{};
  };

  struct NodeSlice {
    uint16_t n_groups{0};
    std::array<PaddedAtomic, (kMaxElements / kGroupSize)> masks;
  };

  std::optional<uint16_t> SearchSlice(NodeSlice &slice);

  const uint16_t n_elements_;
  uint16_t n_nodes_ = 1;
  std::array<int, kMaxElements> worker_to_node_{};
  std::array<NodeSlice, kMaxNodes> node_slices_{};
};

using TaskSignature = std::move_only_function<void(utils::Priority)>;

class TaskCollection {
 public:
  void AddTask(TaskSignature task) { tasks_.emplace_back(std::move(task)); }

  class Task {
   public:
    explicit Task(TaskSignature task)
        : state_(std::make_shared<std::atomic<State>>(State::IDLE)), task_(std::move(task)) {}

    enum class State : uint8_t { IDLE, SCHEDULED, FINISHED };
    std::shared_ptr<std::atomic<State>> state_;
    TaskSignature task_;
  };

  Task &operator[](size_t index) { return tasks_[index]; }
  TaskSignature WrapTask(size_t index);
  void Wait();
  void WaitOrSteal();
  size_t Size() const { return tasks_.size(); }

 private:
  std::vector<Task> tasks_;
};

class PriorityThreadPool {
 public:
  using TaskID = uint64_t;

  PriorityThreadPool(uint16_t mixed_work_threads_count, uint16_t high_priority_threads_count);
  PriorityThreadPool(uint16_t mixed_work_threads_count, uint16_t high_priority_threads_count,
                     const numa::NUMATopology &topology);
  ~PriorityThreadPool();

  void AwaitShutdown();
  void ShutDown();
  void ScheduledAddTask(TaskSignature new_task, Priority priority);
  void ScheduledCollection(TaskCollection &collection);

  class Worker {
   public:
    struct Work {
      TaskID id;
      mutable TaskSignature work;
      bool operator<(const Work &other) const { return id < other.id; }
    };

    void push(TaskSignature new_task, TaskID id);
    void stop();

    template <Priority ThreadPriority>
    void operator()(uint16_t worker_id, const std::vector<std::unique_ptr<Worker>> &workers_pool, HotMask &hot_threads);

   private:
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::priority_queue<Work> work_;
    std::atomic_bool has_pending_work_{false};
    std::atomic_bool working_{false};
    std::atomic_bool run_{true};
    std::atomic<TaskID> last_task_{0};

    int numa_node_{-1};

    friend class PriorityThreadPool;
  };

 private:
  uint16_t GetNextWorkerID(int current_numa_node);

  std::stop_source pool_stop_source_;
  std::vector<std::unique_ptr<Worker>> workers_;
  std::vector<std::unique_ptr<Worker>> hp_workers_;
  HotMask hot_threads_;
  std::vector<std::jthread> pool_;
  utils::Scheduler monitoring_;

  std::atomic<TaskID> task_id_;
  std::optional<numa::NUMATopology> numa_topology_;

  // Pre-cached mapping of NUMA nodes to global worker indices
  std::vector<std::vector<uint16_t>> node_to_workers_;

  std::vector<Worker *> GetWorkersInNUMAGroup(int numa_node) const;
  Worker *GetWorkerFromDifferentNUMA(int exclude_numa_node) const;
};

}  // namespace memgraph::utils
