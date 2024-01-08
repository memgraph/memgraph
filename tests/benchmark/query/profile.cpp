// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <memory>
#include <vector>

#include <benchmark/benchmark.h>

#include "query/context.hpp"
#include "utils/tsc.hpp"

/*
 * We're benchmarking three different implementations of the profiling
 * machinery:
 *
 * 1. Using `std::unordered_map` to associate every logical operator with its
 * profiling stats.
 *
 * 2. Using a tree of profiling stats whose structure corresponds to the logical
 * operators that were executed (no explicit association exists between a
 * logical operator and its profiling stats).
 *
 * 3. The same as 2. but storing the nodes of the tree within a contiguous block
 * of storage (pool).
 */

namespace {

//////////////////////////////////////////////////////////////////////////////
//
// NAryTree

/*
 * A utility to help us imitate the traversal of a plan (and therefore the
 * profiling statistics as well).
 */
struct NAryTree {
  NAryTree(int64_t id) : id(id) {}

  int64_t id;
  std::vector<std::unique_ptr<NAryTree>> children;
};

std::unique_ptr<NAryTree> LiftTreeHelper(const std::vector<int64_t> &flattened, size_t *index) {
  int64_t id = flattened[(*index)++];
  size_t num_children = flattened[(*index)++];
  auto node = std::make_unique<NAryTree>(id);

  if (num_children == 0) {
    return node;
  }

  for (int64_t i = 0; i < num_children; ++i) {
    node->children.emplace_back(LiftTreeHelper(flattened, index));
  }

  return node;
}

/*
 * A utility that produces instances of `NAryTree` given its "flattened"
 * (serialized) form. A node (without its children) is serialized as a sequence
 * of integers: `<node-id> <node-num-children>`. A tree is serialized by
 * serializing its root node and then recursively (depth-first) serializing its
 * subtrees (children).
 *
 * As an example, here's the general form of the produced serialization,
 * starting from some root node: `<root-id> <root-num-children>
 * <root-child-1-id> <root-child-1-num-children> <root-child-1-child-1-id>
 * <root-child-1-child-1-num-children> ... <root-child-2-id>
 * <root-child-2-num-children> ...`.
 */
std::unique_ptr<NAryTree> LiftTree(const std::vector<int64_t> &flattened) {
  size_t index = 0;
  return LiftTreeHelper(flattened, &index);
}

//////////////////////////////////////////////////////////////////////////////
///
/// Map variant

namespace map {

//////////////////////////////////////////////////////////////////////////////
//
// ProfilingStats

struct ProfilingStats {
  // We're starting from -1 to facilitate unconditional incrementing within
  // `Pull` (which makes the code of the `Pull` easier to follow). The last
  // increment (when `Pull` returns `false`) will be superfluous so compensate
  // for it here.
  int64_t actual_hits{-1};
  unsigned long long elapsed_time{0};
};

//////////////////////////////////////////////////////////////////////////////
//
// Context

struct EvaluationContext {
  bool is_profile_query{true};
  std::unordered_map<std::uintptr_t, ProfilingStats> operator_stats;
};

class Context {
 public:
  Context() = default;
  Context(const Context &) = delete;
  Context &operator=(const Context &) = delete;
  Context(Context &&) = default;
  Context &operator=(Context &&) = default;

  EvaluationContext evaluation_context_;
};

//////////////////////////////////////////////////////////////////////////////
//
// ScopedProfile

struct ScopedProfile {
  ScopedProfile(std::uintptr_t key, Context *context) : context(context) {
    if (context->evaluation_context_.is_profile_query) {
      stats = &context->evaluation_context_.operator_stats[key];
      start_time = memgraph::utils::ReadTSC();
      stats->actual_hits++;
    }
  }

  ~ScopedProfile() {
    if (context->evaluation_context_.is_profile_query) {
      stats->elapsed_time += memgraph::utils::ReadTSC() - start_time;
    }
  }

  Context *context = nullptr;
  ProfilingStats *stats = nullptr;
  unsigned long long start_time{};
};

//////////////////////////////////////////////////////////////////////////////
//
// Profile

void ProfileHelper(const NAryTree *tree, Context *context) {
  ScopedProfile profile(reinterpret_cast<std::uintptr_t>(tree), context);

  for (auto &child : tree->children) {
    ProfileHelper(child.get(), context);
  }
}

void Profile(const std::unique_ptr<NAryTree> &tree) {
  Context context;
  context.evaluation_context_.is_profile_query = true;
  ProfileHelper(tree.get(), &context);
}

}  // namespace map

//////////////////////////////////////////////////////////////////////////////
///
/// Tree variant

namespace tree {

//////////////////////////////////////////////////////////////////////////////
///
/// ProfilingStats

struct ProfilingStats {
  ProfilingStats() : ProfilingStats(-1, 0) {}

  ProfilingStats(int64_t actual_hits, unsigned long long elapsed_time)
      : actual_hits(actual_hits), elapsed_time(elapsed_time), key(reinterpret_cast<std::uintptr_t>(nullptr)) {
    // The logical operator with the most children has at most 3 children (but
    // most of the logical operators have just 1 child)
    children.reserve(3);
  }

  // We're starting from -1 to facilitate unconditional incrementing within
  // `Pull` (which makes the code of the `Pull` easier to follow). The last
  // increment (when `Pull` returns `false`) will be superfluous so compensate
  // for it here.
  int64_t actual_hits;
  unsigned long long elapsed_time;
  std::uintptr_t key;
  std::vector<ProfilingStats> children;
};

//////////////////////////////////////////////////////////////////////////////
///
/// Context

struct EvaluationContext {
  bool is_profile_query{true};
  ProfilingStats stats;
  ProfilingStats *stats_root{nullptr};
};

class Context {
 public:
  Context() = default;
  Context(const Context &) = delete;
  Context &operator=(const Context &) = delete;
  Context(Context &&) = default;
  Context &operator=(Context &&) = default;

  EvaluationContext evaluation_context_;
};

//////////////////////////////////////////////////////////////////////////////
///
/// ScopedProfile

struct ScopedProfile {
  ScopedProfile(std::uintptr_t key, Context *context) : context(context) {
    if (context->evaluation_context_.is_profile_query) {
      root = context->evaluation_context_.stats_root;

      // Are we the root logical operator?
      if (!root) {
        stats = &context->evaluation_context_.stats;
      } else {
        stats = nullptr;

        // Was this logical operator already hit on one of the previous pulls?
        for (auto &child : root->children) {
          if (child.key == key) {
            stats = &child;
            break;
          }
        }

        if (!stats) {
          root->children.emplace_back();
          stats = &root->children.back();
        }
      }

      context->evaluation_context_.stats_root = stats;
      stats->actual_hits++;
      stats->key = key;
      start_time = memgraph::utils::ReadTSC();
    }
  }

  ~ScopedProfile() {
    if (context->evaluation_context_.is_profile_query) {
      stats->elapsed_time += memgraph::utils::ReadTSC() - start_time;

      // Restore the old root ("pop")
      context->evaluation_context_.stats_root = root;
    }
  }

  Context *context;
  ProfilingStats *root, *stats;
  unsigned long long start_time;
};

//////////////////////////////////////////////////////////////////////////////
//
// Profile

void ProfileHelper(const NAryTree *tree, Context *context) {
  ScopedProfile profile(reinterpret_cast<std::uintptr_t>(tree), context);

  for (auto &child : tree->children) {
    ProfileHelper(child.get(), context);
  }
}

void Profile(const std::unique_ptr<NAryTree> &tree) {
  Context context;
  context.evaluation_context_.is_profile_query = true;
  ProfileHelper(tree.get(), &context);
}

}  // namespace tree

//////////////////////////////////////////////////////////////////////////////
///
/// Tree + No reserve variant

namespace tree_no_reserve {

//////////////////////////////////////////////////////////////////////////////
///
/// ProfilingStats

struct ProfilingStats {
  // We're starting from -1 to facilitate unconditional incrementing within
  // `Pull` (which makes the code of the `Pull` easier to follow). The last
  // increment (when `Pull` returns `false`) will be superfluous so compensate
  // for it here.
  int64_t actual_hits{-1};
  unsigned long long elapsed_time{0};
  std::uintptr_t key{reinterpret_cast<std::uintptr_t>(nullptr)};
  std::vector<ProfilingStats> children;
};

//////////////////////////////////////////////////////////////////////////////
///
/// Context

struct EvaluationContext {
  bool is_profile_query{true};
  ProfilingStats stats;
  ProfilingStats *stats_root{nullptr};
};

class Context {
 public:
  Context() = default;
  Context(const Context &) = delete;
  Context &operator=(const Context &) = delete;
  Context(Context &&) = default;
  Context &operator=(Context &&) = default;

  EvaluationContext evaluation_context_;
};

//////////////////////////////////////////////////////////////////////////////
///
/// ScopedProfile

struct ScopedProfile {
  ScopedProfile(std::uintptr_t key, Context *context) : context(context) {
    if (context->evaluation_context_.is_profile_query) {
      root = context->evaluation_context_.stats_root;

      // Are we the root logical operator?
      if (!root) {
        stats = &context->evaluation_context_.stats;
      } else {
        stats = nullptr;

        // Was this logical operator already hit on one of the previous pulls?
        for (auto &child : root->children) {
          if (child.key == key) {
            stats = &child;
            break;
          }
        }

        if (!stats) {
          root->children.emplace_back();
          stats = &root->children.back();
        }
      }

      context->evaluation_context_.stats_root = stats;
      stats->actual_hits++;
      stats->key = key;
      start_time = memgraph::utils::ReadTSC();
    }
  }

  ~ScopedProfile() {
    if (context->evaluation_context_.is_profile_query) {
      stats->elapsed_time += memgraph::utils::ReadTSC() - start_time;

      // Restore the old root ("pop")
      context->evaluation_context_.stats_root = root;
    }
  }

  Context *context;
  ProfilingStats *root, *stats;
  unsigned long long start_time;
};

//////////////////////////////////////////////////////////////////////////////
//
// Profile

void ProfileHelper(const NAryTree *tree, Context *context) {
  ScopedProfile profile(reinterpret_cast<std::uintptr_t>(tree), context);

  for (auto &child : tree->children) {
    ProfileHelper(child.get(), context);
  }
}

void Profile(const std::unique_ptr<NAryTree> &tree) {
  Context context;
  context.evaluation_context_.is_profile_query = true;
  ProfileHelper(tree.get(), &context);
}

}  // namespace tree_no_reserve

//////////////////////////////////////////////////////////////////////////////
///
/// Tree + Storage variant

namespace tree_storage {

//////////////////////////////////////////////////////////////////////////////
//
// ProfilingStats

inline constexpr size_t kMaxProfilingStatsChildren = 3;
struct ProfilingStatsStorage;

struct ProfilingStats {
  ProfilingStats();
  ProfilingStats(int64_t actual_hits, unsigned long long elapsed_time);

  size_t EnsureChild(const ProfilingStatsStorage &storage, std::uintptr_t key);

  // We're starting from -1 to facilitate unconditional incrementing within
  // `Pull` (which makes the code of the `Pull` easier to follow). The last
  // increment (when `Pull` returns `false`) will be superfluous so compensate
  // for it here.
  int64_t actual_hits;
  unsigned long long elapsed_time;
  std::uintptr_t key;
  std::array<int64_t, kMaxProfilingStatsChildren> children;
};

//////////////////////////////////////////////////////////////////////////////
///
/// ProfilingStatsStorage

struct ProfilingStatsStorage {
  int64_t Create();
  bool Empty();
  ProfilingStats &Root();
  const ProfilingStats &Root() const;

  ProfilingStats *at(int64_t index);
  const ProfilingStats *at(int64_t index) const;
  ProfilingStats *operator[](int64_t index);
  const ProfilingStats *operator[](int64_t index) const;

  std::vector<ProfilingStats> storage;
};

int64_t ProfilingStatsStorage::Create() {
  storage.emplace_back();
  return storage.size() - 1;
}

bool ProfilingStatsStorage::Empty() { return storage.empty(); }

ProfilingStats &ProfilingStatsStorage::Root() {
  MG_ASSERT(!storage.empty());
  return storage[0];
}

const ProfilingStats &ProfilingStatsStorage::Root() const {
  MG_ASSERT(!storage.empty());
  return storage[0];
}

ProfilingStats *ProfilingStatsStorage::at(int64_t index) {
  MG_ASSERT(0 <= index);
  MG_ASSERT(index < storage.size());
  return &storage[index];
}

const ProfilingStats *ProfilingStatsStorage::at(int64_t index) const {
  MG_ASSERT(0 <= index);
  MG_ASSERT(index < storage.size());
  return &storage[index];
}

ProfilingStats *ProfilingStatsStorage::operator[](int64_t index) { return at(index); }

const ProfilingStats *ProfilingStatsStorage::operator[](int64_t index) const { return at(index); }

//////////////////////////////////////////////////////////////////////////////
//
// ProfilingStats

ProfilingStats::ProfilingStats() : ProfilingStats(-1, 0) {}

ProfilingStats::ProfilingStats(int64_t actual_hits, unsigned long long elapsed_time)
    : actual_hits(actual_hits), elapsed_time(elapsed_time), key(reinterpret_cast<std::uintptr_t>(nullptr)), children{} {
  for (auto &child : children) {
    child = -1;
  }
}

size_t ProfilingStats::EnsureChild(const ProfilingStatsStorage &storage, std::uintptr_t key) {
  size_t size = children.size();
  size_t slot = size;

  for (size_t i = 0; i < size; ++i) {
    if (children[i] == -1 || storage[children[i]]->key == key) {
      slot = i;
      break;
    }
  }

  // Either we found an instance of `ProfilingStats` that was creater earlier
  // and corresponds to the logical operator `op` or we're seeing this
  // operator for the first time and have found a free slot for its
  // `ProfilingStats` instance.
  MG_ASSERT(slot < size);
  return slot;
}

//////////////////////////////////////////////////////////////////////////////
///
/// Context

struct EvaluationContext {
  bool is_profile_query{true};
  ProfilingStatsStorage stats_storage;
  int64_t stats_root{-1};
};

class Context {
 public:
  Context() = default;
  Context(const Context &) = delete;
  Context &operator=(const Context &) = delete;
  Context(Context &&) = default;
  Context &operator=(Context &&) = default;

  EvaluationContext evaluation_context_;
};

//////////////////////////////////////////////////////////////////////////////
//
// ScopedProfile

struct ScopedProfile {
  ScopedProfile(std::uintptr_t key, Context *context) : context(context), old_root_id(-1), stats_id(-1) {
    if (context->evaluation_context_.is_profile_query) {
      stats_id = EnsureStats(context->evaluation_context_.stats_root, key);

      auto &storage = context->evaluation_context_.stats_storage;
      ProfilingStats *stats = storage[stats_id];
      MG_ASSERT(stats);

      stats->actual_hits++;
      start_time = memgraph::utils::ReadTSC();
      stats->key = key;

      // Set the new root ("push")
      old_root_id = context->evaluation_context_.stats_root;
      context->evaluation_context_.stats_root = stats_id;
    }
  }

  int64_t EnsureStats(int64_t root_id, std::uintptr_t key) {
    auto &storage = context->evaluation_context_.stats_storage;

    if (root_id == -1) {
      if (storage.Empty()) {
        return storage.Create();
      }

      return 0;
    }

    ProfilingStats *root = storage[root_id];
    size_t slot = root->EnsureChild(storage, key);
    int64_t stats_id = root->children[slot];

    if (stats_id == -1) {
      stats_id = storage.Create();
      // `Create` might have invalidated the `root` pointer so index `storage`
      // explicitly again
      storage[root_id]->children[slot] = stats_id;
    }

    MG_ASSERT(stats_id >= 0);
    return stats_id;
  }

  ~ScopedProfile() {
    auto &storage = context->evaluation_context_.stats_storage;
    ProfilingStats *stats = storage[stats_id];
    MG_ASSERT(stats);

    if (context->evaluation_context_.is_profile_query) {
      stats->elapsed_time += memgraph::utils::ReadTSC() - start_time;

      // Restore the old root ("pop")
      context->evaluation_context_.stats_root = old_root_id;
    }
  }

  Context *context;
  int64_t old_root_id, stats_id;
  unsigned long long start_time;
};

//////////////////////////////////////////////////////////////////////////////
//
// Profile

void ProfileHelper(const NAryTree *tree, Context *context) {
  ScopedProfile profile(reinterpret_cast<std::uintptr_t>(tree), context);

  for (auto &child : tree->children) {
    ProfileHelper(child.get(), context);
  }
}

void Profile(const std::unique_ptr<NAryTree> &tree) {
  Context context;
  context.evaluation_context_.is_profile_query = true;
  ProfileHelper(tree.get(), &context);
}

}  // namespace tree_storage

}  // namespace

std::vector<std::unique_ptr<NAryTree>> GetTrees() {
  std::vector<std::vector<int64_t>> flat_trees = {
      // {-1, 1, -2, 1, -3, 1, -4, 0},
      // {-1, 1, -2, 1, -3, 3, -4, 1, -5, 1, -6, 0, -7, 1, -8, 1, -6, 0, -6, 0},
      {-1, 1, -2, 2, -3, 2, -4, 2, -5, 1, -6, 1, -7, 0, -7, 0, -7, 0, -3, 1, -4, 2, -5, 1, -6, 1, -7, 0, -7, 0}};

  std::vector<std::unique_ptr<NAryTree>> trees;
  for (auto &flattened : flat_trees) {
    trees.emplace_back(LiftTree(flattened));
  }

  return trees;
}

void BM_PlanProfileMap(benchmark::State &state) {
  auto trees = GetTrees();

  while (state.KeepRunning()) {
    for (auto &tree : trees) {
      map::Profile(tree);
    }
  }
}

void BM_PlanProfileTree(benchmark::State &state) {
  auto trees = GetTrees();

  while (state.KeepRunning()) {
    for (auto &tree : trees) {
      tree::Profile(tree);
    }
  }
}

void BM_PlanProfileTreeNoReserve(benchmark::State &state) {
  auto trees = GetTrees();

  while (state.KeepRunning()) {
    for (auto &tree : trees) {
      tree_no_reserve::Profile(tree);
    }
  }
}

void BM_PlanProfileTreeAndStorage(benchmark::State &state) {
  auto trees = GetTrees();

  while (state.KeepRunning()) {
    for (auto &tree : trees) {
      tree_storage::Profile(tree);
    }
  }
}

BENCHMARK(BM_PlanProfileMap);
BENCHMARK(BM_PlanProfileTree);
BENCHMARK(BM_PlanProfileTreeNoReserve);
BENCHMARK(BM_PlanProfileTreeAndStorage);

BENCHMARK_MAIN();
