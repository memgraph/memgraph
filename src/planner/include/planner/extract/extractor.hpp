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

#include <concepts>
#include <cstdint>
#include <deque>
#include <limits>
#include <span>
#include <utility>
#include <vector>

#include <cassert>

#include <boost/unordered/unordered_flat_map.hpp>

#include "utils/logging.hpp"

#include "planner/extract/pareto_frontier.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::extract {

/// CostResult contract.  Every cost model defines `CostResult` plus an
/// `operator()(ENode const &, ENodeId, span<CostResult const * const> children)`.
/// The span pointees are read-only views into `frontier_map`; copy before
/// consuming by value.  ParetoFrontier-based cost models derive from
/// `CostResultBase` (planner/extract/pareto_frontier.hpp).
///
/// `CostResult` must provide:
///   cost_t                  - scalar cost type, totally_ordered
///   a.merge_in_place(b)     - fold rvalue `b` into `a`
///   a.resolve()             - (enode_id, cost const&) of the chosen alt;
///                             reference is valid for the frontier's lifetime
template <typename CR>
concept CostResultType = std::copyable<CR> && requires(CR &a, CR const &c) {
  typename CR::cost_t;
  requires std::totally_ordered<typename CR::cost_t>;
  { a.merge_in_place(std::declval<CR>()) };
  { c.resolve() } -> std::same_as<std::pair<ENodeId, typename CR::cost_t const &>>;
};

// ============================================================================
// Extraction pipeline types
// ============================================================================

/// Per-eclass frontier during cost propagation.
/// nullopt means "in progress" (cycle detection).
template <typename CostResult>
using EClassFrontier = std::optional<CostResult>;

/// Map from EClassId to its computed frontier.  Passed to the Resolver after
/// ComputeFrontiers populates it.
template <typename CostResult>
using FrontierMap = boost::unordered_flat_map<EClassId, EClassFrontier<CostResult>>;

// ============================================================================
// Extraction stages
// ============================================================================

/// Pool of children-frontier buffers used by ComputeFrontiers' recursive
/// descent.  Each recursive frame acquires one buffer; nested frames acquire
/// fresh buffers further down the pool, then release them on return.  A single
/// shared buffer would not work because the recursive call to ComputeFrontiers
/// happens inside the per-enode loop, which would clobber the caller frame's
/// buffer.  Capacity of inner vectors persists across queries because the
/// outer pool is owned by FrontierContext.
template <CostResultType CostResult>
struct FrontierBufferPool {
 private:
  auto internal_acquire() -> std::vector<CostResult const *> & {
    if (depth == pool_capacity) {
      pool.emplace_back();
      ++pool_capacity;
    }
    auto &buf = pool[depth++];
    buf.clear();
    return buf;
  }

  void internal_release() noexcept { --depth; }

 public:
  struct [[nodiscard]] AcquiredGuard {
    FrontierBufferPool *owner;

    explicit AcquiredGuard(FrontierBufferPool &p) : owner{&p} {}

    ~AcquiredGuard() { owner->internal_release(); }

    AcquiredGuard(AcquiredGuard const &) = delete;
    AcquiredGuard(AcquiredGuard &&) = delete;
    auto operator=(AcquiredGuard const &) -> AcquiredGuard & = delete;
    auto operator=(AcquiredGuard &&) -> AcquiredGuard & = delete;
  };

  auto acquire() -> std::pair<AcquiredGuard, std::vector<CostResult const *> &> {
    return {std::piecewise_construct, std::forward_as_tuple(*this), std::forward_as_tuple(internal_acquire())};
  }

  void clear() noexcept { depth = 0; }

 private:
  // std::deque so references into pool entries remain valid when the pool
  // grows during a deeper recursive frame's acquire().  std::vector would
  // reallocate and dangle the outer caller's children_frontiers reference.
  std::deque<std::vector<CostResult const *>> pool;
  // Cached copy of pool.size(): deque::size() is multi-op (block-pointer
  // subtraction) and is consulted once per recursion frame.  We only ever
  // grow `pool`, so the cache is a single monotonically-increasing counter.
  size_t pool_capacity = 0;
  size_t depth = 0;
};

template <typename CostResult>
struct FrontierContext {
  FrontierMap<CostResult> frontier_map;
  FrontierBufferPool<CostResult> frontier_buffer_pool;

  void clear() {
    frontier_map.clear();
    frontier_buffer_pool.clear();
  }
};

/// Bottom-up cost propagation. Calls cost_model(enode, enode_id, children) for each enode,
/// merges results via CostResult::merge across enodes in the same eclass.
///
/// Returns a pointer into `frontier_map`. nullptr means the eclass is cyclic
/// (either fully unreachable or in-progress on the current recursion path).
/// The pointer is valid until the next mutation of `frontier_map` by the
/// caller.
///
/// Reserves up-front on the top-level call so no rehash invalidates iterators
/// during the recursion; see the reserve site below for the full argument.
template <typename Symbol, typename Analysis, typename CostModel>
  requires CostResultType<typename CostModel::CostResult>
[[nodiscard]] auto ComputeFrontiers(EGraph<Symbol, Analysis> const &egraph, CostModel const &cost_model,
                                    EClassId eclass_id, FrontierContext<typename CostModel::CostResult> &ctx)
    -> CostModel::CostResult const * {
  using CostResult = CostModel::CostResult;

  assert(!egraph.needs_rebuild() && "egraph must be rebuilt before extraction");
  assert(egraph.find(eclass_id) == eclass_id &&
         "ComputeFrontiers requires canonical eclass_id; recursive children are canonical "
         "post-rebuild, so the top-level caller must canonicalize the root");

  auto &out = ctx.frontier_map;

  // Reserve once on the top-level call so no rehash invalidates iterators
  // or pointers across the recursion.  boost::unordered_flat_map has a fixed
  // max_load_factor of 0.875 and rehashes only when size >= max_load at insert
  // time; reserve(N) on an empty map sets max_load >= N, so the next ≤ N
  // emplaces are guaranteed not to rehash.  Total emplaces are bounded by
  // num_classes() because every key is a canonical EClassId (recursive
  // descents read enode.children() which is canonical post-rebuild; the
  // top-level root is asserted canonical above).
  if (out.empty()) out.reserve(egraph.num_classes());
  auto const initial_bucket_count = out.bucket_count();

  if (auto const it = out.find(eclass_id); it != out.end()) {
    return it->second ? &*it->second : nullptr;
  }

  auto const &eclass = egraph.eclass(eclass_id);

  // Mark this e-class as "in progress" with nullopt frontier to detect cycles.
  // Iterator is stable for the duration of this function: the upfront reserve
  // prevents rehash, and erasure of unrelated entries leaves other buckets in
  // place (open addressing).
  auto sentinel_it = out.emplace(eclass_id, std::nullopt).first;
  MG_ASSERT(out.bucket_count() == initial_bucket_count, "sentinel emplace triggered rehash");

  auto eclass_frontier = std::optional<CostResult>{};

  // Per-frame buffer; nested recursive frames get their own slot.
  auto [guard, children_frontiers] = ctx.frontier_buffer_pool.acquire();
  for (auto const &enode_id : eclass.nodes()) {
    auto const &enode = egraph.get_enode(enode_id);

    // Single pass: recurse and capture the returned pointer in one go.  No
    // rehash can happen (see reserve above), so the pointer stays valid for
    // the remainder of this function.  We always recurse for *every* child
    // even when one turns out cyclic: the recursion's side effect (caching
    // a non-cyclic sibling's frontier) is observable by other enodes / the
    // resolver.  Only the pointer collection stops once we know the cost
    // model won't be called for this enode.
    children_frontiers.clear();
    children_frontiers.reserve(enode.children().size());
    auto has_uncomputable_child = false;
    for (auto child : enode.children()) {
      auto const *child_frontier = ComputeFrontiers(egraph, cost_model, child, ctx);
      if (!child_frontier) {
        has_uncomputable_child = true;
        continue;
      }
      if (!has_uncomputable_child) children_frontiers.push_back(child_frontier);
    }
    if (has_uncomputable_child) continue;

    auto enode_frontier = cost_model(enode, enode_id, children_frontiers);

    if (!eclass_frontier) {
      eclass_frontier = std::move(enode_frontier);
    } else {
      eclass_frontier->merge_in_place(std::move(enode_frontier));
    }
  }

  // Each recursive child also asserts bucket_count stability at its own emplace
  // site, so a rehash anywhere in the subtree fires the assertion at the source.
  // Re-checking here makes the iterator-stability invariant locally evident.
  MG_ASSERT(out.bucket_count() == initial_bucket_count, "rehash during child recursion invalidated sentinel_it");

  if (eclass_frontier) {
    sentinel_it->second = std::move(eclass_frontier);
    return &*sentinel_it->second;
  }

  out.erase(sentinel_it);
  return nullptr;
}

// ============================================================================
// DfsPostOrder: resolver scaffolding
// ============================================================================
//
// Callers fill a `vector<Entry>` in children-before-parents order.  The caller
// supplies a `make_entry(key, visit_child) -> Entry` callback that handles
// per-node logic (frontier lookup, alt selection, child-key dispatch); this
// function supplies the recursion, deduplication, and post-order emit.
//
// `seen` is owned by the caller for reuse across queries (clear() between
// calls).  `out` is also caller-owned and must be empty on entry.
//
// make_entry signature: (Key key, auto visit_child) -> Entry
//   - Must call visit_child(child_key) for each child the entry depends on.
//     visit_child recurses into the child and returns its post-order index
//     in `out`; the return is also used internally for cycle-hit caching.
//   - May call visit_child zero times (leaf node).
//   - Must return the Entry to emit for `key` after all children are visited.

/// Child-visitor handed to the per-node callback by `DfsPostOrder`.
/// `visit_child(child_key)` recurses into the child and returns its post-order
/// index in `out`.
template <typename F, typename Key>
concept VisitChildFn = requires(F &&f, Key key) {
  { std::forward<F>(f)(std::move(key)) } -> std::convertible_to<std::uint32_t>;
};

/// Per-node callback contract for `DfsPostOrder`.  `f(key, visit_child)` must
/// accept a `VisitChildFn` and return the `Entry` to emit for `key`.
template <typename F, typename Key, typename Entry>
concept MakeEntryFn = requires(F &&f, Key key) {
  {
    std::forward<F>(f)(std::move(key), [](Key) -> std::uint32_t { return 0; })
  } -> std::convertible_to<Entry>;
};

template <typename Key, typename KeyHash, typename Entry, typename MakeEntry>
  requires MakeEntryFn<MakeEntry, Key, Entry>
void DfsPostOrder(Key root, boost::unordered_flat_map<Key, std::uint32_t, KeyHash> &seen, std::vector<Entry> &out,
                  MakeEntry &&make_entry) {
  constexpr std::uint32_t kCycleSentinel = std::numeric_limits<std::uint32_t>::max();
  // Hold `key` as a stable local across the recursion: `seen.try_emplace` in
  // nested calls may rehash and invalidate any iterator or map-slot reference
  // captured here.  Pass the local to `make_entry` (not `it->first`) and
  // re-lookup the slot for the final write after `make_entry` returns.
  auto visit = [&](this auto const &self, Key key) -> std::uint32_t {
    auto [it, inserted] = seen.try_emplace(key, kCycleSentinel);
    if (!inserted) {
      assert(it->second != kCycleSentinel && "cycle in DfsPostOrder");
      return it->second;
    }
    auto entry = make_entry(key, self);
    auto const idx = static_cast<std::uint32_t>(out.size());
    out.push_back(std::move(entry));
    // Re-look-up rather than reuse `it`: nested `try_emplace`s during
    // `make_entry` may have rehashed `seen` and invalidated it.
    seen.find(key)->second = idx;
    return idx;
  };
  visit(std::move(root));
}

}  // namespace memgraph::planner::core::extract
