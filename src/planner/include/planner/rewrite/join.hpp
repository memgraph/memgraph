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

#include <algorithm>
#include <cstddef>
#include <ranges>
#include <span>
#include <vector>

#include <boost/functional/hash.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <range/v3/view/concat.hpp>
#include <range/v3/view/single.hpp>

#include "planner/egraph/egraph.hpp"
#include "planner/pattern/match.hpp"
#include "planner/pattern/pattern.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

/// Hash key for join operations - hashes shared variable bindings.
class JoinKey {
 public:
  JoinKey() = default;

  template <typename EGraphT>
  JoinKey(JoinMatchView view, std::span<VarLocation const> shared_locs, MatchArena const &arena, EGraphT &egraph)
      : view_(view), shared_locs_(shared_locs), arena_(&arena) {
    std::size_t seed = 0;
    for (auto loc : shared_locs) {
      boost::hash_combine(seed, egraph.find(arena.get(view, loc)));
    }
    hash_ = seed;
  }

  [[nodiscard]] auto hash() const noexcept -> std::size_t { return hash_; }

  [[nodiscard]] auto operator==(JoinKey const &other) const noexcept -> bool {
    if (hash_ != other.hash_ || shared_locs_.size() != other.shared_locs_.size()) return false;
    for (std::size_t i = 0; i < shared_locs_.size(); ++i) {
      if (arena_->get(view_, shared_locs_[i]) != other.arena_->get(other.view_, other.shared_locs_[i])) return false;
    }
    return true;
  }

 private:
  JoinMatchView view_{};
  std::span<VarLocation const> shared_locs_{};
  MatchArena const *arena_ = nullptr;
  std::size_t hash_ = 0;
};

// Boost hash ADL lookup
inline std::size_t hash_value(JoinKey const &key) noexcept { return key.hash(); }

}  // namespace memgraph::planner::core

// Hash specialization for JoinKey (must be in std namespace)
template <>
struct std::hash<memgraph::planner::core::JoinKey> {
  auto operator()(memgraph::planner::core::JoinKey const &key) const noexcept -> std::size_t { return key.hash(); }
};

namespace memgraph::planner::core {

/// Reusable buffers for multi-pattern join operations
struct JoinContext {
  // Flat storage for memory-efficient join operations
  std::vector<PatternMatch> left_flat;    ///< Flat storage: accumulated matches from earlier patterns
  std::size_t left_stride = 0;            ///< PatternMatches per joined match in left_flat
  std::vector<PatternMatch> result_flat;  ///< Flat storage: output of join (becomes next left_flat)
  std::size_t result_stride = 0;          ///< PatternMatches per joined match in result_flat

  std::vector<PatternMatch> right;  ///< Matches from current pattern (single PatternMatch each)
  boost::unordered_flat_map<JoinKey, std::vector<std::size_t>> index;  ///< Hash index for joins

  void clear() {
    left_flat.clear();
    left_stride = 0;
    result_flat.clear();
    result_stride = 0;
    right.clear();
    index.clear();
  }
};

/// One step in a multi-pattern join plan. Uses flat storage (vector + stride).
struct JoinStep {
  std::size_t pattern_index;            ///< Which pattern to match
  std::vector<PatternVar> shared_vars;  ///< Variables shared with earlier patterns
  std::vector<VarLocation> left_locs;   ///< Where shared vars are in accumulated left
  std::vector<VarLocation> right_locs;  ///< Where shared vars are in right PatternMatch

  [[nodiscard]] auto is_cartesian() const -> bool { return shared_vars.empty(); }

  /// Join ctx.left_flat with ctx.right, writing to ctx.result_flat.
  template <typename Symbol, typename Analysis>
  void join(JoinContext &ctx, MatchArena const &arena, EGraph<Symbol, Analysis> &egraph) const {
    std::size_t const num_left = ctx.left_stride > 0 ? ctx.left_flat.size() / ctx.left_stride : 0;
    std::size_t const num_right = ctx.right.size();

    ctx.result_stride = ctx.left_stride + 1;
    ctx.result_flat.clear();

    // View left_flat as chunks of stride elements
    auto left_chunks = ctx.left_flat | std::views::chunk(static_cast<std::ptrdiff_t>(ctx.left_stride));

    // Common output: append left chunk followed by single right element
    auto append_match = [&](auto left, PatternMatch const &r) {
      std::ranges::copy(ranges::views::concat(left, ranges::views::single(r)), std::back_inserter(ctx.result_flat));
    };

    if (is_cartesian()) {
      // Cartesian product: exact size known
      ctx.result_flat.reserve(num_left * num_right * ctx.result_stride);
      for (auto &&[left, r] : std::views::cartesian_product(left_chunks, ctx.right)) {
        append_match(left, r);
      }
      return;
    }

    // Hash join: build index on right
    auto make_key = [&](auto view, auto const &locs) { return JoinKey{view, locs, arena, egraph}; };
    auto to_right = [&](std::size_t idx) -> PatternMatch const & { return ctx.right[idx]; };

    ctx.index.clear();
    ctx.index.reserve(num_right);  // At most num_right unique keys
    for (auto [i, r] : std::views::enumerate(ctx.right)) {
      ctx.index[make_key(std::span{&r, 1}, right_locs)].push_back(static_cast<std::size_t>(i));
    }

    ctx.result_flat.reserve(num_left * ctx.result_stride);
    for (auto left : left_chunks) {
      if (auto it = ctx.index.find(make_key(left, left_locs)); it != ctx.index.end()) {
        for (auto const &r : it->second | std::views::transform(to_right)) {
          append_match(left, r);
        }
      }
    }
  }
};

}  // namespace memgraph::planner::core
