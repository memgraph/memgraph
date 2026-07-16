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
#include <limits>
#include <optional>
#include <span>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

namespace memgraph::planner::core::rewrite {

/// A depth that never gates arming: a pattern with this depth arms whenever its
/// root symbol appears anywhere in the touched-set's parent-closure.
inline constexpr std::size_t kAllDepths = std::numeric_limits<std::size_t>::max();

/// One pattern's arming spec: its root symbol (`nullopt` = symbol-less, i.e.
/// matches any e-class) and its depth `d_P` (root-to-deepest-leaf). A change
/// arms this pattern only when its root symbol lies within `d_P` parent-hops of
/// the change, so `d_P` is the closure radius at which the pattern can re-fire.
template <typename Symbol>
struct PatternArm {
  std::optional<Symbol> root;
  std::size_t depth = kAllDepths;
};

/// Maps a changed symbol to the rules a pass should arm - the dual of the
/// matcher index (`symbol -> e-classes`). Built once from the rule set's pattern
/// roots; consulted each pass with the symbols reached from the touched-set to
/// produce the armed rule set, without running the matcher.
///
/// A rule is indexed under each of its patterns' root symbols, carrying that
/// pattern's depth. A rule with a symbol-less pattern root is **always armed**,
/// since any change could enable it. Rule indices are positions in the rule
/// set's `rules()`.
template <typename Symbol>
class ArmingIndex {
 public:
  /// A rule reachable through some symbol, with the depth that gates it: the
  /// rule arms when the symbol is reached within `depth` parent-hops. A rule
  /// rooted at one symbol by two patterns keeps the deeper depth - the rule
  /// arms whenever the deeper pattern would.
  struct ArmedRule {
    std::size_t rule_idx;
    std::size_t depth;
  };

  ArmingIndex() = default;

  /// Build from each rule's per-pattern arming specs (root symbol + depth), in
  /// rule-index order.
  static auto from_pattern_arms(std::span<std::vector<PatternArm<Symbol>> const> per_rule) -> ArmingIndex {
    ArmingIndex index;
    for (std::size_t rule_idx = 0; rule_idx < per_rule.size(); ++rule_idx) {
      auto const &arms = per_rule[rule_idx];
      bool const has_symbolless_root = std::ranges::any_of(arms, [](auto const &arm) { return !arm.root.has_value(); });
      if (has_symbolless_root) {
        index.always_armed_.push_back(rule_idx);
        continue;  // always armed; no need to also index by its other roots
      }
      for (auto const &arm : arms) {
        auto &rules = index.by_symbol_[*arm.root];
        // Rules are processed in one rule-index-increasing batch each, so a
        // back()-check dedups a rule rooted at the same symbol by two patterns;
        // keep the deeper depth so the rule arms whenever either pattern would.
        if (!rules.empty() && rules.back().rule_idx == rule_idx) {
          rules.back().depth = std::max(rules.back().depth, arm.depth);
        } else {
          rules.push_back({rule_idx, arm.depth});
        }
      }
    }
    return index;
  }

  /// Convenience: index by root symbol alone, leaving every pattern un-gated by
  /// depth (`kAllDepths`). For callers that do not track pattern depth.
  static auto from_root_symbols(std::span<std::vector<std::optional<Symbol>> const> per_rule_roots) -> ArmingIndex {
    std::vector<std::vector<PatternArm<Symbol>>> per_rule;
    per_rule.reserve(per_rule_roots.size());
    for (auto const &roots : per_rule_roots) {
      std::vector<PatternArm<Symbol>> arms;
      arms.reserve(roots.size());
      for (auto const &root : roots) arms.push_back(PatternArm<Symbol>{.root = root, .depth = kAllDepths});
      per_rule.push_back(std::move(arms));
    }
    return from_pattern_arms(per_rule);
  }

  [[nodiscard]] auto always_armed() const -> std::span<std::size_t const> { return always_armed_; }

  /// Rules rooted at `sym`, each with its arming depth (empty span if none).
  [[nodiscard]] auto rules_for_symbol(Symbol sym) const -> std::span<ArmedRule const> {
    auto const it = by_symbol_.find(sym);
    if (it == by_symbol_.end()) return {};
    return it->second;
  }

  /// Fill `armed` given `min_hop` - the shallowest parent-hop at which each
  /// symbol is reached from the pass's touched-set. Arms every always-armed
  /// rule, plus every rule a reached symbol indexes whose pattern depth reaches
  /// that hop (`min_hop(S) <= d_P`): a symbol at hop `h` can only re-enable a
  /// pattern of depth `>= h`. De-duplicates a rule reachable through several
  /// symbols. Inserts without clearing - the caller owns that.
  void collect_armed(boost::unordered_flat_map<Symbol, std::size_t> const &min_hop,
                     boost::unordered_flat_set<std::size_t> &armed) const {
    armed.insert(always_armed_.begin(), always_armed_.end());
    for (auto const &[sym, hop] : min_hop) {
      auto const it = by_symbol_.find(sym);
      if (it == by_symbol_.end()) continue;
      for (auto const &entry : it->second) {
        if (hop <= entry.depth) armed.insert(entry.rule_idx);
      }
    }
  }

 private:
  boost::unordered_flat_map<Symbol, std::vector<ArmedRule>> by_symbol_;
  std::vector<std::size_t> always_armed_;
};

}  // namespace memgraph::planner::core::rewrite
