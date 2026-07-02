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
#include <optional>
#include <ranges>
#include <span>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/rewrite/rule_set.hpp"

namespace memgraph::planner::core::rewrite {

/// Maps a changed symbol to the rules a pass should arm - the dual of the
/// matcher index (`symbol -> e-classes`). Built once from the rule set's pattern
/// roots; consulted each pass with the symbols of the active set to produce the
/// armed rule set, without running the matcher.
///
/// A rule is indexed under each of its patterns' root symbols. A rule with a
/// symbol-less pattern root (one that can match any e-class) is **always armed**,
/// since any change could enable it. Rule indices are positions in the rule
/// set's `rules()`.
template <typename Symbol>
class ArmingIndex {
 public:
  ArmingIndex() = default;

  /// Build from each rule's per-pattern root symbols, in rule-index order
  /// (`nullopt` = a symbol-less root). This is the indexing logic; the
  /// `RuleSet` convenience below feeds it from real patterns.
  static auto from_root_symbols(std::span<std::vector<std::optional<Symbol>> const> per_rule_roots) -> ArmingIndex {
    ArmingIndex index;
    for (std::size_t rule_idx = 0; rule_idx < per_rule_roots.size(); ++rule_idx) {
      auto const &roots = per_rule_roots[rule_idx];
      bool const has_symbolless_root = std::ranges::any_of(roots, [](auto const &root) { return !root.has_value(); });
      if (has_symbolless_root) {
        index.always_armed_.push_back(rule_idx);
        continue;  // always armed; no need to also index by its other roots
      }
      for (auto const &root : roots) {
        auto &rules = index.by_symbol_[*root];
        // A rule with two patterns rooted at the same symbol indexes once.
        if (rules.empty() || rules.back() != rule_idx) rules.push_back(rule_idx);
      }
    }
    return index;
  }

  [[nodiscard]] auto always_armed() const -> std::span<std::size_t const> { return always_armed_; }

  /// Rule indices rooted at `sym` (empty span if none).
  [[nodiscard]] auto rules_for_symbol(Symbol sym) const -> std::span<std::size_t const> {
    auto const it = by_symbol_.find(sym);
    if (it == by_symbol_.end()) return {};
    return it->second;
  }

  /// Fill `armed` with the rule indices a pass should run given the symbols of
  /// its active set: every always-armed rule plus every rule rooted at an active
  /// symbol. The set de-duplicates a rule reachable through several symbols.
  template <typename ActiveSymbols>
  void collect_armed(ActiveSymbols const &active_symbols, boost::unordered_flat_set<std::size_t> &armed) const {
    armed.insert(always_armed_.begin(), always_armed_.end());
    for (auto const &sym : active_symbols) {
      auto const it = by_symbol_.find(sym);
      if (it != by_symbol_.end()) armed.insert(it->second.begin(), it->second.end());
    }
  }

 private:
  boost::unordered_flat_map<Symbol, std::vector<std::size_t>> by_symbol_;
  std::vector<std::size_t> always_armed_;
};

/// Build the arming index for a rule set from its rules' pattern roots.
template <RewritableGraph Graph>
auto BuildArmingIndex(RuleSet<Graph> const &rules) -> ArmingIndex<typename Graph::symbol_type> {
  using Symbol = typename Graph::symbol_type;
  std::vector<std::vector<std::optional<Symbol>>> per_rule_roots;
  auto const rule_ptrs = rules.rules();
  per_rule_roots.reserve(rule_ptrs.size());
  for (auto const &rule : rule_ptrs) per_rule_roots.push_back(rule->pattern_root_symbols());
  return ArmingIndex<Symbol>::from_root_symbols(per_rule_roots);
}

}  // namespace memgraph::planner::core::rewrite
