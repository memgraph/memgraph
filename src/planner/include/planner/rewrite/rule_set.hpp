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
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "planner/rewrite/arming_index.hpp"
#include "planner/rewrite/rule.hpp"

namespace memgraph::planner::core::rewrite {

/// Immutable, shareable collection of rewrite rules plus everything derived from
/// them (the arming index and maximum pattern depth), computed once at
/// construction. Cheap to copy (a single shared_ptr increment), so a shared
/// rule set carries its derived data to every consumer without recomputing it.
template <RewritableGraph Graph>
class RuleSet {
  using Symbol = typename Graph::symbol_type;

 public:
  using RulePtr = std::shared_ptr<RewriteRule<Graph> const>;

  class Builder {
   public:
    auto add_rule(RewriteRule<Graph> rule) -> Builder & {
      rules_.push_back(std::make_shared<RewriteRule<Graph> const>(std::move(rule)));
      return *this;
    }

    auto build() -> RuleSet { return RuleSet{std::move(rules_)}; }

   private:
    std::vector<RulePtr> rules_;
  };

  RuleSet() : RuleSet(std::vector<RulePtr>{}) {}

  template <typename... Rules>
  static auto Build(Rules &&...rules) -> RuleSet {
    std::vector<RulePtr> rule_vec;
    rule_vec.reserve(sizeof...(rules));
    (rule_vec.push_back(std::make_shared<RewriteRule<Graph> const>(std::forward<Rules>(rules))), ...);
    return RuleSet{std::move(rule_vec)};
  }

  RuleSet(RuleSet const &) = default;
  RuleSet(RuleSet &&) noexcept = default;
  auto operator=(RuleSet const &) -> RuleSet & = default;
  auto operator=(RuleSet &&) noexcept -> RuleSet & = default;
  ~RuleSet() = default;

  [[nodiscard]] auto rules() const -> std::span<RulePtr const> { return data_->rules; }

  [[nodiscard]] auto size() const -> std::size_t { return data_->rules.size(); }

  [[nodiscard]] auto empty() const -> bool { return data_->rules.empty(); }

  /// The arming index derived from the rules' pattern roots - the dual of the
  /// matcher index, mapping a changed symbol to the rules a pass should arm.
  [[nodiscard]] auto arming_index() const -> ArmingIndex<Symbol> const & { return data_->arming_index; }

  /// The deepest pattern in the rule set: the parent-closure depth incremental
  /// arming must use so a change at any bind reaches its pattern root.
  [[nodiscard]] auto max_pattern_depth() const -> std::size_t { return data_->max_pattern_depth; }

 private:
  /// Rules and their derived data in one immutable block: a RuleSet copy is a
  /// single shared_ptr increment, and the derived data is built exactly once.
  struct Data {
    std::vector<RulePtr> rules;
    ArmingIndex<Symbol> arming_index;
    std::size_t max_pattern_depth;
  };

  explicit RuleSet(std::vector<RulePtr> rules) {
    auto arming_index = MakeArmingIndex(rules);
    auto const max_pattern_depth = MakeMaxPatternDepth(rules);
    data_ = std::make_shared<Data const>(Data{
        .rules = std::move(rules), .arming_index = std::move(arming_index), .max_pattern_depth = max_pattern_depth});
  }

  /// Index the rules by their patterns' root symbols. A symbol-less root makes a
  /// rule always-armed; a rule rooted at a symbol is armed when that symbol changes.
  static auto MakeArmingIndex(std::span<RulePtr const> rules) -> ArmingIndex<Symbol> {
    std::vector<std::vector<std::optional<Symbol>>> per_rule_roots;
    per_rule_roots.reserve(rules.size());
    for (auto const &rule : rules) per_rule_roots.push_back(rule->pattern_root_symbols());
    return ArmingIndex<Symbol>::from_root_symbols(per_rule_roots);
  }

  static auto MakeMaxPatternDepth(std::span<RulePtr const> rules) -> std::size_t {
    std::size_t max_depth = 0;
    for (auto const &rule : rules) {
      for (auto const &pattern : rule->patterns()) max_depth = std::max(max_depth, pattern.depth());
    }
    return max_depth;
  }

  std::shared_ptr<Data const> data_;
};

}  // namespace memgraph::planner::core::rewrite
