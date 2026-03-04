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

#include <memory>
#include <span>
#include <vector>

#include "planner/rewrite/rule.hpp"

namespace memgraph::planner::core::rewrite {

/// Immutable, shareable collection of rewrite rules. Cheap to copy (shared_ptr).
template <typename Symbol, typename Analysis>
  requires ENodeSymbol<Symbol>
class RuleSet {
 public:
  using RulePtr = std::shared_ptr<RewriteRule<Symbol, Analysis> const>;

  class Builder {
   public:
    auto add_rule(RewriteRule<Symbol, Analysis> rule) -> Builder & {
      rules_.push_back(std::make_shared<RewriteRule<Symbol, Analysis> const>(std::move(rule)));
      return *this;
    }

    auto build() -> RuleSet { return RuleSet{std::make_shared<std::vector<RulePtr> const>(std::move(rules_))}; }

   private:
    std::vector<RulePtr> rules_;
  };

  RuleSet() : rules_(std::make_shared<std::vector<RulePtr> const>()) {}

  template <typename... Rules>
  static auto Build(Rules &&...rules) -> RuleSet {
    std::vector<RulePtr> rule_vec;
    rule_vec.reserve(sizeof...(rules));
    (rule_vec.push_back(std::make_shared<RewriteRule<Symbol, Analysis> const>(std::forward<Rules>(rules))), ...);
    return RuleSet{std::make_shared<std::vector<RulePtr> const>(std::move(rule_vec))};
  }

  RuleSet(RuleSet const &) = default;
  RuleSet(RuleSet &&) noexcept = default;
  auto operator=(RuleSet const &) -> RuleSet & = default;
  auto operator=(RuleSet &&) noexcept -> RuleSet & = default;
  ~RuleSet() = default;

  [[nodiscard]] auto rules() const -> std::span<RulePtr const> { return *rules_; }

  [[nodiscard]] auto size() const -> std::size_t { return rules_->size(); }

  [[nodiscard]] auto empty() const -> bool { return rules_->empty(); }

 private:
  explicit RuleSet(std::shared_ptr<std::vector<RulePtr> const> rules) : rules_(std::move(rules)) {}

  std::shared_ptr<std::vector<RulePtr> const> rules_;
};

}  // namespace memgraph::planner::core::rewrite
