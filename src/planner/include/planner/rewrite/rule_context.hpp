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

#include <cstddef>
#include <vector>

#include "utils/small_vector.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core {

/// Safe context for rule apply functions. Auto-tracks new e-classes and counts rewrites.
template <typename Symbol, typename Analysis>
  requires ENodeSymbol<Symbol>
class RuleContext {
 public:
  RuleContext(EGraph<Symbol, Analysis> &egraph, std::vector<EClassId> &new_eclasses)
      : egraph_(egraph), new_eclasses_(new_eclasses) {}

  RuleContext(RuleContext const &) = delete;
  RuleContext(RuleContext &&) = delete;
  auto operator=(RuleContext const &) -> RuleContext & = delete;
  auto operator=(RuleContext &&) -> RuleContext & = delete;
  ~RuleContext() = default;

  void reset_rewrites() { rewrites_ = 0; }

  [[nodiscard]] auto rewrites() const -> std::size_t { return rewrites_; }

  /// Add e-node, auto-tracking new e-classes.
  auto emplace(Symbol symbol, utils::small_vector<EClassId> children) -> EmplaceResult {
    auto result = egraph_.emplace(symbol, std::move(children));
    if (result.did_insert) {
      new_eclasses_.push_back(result.eclass_id);
    }
    return result;
  }

  auto emplace(Symbol symbol, uint64_t disambiguator) -> EmplaceResult {
    auto result = egraph_.emplace(symbol, disambiguator);
    if (result.did_insert) {
      new_eclasses_.push_back(result.eclass_id);
    }
    return result;
  }

  /// Merge e-classes, auto-counting rewrites.
  auto merge(EClassId a, EClassId b) -> EClassId {
    auto [canonical, did_merge] = egraph_.merge(a, b);
    if (did_merge) {
      ++rewrites_;
    }
    return canonical;
  }

  [[nodiscard]] auto find(EClassId id) const -> EClassId { return egraph_.find(id); }

 private:
  EGraph<Symbol, Analysis> &egraph_;
  std::vector<EClassId> &new_eclasses_;
  std::size_t rewrites_ = 0;
};

}  // namespace memgraph::planner::core
