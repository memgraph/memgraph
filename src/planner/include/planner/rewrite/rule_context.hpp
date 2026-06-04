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
#include <utility>
#include <vector>

#include "utils/small_vector.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::rewrite {

/// A graph the rewrite engine can drive. A bare `EGraph` is its own core; a
/// `TypedEGraph` wraps one and exposes `core()` plus typed `Make<S>` for
/// interning. The engine talks to either through this concept, so a rule that
/// only merges runs over a bare e-graph while a rule that mints interned nodes
/// runs over a typed one - without the engine knowing the domain's symbols.
template <typename G>
concept TypedGraph = requires(G &g) { g.core(); };

/// The core `EGraph` underlying any graph the engine drives: itself for a bare
/// e-graph, `core()` for a typed one.
template <typename Symbol, typename Analysis, typename Graph>
auto rule_core(Graph &g) -> EGraph<Symbol, Analysis> & {
  if constexpr (TypedGraph<Graph>) {
    return g.core();
  } else {
    return g;
  }
}

/// Safe context for rule apply functions. Auto-tracks new e-classes and counts rewrites.
template <typename Symbol, typename Analysis, typename Graph = EGraph<Symbol, Analysis>>
  requires ENodeSymbol<Symbol>
class RuleContext {
 public:
  RuleContext(Graph &graph, std::vector<EClassId> &new_eclasses) : graph_(graph), new_eclasses_(new_eclasses) {}

  RuleContext(RuleContext const &) = delete;
  RuleContext(RuleContext &&) = delete;
  auto operator=(RuleContext const &) -> RuleContext & = delete;
  auto operator=(RuleContext &&) -> RuleContext & = delete;
  ~RuleContext() = default;

  void reset_rewrites() { rewrites_ = 0; }

  [[nodiscard]] auto rewrites() const -> std::size_t { return rewrites_; }

  /// Add e-node, auto-tracking new e-classes.
  auto emplace(Symbol symbol, utils::small_vector<EClassId> children) -> EmplaceResult {
    auto result = core().emplace(symbol, std::move(children));
    if (result.did_insert) {
      new_eclasses_.push_back(result.eclass_id);
    }
    return result;
  }

  auto emplace(Symbol symbol, uint64_t disambiguator) -> EmplaceResult {
    auto result = core().emplace(symbol, disambiguator);
    if (result.did_insert) {
      new_eclasses_.push_back(result.eclass_id);
    }
    return result;
  }

  /// Construct (or find) the e-class for `S(args...)` through the typed graph's
  /// `Make<S>`, so the new node is interned and its analysis seeded by the
  /// symbol's own trait. The resulting e-class is tracked for matcher reindex.
  /// Available only when the graph is typed.
  template <Symbol S, typename... Args>
    requires TypedGraph<Graph>
  auto Make(Args &&...args) -> EClassId {
    auto id = graph_.template Make<S>(std::forward<Args>(args)...);
    new_eclasses_.push_back(id);
    return id;
  }

  /// Merge e-classes, auto-counting rewrites.
  auto merge(EClassId a, EClassId b) -> EClassId {
    auto [canonical, did_merge] = core().merge(a, b);
    if (did_merge) {
      ++rewrites_;
    }
    return canonical;
  }

  [[nodiscard]] auto find(EClassId id) const -> EClassId { return core().find(id); }

  /// The analysis facts of `id`'s e-class. This is the read a fact-gated rule
  /// uses to route a semantic precondition through analysis rather than e-node
  /// shape.
  [[nodiscard]] auto analysis(EClassId id) const -> Analysis const & {
    return core().eclass(core().find(id)).analysis();
  }

 private:
  auto core() const -> EGraph<Symbol, Analysis> & { return rule_core<Symbol, Analysis>(graph_); }

  Graph &graph_;
  std::vector<EClassId> &new_eclasses_;
  std::size_t rewrites_ = 0;
};

}  // namespace memgraph::planner::core::rewrite
