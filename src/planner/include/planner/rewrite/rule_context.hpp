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
#include <cstddef>
#include <type_traits>
#include <utility>
#include <vector>

#include "utils/small_vector.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::rewrite {

/// A graph the rewrite engine can drive: it exposes its symbol and analysis
/// types and a `core()` returning the underlying `EGraph`. A bare `EGraph` is
/// its own core; a `TypedEGraph` returns the one it wraps. The engine derives
/// `Symbol`/`Analysis` from the graph and never names the domain's symbols.
template <typename G>
concept RewritableGraph = ENodeSymbol<typename G::symbol_type> && requires(G &g) {
  typename G::symbol_type;
  typename G::analysis_type;
  { g.core() } -> std::same_as<EGraph<typename G::symbol_type, typename G::analysis_type> &>;
};

/// An analysis that carries no facts: a default-constructed `Analysis{}` is a
/// valid seed for any e-class, so a seed-less insert is sound. A fact-carrying
/// analysis is not empty and must be seeded per-kind via `Make<S>` instead.
template <typename Analysis>
concept AnalysisFree = std::is_empty_v<Analysis>;

/// Safe context for rule apply functions. Auto-tracks new e-classes and counts rewrites.
template <RewritableGraph Graph>
class RuleContext {
  using Symbol = typename Graph::symbol_type;
  using Analysis = typename Graph::analysis_type;

 public:
  RuleContext(Graph &graph, std::vector<EClassId> &new_eclasses) : graph_(graph), new_eclasses_(new_eclasses) {}

  RuleContext(RuleContext const &) = delete;
  RuleContext(RuleContext &&) = delete;
  auto operator=(RuleContext const &) -> RuleContext & = delete;
  auto operator=(RuleContext &&) -> RuleContext & = delete;
  ~RuleContext() = default;

  void reset_rewrites() { rewrites_ = 0; }

  [[nodiscard]] auto rewrites() const -> std::size_t { return rewrites_; }

  /// Add an e-node, auto-tracking new e-classes. Seed-less, so constrained to
  /// `AnalysisFree` graphs; a fact-carrying analysis must be seeded via `Make<S>`
  /// (a seed-less insert would default the arm to the wrong kind/facts).
  auto emplace(Symbol symbol, utils::small_vector<EClassId> children) -> EmplaceResult
    requires AnalysisFree<Analysis>
  {
    auto result = core().emplace(symbol, std::move(children));
    if (result.did_insert) {
      new_eclasses_.push_back(result.eclass_id);
    }
    return result;
  }

  auto emplace(Symbol symbol, uint64_t disambiguator) -> EmplaceResult
    requires AnalysisFree<Analysis>
  {
    auto result = core().emplace(symbol, disambiguator);
    if (result.did_insert) {
      new_eclasses_.push_back(result.eclass_id);
    }
    return result;
  }

  /// Construct (or find) `S(args...)` through the graph's typed `Make<S>`, so
  /// the node is interned and its analysis seeded by the symbol's trait. A fresh
  /// insert is tracked for matcher reindex; a hash-cons hit is already indexed.
  /// Constrained to graphs that intern - absent on a bare `EGraph`.
  template <Symbol S, typename... Args>
    requires requires(Graph &g) { g.template Make<S>(std::declval<Args>()...); }
  auto Make(Args &&...args) -> EClassId {
    auto result = graph_.template Make<S>(std::forward<Args>(args)...);
    if (result.did_insert) {
      new_eclasses_.push_back(result.eclass_id);
    }
    return result.eclass_id;
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

  /// The analysis facts of `id`'s e-class - the read a fact-gated rule uses to
  /// route a precondition through analysis rather than e-node shape.
  [[nodiscard]] auto analysis(EClassId id) const -> Analysis const & { return core().analysis_of(id); }

 private:
  auto core() -> EGraph<Symbol, Analysis> & { return graph_.core(); }

  auto core() const -> EGraph<Symbol, Analysis> const & { return graph_.core(); }

  Graph &graph_;
  std::vector<EClassId> &new_eclasses_;
  std::size_t rewrites_ = 0;
};

}  // namespace memgraph::planner::core::rewrite
