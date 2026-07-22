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
#include <vector>

#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/rewrite/rule_latch.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::rewrite {

/// The per-pass driver the saturation loop drives, so the loop is mode-agnostic.
/// A driver owns the touched-set lifecycle and hands the loop its two per-rule
/// inputs.
///
/// Hook order within one saturate(): begin() once, then per pass
/// before_pass() -> [rules run, reading armed()/active()] -> after_pass(). The
/// terminating (zero-rewrite) pass skips after_pass, since no pass follows it.
///
/// armed() is a dense predicate indexed by rule position (nullptr = run every
/// rule); active() returns a RootRestriction the matcher applies to root-symbol
/// iteration. The hooks take the e-graph by mutable reference because before_pass
/// may drain its touched-set.
template <typename S, typename EG>
concept PassDriver = requires(S s, EG &eg) {
  s.begin(eg);
  s.before_pass(eg);
  s.after_pass(eg);
  { s.armed() } -> std::convertible_to<std::vector<std::uint8_t> const *>;
  { s.active() } -> std::convertible_to<RootRestriction>;
};

/// Every rule every pass: the reference driver and differential oracle. Arms
/// nothing (armed() is nullptr, so the loop runs all rules) and never touches
/// the e-graph's touched-set, so a Full saturate's changes survive to seed a
/// later incremental saturate's first arm.
struct FullDriver {
  template <typename EG>
  void begin(EG & /*egraph*/) {}

  template <typename EG>
  void before_pass(EG & /*egraph*/) {}

  template <typename EG>
  void after_pass(EG & /*egraph*/) {}

  static auto armed() -> std::vector<std::uint8_t> const * { return nullptr; }

  static auto active() -> RootRestriction { return RootRestriction::MatchAll(); }
};

/// Only the rules a pass could newly enable: drives a borrowed RuleLatch. begin
/// and after_pass arm from the e-graph's touched-set (begin off the entry state,
/// after_pass off the pass's own changes); before_pass drains the touched-set so
/// each pass captures only what it changed. The latch is owned by the Rewriter
/// and outlives this per-call driver.
template <typename Symbol, typename Analysis>
class IncrementalDriver {
 public:
  explicit IncrementalDriver(RuleLatch<Symbol, Analysis> &latch) : latch_(&latch) {}

  void begin(EGraph<Symbol, Analysis> &egraph) { latch_->arm(egraph); }

  void before_pass(EGraph<Symbol, Analysis> &egraph) { egraph.clear_touched(); }

  void after_pass(EGraph<Symbol, Analysis> &egraph) { latch_->arm(egraph); }

  [[nodiscard]] auto armed() const -> std::vector<std::uint8_t> const * { return &latch_->armed(); }

  [[nodiscard]] auto active() const -> RootRestriction { return latch_->active(); }

 private:
  RuleLatch<Symbol, Analysis> *latch_;
};

}  // namespace memgraph::planner::core::rewrite
