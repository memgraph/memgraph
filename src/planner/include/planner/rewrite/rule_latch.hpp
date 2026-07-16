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

#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/rewrite/active_set.hpp"
#include "planner/rewrite/arming_index.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::rewrite {

/// The incremental-saturation scheduler: decides, each pass, which rules a pass
/// could newly enable (the *armed* set) and - when the change is a sparse slice
/// of the graph - which e-classes a rule's matcher may restrict its root
/// iteration to (the *active* set). It never runs the matcher; arming is a
/// symbol-set lookup proportional to what changed.
///
/// Seeded from a rule set's arming index and maximum pattern depth via reset(),
/// then driven once per pass with arm(egraph). The first arm() after a reset
/// arms every rule (the whole graph is treated as touched); each later arm()
/// reads the e-graph's per-pass touched-set and arms only the rules a change
/// there could re-enable.
///
/// Invariants:
/// - The scratch buffers are reused across passes, so a latch must be a
///   long-lived member (one per egraph), not reconstructed per arm().
/// - It is scoped to a single egraph. active() holds that egraph's EClassIds, so
///   reuse across egraphs requires a reset() first.
/// - active() returns nullptr to mean "match every candidate"; a non-null but
///   EMPTY set means "restrict to nothing" (a settled, sparse graph), which is
///   distinct. A rule whose pattern root is symbol-less ignores the active set
///   (the matcher consults it only while iterating a symbol root), so an
///   always-armed rule still fires under an empty active set.
template <typename Symbol, typename Analysis>
class RuleLatch {
 public:
  /// Seed from a rule set's derived data and arm every rule on the next arm().
  /// Clears any prior scratch. `index` must outlive the latch - the rule set owns
  /// it, and a RuleLatch is scoped to one rule set until the next reset().
  void reset(ArmingIndex<Symbol> const &index, std::size_t max_pattern_depth, std::size_t num_rules) {
    index_ = &index;
    max_pattern_depth_ = max_pattern_depth;
    num_rules_ = num_rules;
    full_arm_pending_ = true;
    armed_.clear();
    active_symbols_.clear();
    active_eclasses_.clear();
    active_sparse_ = false;
  }

  /// Arm the rules the coming pass should run. The first call after reset arms
  /// every rule; later calls arm only what the e-graph's touched-set re-enables.
  void arm(EGraph<Symbol, Analysis> const &egraph) {
    if (full_arm_pending_) {
      arm_all();
      full_arm_pending_ = false;
    } else {
      arm_from_touched(egraph);
    }
  }

  /// The armed rule indices (positions in the rule set) for the coming pass.
  [[nodiscard]] auto armed() const -> boost::unordered_flat_set<std::size_t> const & { return armed_; }

  /// The active-set root restriction for the coming pass, or nullptr to match
  /// every candidate (see the class note on the null-vs-empty distinction).
  [[nodiscard]] auto active() const -> boost::unordered_flat_set<EClassId> const * {
    return active_sparse_ ? &active_eclasses_ : nullptr;
  }

 private:
  /// First pass (or post-reset): every rule runs and every candidate matches.
  void arm_all() {
    armed_.clear();
    for (std::size_t i = 0; i < num_rules_; ++i) armed_.insert(i);
    active_eclasses_.clear();
    active_sparse_ = false;  // match every candidate
  }

  /// Later passes: take the e-classes the last pass touched, close under parents
  /// to the max pattern depth, project to their e-node symbols, and map those
  /// through the arming index. Keep the active set for per-candidate matching
  /// only when it is a small slice of the graph; otherwise drop it (keep
  /// capacity) and match via symbol-granularity arming alone, since holding a
  /// large active set live only adds cache pressure for little pruning.
  void arm_from_touched(EGraph<Symbol, Analysis> const &egraph) {
    egraph.touched_eclasses_into(active_eclasses_);                  // canonical touched (reused buffer)
    ComputeActiveSet(egraph, active_eclasses_, max_pattern_depth_);  // close under parents, in place
    active_symbols_.clear();
    for (auto const eclass_id : active_eclasses_) {
      for (auto const enode_id : egraph.eclass(eclass_id).nodes()) {
        active_symbols_.insert(egraph.get_enode(enode_id).symbol());
      }
    }
    armed_.clear();
    index_->collect_armed(active_symbols_, armed_);

    active_sparse_ = active_eclasses_.size() * 2 < egraph.num_classes();
    if (!active_sparse_) active_eclasses_.clear();
  }

  ArmingIndex<Symbol> const *index_ = nullptr;
  std::size_t max_pattern_depth_ = 0;
  std::size_t num_rules_ = 0;
  boost::unordered_flat_set<std::size_t> armed_;
  boost::unordered_flat_set<Symbol> active_symbols_;
  boost::unordered_flat_set<EClassId> active_eclasses_;
  bool active_sparse_ = false;
  bool full_arm_pending_ = true;
};

}  // namespace memgraph::planner::core::rewrite
