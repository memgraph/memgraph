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
#include <cstdint>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/rewrite/arming_index.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::rewrite {

/// The incremental-saturation scheduler: decides, each pass, which rules a pass
/// could newly enable (the *armed* set) and - when the change is a sparse slice
/// of the graph - which e-classes a rule's matcher may restrict its root
/// iteration to (the *active* set). It never runs the matcher; arming is a
/// symbol-to-hop lookup proportional to what changed.
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
    armed_.assign(num_rules, 0);
    min_hop_.clear();
    closure_scratch_.clear();
    active_set_.clear();
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

  /// A dense predicate indexed by rule position: non-zero at `i` iff rule `i` is
  /// armed for the coming pass. Sized to the rule count.
  [[nodiscard]] auto armed() const -> std::vector<std::uint8_t> const & { return armed_; }

  /// The active-set root restriction for the coming pass, or nullptr to match
  /// every candidate (see the class note on the null-vs-empty distinction).
  [[nodiscard]] auto active() const -> boost::unordered_flat_set<EClassId> const * {
    return active_sparse_ ? &active_set_ : nullptr;
  }

 private:
  /// First pass (or post-reset): every rule runs and every candidate matches.
  void arm_all() {
    armed_.assign(num_rules_, 1);
    active_set_.clear();
    active_sparse_ = false;  // match every candidate
  }

  /// Later passes: one parent-closure walk from the touched-set serves both
  /// products. For arming, close under parents to the max pattern depth,
  /// recording each symbol's shallowest hop, then arm each pattern only when its
  /// depth reaches that hop (see collect_armed). For the active-set restriction,
  /// keep only the touched classes and their direct parents (hop <= 1):
  /// restriction applies solely to root-entry patterns, which are depth <= 1, so
  /// a new match's root is always the change or a direct parent of it, never a
  /// deeper closure class. The active set is captured as a by-product of the same
  /// walk. Keep it only when it is a small slice of the graph; otherwise drop it
  /// (keep capacity) and match via symbol-granularity arming alone, since holding
  /// a large active set live only adds cache pressure for little pruning.
  void arm_from_touched(EGraph<Symbol, Analysis> const &egraph) {
    egraph.touched_eclasses_into(closure_scratch_);  // canonical touched, hop 0 (reused buffer)
    min_hop_.clear();
    active_set_.clear();

    frontier_.assign(closure_scratch_.begin(), closure_scratch_.end());
    for (auto const eclass_id : frontier_) {
      active_set_.insert(eclass_id);  // hop 0
      project_symbols(egraph, eclass_id, 0);
    }
    for (std::size_t hop = 1; hop <= max_pattern_depth_ && !frontier_.empty(); ++hop) {
      next_frontier_.clear();
      for (auto const eclass_id : frontier_) {
        for (auto const parent_enode : egraph.eclass(eclass_id).parents()) {
          auto const parent = egraph.find(parent_enode);
          if (!closure_scratch_.insert(parent).second) continue;  // first visit = shallowest hop
          next_frontier_.push_back(parent);
          if (hop == 1) active_set_.insert(parent);  // hop 1: direct parents complete the slice
          project_symbols(egraph, parent, hop);
        }
      }
      frontier_.swap(next_frontier_);
    }

    armed_.assign(num_rules_, 0);
    index_->collect_armed(min_hop_, armed_);

    active_sparse_ = active_set_.size() * 2 < egraph.num_classes();
    if (!active_sparse_) active_set_.clear();
  }

  /// Record each of an e-class's e-node symbols at `hop`, keeping the shallowest
  /// sighting. The walk visits each e-class once, at its shallowest hop, so a
  /// later, deeper sighting of a symbol never overwrites an earlier one.
  void project_symbols(EGraph<Symbol, Analysis> const &egraph, EClassId eclass_id, std::size_t hop) {
    for (auto const enode_id : egraph.eclass(eclass_id).nodes()) {
      min_hop_.try_emplace(egraph.get_enode(enode_id).symbol(), hop);
    }
  }

  ArmingIndex<Symbol> const *index_ = nullptr;
  std::size_t max_pattern_depth_ = 0;
  std::size_t num_rules_ = 0;
  std::vector<std::uint8_t> armed_;  // dense predicate indexed by rule position
  boost::unordered_flat_map<Symbol, std::size_t> min_hop_;
  boost::unordered_flat_set<EClassId> closure_scratch_;  // BFS visited set; bounds the arming walk
  boost::unordered_flat_set<EClassId> active_set_;       // touched + direct parents, returned by active()
  std::vector<EClassId> frontier_;                       // BFS scratch, reused across passes
  std::vector<EClassId> next_frontier_;
  bool active_sparse_ = false;
  bool full_arm_pending_ = true;
};

}  // namespace memgraph::planner::core::rewrite
