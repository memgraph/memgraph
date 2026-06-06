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

#include <cstdint>
#include <cstdlib>
#include <vector>

#include "planner/extract/extract.hpp"
#include "query/plan_v2/cost/cost_model.hpp"
#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"
#include "query/plan_v2/egraph/symbol.hpp"
#include "query/plan_v2/resolve/resolver.hpp"
#include "query/plan_v2/test_support/extraction_inputs.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::plan::v2::test {

/// Runs the cost pass and the resolver over an e-graph, exactly as
/// `ConvertToLogicalOperator` does, and exposes the resolved selection - which
/// alt the resolver chose at each position and the scope it threaded to each
/// child - so a test can assert the resolver's decision directly instead of
/// inferring it from a built v1 plan. Construct on the stack from the true
/// root with empty demand, mirroring production; it owns everything the
/// resolved views reference, so it must not be moved while in use.
class ResolveHarness {
 public:
  ResolveHarness(egraph const &e, eclass root) : in_{e} {
    namespace extract = planner::core::extract;
    (void)extract::ComputeFrontiers(in_.core, in_.cost_ctx(), to_core(root), ctx_.frontier_context);
    extract::Resolve<symbol, analysis, CostFrontier>(
        in_.core,
        ResolverKey{.eclass = to_core(root), .in_scope = VariableSet{}, .must_introduce = VariableSet{}},
        [this](ResolverKey const &key,
               extract::FrontierMap<CostFrontier> const &frontier_map,
               auto const & /*egraph_ref*/,
               auto record_child) { return ResolvePlanNode(in_.syms, frontier_map, key, record_child); },
        ctx_);
  }

  ResolveHarness(ResolveHarness const &) = delete;
  ResolveHarness(ResolveHarness &&) = delete;
  auto operator=(ResolveHarness const &) -> ResolveHarness & = delete;
  auto operator=(ResolveHarness &&) -> ResolveHarness & = delete;
  ~ResolveHarness() = default;

  enum class BinderState : std::uint8_t { Alive, Dead };

  /// The symbol of the e-node the resolver chose at `c`. Asserts `c` resolved
  /// under exactly one key (a multi-scope e-class is a setup error here; use
  /// the key-keyed overloads once they exist).
  [[nodiscard]] auto chosen_symbol(eclass c) const -> symbol {
    return in_.core.get_enode(entry_for(to_core(c)).enode_id).symbol();
  }

  /// Whether the binder (Bind / Unwind) at `c` resolved alive or dead. Read
  /// from the resolver's actual threading: an alive binder threads all three
  /// children ([input, sym, payload]); a dead binder elides the sym. Asserts
  /// `c` resolved to a binder under exactly one key.
  [[nodiscard]] auto binder_state(eclass c) const -> BinderState {
    auto const &entry = entry_for(to_core(c));
    auto const sym = in_.core.get_enode(entry.enode_id).symbol();
    MG_ASSERT(sym == symbol::Bind || sym == symbol::Unwind, "ResolveHarness: e-class {} is not a binder", c.value_of());
    return entry.children.count == 3 ? BinderState::Alive : BinderState::Dead;
  }

  /// The key the resolver threaded to each child of the e-node chosen at `c`,
  /// in resolver-visit order. Each key carries the child's `eclass` plus the
  /// `in_scope` / `must_introduce` it was resolved under. Asserts `c` resolved
  /// under exactly one key.
  [[nodiscard]] auto threading(eclass c) const -> std::vector<ResolverKey> {
    auto const &entry = entry_for(to_core(c));
    std::vector<ResolverKey> out;
    out.reserve(entry.children.count);
    for (auto child_idx : ctx_.build.children_of(entry)) out.push_back(key_at(child_idx));
    return out;
  }

  /// The VariableIndex bit a Symbol e-class was assigned, for building the
  /// VariableSets an assertion compares against.
  [[nodiscard]] auto bit_of(eclass sym) const -> std::uint16_t { return in_.pre.variable_index.bit_of(to_core(sym)); }

 private:
  /// The unique ResolverKey emitted at `build_order` position `idx`. The
  /// resolver_seen map is a key->index bijection, so the inverse is single-valued.
  [[nodiscard]] auto key_at(std::uint32_t idx) const -> ResolverKey const & {
    for (auto const &[key, i] : ctx_.resolver_seen)
      if (i == idx) return key;
    MG_ASSERT(false, "ResolveHarness: no resolver key at build-order index {}", idx);
    std::abort();
  }

  /// The unique ResolvedEntry whose key e-class canonicalises to `c`. Asserts
  /// exactly one such entry exists.
  [[nodiscard]] auto entry_for(planner::core::EClassId c) const -> planner::core::extract::ResolvedEntry const & {
    auto const canon = in_.core.find(c);
    planner::core::extract::ResolvedEntry const *found = nullptr;
    for (auto const &[key, idx] : ctx_.resolver_seen) {
      if (in_.core.find(key.eclass) != canon) continue;
      MG_ASSERT(found == nullptr, "ResolveHarness: e-class {} resolved under more than one key", c.value_of());
      found = &ctx_.build.build_order[idx];
    }
    MG_ASSERT(found != nullptr, "ResolveHarness: e-class {} was not resolved (not reachable from root?)", c.value_of());
    return *found;
  }

  ExtractionInputs in_;
  planner::core::extract::ExtractContext<CostFrontier, ResolverKey, std::hash<ResolverKey>> ctx_{};
};

}  // namespace memgraph::query::plan::v2::test
