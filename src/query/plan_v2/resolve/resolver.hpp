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
#include <span>
#include <utility>

#include <boost/container_hash/hash.hpp>

#include "planner/extract/extract.hpp"
#include "query/exceptions.hpp"
#include "query/plan_v2/cost/cost_model.hpp"
#include "query/plan_v2/egraph/alternative.hpp"
#include "query/plan_v2/egraph/child_layout.hpp"
#include "query/plan_v2/egraph/egraph_internal.hpp"
#include "query/plan_v2/egraph/symbol_dispatch.hpp"
#include "query/plan_v2/egraph/symbol_lists.hpp"
#include "query/plan_v2/resolve/extraction_env.hpp"
#include "query/plan_v2/resolve/variable_set.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::plan::v2 {

// ============================================================================
// Resolver: top-down DAG walk picking one alt per (eclass, in_scope,
// must_introduce). Per-symbol child propagation (how each child receives
// in_scope / must_introduce) lives in
// `symbol_resolve_traits<S>::resolve_children`; default is
// `UniformResolveChildren`.
// ============================================================================

struct ResolverKey {
  planner::core::EClassId eclass;
  VariableSet in_scope;
  VariableSet must_introduce;

  bool operator==(ResolverKey const &) const = default;
};

/// Default child propagation: visit each child with the parent's `in_scope`
/// and an empty `must_introduce`. No operator carries scope down to its
/// expression children. ("resolve children" here means recursing into the
/// child eclasses during the top-down resolver walk, unrelated to threads.)
struct UniformResolveChildren {
  static void resolve_children(planner::core::ENode<symbol> const &enode, ResolverKey const &parent_key,
                               VariableSet const & /*chosen_introduces*/, SymbolContext const & /*syms*/,
                               planner::core::extract::ChildSink<ResolverKey> auto visit) {
    for (auto child : enode.children()) visit(ResolverKey{child, parent_key.in_scope, {}});
  }
};

template <symbol S>
struct symbol_resolve_traits;  // primary intentionally undefined; every symbol must specialise.

// Alive Bind / Unwind child threading (both share the [input, sym, payload]
// layout). own_sym is the sym child. Pipe gets:
//   in_scope       = parent.in_scope
//   must_introduce = chosen.introduces − {own_sym}
// Expr gets:
//   in_scope       = parent.in_scope ∪ (chosen.introduces − {own_sym})
//   must_introduce = ∅
inline void ResolveBindUnwindAlive(planner::core::ENode<symbol> const &enode, ResolverKey const &parent_key,
                                   VariableSet const &chosen_introduces, SymbolContext const &syms, auto visit) {
  using namespace child::bind;
  auto const &children = enode.children();
  auto const sym_eclass = children[sym];
  auto pipe_must_introduce = chosen_introduces.difference_bit(syms.variable_index.bit_of(sym_eclass));
  auto expr_in_scope = parent_key.in_scope.set_union(pipe_must_introduce);
  visit(ResolverKey{children[input], parent_key.in_scope, pipe_must_introduce});
  visit(ResolverKey{sym_eclass, parent_key.in_scope, {}});
  visit(ResolverKey{children[expr], std::move(expr_in_scope), {}});
}

inline void ResolveBindDead(planner::core::ENode<symbol> const &enode, ResolverKey const &parent_key, auto visit) {
  visit(ResolverKey{enode.children()[child::bind::input], parent_key.in_scope, parent_key.must_introduce});
}

// Dead Unwind keeps the list child in the resolution (for cost/extraction
// consistency) but elides the sym binding. CardinalityScale takes its row
// count from the list's statically-known length fact and never evaluates the
// list at runtime. Pipe carries the parent's demand through; the list is an
// expression read in the parent's scope.
inline void ResolveUnwindDead(planner::core::ENode<symbol> const &enode, ResolverKey const &parent_key, auto visit) {
  using namespace child::unwind;
  auto const &children = enode.children();
  visit(ResolverKey{children[input], parent_key.in_scope, parent_key.must_introduce});
  visit(ResolverKey{children[list], parent_key.in_scope, {}});
}

// A row-pipe binder (Bind / Unwind) is "alive" when the chosen alt introduces
// its sym, and "dead" when the bound value is unused downstream.
inline bool BinderIsAlive(planner::core::ENode<symbol> const &enode, VariableSet const &chosen_introduces,
                          SymbolContext const &syms) {
  using namespace child::bind;  // sym position shared with unwind
  // Both binders resolve their sym at the same child index; this reads it by
  // bind's constant for Bind and Unwind alike, so the two must agree.
  static_assert(child::bind::sym == child::unwind::sym,
                "BinderIsAlive reads the sym child by bind's index for both Bind and Unwind");
  return enode.children().size() == 3 && chosen_introduces.test(syms.variable_index.bit_of(enode.children()[sym]));
}

template <>
struct symbol_resolve_traits<symbol::Bind> {
  // Alive vs dead derived from `sym ∈ chosen.introduces`. The cost model
  // emits both variants for a Bind enode; whichever the resolver picks
  // decides which propagation rule fires.
  static void resolve_children(planner::core::ENode<symbol> const &enode, ResolverKey const &parent_key,
                               VariableSet const &chosen_introduces, SymbolContext const &syms,
                               planner::core::extract::ChildSink<ResolverKey> auto visit) {
    if (BinderIsAlive(enode, chosen_introduces, syms)) {
      ResolveBindUnwindAlive(enode, parent_key, chosen_introduces, syms, visit);
    } else {
      ResolveBindDead(enode, parent_key, visit);
    }
  }
};

template <>
struct symbol_resolve_traits<symbol::Unwind> {
  // Alive vs dead derived from `sym ∈ chosen.introduces`, mirroring Bind. The
  // dead alt (sym unreferenced, list length known) elides the binding into a
  // CardinalityScale; the alive alt is the row-generative bind.
  static void resolve_children(planner::core::ENode<symbol> const &enode, ResolverKey const &parent_key,
                               VariableSet const &chosen_introduces, SymbolContext const &syms,
                               planner::core::extract::ChildSink<ResolverKey> auto visit) {
    if (BinderIsAlive(enode, chosen_introduces, syms)) {
      ResolveBindUnwindAlive(enode, parent_key, chosen_introduces, syms, visit);
    } else {
      ResolveUnwindDead(enode, parent_key, visit);
    }
  }
};

template <>
struct symbol_resolve_traits<symbol::Subquery> {
  // Scope barrier. Outer pipe must introduce parent's demand minus what the
  // subquery itself exposes; inner is barrier-isolated (in_scope = ∅), with
  // only `exposed_syms` crossing outward via the cost-model's `introduces`.
  static void resolve_children(planner::core::ENode<symbol> const &enode, ResolverKey const &parent_key,
                               VariableSet const & /*chosen_introduces*/, SymbolContext const &syms,
                               planner::core::extract::ChildSink<ResolverKey> auto visit) {
    using namespace child::subquery;
    auto const &children = enode.children();
    DMG_ASSERT(children.size() >= 2, "Subquery enode must have at least outer and inner children");
    auto const exposed = syms.variable_index.to_variable_set(children.subspan(first_exposed));
    auto outer_demand = parent_key.must_introduce.difference(exposed);
    visit(ResolverKey{children[outer], parent_key.in_scope, std::move(outer_demand)});
    visit(ResolverKey{children[inner], VariableSet{}, VariableSet{}});
    for (auto sym_child : children.subspan(first_exposed)) {
      visit(ResolverKey{sym_child, parent_key.in_scope, {}});
    }
  }
};

// Threads a "pipe + non-introducing reader children" operator. The pipe must
// introduce `pipe_must_introduce`; each reader child reads
// parent.in_scope ∪ pipe_must_introduce and introduces nothing. Shared by
// Output (readers = NamedOutput children) and Filter (reader = predicate).
void ResolvePipeThenReaders(planner::core::EClassId pipe, std::span<planner::core::EClassId const> reader_children,
                            ResolverKey const &parent_key, VariableSet const &pipe_must_introduce,
                            planner::core::extract::ChildSink<ResolverKey> auto visit) {
  auto const reader_in_scope = parent_key.in_scope.set_union(pipe_must_introduce);
  visit(ResolverKey{pipe, parent_key.in_scope, pipe_must_introduce});
  for (auto reader : reader_children) {
    visit(ResolverKey{reader, reader_in_scope, {}});
  }
}

template <>
struct symbol_resolve_traits<symbol::Output> {
  // own_syms = {each NamedOutput child's sym}. Pipe gets:
  //   in_scope       = parent.in_scope
  //   must_introduce = chosen.introduces − own_syms
  // NamedOutput children get:
  //   in_scope       = parent.in_scope ∪ (chosen.introduces − own_syms)
  //   must_introduce = ∅
  static void resolve_children(planner::core::ENode<symbol> const &enode, ResolverKey const &parent_key,
                               VariableSet const &chosen_introduces, SymbolContext const &syms,
                               planner::core::extract::ChildSink<ResolverKey> auto visit) {
    using namespace child::output;
    auto const &children = enode.children();
    DMG_ASSERT(!children.empty(), "Output enode must have at least a pipe child");
    auto const own_syms = ExtractOutputOwnSyms(enode, syms.egraph, syms.variable_index);
    auto const pipe_must_introduce = chosen_introduces.difference(own_syms);
    ResolvePipeThenReaders(children[pipe], children.subspan(first_named), parent_key, pipe_must_introduce, visit);
  }
};

template <>
struct symbol_resolve_traits<symbol::Filter> {
  // Output with own_syms = ∅: the pipe must introduce the chosen alt's full set
  // (⊇ predicate.required via FilterFlatMap's pruning), and the predicate reads
  // parent.in_scope ∪ that set.
  static void resolve_children(planner::core::ENode<symbol> const &enode, ResolverKey const &parent_key,
                               VariableSet const &chosen_introduces, SymbolContext const & /*syms*/,
                               planner::core::extract::ChildSink<ResolverKey> auto visit) {
    using namespace child::filter;
    auto const &children = enode.children();
    ResolvePipeThenReaders(children[input], children.subspan(predicate, 1), parent_key, chosen_introduces, visit);
  }
};

// Expression ops and structural leaves: uniform threading.
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define MG_UNIFORM_RESOLVE(SYM) \
  template <>                   \
  struct symbol_resolve_traits<symbol::SYM> : UniformResolveChildren {};

MG_UNIFORM_RESOLVE(Once)
MG_UNIFORM_RESOLVE(Symbol)
MG_UNIFORM_RESOLVE(Literal)
MG_UNIFORM_RESOLVE(ParamLookup)
MG_UNIFORM_RESOLVE(Identifier)
MG_UNIFORM_RESOLVE(NamedOutput)
MG_UNIFORM_RESOLVE(Function)

#define MG_UNIFORM_FROM_OP(Name, AstOp, ...) MG_UNIFORM_RESOLVE(Name)
EGRAPH_BINARY_OPS(MG_UNIFORM_FROM_OP)
EGRAPH_UNARY_OPS(MG_UNIFORM_FROM_OP)
#undef MG_UNIFORM_FROM_OP
#undef MG_UNIFORM_RESOLVE

// NOLINTEND(cppcoreguidelines-macro-usage)

void ResolveChildren(planner::core::ENode<symbol> const &enode, ResolverKey const &parent_key,
                     VariableSet const &chosen_introduces, SymbolContext const &syms,
                     planner::core::extract::ChildSink<ResolverKey> auto visit) {
  DispatchBySymbol(enode.symbol(), [&]<symbol S>() {
    symbol_resolve_traits<S>::resolve_children(enode, parent_key, chosen_introduces, syms, visit);
  });
}

/// Per-node resolve callback. Picks the min-cost alt whose
/// `required ⊆ in_scope` and `introduces ⊇ must_introduce`, classifies
/// frontier failures into PlannerBug vs NotYetImplemented, and dispatches
/// per-operator child threading via `ResolveChildren`.
auto ResolvePlanNode(SymbolContext const &syms, planner::core::extract::FrontierMap<CostFrontier> const &frontier_map,
                     ResolverKey const &key, planner::core::extract::ChildSink<ResolverKey> auto record_child)
    -> planner::core::ENodeId {
  auto const fr_it = frontier_map.find(key.eclass);
  if (fr_it == frontier_map.end() || !fr_it->second.has_value()) ThrowPlannerBug("eclass has no frontier");
  auto valid_alt = [&in_scope = key.in_scope, &demanded = key.must_introduce](Alternative const &alt) {
    return alt.required.is_subset_of(in_scope) && demanded.is_subset_of(alt.introduces);
  };
  auto const *best = planner::core::extract::PickBest(fr_it->second->alts(), valid_alt);
  if (!best) {
    // No alts at all => frontier construction is broken (planner bug).
    // Alts exist but none satisfy the scope predicate => surface as
    // NotYetImplemented (a feature gap, not a bug).
    if (fr_it->second->alts().empty()) {
      ThrowPlannerBug("eclass frontier is empty");
    }
    throw NotYetImplemented{"planner-v2 extraction of this query pattern"};
  }
  auto const &chosen = *best;
  auto const &enode = syms.egraph.get_enode(chosen.enode_id);
  ResolveChildren(enode, key, chosen.introduces, syms, record_child);
  return chosen.enode_id;
}

using ResolvedEntry = planner::core::extract::ResolvedEntry;

}  // namespace memgraph::query::plan::v2

template <>
struct std::hash<memgraph::query::plan::v2::ResolverKey> {
  auto operator()(memgraph::query::plan::v2::ResolverKey const &k) const noexcept -> std::size_t {
    auto h = boost::hash<memgraph::planner::core::EClassId>{}(k.eclass);
    boost::hash_combine(h, k.in_scope.hash());
    boost::hash_combine(h, k.must_introduce.hash());
    return h;
  }
};
