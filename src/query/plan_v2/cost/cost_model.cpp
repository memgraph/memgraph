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

#include "query/plan_v2/cost/cost_model.hpp"

#include <cassert>
#include <utility>

#include "query/exceptions.hpp"
#include "query/plan_v2/egraph/child_layout.hpp"
#include "query/plan_v2/egraph/op_ast_lists.hpp"
#include "query/plan_v2/egraph/symbol_dispatch.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::plan::v2 {

namespace {

// Per-application cost for each expression-op cost class.  Today every value
// is 1.0 -- differentiating without measured data would just calcify guesses
// into the planner.  When benchmark data shows a class should bias one way
// or another (e.g. comparison-heavy projections should prefer a layout that
// avoids re-evaluation), tune the constants here once rather than across N
// switch cases.
//
// Cost-class membership (which symbols are arithmetic / comparison / boolean
// / unary) lives in the EGRAPH_*_OPS sub-X-lists in op_ast_lists.hpp; the
// per-class arms below expand one sub-list each.
inline constexpr double kArithmetic = 1.0;
inline constexpr double kComparison = 1.0;
inline constexpr double kBoolean = 1.0;
inline constexpr double kUnary = 1.0;
/// Identifier reference - pays for the lookup of a bound symbol.
inline constexpr double kIdentifier = 1.0;

/// Per-output-row overhead for the UNWIND operator.  Paid once per row the
/// Unwind produces, on top of the list-expression evaluation cost.
/// Structural placeholder until measured data justifies a value -
/// deliberately small (1.0) so it doesn't dominate other per-row terms.
inline constexpr double kUnwindPerRowOverhead = 1.0;

// Per-alternative leaf costs.  Structural placeholders, 1.0 until measured
// data justifies divergent values.
inline constexpr double kOnceLeaf = 1.0;
inline constexpr double kSymbolLeaf = 1.0;
inline constexpr double kLiteralLeaf = 1.0;
inline constexpr double kParamLookupLeaf = 1.0;

// ============================================================================
// Cost-model algebra
// ============================================================================
// Named operations the per-symbol switch in CostModel::operator() composes.
// Each helper describes *what* the cost model does for a particular enode shape;
// the underlying ParetoFrontier primitives (LazyMap / cartesian_product /
// flat_map) are an implementation detail of these helpers.
//
// Cardinality defaults to 1.0 on per-evaluation operators (binary expressions,
// NamedOutput): each invocation packages one value regardless of the per-value
// shape.  Row-pipe operators (Output, Unwind, Subquery) override cardinality
// inside their dedicated helper.

/// Demand satisfaction: do `provider`'s available bindings cover what
/// `demander` reads?  The recurring check when composing an expression Alt
/// (a Bind/Unwind expr, an Output NamedOutput) onto an input Alt.  A
/// combination that fails this cannot form a satisfiable plan and is skipped.
[[nodiscard]] auto DemandMet(Alternative const &provider, Alternative const &demander) -> bool {
  return demander.required.is_subset_of(provider.introduces);
}

/// Single-alt frontier with no demand.  Terminal cost-model leaves
/// (Once, Literal, Symbol, ParamLookup).  `introduces` is empty for the
/// expression-flavoured leaves; `Once` passes the surrounding scope's
/// outer-scope set (see `ScopeContext`) so operator Alts constructed above
/// it inherit it transitively via `input.introduces`.
auto LeafFrontier(double cost, planner::core::ENodeId enode_id, VariableSet introduces = {}) -> CostFrontier {
  return CostFrontier{{{.cost = cost, .required = {}, .introduces = std::move(introduces), .enode_id = enode_id}}};
}

/// Single-alt frontier demanding `sym_eclass`.  Identifier nodes use this.
auto IdentifierFrontier(double cost, planner::core::EClassId sym_eclass, VariableIndex const &idx,
                        planner::core::ENodeId enode_id) -> CostFrontier {
  return CostFrontier{{{.cost = cost, .required = idx.to_variable_set(sym_eclass), .enode_id = enode_id}}};
}

/// Re-stamp source under `enode_id`, optionally bumping cost.  Used by unary
/// expression operators and by the leading-child re-stamp in Output / Function
/// chains.  View-style: no materialisation if downstream doesn't iterate.
auto Restamp(CostFrontier const &source, double extra_cost, planner::core::ENodeId enode_id) -> CostFrontier {
  return CostFrontier::LazyMap(source, extra_cost, enode_id);
}

/// Cartesian product with cost summation and required-set union.  Default
/// shape for binary expressions and NamedOutput pairs.
auto BinaryCombine(CostFrontier const &lhs, CostFrontier const &rhs, double extra_cost, planner::core::ENodeId enode_id)
    -> CostFrontier {
  return CostFrontier::cartesian_product(lhs, rhs, [extra_cost, enode_id](Alternative const &a, Alternative const &b) {
    return Alternative{
        .cost = extra_cost + a.cost + b.cost, .required = a.required.set_union(b.required), .enode_id = enode_id};
  });
}

/// Output × NamedOutput combine.  The row pipe's per-output-row evaluation
/// scales `named_out`'s scalar cost by the input pipe's cardinality.  The
/// emitted operator Alt has `required = ∅`, so (input, named_out)
/// combinations whose NamedOutput expression demands symbols the row pipe
/// doesn't `introduce` cannot form a satisfiable plan and are skipped at
/// construction.  Such combinations arise when the row pipe is a dead-Bind
/// variant whose sym is referenced by the NamedOutput's expression; the
/// alive-Bind variant is also in the row pipe's frontier and emits the
/// satisfiable combination.
///
/// NamedOutput sym injection is done in the dispatching `case symbol::Output`
/// arm after all NamedOutputs are folded in; `OutputCombine` itself only
/// touches cost/cardinality so it stays composable with the fold.
auto OutputCombine(CostFrontier const &row_pipe, CostFrontier const &named_out, planner::core::ENodeId enode_id)
    -> CostFrontier {
  return CostFrontier::cartesian_product_if(
      row_pipe,
      named_out,
      [](Alternative const &row_alt, Alternative const &named_alt) {
        DMG_ASSERT(row_alt.required.empty(), "operator Alt must have empty required");
        return DemandMet(row_alt, named_alt);
      },
      [enode_id](Alternative const &row_alt, Alternative const &named_alt) {
        return Alternative{.cost = row_alt.cost + (row_alt.cardinality * named_alt.cost),
                           .cardinality = row_alt.cardinality,
                           .required = {},
                           .introduces = row_alt.introduces,
                           .enode_id = enode_id};
      });
}

/// Bind's alive-branch cost.  Bind preserves input cardinality but evaluates
/// `expr` once per input row (Produce semantics in v1), so `expr_cost` is
/// scaled by `input_cardinality`.  For standalone Binds above Once
/// (cardinality 1) this collapses to `expr_cost`; for Binds inside a row
/// pipeline (above Unwind, scans) it correctly amortises per-row.
[[nodiscard]] auto BindAliveCost(double input_cost, double sym_cost, double expr_cost, double input_cardinality)
    -> double {
  return input_cost + sym_cost + (input_cardinality * expr_cost);
}

/// Bind flat-map: for each input alt, emit an alive variant when sym has a
/// consumer (the egraph-wide `referenced_syms` filter) and a dead variant
/// always.  Bind's emitted Alts are operator Alts: `required = ∅` and
/// `introduces = input.introduces ∪ {sym}` (alive) or
/// `introduces = input.introduces` (dead).  Outer-scope inheritance
/// is delivered via `ScopeContext`'s seeding of `Once.introduces` at scope-root,
/// so by the time a Bind is constructed, every relevant in-scope symbol is in
/// `input_alt.introduces`.
///
/// (input, expr) combinations where `expr.required ⊄ input.introduces` are
/// skipped: they cannot form a satisfiable plan, but other (input, expr) pairs
/// in the cartesian cross do (e.g. the alive-Bind variant of the row pipe with
/// the same expr), so the frontier still emits the valid combinations.  This
/// also subsumes the post-inline-rewrite self-referential case where
/// `Identifier(sym)` is unioned into expr's e-class.
auto BindFlatMap(CostFrontier const &input, CostFrontier const &expr, planner::core::EClassId sym_eclass,
                 double sym_cost, VariableSet const &referenced_syms, VariableIndex const &idx,
                 planner::core::ENodeId enode_id) -> CostFrontier {
  auto const sym_bit = idx.bit_of(sym_eclass);
  return CostFrontier::flat_map(input, [&, enode_id, sym_bit](Alternative const &input_alt, auto emit) {
    DMG_ASSERT(input_alt.required.empty(), "operator Alt must have empty required");
    // Alive: sym has at least one Identifier reference somewhere in the e-graph.
    if (referenced_syms.test(sym_bit)) {
      for (Alternative const &expr_alt : expr.alts()) {
        if (!DemandMet(input_alt, expr_alt)) continue;  // demand unsatisfiable by this input
        // Bind is one-shot, not a row-pipe: passes input's cardinality
        // through unchanged.  expr is evaluated once at bind-time.
        emit({.cost = BindAliveCost(input_alt.cost, sym_cost, expr_alt.cost, input_alt.cardinality),
              .cardinality = input_alt.cardinality,
              .required = {},
              .introduces = input_alt.introduces.union_bit(sym_bit),
              .enode_id = enode_id});
      }
    }
    // Dead: expr is not evaluated; pass input through unchanged.  Cost is
    // just `input_alt.cost`.  Alive vs dead is derived at read sites from
    // `sym ∈ chosen.introduces`.
    emit({.cost = input_alt.cost,
          .cardinality = input_alt.cardinality,
          .required = {},
          .introduces = input_alt.introduces,
          .enode_id = enode_id});
  });
}

/// Unwind flat-map: row-generative.  Output cardinality is the product of
/// input's and list's cardinalities; cost is input's pipeline plus per-row
/// evaluation of the list expression with a structural overhead.  Always
/// emits Alive because Unwind always introduces sym.  Operator-Alt dichotomy
/// applies: emitted `required = ∅`.  (input, list)
/// combinations whose list-expression demand isn't satisfied by `input.introduces`
/// are skipped (see BindFlatMap header for rationale).
auto UnwindFlatMap(CostFrontier const &input, CostFrontier const &list, planner::core::EClassId sym_eclass,
                   double sym_cost, VariableIndex const &idx, planner::core::ENodeId enode_id) -> CostFrontier {
  auto const sym_bit = idx.bit_of(sym_eclass);
  return CostFrontier::flat_map(input, [&, enode_id, sym_bit](Alternative const &input_alt, auto emit) {
    DMG_ASSERT(input_alt.required.empty(), "operator Alt must have empty required");
    for (Alternative const &list_alt : list.alts()) {
      if (!DemandMet(input_alt, list_alt)) continue;  // demand unsatisfiable by this input
      auto const cost = input_alt.cost + ((list_alt.cost + kUnwindPerRowOverhead) * input_alt.cardinality) + sym_cost;
      auto const cardinality = input_alt.cardinality * list_alt.cardinality;
      emit({.cost = cost,
            .cardinality = cardinality,
            .required = {},
            .introduces = input_alt.introduces.union_bit(sym_bit),
            .enode_id = enode_id});
    }
  });
}

/// Subquery flat-map: scope-barrier row-pipe.  Non-importing CALL only - an
/// importing inner is pruned to an empty frontier by its Output and rejected by
/// the Subquery cost guard, so every inner alt reaching here is self-contained
/// (`required = ∅`).  Inner introductions are STRIPPED at the boundary; only
/// the `exposed_syms` children cross into the outer scope, unioned with
/// outer's own introductions.
auto SubqueryFlatMap(CostFrontier const &outer, CostFrontier const &inner, VariableSet exposed_syms,
                     planner::core::ENodeId enode_id) -> CostFrontier {
  return CostFrontier::flat_map(
      outer, [&inner, exposed_syms = std::move(exposed_syms), enode_id](Alternative const &outer_alt, auto emit) {
        DMG_ASSERT(outer_alt.required.empty(), "operator Alt must have empty required");
        for (Alternative const &inner_alt : inner.alts()) {
          DMG_ASSERT(inner_alt.required.empty(), "non-importing CALL: inner alt must be self-contained");

          // BARRIER: outer.introduces ∪ exposed_syms; inner.introduces is dropped.
          // Operator-Alt dichotomy: the emitted Subquery Alt has empty required.
          emit({.cost = outer_alt.cost + (outer_alt.cardinality * inner_alt.cost),
                .cardinality = outer_alt.cardinality * inner_alt.cardinality,
                .required = {},
                .introduces = outer_alt.introduces.set_union(exposed_syms),
                .enode_id = enode_id});
        }
      });
}

/// Function combine: cartesian product over arg frontiers (cost-sum and
/// required-union via BinaryCombine), then per-alt cardinality override
/// because function cardinality is not the product of arg cardinalities -
/// args are scalars by construction.  Structural +1 cost accounts for the
/// function-call overhead.
auto FunctionCombine(std::span<CostFrontier const *const> args, double cardinality, planner::core::ENodeId enode_id)
    -> CostFrontier {
  auto result = args.empty() ? LeafFrontier(0.0, enode_id) : Restamp(*args[0], 0.0, enode_id);
  for (auto const *arg : args.empty() ? args : args.subspan(1)) {
    result = BinaryCombine(result, *arg, 0.0, enode_id);
  }
  result.mutate_pruning_invariant_preserving([&](Alternative &alt) {
    alt.cost += 1.0;
    alt.cardinality = cardinality;
    alt.enode_id = enode_id;
  });
  return result;
}

// ============================================================================
// symbol_cost_traits<S> - per-symbol cost emission, dispatched on enode.symbol()
// and mirroring symbol_resolve_traits<S>.  Class-bucketed symbols (arithmetic /
// comparison / boolean / unary) are macro-generated from the EGRAPH_*_OPS
// X-lists; structural and scope-threading symbols are hand-written.  Primary
// template undefined: a missing specialisation is a compile error.
// ============================================================================

template <symbol S>
struct symbol_cost_traits;

using ENodeT = planner::core::ENode<symbol>;
using ENodeId = planner::core::ENodeId;
using CostChildren = std::span<CostFrontier const *const>;

using enum symbol;

// ---- Leaves --------------------------------------------------------------

template <>
struct symbol_cost_traits<Once> {
  // Seed Once.introduces with the surrounding scope's outer-scope set so every
  // operator Alt above it inherits it transitively.  For a top-level query
  // outer_scope is empty; for an inner block of an importing CALL / Apply /
  // Cartesian (future work) outer_scope carries what the outer pipeline binds.
  static auto cost(ENodeT const &, ENodeId id, CostChildren, CostCtx const &ctx) -> CostFrontier {
    return LeafFrontier(kOnceLeaf, id, ctx.syms.outer_scope);
  }
};

template <>
struct symbol_cost_traits<Literal> {
  static auto cost(ENodeT const &, ENodeId id, CostChildren, CostCtx const &) -> CostFrontier {
    return LeafFrontier(kLiteralLeaf, id);
  }
};

template <>
struct symbol_cost_traits<Symbol> {
  static auto cost(ENodeT const &, ENodeId id, CostChildren, CostCtx const &) -> CostFrontier {
    return LeafFrontier(kSymbolLeaf, id);
  }
};

template <>
struct symbol_cost_traits<ParamLookup> {
  static auto cost(ENodeT const &, ENodeId id, CostChildren, CostCtx const &) -> CostFrontier {
    return LeafFrontier(kParamLookupLeaf, id);
  }
};

// ---- Identifier ----------------------------------------------------------

template <>
struct symbol_cost_traits<Identifier> {
  static auto cost(ENodeT const &n, ENodeId id, CostChildren children, CostCtx const &ctx) -> CostFrontier {
    using namespace child::identifier;
    assert(!children.empty() && "Identifier must have its symbol child frontier");
    auto const sym_eclass = n.children()[sym];
    auto const &[_, child_cost] = children[sym]->resolve();
    return IdentifierFrontier(kIdentifier + child_cost, sym_eclass, ctx.syms.variable_index, id);
  }
};

// ---- Bind / Unwind / Subquery / Output / NamedOutput / Function ----------

template <>
struct symbol_cost_traits<Bind> {
  // Alive/dead variants per input alt.  Alive vs dead is derived at read sites
  // from `sym ∈ chosen.introduces`.
  static auto cost(ENodeT const &n, ENodeId id, CostChildren children, CostCtx const &ctx) -> CostFrontier {
    using namespace child::bind;
    auto const sym_eclass = n.children()[sym];
    auto const &[_, sym_cost] = children[sym]->resolve();
    return BindFlatMap(
        *children[input], *children[expr], sym_eclass, sym_cost, ctx.syms.referenced_syms, ctx.syms.variable_index, id);
  }
};

template <>
struct symbol_cost_traits<Unwind> {
  // Row-generative.  Cardinality is input × list; cost mirrors per-row
  // evaluation of the list expression.  Always Alive (Unwind always
  // introduces sym), so ResolveChildren dispatches like alive Bind.
  static auto cost(ENodeT const &n, ENodeId id, CostChildren children, CostCtx const &ctx) -> CostFrontier {
    using namespace child::unwind;
    auto const sym_eclass = n.children()[sym];
    auto const &[_, sym_cost] = children[sym]->resolve();
    return UnwindFlatMap(*children[input], *children[list], sym_eclass, sym_cost, ctx.syms.variable_index, id);
  }
};

template <>
struct symbol_cost_traits<Subquery> {
  // Scope-barrier row-pipe.  Children layout is [outer, inner, exposed_sym...].
  // Inner's introduces are STRIPPED at the barrier; only exposed_syms cross
  // into the outer scope.  Importing-CALL is unsupported: an importing inner
  // references a symbol bound only in the outer scope, which the inner Output's
  // satisfiability filter prunes - leaving inner with no self-contained alt.
  // Throw up front so that surfaces as the real cause rather than a downstream
  // "root frontier has no self-contained alternative" PlannerBug.
  static auto cost(ENodeT const &n, ENodeId id, CostChildren children, CostCtx const &ctx) -> CostFrontier {
    using namespace child::subquery;
    bool const has_self_contained_inner =
        std::ranges::any_of(children[inner]->alts(), [](Alternative const &a) { return a.required.empty(); });
    if (!has_self_contained_inner) {
      throw NotYetImplemented{"importing CALL subqueries"};
    }
    auto exposed_syms = ctx.syms.variable_index.to_variable_set(n.children().subspan(first_exposed));
    return SubqueryFlatMap(*children[outer], *children[inner], std::move(exposed_syms), id);
  }
};

template <>
struct symbol_cost_traits<Output> {
  // Row-pipe.  Re-stamp child[0]'s row pipe, then fold each NamedOutput in
  // with per-row-scaled evaluation cost.  Per-row scaling lets the planner
  // prefer a one-shot Bind over an inlined alternative when the row pipe is
  // wide (e.g. UNWIND range(0, 100)).
  //
  // Output's own_syms are the syms each NamedOutput child binds (the first
  // child of each NamedOutput enode).  Under the uniform `own_syms` rule
  // every operator's `introduces = input.introduces ∪ own_syms`; Output's
  // NamedOutput syms are exposed to the resolver's `in_scope` at Output's
  // position and become part of the `chosen.introduces` it commits upward.
  static auto cost(ENodeT const &n, ENodeId id, CostChildren children, CostCtx const &ctx) -> CostFrontier {
    using namespace child::output;
    auto const own_syms = ExtractOutputOwnSyms(n, ctx.syms.egraph, ctx.syms.variable_index);
    auto result = Restamp(*children[pipe], 0.0, id);
    for (auto const *named_out : children.subspan(first_named)) {
      result = OutputCombine(result, *named_out, id);
    }
    // Inject own_syms into every Alt's introduces.  Larger introduces is
    // "better" on the Pareto dim, so this preserves the pruning invariant.
    if (!own_syms.empty()) {
      result.mutate_pruning_invariant_preserving(
          [&own_syms](Alternative &alt) { alt.introduces = alt.introduces.set_union(own_syms); });
    }
    return result;
  }
};

template <>
struct symbol_cost_traits<NamedOutput> {
  // sym × expr cartesian product, +1 per pair.  Structural (not an expression
  // operator), so a fixed cost rather than a cost-class constant.
  static auto cost(ENodeT const &, ENodeId id, CostChildren children, CostCtx const &) -> CostFrontier {
    using namespace child::named_out;
    return BinaryCombine(*children[sym], *children[expr], 1.0, id);
  }
};

template <>
struct symbol_cost_traits<Function> {
  // Per-class cost-sum chain over args, then override cardinality with the
  // estimator's output.  Function cardinality is *not* the product of arg
  // cardinalities (args are scalars).
  static auto cost(ENodeT const &n, ENodeId id, CostChildren children, CostCtx const &ctx) -> CostFrontier {
    return FunctionCombine(children, ctx.estimator.Estimate(n, n.children()), id);
  }
};

// ---- Class-bucketed expression operators ---------------------------------
//
// Cost-class membership lives in EGRAPH_{ARITHMETIC,COMPARISON,BOOLEAN,UNARY}_OPS
// X-lists; per-symbol trait specs are macro-generated from those lists so the
// bucket constants (kArithmetic, kComparison, kBoolean, kUnary) stay
// file-local and class membership stays single-sourced.

// NOLINTBEGIN(cppcoreguidelines-macro-usage,bugprone-macro-parentheses)
#define MG_BINARY_COST_TRAIT(Name, ClassConst)                                                             \
  template <>                                                                                              \
  struct symbol_cost_traits<Name> {                                                                        \
    static auto cost(ENodeT const &, ENodeId id, CostChildren children, CostCtx const &) -> CostFrontier { \
      return BinaryCombine(*children[child::binary::lhs], *children[child::binary::rhs], ClassConst, id);  \
    }                                                                                                      \
  };

#define MG_ARITH(Name, ...) MG_BINARY_COST_TRAIT(Name, kArithmetic)
EGRAPH_ARITHMETIC_OPS(MG_ARITH)
#undef MG_ARITH

#define MG_CMP(Name, ...) MG_BINARY_COST_TRAIT(Name, kComparison)
EGRAPH_COMPARISON_OPS(MG_CMP)
#undef MG_CMP

#define MG_BOOL(Name, ...) MG_BINARY_COST_TRAIT(Name, kBoolean)
EGRAPH_BOOLEAN_OPS(MG_BOOL)
#undef MG_BOOL

#undef MG_BINARY_COST_TRAIT

// Unary expression operators (Not, UnaryMinus, UnaryPlus): re-stamp child
// with per-class cost.  View-style; chains of unary ops collapse into one
// materialisation.  Identifier is also unary-arity but scored separately
// (EGRAPH_UNARY_OPS excludes it).
#define MG_UNARY_COST_TRAIT(Name, ...)                                                                     \
  template <>                                                                                              \
  struct symbol_cost_traits<Name> {                                                                        \
    static auto cost(ENodeT const &, ENodeId id, CostChildren children, CostCtx const &) -> CostFrontier { \
      return Restamp(*children[child::unary::operand], kUnary, id);                                        \
    }                                                                                                      \
  };
EGRAPH_UNARY_OPS(MG_UNARY_COST_TRAIT)
#undef MG_UNARY_COST_TRAIT
// NOLINTEND(cppcoreguidelines-macro-usage,bugprone-macro-parentheses)

}  // namespace

auto ExtractOutputOwnSyms(planner::core::ENode<symbol> const &output_enode, EGraph const &egraph,
                          VariableIndex const &idx) -> VariableSet {
  VariableSet out;
  for (auto const no_eclass : output_enode.children().subspan(child::output::first_named)) {
    auto const &cls = egraph.eclass(no_eclass);
    DMG_ASSERT(cls.nodes().size() == 1, "NamedOutput e-class must be a singleton");
    auto const &no_enode = egraph.get_enode(cls.nodes().front());
    DMG_ASSERT(!no_enode.children().empty(), "NamedOutput enode must have at least one child (sym leaf)");
    out.set(idx.bit_of(no_enode.children()[child::named_out::sym]));
  }
  return out;
}

auto CostCtx::operator()(planner::core::ENode<symbol> const &current, planner::core::ENodeId enode_id,
                         std::span<CostResult const *const> children) const -> CostResult {
  return DispatchBySymbol(current.symbol(),
                          [&]<symbol S>() { return symbol_cost_traits<S>::cost(current, enode_id, children, *this); });
}

}  // namespace memgraph::query::plan::v2
