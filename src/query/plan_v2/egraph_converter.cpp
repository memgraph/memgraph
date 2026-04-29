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

#include "query/plan_v2/egraph_converter.hpp"

#include <limits>
#include <utility>

#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

#include "planner/extract/extractor.hpp"
#include "query/plan/operator.hpp"
#include "query/plan_v2/bind_semantics.hpp"
#include "query/plan_v2/egraph_internal.hpp"
#include "query/plan_v2/expression_cost.hpp"
#include "utils/tag.hpp"

namespace memgraph::query::plan::v2 {

// ============================================================================
// Plan extraction cost model — Pareto frontier with symbol demand tracking
// ============================================================================
namespace {

using bind::SymbolSet;

struct Alternative {
  double cost;
  SymbolSet required;               // Symbols that MUST be bound by ancestors
  planner::core::ENodeId enode_id;  // Which enode achieves this alternative

  auto dominated_by(Alternative const &other) const -> bool {
    return other.cost <= cost && std::ranges::includes(required, other.required);
  }
};

struct AlternativeDominance {
  static auto operator()(Alternative const &a, Alternative const &b) -> bool { return a.dominated_by(b); }
};

/// CostFrontier: ParetoFrontier with resolve/min_cost for the extraction contract.
/// merge is inherited from ParetoFrontier (union + prune).
struct CostFrontier : planner::core::extract::CostResultBase<CostFrontier, Alternative, AlternativeDominance> {
  using CostResultBase::CostResultBase;
};

/// Resolver-side tie-break: cheapest feasible alive vs dead Bind cost under
/// the resolver's `provided` context.  Sole caller: PlanResolver::visit_bind_children.
///
/// PlanCostModel::Bind does NOT use this — it builds the full alive/dead frontier
/// directly via flat_map (no demand filtering at frontier-build time).
///
/// Filtering: a non-empty `provided` enables the required-subset check on input
/// (and on expr in the alive branch).  Empty `provided` means "no demand context"
/// and skips filtering — the resolver will rely on the per-eclass pick_compatible
/// pass to enforce feasibility on the chosen branch.
///
/// The alive/dead algebra (predicate, cost formulas, kSymbolCost invariant) lives
/// in bind:: — see src/query/plan_v2/bind_semantics.hpp.
///
/// Tie-break rule: ties go to dead (less work). Callers compare with strict `<`.
struct BindBranchCosts {
  double alive;
  double dead;
};

static auto BestBindBranchCostsForResolve(CostFrontier const &input_frontier, double sym_cost,
                                          CostFrontier const &expr_frontier, planner::core::EClassId sym_eclass,
                                          SymbolSet const &provided) -> BindBranchCosts {
  // When provided is non-empty we filter alternatives by required ⊆ provided.
  // For the alive branch the input alt may demand sym_eclass, so we check against
  // provided ∪ {sym_eclass}.  For the dead branch we check against provided directly.
  bool const filtering = !provided.empty();

  SymbolSet alive_provided;
  if (filtering) {
    alive_provided = provided;
    alive_provided.insert(sym_eclass);
  }

  auto best_alive = std::numeric_limits<double>::infinity();
  auto best_dead = std::numeric_limits<double>::infinity();

  for (auto const &input_alt : input_frontier.alts()) {
    if (bind::IsAlive(input_alt.required, sym_eclass)) {
      if (filtering && !bind::IsCompatible(input_alt.required, alive_provided)) continue;
      for (auto const &expr_alt : expr_frontier.alts()) {
        if (filtering && !bind::IsCompatible(expr_alt.required, provided)) continue;
        best_alive = std::min(best_alive, bind::AliveCost(input_alt.cost, sym_cost, expr_alt.cost));
      }
    } else {
      if (filtering && !bind::IsCompatible(input_alt.required, provided)) continue;
      best_dead = std::min(best_dead, bind::DeadCost(input_alt.cost));
    }
  }

  return {best_alive, best_dead};
}

// Combine two frontiers with cost summation and required-set union.
// Stateless functor — set_union's intermediate buffer is stack-allocated per
// call.  Typical demand-union sizes fit in the inline capacity; queries that
// exceed it pay one extra heap event over the SymbolSet's own allocation.
struct CombineAltsFn {
  double extra_cost;
  planner::core::ENodeId enode_id;

  auto operator()(Alternative const &l, Alternative const &r) const -> Alternative {
    boost::container::small_vector<planner::core::EClassId, 16> buf;
    buf.reserve(l.required.size() + r.required.size());
    std::ranges::set_union(l.required, r.required, std::back_inserter(buf));
    // set_union on two sorted flat_sets produces sorted unique output —
    // ordered_unique_range skips redundant sorting in the flat_set constructor.
    SymbolSet required(boost::container::ordered_unique_range, buf.begin(), buf.end());
    return {.cost = extra_cost + l.cost + r.cost, .required = std::move(required), .enode_id = enode_id};
  }
};

auto CombineAlts(double extra_cost, planner::core::ENodeId enode_id) -> CombineAltsFn {
  return CombineAltsFn{extra_cost, enode_id};
}

/// Map over a single frontier — adjust each alternative's cost by `extra_cost`
/// and re-stamp `enode_id`.  The 1-frontier sibling of CombineAlts; used for
/// pass-through nodes (unary operators, Output's input child) where the output
/// frontier shape mirrors a single input.  The transformation is monotone in
/// cost and preserves the required-set, so the Pareto invariant is preserved
/// — from_unpruned's prune pass is a no-op on already-Pareto-pruned input.
auto MapAlts(CostFrontier const &input, double extra_cost, planner::core::ENodeId enode_id) -> CostFrontier {
  std::vector<Alternative> out;
  out.reserve(input.alts().size());
  for (auto const &alt : input.alts()) {
    out.push_back({.cost = alt.cost + extra_cost, .required = alt.required, .enode_id = enode_id});
  }
  return CostFrontier::from_unpruned(std::move(out));
}

struct PlanCostModel {
  using CostResult = CostFrontier;

  auto operator()(planner::core::ENode<symbol> const &current, planner::core::ENodeId enode_id,
                  std::span<CostResult const> children) const -> CostResult {
    switch (current.symbol()) {
      // Leaf nodes: single alternative, no demand
      case symbol::Once:
      case symbol::Literal:
      case symbol::Symbol:  // Leaf invariant — see bind::kSymbolCost.
      case symbol::ParamLookup:
        return CostResult{{{.cost = bind::kSymbolCost, .required = {}, .enode_id = enode_id}}};

      // Identifier: demands its symbol child to be bound
      case symbol::Identifier: {
        assert(!children.empty() && "Identifier must have its symbol child frontier");
        auto sym_eclass = current.children()[0];
        return CostResult{{{.cost = expression_cost::kIdentifier + CostFrontier::min_cost(children[0]),
                            .required = {sym_eclass},
                            .enode_id = enode_id}}};
      }

      // Bind: alive if sym demanded, dead otherwise.
      // Builds the full alive/dead frontier here; the resolver later picks one
      // branch using BestBindBranchCostsForResolve under its `provided` context.
      case symbol::Bind: {
        auto const &input_frontier = children[0];
        auto const &sym_frontier = children[1];
        auto const &expr_frontier = children[2];
        auto sym_eclass = current.children()[1];
        auto sym_cost = CostFrontier::min_cost(sym_frontier);

        return CostFrontier::flat_map(input_frontier, [&](auto const &input_alt, auto emit) {
          if (bind::IsAlive(input_alt.required, sym_eclass)) {
            for (auto const &expr_alt : expr_frontier.alts()) {
              auto required = bind::AliveRequired(input_alt.required, sym_eclass, expr_alt.required);
              emit({.cost = bind::AliveCost(input_alt.cost, sym_cost, expr_alt.cost),
                    .required = std::move(required),
                    .enode_id = enode_id});
            }
          } else {
            emit({.cost = bind::DeadCost(input_alt.cost), .required = input_alt.required, .enode_id = enode_id});
          }
        });
      }

      // Binary expression operators (arithmetic / comparison / boolean):
      // lhs × rhs cartesian product, with the per-class cost looked up via the
      // symbol's descriptor.  Adding a new binary operator is one descriptor
      // specialisation in private_symbol.hpp — no new case arms here.
      case symbol::Add:
      case symbol::Sub:
      case symbol::Mul:
      case symbol::Div:
      case symbol::Mod:
      case symbol::Exp:
      case symbol::Eq:
      case symbol::Neq:
      case symbol::Lt:
      case symbol::Lte:
      case symbol::Gt:
      case symbol::Gte:
      case symbol::And:
      case symbol::Or:
      case symbol::Xor: {
        auto const cost = expression_cost::FromClass(CostClassOf(current.symbol()));
        return CostFrontier::combine(children[0], children[1], CombineAlts(cost, enode_id));
      }

      // Unary expression operators: pass through child, +kUnary, re-stamp
      // enode_id so the Builder dispatches *this* unary node via enode.symbol().
      // Same dispatch story as binary: cost via descriptor.
      case symbol::Not:
      case symbol::UnaryMinus:
      case symbol::UnaryPlus: {
        auto const cost = expression_cost::FromClass(CostClassOf(current.symbol()));
        return MapAlts(children[0], cost, enode_id);
      }

      // Output: re-stamp child[0]'s frontier (no extra cost) so all alternatives
      // dispatch through this Output enode in the Builder, then fold in each
      // NamedOutput child via CombineAlts (which re-stamps again per pair).
      // The MapAlts pass is required when children.size() == 1 (no NamedOutputs);
      // otherwise CombineAlts would handle re-stamping on its own.
      case symbol::Output: {
        auto result = MapAlts(children[0], 0.0, enode_id);
        for (size_t i = 1; i < children.size(); ++i) {
          result = CostFrontier::combine(result, children[i], CombineAlts(0.0, enode_id));
        }
        return result;
      }

      // NamedOutput: sym × expr cartesian product, +1 per pair.  Structural
      // (not an expression operator), so kept at a fixed cost rather than
      // sourced from expression_cost.
      case symbol::NamedOutput:
        return CostFrontier::combine(children[0], children[1], CombineAlts(1.0, enode_id));
    }
    std::unreachable();
  }
};

/// Context-aware top-down resolution of demand frontiers.
/// Propagates a "provided" set (symbols bound by Bind ancestors) to ensure
/// child selections are consistent with parent alive/dead decisions.
/// Bind-aware Resolver adapter.  Default-constructible, stateless; each
/// operator() call constructs an Impl scoped to the call that owns the
/// recursion-shared `resolved` map.
struct PlanResolver {
  using EClassId = planner::core::EClassId;
  using Selection = planner::core::extract::Selection<double>;
  using SelectionMap = planner::core::extract::SelectionMap<double>;
  using FrontierMap = planner::core::extract::FrontierMap<CostFrontier>;
  using EGraph = planner::core::EGraph<symbol, analysis>;

  auto operator()(EGraph const &egraph, FrontierMap const &frontier_map, EClassId root) const -> SelectionMap {
    Impl impl{egraph, frontier_map};
    impl.resolve_impl(root, SymbolSet{});
    return std::move(impl).take();
  }

 private:
  struct ResolvedEntry {
    Selection sel;
    SymbolSet required;
  };

  /// Per-call working state.  Lives only for the duration of one operator().
  struct Impl {
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    EGraph const &egraph;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    FrontierMap const &frontier_map;
    std::unordered_map<EClassId, ResolvedEntry> resolved;

    auto take() && -> SelectionMap {
      SelectionMap result;
      result.reserve(resolved.size());
      for (auto &&[id, entry] : resolved) {
        result.emplace(id, std::move(entry.sel));
      }
      return result;
    }

    /// Pick cheapest alt whose required is a subset of provided.
    auto pick_compatible(CostFrontier const &frontier, SymbolSet const &provided) -> Alternative const & {
      Alternative const *best = nullptr;
      for (auto const &alt : frontier.alts()) {
        if (bind::IsCompatible(alt.required, provided)) {
          if (!best || alt.cost < best->cost) {
            best = &alt;
          }
        }
      }
      if (!best) {
        throw QueryException{
            "Plan extraction failed: no compatible alternative at this node — "
            "a symbol is demanded that no ancestor can provide. "
            "This usually means an Identifier node was not inlined by the rewrite pass."};
      }
      return *best;
    }

    /// Bind-specific child visitation with alive/dead logic.
    /// Branch selection uses BestBindBranchCostsForResolve under the current `bind_provided`.
    void visit_bind_children(EClassId input_eclass, EClassId sym_eclass, EClassId expr_eclass,
                             SymbolSet const &bind_provided) {
      auto input_it = frontier_map.find(input_eclass);
      assert(input_it != frontier_map.end() && input_it->second.has_value());
      auto const &input_frontier = *input_it->second;

      auto sym_it = frontier_map.find(sym_eclass);
      assert(sym_it != frontier_map.end() && sym_it->second.has_value());
      // Symbol leaf invariant — see bind::kSymbolCost.  If this fires, a
      // rewrite has introduced Symbol alternatives and the Bind algebra must be
      // generalised to enumerate them in the alive-branch cost.
      assert(sym_it->second->alts().size() == 1 && sym_it->second->alts().front().required.empty() &&
             "Symbol eclass invariant violated: see bind::kSymbolCost");
      auto sym_cost = CostFrontier::min_cost(*sym_it->second);

      auto expr_it = frontier_map.find(expr_eclass);
      assert(expr_it != frontier_map.end() && expr_it->second.has_value());
      auto const &expr_frontier = *expr_it->second;

      auto const [best_alive_cost, best_dead_cost] =
          BestBindBranchCostsForResolve(input_frontier, sym_cost, expr_frontier, sym_eclass, bind_provided);

      // Alive wins if strictly cheaper; ties go to dead (less work)
      if (best_alive_cost < best_dead_cost) {
        // Alive: visit all three children (input with extra sym provided, sym, expr)
        auto alive_provided = bind_provided;
        alive_provided.insert(sym_eclass);
        resolve_impl(input_eclass, alive_provided);
        // sym_eclass is a leaf Symbol with required={} (asserted above); compatible
        // with any provided set. We pass bind_provided (not alive_provided) because
        // Symbol does not demand the symbol it defines — observationally equivalent
        // to alive_provided under the leaf invariant, but more honest about intent.
        resolve_impl(sym_eclass, bind_provided);
        resolve_impl(expr_eclass, bind_provided);
      } else {
        // Dead: only visit input — sym and expr are unreachable.
        // Erase any stale sym/expr entries from a prior alive resolution
        // (cascade alive->dead transition).
        resolved.erase(sym_eclass);
        resolved.erase(expr_eclass);
        resolve_impl(input_eclass, bind_provided);
      }
    }

    void resolve_impl(EClassId eclass_id, SymbolSet const &provided) {
      if (auto existing = resolved.find(eclass_id); existing != resolved.end()) {
        // DAG: this eclass was already resolved from a different parent.
        // Check if the cached selection is compatible with this parent's provided set.
        if (bind::IsCompatible(existing->second.required, provided)) return;  // still feasible
        // Incompatible: the cached alt demands symbols this parent doesn't provide.
        // Re-resolve with the more restrictive provided set, then cascade to children
        // so they are also re-resolved with the new context.
        auto it = frontier_map.find(eclass_id);
        assert(it != frontier_map.end() && it->second.has_value());
        auto const &chosen = pick_compatible(*it->second, provided);
        existing->second = ResolvedEntry{Selection{chosen.enode_id, chosen.cost}, chosen.required};
        // Cascade: visit children of the new selection so stale cached values are updated.
        // Bind nodes need special alive/dead child-visit logic.
        auto const &enode = egraph.get_enode(chosen.enode_id);
        if (enode.symbol() == symbol::Bind && enode.children().size() == 3) {
          visit_bind_children(enode.children()[0], enode.children()[1], enode.children()[2], provided);
        } else {
          for (auto child : enode.children()) {
            resolve_impl(child, provided);
          }
        }
        return;
      }

      auto it = frontier_map.find(eclass_id);
      assert(it != frontier_map.end() && it->second.has_value());

      auto const &frontier = *it->second;
      auto const &chosen = pick_compatible(frontier, provided);
      resolved[eclass_id] = ResolvedEntry{Selection{chosen.enode_id, chosen.cost}, chosen.required};

      auto const &enode = egraph.get_enode(chosen.enode_id);
      auto const &children = enode.children();

      if (enode.symbol() == symbol::Bind && children.size() == 3) {
        visit_bind_children(children[0], children[1], children[2], provided);
      } else {
        for (auto child : children) {
          resolve_impl(child, provided);
        }
      }
    }
  };
};

}  // namespace

// We can build operators or expressions
// Operators -> used once in the build
// Expression -> can be reused
using LogicalOperatorPtr = std::shared_ptr<LogicalOperator>;
using BuildResult = std::variant<LogicalOperatorPtr, Expression *, Symbol, NamedExpression *>;
using enode_ref = planner::core::ENode<symbol> const &;
using child_ref = std::reference_wrapper<BuildResult const>;
using children_ref = std::span<child_ref const>;

namespace {
template <typename T, std::size_t idx>
auto ExtractAndValidate(children_ref children) -> const T & {
  if (children.size() <= idx) throw QueryException{"Planner error, missing child node"};
  const auto *ptr = std::get_if<T>(&children[idx].get());
  if (!ptr) throw QueryException{"Planner error, child node is incorrect type"};
  return *ptr;
}

template <typename T>
auto Validate(child_ref child) -> const T & {
  const auto *ptr = std::get_if<T>(&child.get());
  if (!ptr) throw QueryException{"Planner error, child node is incorrect type"};
  return *ptr;
}
}  // namespace

struct Builder {
  auto Build(enode_ref node, children_ref children) -> BuildResult {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define X(SYM)      \
  case symbol::SYM: \
    return Build(utils::tag_v<symbol::SYM>, node, children)

    switch (node.symbol()) {
      X(Once);
      X(Bind);
      X(Symbol);
      X(Literal);
      X(Identifier);
      X(Output);
      X(NamedOutput);
      X(ParamLookup);
#define MG_DISPATCH_OP(Name, ...) X(Name);
      EGRAPH_BINARY_OPS(MG_DISPATCH_OP)
      EGRAPH_UNARY_OPS(MG_DISPATCH_OP)
#undef MG_DISPATCH_OP
    }
#undef X
  }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  auto Build(utils::tag_value<symbol::Once> /*tag*/, enode_ref /*node*/, children_ref /*children*/) -> BuildResult {
    return std::make_unique<Once>();
  }

  auto Build(utils::tag_value<symbol::Bind> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    auto const &input = ExtractAndValidate<LogicalOperatorPtr, 0>(children);
    auto const &sym = ExtractAndValidate<Symbol, 1>(children);
    auto const &expression = ExtractAndValidate<Expression *, 2>(children);

    // TODO/NOTE: lost token_position_, and is_aliased_
    auto *named_expression = ast_storage_.Create<NamedExpression>(sym.name(), expression);
    named_expression->MapTo(sym);

    if (input->GetTypeInfo() == Produce::kType) {
      auto const &produce = static_pointer_cast<Produce>(input);
      // TODO: check if its ok to steal from the other produce (Operators make a
      // tree, we are skipping hence unused)
      auto named_expressions = produce->named_expressions_;
      named_expressions.emplace_back(named_expression);
      return std::make_shared<Produce>(produce->input(), named_expressions);
    }
    return std::make_shared<Produce>(input, std::vector{named_expression});
  }

  auto Build(utils::tag_value<symbol::Symbol> /*tag*/, enode_ref node, children_ref /*children*/) -> BuildResult {
    auto const sym_pos = static_cast<int32_t>(node.disambiguator());
    auto const it = reverse_symbol_name_store_.find(sym_pos);
    if (it == reverse_symbol_name_store_.end()) [[unlikely]] {
      throw QueryException{"Planner error, symbol not found in store"};
    }
    // TODO/NOTE: lost user_declared_, type_, and token_position_
    return symbol_table_.CreateSymbol(it->second, false /*TODO*/);
  }

  auto Build(utils::tag_value<symbol::Literal> /*tag*/, enode_ref node, children_ref /*children*/) -> BuildResult {
    auto const dis = node.disambiguator();
    auto const it = reverse_literal_store_.find(dis);
    if (it == reverse_literal_store_.end()) [[unlikely]] {
      throw QueryException{"Planner error, literal not found in store"};
    }
    return ast_storage_.Create<PrimitiveLiteral>(it->second);
  }

  auto Build(utils::tag_value<symbol::Identifier> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    auto const &sym = ExtractAndValidate<Symbol, 0>(children);
    auto *identifier = ast_storage_.Create<Identifier>(sym.name(), sym.user_declared());
    identifier->MapTo(sym);
    return identifier;
  }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  auto Build(utils::tag_value<symbol::Output> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    auto const &input = ExtractAndValidate<LogicalOperatorPtr, 0>(children);
    auto named_expressions =
        children | std::views::drop(1) | std::views::transform(Validate<NamedExpression *>) | ranges::to<std::vector>;
    return std::make_shared<Produce>(input, std::move(named_expressions));
  }

  auto Build(utils::tag_value<symbol::NamedOutput> /*tag*/, enode_ref node, children_ref children) -> BuildResult {
    auto const &sym = ExtractAndValidate<Symbol, 0>(children);
    auto const &expression = ExtractAndValidate<Expression *, 1>(children);

    auto const name_it = reverse_name_store_.find(node.disambiguator());
    DMG_ASSERT(name_it != reverse_name_store_.end());

    // TODO/NOTE: lost token_position_, and is_aliased_
    auto *named_expression = ast_storage_.Create<NamedExpression>(name_it->second, expression);
    named_expression->MapTo(sym);
    return named_expression;
  }

  auto Build(utils::tag_value<symbol::ParamLookup> /*tag*/, enode_ref node, children_ref /*children*/) -> BuildResult {
    auto const dis = node.disambiguator();
    return ast_storage_.Create<ParameterLookup>(dis);
  }

  // Binary operator helpers
  template <typename AstOp>
  auto BuildBinaryOp(children_ref children) -> BuildResult {
    auto const &lhs = ExtractAndValidate<Expression *, 0>(children);
    auto const &rhs = ExtractAndValidate<Expression *, 1>(children);
    return ast_storage_.Create<AstOp>(lhs, rhs);
  }

  // Unary operator helpers
  template <typename AstOp>
  auto BuildUnaryOp(children_ref children) -> BuildResult {
    auto const &operand = ExtractAndValidate<Expression *, 0>(children);
    return ast_storage_.Create<AstOp>(operand);
  }

  // Binary / unary Build overloads — generated from the X-lists.
  // NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define MG_BUILD_BINARY(Name, AstOp)                                                                             \
  auto Build(utils::tag_value<symbol::Name> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult { \
    return BuildBinaryOp<AstOp>(children);                                                                       \
  }
  EGRAPH_BINARY_OPS(MG_BUILD_BINARY)
#undef MG_BUILD_BINARY

#define MG_BUILD_UNARY(Name, AstOp)                                                                              \
  auto Build(utils::tag_value<symbol::Name> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult { \
    return BuildUnaryOp<AstOp>(children);                                                                        \
  }
  EGRAPH_UNARY_OPS(MG_BUILD_UNARY)
#undef MG_BUILD_UNARY

  // NOLINTEND(cppcoreguidelines-macro-usage)

  Builder(std::map<storage::ExternalPropertyValue, uint64_t> const &literal_store,
          std::map<std::string, uint64_t> const &name_store, std::map<int32_t, std::string> const &symbol_name_store) {
    reverse_literal_store_.reserve(literal_store.size());
    for (auto const &[val, id] : literal_store) {
      reverse_literal_store_.emplace(id, val);
    }

    reverse_name_store_.reserve(name_store.size());
    for (auto const &[val, id] : name_store) {
      reverse_name_store_.emplace(id, val);
    }

    reverse_symbol_name_store_.reserve(symbol_name_store.size());
    for (auto const &[pos, name] : symbol_name_store) {
      reverse_symbol_name_store_.emplace(pos, name);
    }
  }

  boost::unordered_flat_map<uint64_t, storage::ExternalPropertyValue> reverse_literal_store_;
  boost::unordered_flat_map<uint64_t, std::string> reverse_name_store_;
  boost::unordered_flat_map<int32_t, std::string> reverse_symbol_name_store_;

  AstStorage ast_storage_;
  SymbolTable symbol_table_;
};

/// Convert an egraph (after rewrite saturation) into a concrete LogicalOperator tree.
///
/// Precondition: after all rewrites, every Identifier eclass must be merged with at least one
/// alternative whose `required == {}`. This is guaranteed when InlineRule has been applied to
/// saturation for all Bind/Identifier pairs in the egraph. Extensions that add new binding
/// forms (scan operators, UNWIND, etc.) must ensure a corresponding rewrite or direct enode
/// alternative satisfies this invariant.
///
/// If the invariant is violated, the function throws QueryException rather than invoking
/// undefined behaviour.
auto ConvertToLogicalOperator(egraph const &e, eclass root)
    -> std::tuple<std::unique_ptr<LogicalOperator>, double, AstStorage, SymbolTable> {
  auto const &impl = internal::get_impl(e);

  /// STAGE: Multi-alt extraction from EGraph using PlanCostModel
  auto const true_root = internal::to_core_id(root);
  namespace extract = planner::core::extract;

  // Root-satisfiability precondition: ComputeFrontiers must have produced at
  // least one self-contained alternative for the root (required == {}).
  // We compute frontiers eagerly here so we can validate before resolve;
  // the same frontier map is then handed to Extract via the ExtractionContext.
  // TODO(planner-v2): hold this context per session so buffers are reused
  // across queries (issue: ConvertToLogicalOperator is currently called once
  // per query with a fresh context).
  extract::ExtractionContext<CostFrontier> ctx;
  (void)extract::detail::ComputeFrontiers(impl.egraph_, PlanCostModel{}, true_root, ctx.frontier_map);

  auto const root_it = ctx.frontier_map.find(true_root);
  if (root_it == ctx.frontier_map.end() || !root_it->second.has_value()) {
    throw QueryException{"Plan extraction failed: root eclass has no frontier"};
  }
  auto const &root_frontier = *root_it->second;
  bool root_satisfiable =
      std::ranges::any_of(root_frontier.alts(), [](Alternative const &a) { return a.required.empty(); });
  if (!root_satisfiable) {
    throw QueryException{
        "Plan extraction failed: root frontier has no self-contained alternative. "
        "All paths through the plan demand symbols that cannot be provided. "
        "Ensure all Identifier references are resolved by rewrites before extraction."};
  }

  // Resolve + collect-deps + topo-sort.  ComputeFrontiers above already
  // populated ctx.frontier_map; Extract picks up there and runs the rest of
  // the pipeline.
  ctx.selection = PlanResolver{}(impl.egraph_, ctx.frontier_map, true_root);
  ctx.in_degree = extract::detail::CollectDependencies(impl.egraph_, ctx.selection, true_root);
  ctx.order = extract::detail::TopologicalSort(impl.egraph_, ctx.selection, std::move(ctx.in_degree));
  auto const &selection = ctx.order;

  /// STAGE: Build selected (LogicalOperator, Expression *, Symbol, NamedExpression *, etc)
  /// Dead Binds are already handled: PlanResolver skips sym/expr children
  /// for dead Binds, so they're absent from the selection. CollectDependencies
  /// only walks resolved children, so dead Bind's sym/expr never enter the topo sort.
  auto builder = Builder{impl.storage<symbol::Literal>().store,
                         impl.storage<symbol::NamedOutput>().store,
                         impl.storage<symbol::Symbol>().store};
  auto build_cache = boost::unordered_flat_map<planner::core::EClassId, BuildResult>{};
  auto const cache_lookup = [&](const planner::core::EClassId id) {
    auto const it = build_cache.find(id);
    DMG_ASSERT(it != build_cache.end(), "Building bottom up we should be able to find our child");
    return std::cref(it->second);
  };
  // Use reverse order to build from bottom up
  // This is so we can use the build_cache
  auto children_refs = std::vector<child_ref>{};
  for (auto [eclass_id, enode_id] : std::views::reverse(selection)) {
    auto const &enode = impl.egraph_.get_enode(enode_id);

    // Dead Bind detection: PlanResolver excludes sym/expr e-classes from the resolved
    // selection when it determines the Bind is dead (sym not needed downstream).
    // CollectDependencies skips children absent from the selection, so dead sym/expr
    // never enter the topo sort. TopologicalSort therefore never emits them, and the
    // builder loop never inserts them into build_cache.
    //
    // Invariant: sym (children()[1]) is in build_cache <-> Bind is alive.
    // Note: this is the *post-resolution observation* of aliveness — the resolver
    // has already decided alive vs dead via bind::IsAlive on the input alt's demand
    // set, and the topo-sort either includes or excludes sym based on that.  We
    // observe the result here rather than re-deciding.  Checking sym (not expr) is
    // consistent with the resolver's predicate which keys on the symbol being bound.
    if (enode.symbol() == symbol::Bind) {
      assert(enode.children().size() == 3 && "Bind must have exactly 3 children");
      if (!build_cache.contains(enode.children()[1])) {  // sym absent => dead Bind
        build_cache[eclass_id] = build_cache.at(enode.children()[0]);
        continue;
      }
    }

    children_refs.clear();
    children_refs.reserve(enode.children().size());
    std::ranges::copy(enode.children() | std::views::transform(cache_lookup), std::back_inserter(children_refs));
    build_cache[eclass_id] = builder.Build(enode, children_refs);
  }

  // STAGE: Get the built root as std::unique_ptr<LogicalOperator>
  auto *ptr = std::get_if<LogicalOperatorPtr>(&build_cache[true_root]);
  if (!ptr) throw QueryException{"Root should be LogicalOperator"};
  auto &result = *ptr;

  auto unique_result = result->Clone(&builder.ast_storage_);
  // TODO: return real cost from resolved root once Selection::cost type stabilises
  auto root_cost = 0.0;
  if (auto it = ctx.selection.find(true_root); it != ctx.selection.end()) root_cost = it->second.cost;
  return {std::move(unique_result), root_cost, std::move(builder.ast_storage_), std::move(builder.symbol_table_)};
}
}  // namespace memgraph::query::plan::v2
