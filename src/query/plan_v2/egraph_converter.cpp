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

#include <boost/container/flat_set.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>

#include "planner/extract/extractor.hpp"
#include "planner/extract/pareto_frontier.hpp"
#include "query/plan/operator.hpp"
#include "query/plan_v2/egraph_internal.hpp"
#include "utils/tag.hpp"

namespace memgraph::query::plan::v2 {

// ============================================================================
// Plan extraction cost model — Pareto frontier with symbol demand tracking
// ============================================================================
namespace {

using SymbolSet = boost::container::flat_set<planner::core::EClassId>;

struct Alternative {
  double cost;
  SymbolSet required;               // Symbols that MUST be bound by ancestors
  planner::core::ENodeId enode_id;  // Which enode achieves this alternative

  auto dominated_by(Alternative const &other) const -> bool {
    return other.cost <= cost && std::ranges::includes(required, other.required);
  }
};

struct AlternativeDominance {
  auto operator()(Alternative const &a, Alternative const &b) const -> bool { return a.dominated_by(b); }
};

using CostFrontier = planner::core::extract::ParetoFrontier<Alternative, AlternativeDominance>;

// Convenience: combine two frontiers with cost summation and required-set union.
auto CombineAlts(double extra_cost, planner::core::ENodeId enode_id) {
  return [extra_cost, enode_id](Alternative const &l, Alternative const &r) -> Alternative {
    auto required = l.required;
    required.insert(r.required.begin(), r.required.end());
    return {.cost = extra_cost + l.cost + r.cost, .required = std::move(required), .enode_id = enode_id};
  };
}

/// Find the cheapest alternative in a frontier. Returns nullptr if empty.
[[nodiscard]] auto best_alt(CostFrontier const &f) -> Alternative const * {
  auto it = std::ranges::min_element(f.alts, {}, &Alternative::cost);
  return it != f.alts.end() ? &*it : nullptr;
}

[[nodiscard]] auto min_cost(CostFrontier const &f) -> double {
  auto const *alt = best_alt(f);
  return alt ? alt->cost : std::numeric_limits<double>::infinity();
}

struct PlanCostModel {
  // Tag auto-detected: CostResult is a ParetoFrontier → pareto_frontier_tag
  // merge auto-provided: ParetoFrontier::merge (union + prune)
  using CostResult = CostFrontier;

  auto operator()(planner::core::ENode<symbol> const &current, planner::core::ENodeId enode_id,
                  std::span<CostResult const> children) const -> CostResult {
    switch (current.symbol()) {
      // Leaf nodes: single alternative, no demand
      case symbol::Once:
      case symbol::Literal:
      case symbol::Symbol:
      case symbol::ParamLookup:
        return CostResult{{{.cost = 1.0, .required = {}, .enode_id = enode_id}}};

      // Identifier: demands its symbol child to be bound
      case symbol::Identifier: {
        auto sym_eclass = current.children()[0];
        auto child_cost = children.empty() ? 0.0 : min_cost(children[0]);
        return CostResult{{{.cost = 1.0 + child_cost, .required = {sym_eclass}, .enode_id = enode_id}}};
      }

      // Bind: alive if sym demanded, dead otherwise
      // N.B. The alive/dead logic here is mirrored in ResolvePlanSelection's Bind branch.
      case symbol::Bind: {
        auto const &input_frontier = children[0];
        auto const &sym_frontier = children[1];
        auto const &expr_frontier = children[2];
        auto sym_eclass = current.children()[1];
        auto sym_cost = min_cost(sym_frontier);

        return CostFrontier::flat_map(input_frontier, [&](auto const &input_alt, auto emit) {
          if (input_alt.required.contains(sym_eclass)) {
            // Alive: sym is needed — must pay sym+expr costs
            for (auto const &expr_alt : expr_frontier.alts) {
              auto required = input_alt.required;
              required.erase(sym_eclass);
              required.insert(expr_alt.required.begin(), expr_alt.required.end());
              emit({.cost = input_alt.cost + sym_cost + expr_alt.cost,
                    .required = std::move(required),
                    .enode_id = enode_id});
            }
          } else {
            // Dead: sym not needed — skip sym+expr cost entirely
            emit({.cost = input_alt.cost, .required = input_alt.required, .enode_id = enode_id});
          }
        });
      }

      // Binary operators: combine lhs × rhs + 1
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
      case symbol::Xor:
        return CostFrontier::combine(children[0], children[1], CombineAlts(1.0, enode_id));

      // Unary operators: pass through child + 1
      case symbol::Not:
      case symbol::UnaryMinus:
      case symbol::UnaryPlus: {
        auto result = children[0];
        for (auto &alt : result.alts) {
          alt.cost += 1.0;
          alt.enode_id = enode_id;
        }
        return result;
      }

      // Output: fold input × named_output₁ × named_output₂ × ...
      case symbol::Output: {
        auto result = children[0];
        for (auto &alt : result.alts) alt.enode_id = enode_id;
        for (size_t i = 1; i < children.size(); ++i) {
          result = CostFrontier::combine(result, children[i], CombineAlts(0.0, enode_id));
        }
        return result;
      }

      // NamedOutput: combine sym × expr + 1
      case symbol::NamedOutput:
        return CostFrontier::combine(children[0], children[1], CombineAlts(1.0, enode_id));
    }
    __builtin_unreachable();
  }

  static auto min_cost(CostResult const &f) -> double { return ::memgraph::query::plan::v2::min_cost(f); }

  static auto resolve(CostResult const &frontier) -> planner::core::ENodeId {
    auto const *alt = best_alt(frontier);
    assert(alt && "resolve called on empty frontier");
    return alt->enode_id;
  }
};

/// Context-aware top-down resolution of demand frontiers.
/// Propagates a "provided" set (symbols bound by Bind ancestors) to ensure
/// child selections are consistent with parent alive/dead decisions.
auto ResolvePlanSelection(planner::core::EGraph<symbol, analysis> const &egraph,
                          std::unordered_map<planner::core::EClassId,
                                             planner::core::extract::EClassFrontier<CostFrontier>> const &frontier_map,
                          planner::core::EClassId root) {
  using Traits = planner::core::extract::CostModelTraits<PlanCostModel>;
  using EClassId = planner::core::EClassId;
  using Selection = planner::core::extract::Selection<Traits::CostType>;

  auto resolved = std::unordered_map<EClassId, Selection>{};
  // Track the required set of each resolved eclass for DAG compatibility checks.
  auto resolved_required = std::unordered_map<EClassId, SymbolSet>{};

  // Pick cheapest alt whose required ⊆ provided.
  auto pick_compatible = [](CostFrontier const &frontier, SymbolSet const &provided) -> Alternative const & {
    Alternative const *best = nullptr;
    for (auto const &alt : frontier.alts) {
      if (std::ranges::includes(provided, alt.required)) {
        if (!best || alt.cost < best->cost) {
          best = &alt;
        }
      }
    }
    if (!best) {
      // Fallback: shouldn't happen with correct bottom-up computation
      best = best_alt(frontier);
      assert(best && "pick_compatible called on empty frontier");
    }
    return *best;
  };

  auto resolve_recursive = [&](this auto const &self, EClassId eclass_id, SymbolSet const &provided) -> void {
    if (auto existing = resolved.find(eclass_id); existing != resolved.end()) {
      // DAG: this eclass was already resolved from a different parent.
      // Check if the cached selection is compatible with this parent's provided set.
      assert(resolved_required.contains(eclass_id) && "resolved and resolved_required must be written together");
      if (std::ranges::includes(provided, resolved_required.at(eclass_id))) return;  // still feasible
      // Incompatible: the cached alt demands symbols this parent doesn't provide.
      // Re-resolve with the more restrictive provided set, then cascade to children
      // so they are also re-resolved with the new context.
      auto it = frontier_map.find(eclass_id);
      assert(it != frontier_map.end() && it->second.has_value());
      auto const &chosen = pick_compatible(*it->second, provided);
      existing->second = Selection{chosen.enode_id, chosen.cost};
      resolved_required[eclass_id] = chosen.required;
      // Cascade: visit children of the new selection so stale cached values are updated.
      auto const &enode = egraph.get_enode(chosen.enode_id);
      for (auto child : enode.children()) {
        self(child, provided);
      }
      return;
    }

    auto it = frontier_map.find(eclass_id);
    assert(it != frontier_map.end() && it->second.has_value());

    auto const &frontier = *it->second;
    auto const &chosen = pick_compatible(frontier, provided);
    resolved[eclass_id] = Selection{chosen.enode_id, chosen.cost};
    resolved_required[eclass_id] = chosen.required;

    auto const &enode = egraph.get_enode(chosen.enode_id);
    auto const &children = enode.children();

    if (enode.symbol() == symbol::Bind && children.size() == 3) {
      // N.B. The alive/dead logic here mirrors PlanCostModel::operator() for symbol::Bind.
      auto input_eclass = children[0];
      auto sym_eclass = children[1];
      auto expr_eclass = children[2];

      // Re-derive alive/dead given the provided context
      auto input_it = frontier_map.find(input_eclass);
      assert(input_it != frontier_map.end() && input_it->second.has_value());
      auto const &input_frontier = *input_it->second;

      auto sym_it = frontier_map.find(sym_eclass);
      assert(sym_it != frontier_map.end() && sym_it->second.has_value());
      auto sym_cost = min_cost(*sym_it->second);

      auto expr_it = frontier_map.find(expr_eclass);
      assert(expr_it != frontier_map.end() && expr_it->second.has_value());
      auto const &expr_frontier = *expr_it->second;

      // Alive: input alt must have sym in required, required ⊆ provided ∪ {sym}
      auto alive_provided = provided;
      alive_provided.insert(sym_eclass);

      auto best_alive_cost = std::numeric_limits<double>::infinity();
      for (auto const &input_alt : input_frontier.alts) {
        if (input_alt.required.contains(sym_eclass) && std::ranges::includes(alive_provided, input_alt.required)) {
          for (auto const &expr_alt : expr_frontier.alts) {
            if (std::ranges::includes(provided, expr_alt.required)) {
              best_alive_cost = std::min(best_alive_cost, input_alt.cost + sym_cost + expr_alt.cost);
            }
          }
        }
      }

      // Dead: input alt must NOT have sym in required, required ⊆ provided
      auto best_dead_cost = std::numeric_limits<double>::infinity();
      for (auto const &input_alt : input_frontier.alts) {
        if (!input_alt.required.contains(sym_eclass) && std::ranges::includes(provided, input_alt.required)) {
          best_dead_cost = std::min(best_dead_cost, input_alt.cost);
        }
      }

      // Alive wins if strictly cheaper; ties go to dead (less work)
      if (best_alive_cost < best_dead_cost) {
        // Alive: visit all three children (input with extra sym provided, sym, expr)
        self(input_eclass, alive_provided);
        self(sym_eclass, provided);
        self(expr_eclass, provided);
      } else {
        // Dead: only visit input — sym and expr are unreachable
        self(input_eclass, provided);
      }
    } else {
      for (auto child : children) {
        self(child, provided);
      }
    }
  };

  resolve_recursive(root, SymbolSet{});
  return resolved;
}

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
      X(Add);
      X(Sub);
      X(Mul);
      X(Div);
      X(Mod);
      X(Exp);
      X(Eq);
      X(Neq);
      X(Lt);
      X(Lte);
      X(Gt);
      X(Gte);
      X(And);
      X(Or);
      X(Xor);
      X(Not);
      X(UnaryMinus);
      X(UnaryPlus);
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

  // Arithmetic operators
  auto Build(utils::tag_value<symbol::Add> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<AdditionOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Sub> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<SubtractionOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Mul> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<MultiplicationOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Div> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<DivisionOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Mod> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<ModOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Exp> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<ExponentiationOperator>(children);
  }

  // Comparison operators
  auto Build(utils::tag_value<symbol::Eq> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<EqualOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Neq> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<NotEqualOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Lt> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<LessOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Lte> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<LessEqualOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Gt> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<GreaterOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Gte> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<GreaterEqualOperator>(children);
  }

  // Boolean operators
  auto Build(utils::tag_value<symbol::And> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<AndOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Or> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<OrOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Xor> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildBinaryOp<XorOperator>(children);
  }

  auto Build(utils::tag_value<symbol::Not> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildUnaryOp<NotOperator>(children);
  }

  // Unary operators
  auto Build(utils::tag_value<symbol::UnaryMinus> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildUnaryOp<UnaryMinusOperator>(children);
  }

  auto Build(utils::tag_value<symbol::UnaryPlus> /*tag*/, enode_ref /*node*/, children_ref children) -> BuildResult {
    return BuildUnaryOp<UnaryPlusOperator>(children);
  }

  Builder(std::map<storage::ExternalPropertyValue, uint64_t> const &literal_store,
          std::map<std::string, uint64_t> const &name_store, std::map<int32_t, std::string> const &symbol_name_store)
      : literal_store_(literal_store), name_store_(name_store), symbol_name_store_(symbol_name_store) {
    for (auto const &[val, id] : literal_store_.get()) {
      reverse_literal_store_[id] = val;
    }

    for (auto const &[val, id] : name_store_.get()) {
      reverse_name_store_[id] = val;
    }

    for (auto const &[pos, name] : symbol_name_store_.get()) {
      reverse_symbol_name_store_[pos] = name;
    }
  }

  std::reference_wrapper<std::map<storage::ExternalPropertyValue, uint64_t> const> literal_store_;
  std::map<uint64_t, storage::ExternalPropertyValue> reverse_literal_store_;
  std::reference_wrapper<std::map<std::string, uint64_t> const> name_store_;
  std::map<uint64_t, std::string> reverse_name_store_;
  std::reference_wrapper<std::map<int32_t, std::string> const> symbol_name_store_;
  std::map<int32_t, std::string> reverse_symbol_name_store_;

  AstStorage ast_storage_;
  SymbolTable symbol_table_;
};

auto ConvertToLogicalOperator(egraph const &e, eclass root)
    -> std::tuple<std::unique_ptr<LogicalOperator>, double, AstStorage, SymbolTable> {
  auto const &impl = internal::get_impl(e);

  /// STAGE: Multi-alt extraction from EGraph using PlanCostModel
  auto const true_root = internal::to_core_id(root);
  namespace extract = planner::core::extract;
  auto frontier_map = std::unordered_map<planner::core::EClassId, extract::EClassFrontier<PlanCostModel::CostResult>>{};
  extract::ComputeFrontiers(impl.egraph_, PlanCostModel{}, true_root, frontier_map);
  auto resolved = ResolvePlanSelection(impl.egraph_, frontier_map, true_root);
  auto in_degree = extract::CollectDependencies(impl.egraph_, resolved, true_root);
  auto const selection = extract::TopologicalSort(impl.egraph_, resolved, std::move(in_degree));

  /// STAGE: Build selected (LogicalOperator, Expression *, Symbol, NamedExpression *, etc)
  /// Dead Binds are already handled: ResolvePlanSelection skips sym/expr children
  /// for dead Binds, so they're absent from the selection. CollectDependencies
  /// only walks resolved children, so dead Bind's sym/expr never enter the topo sort.
  auto builder = Builder{impl.storage<symbol::Literal>().store,
                         impl.storage<symbol::NamedOutput>().store,
                         impl.storage<symbol::Symbol>().store};
  auto build_cache = std::map<planner::core::EClassId, BuildResult>{};
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

    // Dead Bind: sym/expr children were not resolved (skipped by ResolvePlanSelection).
    // Pass through the input operator directly.
    if (enode.symbol() == symbol::Bind && !build_cache.contains(enode.children()[1])) {
      build_cache[eclass_id] = build_cache.at(enode.children()[0]);
      continue;
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

  // TODO: make the rest of query plan root? use a shared_ptr (hence avoid this clone)
  auto unique_result = result->Clone(&builder.ast_storage_);
  return {std::move(unique_result), 0.0, std::move(builder.ast_storage_), std::move(builder.symbol_table_)};

  // egraph extraction -> subgraph of the egraph (one Enode per EClass)
  // start for a root
  // must be able to handle cycles -> Extraction should have no cycles (cycles
  // only useful to aid rewrites)

  // Subgraph -> LogicalOperator + AST Expressions + Symbol table
  // NOTE: we lost a NamedExpressions name when we did BIND for WITH
  //       that shoudl be tracked for the enode (not the disambiguator)
  //       so we can use it again when

  // WITH 1 AS X, 1 AS Y WITH Y AS RES

  //  (BIND (0) input_1 (SYM 0) (LITERAL 1))
  //  (BIND (1) input_2 (SYM 1) (LITERAL 1))
  //  (IDENT (SYM 1))

  // $a=(IDENT X), (BIND _ X $b) -> MERGE $a, $b
}
}  // namespace memgraph::query::plan::v2
