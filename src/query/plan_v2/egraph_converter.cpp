// Copyright 2025 Memgraph Ltd.
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

#include "planner/core/extractor.hpp"
#include "query/plan/operator.hpp"
#include "query/plan_v2/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

struct CostModel {
  struct CostResult {
    double cost{};
    double cardinality{};

    friend auto operator<(CostResult const &lhs, CostResult const &rhs) { return lhs.cost < rhs.cost; }
  };

  static auto operator()(planner::core::ENode<symbol> const &current, std::span<CostResult const> children)
      -> CostResult {
    // TODO: build a better cost calculator
    auto children_sum =
        std::ranges::fold_left(children, 0.0, [](double acc, CostResult const &val) { return acc + val.cost; });
    return {1.0 + children_sum, 1.0};
  }
};

// TODO: make a concept to check cost model is valid
//  static_assert(some_concept<CostModel>);

// We can build operators or expressions
// Operators -> used once in the build
// Expression -> can be reused
using LogicalOperatorPtr = std::shared_ptr<LogicalOperator>;
using ChildThing = std::variant<LogicalOperatorPtr, Expression *, Symbol, NamedExpression *>;
using enode_ref = planner::core::ENode<symbol> const &;
using child_ref = std::reference_wrapper<ChildThing const>;
using children_ref = std::span<child_ref const>;

template <typename T, std::size_t idx>
auto ExtractAndValidate(children_ref children) -> const T & {
  if (children.size() <= idx) throw QueryException{"Planner error, missing child node"};
  auto ptr = std::get_if<T>(&children[idx].get());
  if (!ptr) throw QueryException{"Planner error, child node is incorrect type"};
  return *ptr;
}

template <typename T>
auto Validate(child_ref child) -> const T & {
  auto ptr = std::get_if<T>(&child.get());
  if (!ptr) throw QueryException{"Planner error, child node is incorrect type"};
  return *ptr;
}

struct Builder {
  auto Build(enode_ref node, children_ref children) -> ChildThing {
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
    }
#undef X
  }

  auto Build(utils::tag_value<symbol::Once>, enode_ref /*node*/, children_ref /*children*/) -> ChildThing {
    return std::make_unique<Once>();
  }

  auto Build(utils::tag_value<symbol::Bind>, enode_ref /*node*/, children_ref children) -> ChildThing {
    auto const &input = ExtractAndValidate<LogicalOperatorPtr, 0>(children);
    auto const &sym = ExtractAndValidate<Symbol, 1>(children);
    auto const &expression = ExtractAndValidate<Expression *, 2>(children);

    // TODO/NOTE: lost token_position_, and is_aliased_
    auto named_expression = ast_storage_.Create<NamedExpression>(sym.name(), expression);
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

  auto Build(utils::tag_value<symbol::Symbol>, enode_ref /*node*/, children_ref /*children*/) -> ChildThing {
    auto const frame_position = next_frame_position_++;
    // TODO/NOTE: lost user_declared_, type_, and token_position_
    return Symbol{"TODO", frame_position, false /*TODO*/};
  }

  auto Build(utils::tag_value<symbol::Literal>, enode_ref node, children_ref children) -> ChildThing {
    auto const dis = node.disambiguator();
    auto const it = reverse_literal_store_.find(dis);
    if (it == reverse_literal_store_.end()) [[unlikely]] {
      throw QueryException{"Planner error, child node is incorrect type"};
    }
    return ast_storage_.Create<PrimitiveLiteral>(it->second);
  }

  auto Build(utils::tag_value<symbol::Identifier>, enode_ref node, children_ref children) -> ChildThing {
    auto const &sym = ExtractAndValidate<Symbol, 0>(children);
    auto identifier = ast_storage_.Create<Identifier>(sym.name(), sym.user_declared());
    identifier->MapTo(sym);
    return identifier;
  }

  auto Build(utils::tag_value<symbol::Output>, enode_ref node, children_ref children) -> ChildThing {
    auto const &input = ExtractAndValidate<LogicalOperatorPtr, 0>(children);
    auto named_expressions =
        children | std::views::drop(1) | std::views::transform(Validate<NamedExpression *>) | ranges::to<std::vector>;
    return std::make_shared<Produce>(input, std::move(named_expressions));
  }

  auto Build(utils::tag_value<symbol::NamedOutput>, enode_ref node, children_ref children) -> ChildThing {
    auto const &sym = ExtractAndValidate<Symbol, 0>(children);
    auto const &expression = ExtractAndValidate<Expression *, 1>(children);

    // TODO/NOTE: lost token_position_, and is_aliased_
    auto named_expression = ast_storage_.Create<NamedExpression>(sym.name(), expression);
    named_expression->MapTo(sym);
    return named_expression;
  }

  auto Build(utils::tag_value<symbol::ParamLookup>, enode_ref node, children_ref children) -> ChildThing {
    auto const dis = node.disambiguator();
    return ast_storage_.Create<ParameterLookup>(dis);
  }

  Builder(std::map<storage::ExternalPropertyValue, uint64_t> const &literal_store,
          std::map<std::string, uint64_t> const &name_store)
      : literal_store_(literal_store), name_store_(name_store) {}

  std::map<storage::ExternalPropertyValue, uint64_t> const &literal_store_;
  std::map<uint64_t, storage::ExternalPropertyValue> reverse_literal_store_;  // TODO make the reverse mapping
  std::map<std::string, uint64_t> const &name_store_;

  AstStorage ast_storage_{};
  int next_frame_position_ = 0;
};

auto ConvertToLogicalOperator(egraph const &e, eclass root) -> std::tuple<std::unique_ptr<LogicalOperator>, double> {
  auto const &impl = internal::get_impl(e);
  // Access the internal egraph through the accessor
  auto const &internal_egraph = impl.egraph_;
  auto const &literal_store_ = impl.literal_store_;
  auto const &name_store_ = impl.name_store_;

  auto extractor = planner::core::Extractor{internal_egraph, CostModel{}};
  auto true_root = internal::to_core_id(root);
  auto thing = extractor.Extract(true_root);

  // change the order to bottom up
  std::ranges::reverse(thing);

  auto builder = Builder{literal_store_, name_store_};

  auto dynamic_programming_cache = std::map<planner::core::EClassId, ChildThing>{};
  auto const cache_lookup = [&](const planner::core::EClassId id) {
    auto const it = dynamic_programming_cache.find(id);
    DMG_ASSERT(it != dynamic_programming_cache.end(), "Building bottom up we should be able to find our child");
    return std::cref(it->second);
  };
  for (auto [eclass_id, enode_id] : thing) {
    auto const &enode = internal_egraph.get_enode(enode_id);
    auto xx = enode.children() | std::views::transform(cache_lookup) | ranges::to<std::vector>;
    dynamic_programming_cache[eclass_id] = builder.Build(enode, xx);
  }

  auto result = dynamic_programming_cache[true_root];

  // TODO: build LogicalOperator

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

  return {nullptr, 0.0};
}
}  // namespace memgraph::query::plan::v2
