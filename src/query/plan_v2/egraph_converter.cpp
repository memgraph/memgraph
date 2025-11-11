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
using ChildThing = std::variant<std::unique_ptr<LogicalOperator>, Expression *, Symbol>;

struct Builder {
  using enode_ref = planner::core::ENode<symbol> const &;
  using children_ref = std::span<std::reference_wrapper<ChildThing const> const>;

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

  auto Build(utils::tag_value<symbol::Once>, enode_ref node, children_ref children) -> ChildThing {
    return std::make_unique<Once>();
  }
  auto Build(utils::tag_value<symbol::Bind>, enode_ref node, children_ref children) -> ChildThing { throw 1; }
  auto Build(utils::tag_value<symbol::Symbol>, enode_ref node, children_ref children) -> ChildThing {
    //      : name_(std::move(name)), <-- preserve
    // position_(position), <-- generated
    // user_declared_(user_declared), <-- preserve
    // type_(type), <-- Analysis/preserved
    // token_position_(token_position) {} <-- preserved
    // TODO: do we also need to gether info for our frame?

    return Symbol{};
  }
  auto Build(utils::tag_value<symbol::Literal>, enode_ref node, children_ref children) -> ChildThing { throw 1; }
  auto Build(utils::tag_value<symbol::Identifier>, enode_ref node, children_ref children) -> ChildThing { throw 1; }
  auto Build(utils::tag_value<symbol::Output>, enode_ref node, children_ref children) -> ChildThing { throw 1; }
  auto Build(utils::tag_value<symbol::NamedOutput>, enode_ref node, children_ref children) -> ChildThing { throw 1; }
  auto Build(utils::tag_value<symbol::ParamLookup>, enode_ref node, children_ref children) -> ChildThing { throw 1; }

  Builder(std::map<storage::ExternalPropertyValue, uint64_t> const &literal_store,
          std::map<std::string, uint64_t> const &name_store)
      : literal_store_(literal_store), name_store_(name_store) {}

  std::map<storage::ExternalPropertyValue, uint64_t> const &literal_store_;
  std::map<std::string, uint64_t> const &name_store_;
};
auto ConvertToLogicalOperator(egraph const &e, eclass root) -> std::tuple<std::unique_ptr<LogicalOperator>, double> {
  // Access the internal egraph through the accessor
  auto const &internal_egraph = internal::get_egraph(e);
  // TODO: Get pimpl we need access to proper versions of:
  std::map<storage::ExternalPropertyValue, uint64_t> literal_store_;
  std::map<std::string, uint64_t> name_store_;

  auto extractor = planner::core::Extractor{internal_egraph, CostModel{}};
  auto true_root = internal::to_core_id(root);
  auto thing = extractor.Extract(true_root);

  // change the order to bottom up
  std::ranges::reverse(thing);

  auto builder = Builder{literal_store_, name_store_};

  auto dynamic_programming_cache = std::map<planner::core::EClassId, ChildThing>{};

  for (auto [eclass_id, enode_id] : thing) {
    auto const &enode = internal_egraph.get_enode(enode_id);
    // build enode_id
    auto xx =
        enode.children() | std::views::transform([&](const planner::core::EClassId id) {
          const auto it = dynamic_programming_cache.find(id);
          DMG_ASSERT(it != dynamic_programming_cache.end(), "Building bottom up we should be able to find our child");
          return std::cref(it->second);
        }) |
        ranges::to<std::vector>;

    // track
    dynamic_programming_cache[eclass_id] = builder.Build(enode, xx);
  }

  dynamic_programming_cache[true_root];

  // TODO: build LogicalOperator

  // egraph extraction -> subgraph of the egraph (one Enode per EClass)
  // start for a root
  // must be able to handle cycles -> Extraction should have no cycles (cycles only useful to aid rewrites)

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
