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

#include "query/plan_v2/ast_converter.hpp"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"

using memgraph::query::plan::v2::egraph;

namespace memgraph::query {
namespace {
struct ast_converter_visitor : HierarchicalTreeVisitor {
  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::ReturnType;
  using HierarchicalTreeVisitor::Visit;

  ReturnType Visit(ParameterLookup &) override { return true; }
  ReturnType Visit(PrimitiveLiteral &) override { return true; }
  ReturnType Visit(Identifier &) override { return true; }
  ReturnType Visit(EnumValueAccess &) override { return true; }

  bool PreVisit(CypherQuery &tree) override {
    MG_ASSERT(tree.cypher_unions_.empty(), "Unions not implemented yet");
    tree.memory_limit_;          // TODO
    tree.memory_scale_;          // TODO
    tree.pre_query_directives_;  // TODO

    if (!tree.single_query_->Accept(*this)) return false;

    return true;
  }
  bool PostVisit(CypherQuery &) override { return true; }

  bool PreVisit(SingleQuery &tree) override {
    for (auto &clause : tree.clauses_) {
      if (!clause->Accept(*this)) return false;
    }

    return true;
  }
  bool PostVisit(SingleQuery &) override { return true; }

  bool PreVisit(With &tree) override {
    // TODO: reuse this for also RETURN
    auto const &body = tree.body_;

    // Produce all_identifiers, named_expressions
    // Distinct tree.body_.distinct;
    // OrderBy order_by(maybe empty)
    // Skip skip?
    // Limit limit?
    // Where

    // The body produces new bound identifiers
    // this is an optional where clause which is permitted by the Cypher grammar
    if (tree.where_) {
      if (!tree.where_->Accept(*this)) return false;
    }

    return true;
  }

  bool PreVisit(Where &tree) override { return true; }

  bool PostVisit(With &) override { return true; }

  bool PreVisit(CallSubquery &) override { return true; }
  bool PreVisit(Exists &) override { return true; }
  bool PreVisit(Foreach &) override { return true; }
  bool PreVisit(LoadCsv &) override { return true; }
  bool PreVisit(RegexMatch &) override { return true; }
  bool PreVisit(Unwind &) override { return true; }
  bool PreVisit(Merge &) override { return true; }
  bool PreVisit(RemoveLabels &) override { return true; }
  bool PreVisit(RemoveProperty &) override { return true; }
  bool PreVisit(SetLabels &) override { return true; }
  bool PreVisit(SetProperties &) override { return true; }
  bool PreVisit(SetProperty &) override { return true; }
  bool PreVisit(Delete &) override { return true; }
  bool PreVisit(EdgeAtom &) override { return true; }
  bool PreVisit(NodeAtom &) override { return true; }
  bool PreVisit(Pattern &) override { return true; }

  bool PreVisit(Return &) override { return true; }
  bool PreVisit(Match &) override { return true; }
  bool PreVisit(Create &) override { return true; }
  bool PreVisit(CallProcedure &) override { return true; }
  bool PreVisit(ListComprehension &) override { return true; }
  bool PreVisit(None &) override { return true; }
  bool PreVisit(Any &) override { return true; }
  bool PreVisit(Single &) override { return true; }
  bool PreVisit(All &) override { return true; }
  bool PreVisit(Extract &) override { return true; }
  bool PreVisit(Coalesce &) override { return true; }
  bool PreVisit(Reduce &) override { return true; }
  bool PreVisit(Function &) override { return true; }
  bool PreVisit(Aggregation &) override { return true; }
  bool PreVisit(LabelsTest &) override { return true; }
  bool PreVisit(AllPropertiesLookup &) override { return true; }
  bool PreVisit(PropertyLookup &) override { return true; }
  bool PreVisit(MapProjectionLiteral &) override { return true; }
  bool PreVisit(MapLiteral &) override { return true; }
  bool PreVisit(ListLiteral &) override { return true; }
  bool PreVisit(IsNullOperator &) override { return true; }
  bool PreVisit(UnaryMinusOperator &) override { return true; }
  bool PreVisit(UnaryPlusOperator &) override { return true; }
  bool PreVisit(IfOperator &) override { return true; }
  bool PreVisit(ListSlicingOperator &) override { return true; }
  bool PreVisit(SubscriptOperator &) override { return true; }
  bool PreVisit(InListOperator &) override { return true; }
  bool PreVisit(RangeOperator &) override { return true; }
  bool PreVisit(GreaterEqualOperator &) override { return true; }
  bool PreVisit(LessEqualOperator &) override { return true; }
  bool PreVisit(GreaterOperator &) override { return true; }
  bool PreVisit(LessOperator &) override { return true; }
  bool PreVisit(EqualOperator &) override { return true; }
  bool PreVisit(NotEqualOperator &) override { return true; }
  bool PreVisit(ExponentiationOperator &) override { return true; }
  bool PreVisit(ModOperator &) override { return true; }
  bool PreVisit(DivisionOperator &) override { return true; }
  bool PreVisit(MultiplicationOperator &) override { return true; }
  bool PreVisit(SubtractionOperator &) override { return true; }
  bool PreVisit(AdditionOperator &) override { return true; }
  bool PreVisit(NotOperator &) override { return true; }
  bool PreVisit(AndOperator &) override { return true; }
  bool PreVisit(XorOperator &) override { return true; }
  bool PreVisit(OrOperator &) override { return true; }
  bool PreVisit(NamedExpression &) override { return true; }
  bool PreVisit(CypherUnion &) override { return true; }

  bool PostVisit(CallSubquery &) override { return true; }
  bool PostVisit(Exists &) override { return true; }
  bool PostVisit(Foreach &) override { return true; }
  bool PostVisit(LoadCsv &) override { return true; }
  bool PostVisit(RegexMatch &) override { return true; }
  bool PostVisit(Unwind &) override { return true; }
  bool PostVisit(Merge &) override { return true; }
  bool PostVisit(RemoveLabels &) override { return true; }
  bool PostVisit(RemoveProperty &) override { return true; }
  bool PostVisit(SetLabels &) override { return true; }
  bool PostVisit(SetProperties &) override { return true; }
  bool PostVisit(SetProperty &) override { return true; }
  bool PostVisit(Where &) override { return true; }
  bool PostVisit(Delete &) override { return true; }
  bool PostVisit(EdgeAtom &) override { return true; }
  bool PostVisit(NodeAtom &) override { return true; }
  bool PostVisit(Pattern &) override { return true; }

  bool PostVisit(Return &) override { return true; }
  bool PostVisit(Match &) override { return true; }
  bool PostVisit(Create &) override { return true; }
  bool PostVisit(CallProcedure &) override { return true; }
  bool PostVisit(ListComprehension &) override { return true; }
  bool PostVisit(None &) override { return true; }
  bool PostVisit(Any &) override { return true; }
  bool PostVisit(Single &) override { return true; }
  bool PostVisit(All &) override { return true; }
  bool PostVisit(Extract &) override { return true; }
  bool PostVisit(Coalesce &) override { return true; }
  bool PostVisit(Reduce &) override { return true; }
  bool PostVisit(Function &) override { return true; }
  bool PostVisit(Aggregation &) override { return true; }
  bool PostVisit(LabelsTest &) override { return true; }
  bool PostVisit(AllPropertiesLookup &) override { return true; }
  bool PostVisit(PropertyLookup &) override { return true; }
  bool PostVisit(MapProjectionLiteral &) override { return true; }
  bool PostVisit(MapLiteral &) override { return true; }
  bool PostVisit(ListLiteral &) override { return true; }
  bool PostVisit(IsNullOperator &) override { return true; }
  bool PostVisit(UnaryMinusOperator &) override { return true; }
  bool PostVisit(UnaryPlusOperator &) override { return true; }
  bool PostVisit(IfOperator &) override { return true; }
  bool PostVisit(ListSlicingOperator &) override { return true; }
  bool PostVisit(SubscriptOperator &) override { return true; }
  bool PostVisit(InListOperator &) override { return true; }
  bool PostVisit(RangeOperator &) override { return true; }
  bool PostVisit(GreaterEqualOperator &) override { return true; }
  bool PostVisit(LessEqualOperator &) override { return true; }
  bool PostVisit(GreaterOperator &) override { return true; }
  bool PostVisit(LessOperator &) override { return true; }
  bool PostVisit(EqualOperator &) override { return true; }
  bool PostVisit(NotEqualOperator &) override { return true; }
  bool PostVisit(ExponentiationOperator &) override { return true; }
  bool PostVisit(ModOperator &) override { return true; }
  bool PostVisit(DivisionOperator &) override { return true; }
  bool PostVisit(MultiplicationOperator &) override { return true; }
  bool PostVisit(SubtractionOperator &) override { return true; }
  bool PostVisit(AdditionOperator &) override { return true; }
  bool PostVisit(NotOperator &) override { return true; }
  bool PostVisit(AndOperator &) override { return true; }
  bool PostVisit(XorOperator &) override { return true; }
  bool PostVisit(OrOperator &) override { return true; }
  bool PostVisit(NamedExpression &) override { return true; }
  bool PostVisit(CypherUnion &) override { return true; }

  bool PreVisit(PatternComprehension &) override { return true; }
  bool PostVisit(PatternComprehension &) override { return true; }

  auto GetEgraph() && -> egraph { return std::move(egraph_); }

 private:
  egraph egraph_;
};
}  // namespace
}  // namespace memgraph::query

namespace memgraph::query::plan::v2 {

auto ConvertToEgraph(CypherQuery const &query, SymbolTable const &symbol_table) -> egraph {
  auto visitor = ast_converter_visitor{};
  // TODO: fix HierarchicalTreeVisitor to allow const
  const_cast<CypherQuery &>(query).Accept(visitor);

  return egraph{};
}

}  // namespace memgraph::query::plan::v2
