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
struct AstConverterVisitor : HierarchicalTreeVisitor {
  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::ReturnType;
  using HierarchicalTreeVisitor::Visit;

  ReturnType Visit(ParameterLookup &expr) override {
    // NOTE: with the stripper it mans that literals are also parameters
    //       hence we can't distinguish between literals vs parameters
    //       we will not do any optimisations around ParameterLookup at plan time (because we still need to cache)
    builder_stack_.emplace_back(egraph_.MakeParameterLookup(expr.token_position_));
    return true;
  }
  ReturnType Visit(PrimitiveLiteral &expr) override {
    builder_stack_.emplace_back(egraph_.MakeLiteral(expr.value_));
    return true;
  }
  ReturnType Visit(Identifier &expr) override {
    auto sym = egraph_.MakeSymbol(expr.symbol_pos_);
    builder_stack_.emplace_back(egraph_.MakeIdentifier(sym));
    return true;
  }
  ReturnType Visit(EnumValueAccess &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(CypherQuery &op) override {
    MG_ASSERT(op.memory_limit_ == nullptr, "Memory limit not implemented yet");
    (void)op.memory_scale_;  // TODO
    MG_ASSERT(op.pre_query_directives_.commit_frequency_ == nullptr, "Commit frequencey not implemented yet");
    MG_ASSERT(op.pre_query_directives_.hops_limit_ == nullptr, "Hop limit not implemented yet");
    MG_ASSERT(op.pre_query_directives_.index_hints_.empty(), "Index hints not implemented yet");

    return true;
  }
  bool PostVisit(CypherQuery &) override { return true; }

  // bool PreVisit(SingleQuery &) override { return true; }
  // bool PostVisit(SingleQuery &) override { return true; }

  // bool PreVisit(With &tree) override {
  //   // visiting order of the With is done by the With::Accept
  //   return true;
  // }
  // bool PostVisit(With &) override { return true; }

  bool PreVisit(NamedExpression &expr) override {
    EnsureInput();
    return true;
  }

  bool PostVisit(NamedExpression &op) override {
    auto expr = PopStack();
    auto input = PopStack();
    DMG_ASSERT(op.symbol_pos_ != -1, "AST symbol should have already been mapped into the frame");
    auto sym = egraph_.MakeSymbol(op.symbol_pos_);
    auto bind = egraph_.MakeBind(input, sym, expr);
    builder_stack_.emplace_back(bind);

    // Any analysis to add to the bind EClass?
    // -> Bind has a name `named_expression.name_`
    // for debug purpose
    // NamedOutput its part of the ENode info

    return true;
  }

  bool PreVisit(Return &op) override {
    // Note: SymbolGenerator has already semantically checked that the names are unique
    std::vector<plan::v2::eclass> named_outputs;
    for (auto &named_expr : op.body_.named_expressions) {
      if (!named_expr->expression_->Accept(*this)) {
        return false;
      }

      auto expr = PopStack();
      auto sym = egraph_.MakeSymbol(named_expr->symbol_pos_);
      auto named_output = egraph_.MakeNamedOutput(named_expr->name_, sym, expr);
      named_outputs.emplace_back(named_output);
    }

    EnsureInput();
    auto input = PopStack();
    builder_stack_.emplace_back(egraph_.MakeOutputs(input, std::move(named_outputs)));

    auto const &body = op.body_;
    for (auto &order_by : body.order_by) {
      if (!order_by.expression->Accept(*this)) {
        return false;
      }
    }
    if (body.skip && !body.skip->Accept(*this)) {
      return false;
    }
    if (body.limit && !body.limit->Accept(*this)) {
      return false;
    }
    return false;
  }

  bool PostVisit(Return &) override { return true; }

  bool PreVisit(Where &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(CallSubquery &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Exists &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Foreach &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(LoadCsv &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(RegexMatch &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Unwind &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Merge &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(RemoveLabels &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(RemoveProperty &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(SetLabels &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(SetProperties &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(SetProperty &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Delete &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(EdgeAtom &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(NodeAtom &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Pattern &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Match &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Create &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(CallProcedure &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(ListComprehension &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(None &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Any &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Single &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(All &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Extract &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Coalesce &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Reduce &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Function &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(Aggregation &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(LabelsTest &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(AllPropertiesLookup &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(PropertyLookup &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(MapProjectionLiteral &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(MapLiteral &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(ListLiteral &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(IsNullOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(UnaryMinusOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(UnaryPlusOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(IfOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(ListSlicingOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(SubscriptOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(InListOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(RangeOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(GreaterEqualOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(LessEqualOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(GreaterOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(LessOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(EqualOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(NotEqualOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(ExponentiationOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(ModOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(DivisionOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(MultiplicationOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(SubtractionOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(AdditionOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(NotOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(AndOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(XorOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(OrOperator &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(CypherUnion &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }
  bool PreVisit(PatternComprehension &) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

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

  bool PostVisit(CypherUnion &) override { return true; }

  bool PostVisit(PatternComprehension &) override { return true; }

  auto GetEgraph() && -> std::tuple<egraph, plan::v2::eclass> {
    MG_ASSERT(builder_stack_.size() == 1, "should have a root");
    return {std::move(egraph_), builder_stack_.back()};
  }

 private:
  auto PopStack() -> plan::v2::eclass {
    DMG_ASSERT(!builder_stack_.empty());
    const auto res = builder_stack_.back();
    builder_stack_.pop_back();
    return res;
  }

  void EnsureInput() {
    if (builder_stack_.empty()) {
      builder_stack_.emplace_back(egraph_.MakeOnce());
    }
  }

  egraph egraph_;
  std::vector<plan::v2::eclass> builder_stack_;
};
}  // namespace
}  // namespace memgraph::query

namespace memgraph::query::plan::v2 {

auto ConvertToEgraph(CypherQuery const &query, SymbolTable const &symbol_table) -> std::tuple<egraph, eclass> {
  auto visitor = AstConverterVisitor{};
  // TODO: fix HierarchicalTreeVisitor to allow const
  const_cast<CypherQuery &>(query).Accept(visitor);
  return std::move(visitor).GetEgraph();
}

}  // namespace memgraph::query::plan::v2
