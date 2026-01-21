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
    // TODO: is token_position_ right? rename
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

  ReturnType Visit(EnumValueAccess & /*enum_value_access*/) override {
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

  bool PostVisit(CypherQuery & /*cypher_query*/) override { return true; }

  // bool PreVisit(SingleQuery &) override { return true; }
  // bool PostVisit(SingleQuery &) override { return true; }

  // bool PreVisit(With &tree) override {
  //   // visiting order of the With is done by the With::Accept
  //   return true;
  // }
  // bool PostVisit(With &) override { return true; }

  bool PreVisit(NamedExpression & /*expr*/) override {
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
    for (const auto &order_by : body.order_by) {
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

  bool PostVisit(Return & /*return_*/) override { return true; }

  bool PreVisit(Where & /*where*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(CallSubquery & /*call_subquery*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Exists & /*exists*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Foreach & /*foreach*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(LoadCsv & /*load_csv*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(RegexMatch & /*regex_match*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Unwind & /*unwind*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Merge & /*merge*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(RemoveLabels & /*remove_labels*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(RemoveProperty & /*remove_property*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(SetLabels & /*set_labels*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(SetProperties & /*set_properties*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(SetProperty & /*set_property*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Delete & /*delete_*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(EdgeAtom & /*edge_atom*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(NodeAtom & /*node_atom*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Pattern & /*pattern*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Match & /*match*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Create & /*create*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(CallProcedure & /*call_procedure*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(ListComprehension & /*list_comprehension*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(None & /*none*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Any & /*any*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Single & /*single*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(All & /*all*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Extract & /*extract*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Coalesce & /*coalesce*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Reduce & /*reduce*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Function & /*function*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(Aggregation & /*aggregation*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(LabelsTest & /*labels_test*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(AllPropertiesLookup & /*all_properties_lookup*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(PropertyLookup & /*property_lookup*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(MapProjectionLiteral & /*map_projection_literal*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(MapLiteral & /*map_literal*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(ListLiteral & /*list_literal*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(IsNullOperator & /*is_null_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(UnaryMinusOperator & /*unary_minus_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(UnaryPlusOperator & /*unary_plus_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(IfOperator & /*if_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(ListSlicingOperator & /*list_slicing_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(SubscriptOperator & /*subscript_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(InListOperator & /*in_list_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(RangeOperator & /*range_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(GreaterEqualOperator & /*greater_equal_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(LessEqualOperator & /*less_equal_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(GreaterOperator & /*greater_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(LessOperator & /*less_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(EqualOperator & /*equal_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(NotEqualOperator & /*not_equal_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(ExponentiationOperator & /*exponentiation_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(ModOperator & /*mod_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(DivisionOperator & /*division_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(MultiplicationOperator & /*multiplication_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(SubtractionOperator & /*subtraction_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(AdditionOperator & /*addition_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(NotOperator & /*not_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(AndOperator & /*and_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(XorOperator & /*xor_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(OrOperator & /*or_operator*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(CypherUnion & /*cypher_union*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PreVisit(PatternComprehension & /*pattern_comprehension*/) override {
    MG_ASSERT(false, "not implemented yet");
    return true;
  }

  bool PostVisit(CallSubquery & /*call_subquery*/) override { return true; }

  bool PostVisit(Exists & /*exists*/) override { return true; }

  bool PostVisit(Foreach & /*foreach*/) override { return true; }

  bool PostVisit(LoadCsv & /*load_csv*/) override { return true; }

  bool PostVisit(RegexMatch & /*regex_match*/) override { return true; }

  bool PostVisit(Unwind & /*unwind*/) override { return true; }

  bool PostVisit(Merge & /*merge*/) override { return true; }

  bool PostVisit(RemoveLabels & /*remove_labels*/) override { return true; }

  bool PostVisit(RemoveProperty & /*remove_property*/) override { return true; }

  bool PostVisit(SetLabels & /*set_labels*/) override { return true; }

  bool PostVisit(SetProperties & /*set_properties*/) override { return true; }

  bool PostVisit(SetProperty & /*set_property*/) override { return true; }

  bool PostVisit(Where & /*where*/) override { return true; }

  bool PostVisit(Delete & /*delete_*/) override { return true; }

  bool PostVisit(EdgeAtom & /*edge_atom*/) override { return true; }

  bool PostVisit(NodeAtom & /*node_atom*/) override { return true; }

  bool PostVisit(Pattern & /*pattern*/) override { return true; }

  bool PostVisit(Match & /*match*/) override { return true; }

  bool PostVisit(Create & /*create*/) override { return true; }

  bool PostVisit(CallProcedure & /*call_procedure*/) override { return true; }

  bool PostVisit(ListComprehension & /*list_comprehension*/) override { return true; }

  bool PostVisit(None & /*none*/) override { return true; }

  bool PostVisit(Any & /*any*/) override { return true; }

  bool PostVisit(Single & /*single*/) override { return true; }

  bool PostVisit(All & /*all*/) override { return true; }

  bool PostVisit(Extract & /*extract*/) override { return true; }

  bool PostVisit(Coalesce & /*coalesce*/) override { return true; }

  bool PostVisit(Reduce & /*reduce*/) override { return true; }

  bool PostVisit(Function & /*function*/) override { return true; }

  bool PostVisit(Aggregation & /*aggregation*/) override { return true; }

  bool PostVisit(LabelsTest & /*labels_test*/) override { return true; }

  bool PostVisit(AllPropertiesLookup & /*all_properties_lookup*/) override { return true; }

  bool PostVisit(PropertyLookup & /*property_lookup*/) override { return true; }

  bool PostVisit(MapProjectionLiteral & /*map_projection_literal*/) override { return true; }

  bool PostVisit(MapLiteral & /*map_literal*/) override { return true; }

  bool PostVisit(ListLiteral & /*list_literal*/) override { return true; }

  bool PostVisit(IsNullOperator & /*is_null_operator*/) override { return true; }

  bool PostVisit(UnaryMinusOperator & /*unary_minus_operator*/) override { return true; }

  bool PostVisit(UnaryPlusOperator & /*unary_plus_operator*/) override { return true; }

  bool PostVisit(IfOperator & /*if_operator*/) override { return true; }

  bool PostVisit(ListSlicingOperator & /*list_slicing_operator*/) override { return true; }

  bool PostVisit(SubscriptOperator & /*subscript_operator*/) override { return true; }

  bool PostVisit(InListOperator & /*in_list_operator*/) override { return true; }

  bool PostVisit(RangeOperator & /*range_operator*/) override { return true; }

  bool PostVisit(GreaterEqualOperator & /*greater_equal_operator*/) override { return true; }

  bool PostVisit(LessEqualOperator & /*less_equal_operator*/) override { return true; }

  bool PostVisit(GreaterOperator & /*greater_operator*/) override { return true; }

  bool PostVisit(LessOperator & /*less_operator*/) override { return true; }

  bool PostVisit(EqualOperator & /*equal_operator*/) override { return true; }

  bool PostVisit(NotEqualOperator & /*not_equal_operator*/) override { return true; }

  bool PostVisit(ExponentiationOperator & /*exponentiation_operator*/) override { return true; }

  bool PostVisit(ModOperator & /*mod_operator*/) override { return true; }

  bool PostVisit(DivisionOperator & /*division_operator*/) override { return true; }

  bool PostVisit(MultiplicationOperator & /*multiplication_operator*/) override { return true; }

  bool PostVisit(SubtractionOperator & /*subtraction_operator*/) override { return true; }

  bool PostVisit(AdditionOperator & /*addition_operator*/) override { return true; }

  bool PostVisit(NotOperator & /*not_operator*/) override { return true; }

  bool PostVisit(AndOperator & /*and_operator*/) override { return true; }

  bool PostVisit(XorOperator & /*xor_operator*/) override { return true; }

  bool PostVisit(OrOperator & /*or_operator*/) override { return true; }

  bool PostVisit(CypherUnion & /*cypher_union*/) override { return true; }

  bool PostVisit(PatternComprehension & /*pattern_comprehension*/) override { return true; }

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

auto ConvertToEgraph(CypherQuery const &query, SymbolTable const & /*symbol_table*/) -> std::tuple<egraph, eclass> {
  auto visitor = AstConverterVisitor{};
  // TODO: fix HierarchicalTreeVisitor to allow const
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  const_cast<CypherQuery &>(query).Accept(visitor);
  return std::move(visitor).GetEgraph();
}

}  // namespace memgraph::query::plan::v2
