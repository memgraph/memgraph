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

#include "query/frontend/semantic/graph_free.hpp"

#include <algorithm>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/query/aggregation.hpp"
#include "query/frontend/ast/query/named_expression.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query {
namespace {

// Derives from ExpressionVisitor, whose Visit overloads are all pure, so a newly added expression type
// fails to compile until it is classified here.
//
// Identifiers, subscript and slicing are admitted on one invariant: the clause walker below binds no
// graph state, so no vertex or edge can be in scope, and those are the only operands that would send
// the evaluator to the accessor. Expressions that bind an identifier themselves are rejected to keep
// that invariant in one place.
class GraphReachChecker final : public ExpressionVisitor<void> {
 public:
  using ExpressionVisitor::Visit;

  bool ReachesGraph() const { return reaches_graph_; }

 private:
  void Reject() { reaches_graph_ = true; }

  void Recurse(Expression *expression) {
    if (reaches_graph_ || expression == nullptr) return;
    expression->Accept(*this);
  }

  template <typename TOperator>
  void RecurseBinary(TOperator &op) {
    Recurse(op.expression1_);
    Recurse(op.expression2_);
  }

  template <typename TOperator>
  void RecurseUnary(TOperator &op) {
    Recurse(op.expression_);
  }

  // Values the query already carries.
  void Visit(PrimitiveLiteral & /*unused*/) override {}

  void Visit(ParameterLookup & /*unused*/) override {}

  void Visit(Identifier & /*unused*/) override {}

  void Visit(NamedExpression &named_expression) override { Recurse(named_expression.expression_); }

  void Visit(ListLiteral &list_literal) override {
    std::ranges::for_each(list_literal.elements_, [this](auto *element) { Recurse(element); });
  }

  void Visit(MapLiteral &map_literal) override {
    std::ranges::for_each(map_literal.elements_, [this](auto const &entry) { Recurse(entry.second); });
  }

  void Visit(Coalesce &coalesce) override {
    std::ranges::for_each(coalesce.expressions_, [this](auto *expression) { Recurse(expression); });
  }

  void Visit(IfOperator &op) override {
    Recurse(op.condition_);
    Recurse(op.then_expression_);
    Recurse(op.else_expression_);
  }

  void Visit(ListSlicingOperator &op) override {
    Recurse(op.list_);
    Recurse(op.lower_bound_);
    Recurse(op.upper_bound_);
  }

  void Visit(RegexMatch &op) override {
    Recurse(op.string_expr_);
    Recurse(op.regex_);
  }

  void Visit(OrOperator &op) override { RecurseBinary(op); }

  void Visit(XorOperator &op) override { RecurseBinary(op); }

  void Visit(AndOperator &op) override { RecurseBinary(op); }

  void Visit(AdditionOperator &op) override { RecurseBinary(op); }

  void Visit(SubtractionOperator &op) override { RecurseBinary(op); }

  void Visit(MultiplicationOperator &op) override { RecurseBinary(op); }

  void Visit(DivisionOperator &op) override { RecurseBinary(op); }

  void Visit(ModOperator &op) override { RecurseBinary(op); }

  void Visit(ExponentiationOperator &op) override { RecurseBinary(op); }

  void Visit(NotEqualOperator &op) override { RecurseBinary(op); }

  void Visit(EqualOperator &op) override { RecurseBinary(op); }

  void Visit(LessOperator &op) override { RecurseBinary(op); }

  void Visit(GreaterOperator &op) override { RecurseBinary(op); }

  void Visit(LessEqualOperator &op) override { RecurseBinary(op); }

  void Visit(GreaterEqualOperator &op) override { RecurseBinary(op); }

  void Visit(RangeOperator &op) override { RecurseBinary(op); }

  void Visit(InListOperator &op) override { RecurseBinary(op); }

  void Visit(SubscriptOperator &op) override { RecurseBinary(op); }

  void Visit(Aggregation &op) override { RecurseBinary(op); }

  void Visit(NotOperator &op) override { RecurseUnary(op); }

  void Visit(UnaryPlusOperator &op) override { RecurseUnary(op); }

  void Visit(UnaryMinusOperator &op) override { RecurseUnary(op); }

  void Visit(IsNullOperator &op) override { RecurseUnary(op); }

  // Names graph state.
  void Visit(PropertyLookup & /*unused*/) override { Reject(); }

  void Visit(AllPropertiesLookup & /*unused*/) override { Reject(); }

  void Visit(MapProjectionLiteral & /*unused*/) override { Reject(); }

  void Visit(LabelsTest & /*unused*/) override { Reject(); }

  void Visit(EdgeTypesTest & /*unused*/) override { Reject(); }

  void Visit(Exists & /*unused*/) override { Reject(); }

  void Visit(PatternComprehension & /*unused*/) override { Reject(); }

  // Resolved through the accessor regardless of its operands.
  void Visit(Function & /*unused*/) override { Reject(); }

  void Visit(EnumValueAccess & /*unused*/) override { Reject(); }

  // Binds an identifier of its own.
  void Visit(Reduce & /*unused*/) override { Reject(); }

  void Visit(Extract & /*unused*/) override { Reject(); }

  void Visit(All & /*unused*/) override { Reject(); }

  void Visit(Single & /*unused*/) override { Reject(); }

  void Visit(Any & /*unused*/) override { Reject(); }

  void Visit(None & /*unused*/) override { Reject(); }

  void Visit(ListComprehension & /*unused*/) override { Reject(); }

  bool reaches_graph_{false};
};

// Accepts an Expression or a NamedExpression; both are visitable by an ExpressionVisitor.
bool ReachesGraph(auto *node) {
  if (node == nullptr) return false;
  GraphReachChecker visitor;
  node->Accept(visitor);
  return visitor.ReachesGraph();
}

bool AnyReachesGraph(auto const &nodes) {
  return std::ranges::any_of(nodes, [](auto *node) { return ReachesGraph(node); });
}

bool BodyReachesGraph(const ReturnBody &body) {
  // `RETURN *` projects what is in scope without naming it, so there is nothing to check.
  if (body.all_identifiers) return true;
  if (ReachesGraph(body.skip) || ReachesGraph(body.limit)) return true;
  if (AnyReachesGraph(body.named_expressions)) return true;
  return std::ranges::any_of(body.order_by,
                             [](SortItem const &sort_item) { return ReachesGraph(sort_item.expression); });
}

// Default-reject: an unrecognised clause is assumed to reach the graph. Unlike the expression side, a
// missed clause costs a fast path rather than safety, so falling through is the right failure.
bool ClauseReachesGraph(Clause *clause) {
  if (auto *return_clause = utils::Downcast<Return>(clause)) {
    return BodyReachesGraph(return_clause->body_);
  }
  if (auto *with_clause = utils::Downcast<With>(clause)) {
    return BodyReachesGraph(with_clause->body_) ||
           (with_clause->where_ != nullptr && ReachesGraph(with_clause->where_->expression_));
  }
  if (auto *unwind = utils::Downcast<Unwind>(clause)) {
    return ReachesGraph(unwind->named_expression_);
  }
  if (auto *call_procedure = utils::Downcast<CallProcedure>(clause)) {
    if (!call_procedure->graph_free_) return true;
    return AnyReachesGraph(call_procedure->arguments_) ||
           (call_procedure->where_ != nullptr && ReachesGraph(call_procedure->where_->expression_));
  }
  return true;
}

bool SingleQueryReachesGraph(SingleQuery *single_query) {
  if (single_query == nullptr) return true;
  return std::ranges::any_of(single_query->clauses_, ClauseReachesGraph);
}

// Every directive either steers a scan or needs a transaction to act on. Bound by structured binding so
// that a directive added later breaks the build here rather than being silently admitted.
bool HasPreQueryDirectives(const PreQueryDirectives &directives) {
  auto const &[index_hints, hops_limit, commit_frequency, parallel_execution, num_threads] = directives;
  return !index_hints.empty() || hops_limit != nullptr || commit_frequency != nullptr || parallel_execution ||
         num_threads != nullptr;
}

}  // namespace

bool IsGraphFree(const CypherQuery &query) {
  if (HasPreQueryDirectives(query.pre_query_directives_)) return false;
  if (SingleQueryReachesGraph(query.single_query_)) return false;
  return std::ranges::none_of(query.cypher_unions_, [](CypherUnion *cypher_union) {
    return cypher_union == nullptr || SingleQueryReachesGraph(cypher_union->single_query_);
  });
}

}  // namespace memgraph::query
