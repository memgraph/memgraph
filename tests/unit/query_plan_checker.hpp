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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <climits>
#include <utility>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/preprocess.hpp"
#include "query/plan/rule_based_planner.hpp"
#include "storage/v2/enum.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query::plan {

class BaseOpChecker {
 public:
  virtual ~BaseOpChecker() = default;

  virtual void CheckOp(LogicalOperator &, const SymbolTable &) = 0;
};

class PlanChecker : public virtual HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  PlanChecker(const std::list<std::unique_ptr<BaseOpChecker>> &checkers, const SymbolTable &symbol_table)
      : symbol_table_(symbol_table) {
    for (const auto &checker : checkers) checkers_.emplace_back(checker.get());
  }

  PlanChecker(const std::list<BaseOpChecker *> &checkers, const SymbolTable &symbol_table)
      : checkers_(checkers), symbol_table_(symbol_table) {}

#define PRE_VISIT(TOp)              \
  bool PreVisit(TOp &op) override { \
    CheckOp(op);                    \
    return true;                    \
  }

#define VISIT(TOp)               \
  bool Visit(TOp &op) override { \
    CheckOp(op);                 \
    return true;                 \
  }

  PRE_VISIT(CreateNode);
  PRE_VISIT(CreateExpand);
  PRE_VISIT(Delete);
  PRE_VISIT(ScanAll);
  PRE_VISIT(ScanAllByLabel);
  PRE_VISIT(ScanAllByLabelProperties);
  PRE_VISIT(ScanAllByEdgeType);
  PRE_VISIT(ScanAllByEdgeTypeProperty);
  PRE_VISIT(ScanAllByEdgeTypePropertyValue);
  PRE_VISIT(ScanAllByEdgeTypePropertyRange);
  PRE_VISIT(ScanAllByEdgeProperty);
  PRE_VISIT(ScanAllByEdgePropertyValue);
  PRE_VISIT(ScanAllByEdgePropertyRange);
  PRE_VISIT(ScanAllByEdgeId);
  PRE_VISIT(ScanAllById);
  PRE_VISIT(Expand);
  PRE_VISIT(ExpandVariable);
  PRE_VISIT(ConstructNamedPath);
  PRE_VISIT(EmptyResult);
  PRE_VISIT(Produce);
  PRE_VISIT(SetProperty);
  PRE_VISIT(SetProperties);
  PRE_VISIT(SetLabels);
  PRE_VISIT(RemoveProperty);
  PRE_VISIT(RemoveLabels);
  PRE_VISIT(EdgeUniquenessFilter);
  PRE_VISIT(Accumulate);
  PRE_VISIT(Aggregate);
  PRE_VISIT(Skip);
  PRE_VISIT(Limit);
  PRE_VISIT(OrderBy);
  PRE_VISIT(EvaluatePatternFilter);
  PRE_VISIT(SetNestedProperty);
  PRE_VISIT(RemoveNestedProperty);

  bool PreVisit(Merge &op) override {
    CheckOp(op);
    op.input()->Accept(*this);
    return false;
  }
  bool PreVisit(Optional &op) override {
    CheckOp(op);
    op.input()->Accept(*this);
    return false;
  }
  PRE_VISIT(Unwind);
  PRE_VISIT(Distinct);

  bool PreVisit(Foreach &op) override {
    CheckOp(op);
    return false;
  }

  bool PreVisit(Filter &op) override {
    CheckOp(op);
    op.input()->Accept(*this);
    return false;
  }

  bool Visit(Once &) override {
    // Ignore checking Once, it is implicitly at the end.
    return true;
  }

  bool PreVisit(Cartesian &op) override {
    CheckOp(op);
    return false;
  }

  bool PreVisit(HashJoin &op) override {
    CheckOp(op);
    return false;
  }

  bool PreVisit(IndexedJoin &op) override {
    CheckOp(op);
    return false;
  }

  bool PreVisit(Apply &op) override {
    CheckOp(op);
    op.input()->Accept(*this);
    return false;
  }

  bool PreVisit(Union &op) override {
    CheckOp(op);
    return false;
  }

  PRE_VISIT(CallProcedure);

  bool PreVisit(RollUpApply &op) override {
    CheckOp(op);
    return false;
  }

  PRE_VISIT(PeriodicCommit);
  PRE_VISIT(LoadCsv);

  bool PreVisit(PeriodicSubquery &op) override {
    CheckOp(op);
    op.input()->Accept(*this);
    return false;
  }

#undef PRE_VISIT
#undef VISIT

  void CheckOp(LogicalOperator &op) {
    ASSERT_FALSE(checkers_.empty());
    checkers_.back()->CheckOp(op, symbol_table_);
    checkers_.pop_back();
  }

  std::list<BaseOpChecker *> checkers_;
  const SymbolTable &symbol_table_;
};

template <class TOp>
class OpChecker : public BaseOpChecker {
 public:
  void CheckOp(LogicalOperator &op, const SymbolTable &symbol_table) override {
    auto *expected_op = dynamic_cast<TOp *>(&op);
    ASSERT_TRUE(expected_op) << "op is '" << op.GetTypeInfo().name << "' expected '" << TOp::kType.name << "'!";
    ExpectOp(*expected_op, symbol_table);
  }

  virtual void ExpectOp(TOp &, const SymbolTable &) {}
};

using ExpectOnce = OpChecker<Once>;
using ExpectCreateNode = OpChecker<CreateNode>;
using ExpectCreateExpand = OpChecker<CreateExpand>;
using ExpectDelete = OpChecker<Delete>;
using ExpectScanAll = OpChecker<ScanAll>;
using ExpectScanAllByEdgeType = OpChecker<ScanAllByEdgeType>;
using ExpectScanAllByEdgeId = OpChecker<ScanAllByEdgeId>;
using ExpectScanAllById = OpChecker<ScanAllById>;
using ExpectExpand = OpChecker<Expand>;
using ExpectConstructNamedPath = OpChecker<ConstructNamedPath>;
using ExpectProduce = OpChecker<Produce>;
using ExpectEmptyResult = OpChecker<EmptyResult>;
using ExpectSetProperty = OpChecker<SetProperty>;
using ExpectSetProperties = OpChecker<SetProperties>;
using ExpectSetLabels = OpChecker<SetLabels>;
using ExpectRemoveProperty = OpChecker<RemoveProperty>;
using ExpectRemoveLabels = OpChecker<RemoveLabels>;
using ExpectEdgeUniquenessFilter = OpChecker<EdgeUniquenessFilter>;
using ExpectSkip = OpChecker<Skip>;
using ExpectLimit = OpChecker<Limit>;
using ExpectOrderBy = OpChecker<OrderBy>;
using ExpectUnwind = OpChecker<Unwind>;
using ExpectDistinct = OpChecker<Distinct>;
using ExpectEvaluatePatternFilter = OpChecker<EvaluatePatternFilter>;
using ExpectPeriodicCommit = OpChecker<PeriodicCommit>;
using ExpectLoadCsv = OpChecker<LoadCsv>;
using ExpectBasicCallProcedure = OpChecker<CallProcedure>;
using ExpectSetNestedProperty = OpChecker<SetNestedProperty>;
using ExpectRemoveNestedProperty = OpChecker<RemoveNestedProperty>;

class ExpectFilter : public OpChecker<Filter> {
 public:
  explicit ExpectFilter(const std::vector<std::list<BaseOpChecker *>> &pattern_filters = {})
      : pattern_filters_(pattern_filters) {}

  void ExpectOp(Filter &filter, const SymbolTable &symbol_table) override {
    for (auto i = 0; i < filter.pattern_filters_.size(); i++) {
      PlanChecker check_updates(pattern_filters_[i], symbol_table);

      filter.pattern_filters_[i]->Accept(check_updates);
    }
    // ordering in AND Operator must be ..., exists, exists, exists.
    auto *expr = filter.expression_;
    std::vector<Expression *> filter_expressions;
    while (auto *and_operator = utils::Downcast<AndOperator>(expr)) {
      auto *expr1 = and_operator->expression1_;
      auto *expr2 = and_operator->expression2_;
      filter_expressions.emplace_back(expr1);
      expr = expr2;
    }
    if (expr) filter_expressions.emplace_back(expr);

    auto it = filter_expressions.begin();
    for (; it != filter_expressions.end(); it++) {
      if ((*it)->GetTypeInfo().name == query::Exists::kType.name) {
        break;
      }
    }
    while (it != filter_expressions.end()) {
      ASSERT_TRUE((*it)->GetTypeInfo().name == query::Exists::kType.name)
          << "Filter expression is '" << (*it)->GetTypeInfo().name << "' expected '" << query::Exists::kType.name
          << "'!";
      it++;
    }
  }

  std::vector<std::list<BaseOpChecker *>> pattern_filters_;
};

class ExpectForeach : public OpChecker<Foreach> {
 public:
  ExpectForeach(const std::list<BaseOpChecker *> &input, const std::list<BaseOpChecker *> &updates)
      : input_(input), updates_(updates) {}

  void ExpectOp(Foreach &foreach, const SymbolTable &symbol_table) override {
    PlanChecker check_input(input_, symbol_table);
    foreach
      .input_->Accept(check_input);
    PlanChecker check_updates(updates_, symbol_table);
    foreach
      .update_clauses_->Accept(check_updates);
  }

 private:
  std::list<BaseOpChecker *> input_;
  std::list<BaseOpChecker *> updates_;
};

class ExpectApply : public OpChecker<Apply> {
 public:
  explicit ExpectApply(const std::list<BaseOpChecker *> &subquery) : subquery_(subquery) {}

  void ExpectOp(Apply &apply, const SymbolTable &symbol_table) override {
    PlanChecker check_subquery(subquery_, symbol_table);
    apply.subquery_->Accept(check_subquery);
  }

 private:
  std::list<BaseOpChecker *> subquery_;
};

class ExpectUnion : public OpChecker<Union> {
 public:
  ExpectUnion(const std::list<BaseOpChecker *> &left, const std::list<BaseOpChecker *> &right)
      : left_(left), right_(right) {}

  void ExpectOp(Union &union_op, const SymbolTable &symbol_table) override {
    PlanChecker check_left_op(left_, symbol_table);
    union_op.left_op_->Accept(check_left_op);
    PlanChecker check_right_op(right_, symbol_table);
    union_op.right_op_->Accept(check_right_op);
  }

 private:
  std::list<BaseOpChecker *> left_;
  std::list<BaseOpChecker *> right_;
};

class ExpectExpandVariable : public OpChecker<ExpandVariable> {
 public:
  void ExpectOp(ExpandVariable &op, const SymbolTable &) override {
    EXPECT_EQ(op.type_, memgraph::query::EdgeAtom::Type::DEPTH_FIRST);
  }
};

class ExpectExpandBfs : public OpChecker<ExpandVariable> {
 public:
  void ExpectOp(ExpandVariable &op, const SymbolTable &) override {
    EXPECT_EQ(op.type_, memgraph::query::EdgeAtom::Type::BREADTH_FIRST);
  }
};

class ExpectExpandKShortest : public OpChecker<ExpandVariable> {
 public:
  void ExpectOp(ExpandVariable &op, const SymbolTable &) override {
    EXPECT_EQ(op.type_, memgraph::query::EdgeAtom::Type::KSHORTEST);
  }
};

class ExpectAccumulate : public OpChecker<Accumulate> {
 public:
  explicit ExpectAccumulate(const std::unordered_set<Symbol> &symbols) : symbols_(symbols) {}

  void ExpectOp(Accumulate &op, const SymbolTable &) override {
    std::unordered_set<Symbol> got_symbols(op.symbols_.begin(), op.symbols_.end());
    EXPECT_EQ(symbols_, got_symbols);
  }

 private:
  const std::unordered_set<Symbol> symbols_;
};

class ExpectAggregate : public OpChecker<Aggregate> {
 public:
  ExpectAggregate(const std::vector<memgraph::query::Aggregation *> &aggregations,
                  const std::unordered_set<memgraph::query::Expression *> &group_by)
      : aggregations_(aggregations), group_by_(group_by) {}

  void ExpectOp(Aggregate &op, const SymbolTable &symbol_table) override {
    auto aggr_it = aggregations_.begin();
    for (const auto &aggr_elem : op.aggregations_) {
      ASSERT_NE(aggr_it, aggregations_.end());
      auto *aggr = *aggr_it++;
      // TODO: Proper expression equality
      EXPECT_EQ(typeid(aggr_elem.arg1).hash_code(), typeid(aggr->expression1_).hash_code());
      EXPECT_EQ(typeid(aggr_elem.arg2).hash_code(), typeid(aggr->expression2_).hash_code());
      EXPECT_EQ(aggr_elem.op, aggr->op_);
      EXPECT_EQ(aggr_elem.distinct, aggr->distinct_);
      EXPECT_EQ(aggr_elem.output_sym, symbol_table.at(*aggr));
    }
    EXPECT_EQ(aggr_it, aggregations_.end());
    // TODO: Proper group by expression equality
    std::unordered_set<size_t> got_group_by;
    std::unordered_set<size_t> expected_group_by;
    for (auto *expr : op.group_by_) got_group_by.insert(typeid(*expr).hash_code());
    auto is_constant = [](const Expression *expression) {
      return utils::Downcast<const PrimitiveLiteral>(expression) || utils::Downcast<const ParameterLookup>(expression);
    };
    std::ranges::for_each(group_by_, [&expected_group_by, &is_constant](auto *expr) {
      if (!is_constant(expr)) {
        expected_group_by.insert(typeid(*expr).hash_code());
      }
    });
    EXPECT_EQ(got_group_by, expected_group_by);
  }

 private:
  std::vector<memgraph::query::Aggregation *> aggregations_;
  std::unordered_set<memgraph::query::Expression *> group_by_;
};

class ExpectMerge : public OpChecker<Merge> {
 public:
  ExpectMerge(const std::list<BaseOpChecker *> &on_match, const std::list<BaseOpChecker *> &on_create)
      : on_match_(on_match), on_create_(on_create) {}

  void ExpectOp(Merge &merge, const SymbolTable &symbol_table) override {
    PlanChecker check_match(on_match_, symbol_table);
    merge.merge_match_->Accept(check_match);
    PlanChecker check_create(on_create_, symbol_table);
    merge.merge_create_->Accept(check_create);
  }

 private:
  const std::list<BaseOpChecker *> &on_match_;
  const std::list<BaseOpChecker *> &on_create_;
};

class ExpectOptional : public OpChecker<Optional> {
 public:
  explicit ExpectOptional(const std::list<BaseOpChecker *> &optional) : optional_(optional) {}

  ExpectOptional(const std::vector<Symbol> &optional_symbols, const std::list<BaseOpChecker *> &optional)
      : optional_symbols_(optional_symbols), optional_(optional) {}

  void ExpectOp(Optional &optional, const SymbolTable &symbol_table) override {
    if (!optional_symbols_.empty()) {
      EXPECT_THAT(optional.optional_symbols_, testing::UnorderedElementsAreArray(optional_symbols_));
    }
    PlanChecker check_optional(optional_, symbol_table);
    optional.optional_->Accept(check_optional);
  }

 private:
  std::vector<Symbol> optional_symbols_;
  const std::list<BaseOpChecker *> &optional_;
};

class ExpectScanAllByLabel : public OpChecker<ScanAllByLabel> {
 public:
  explicit ExpectScanAllByLabel(std::optional<memgraph::storage::LabelId> label = std::nullopt) : label_(label) {}

  void ExpectOp(ScanAllByLabel &scan_all, const SymbolTable &) override {
    if (label_) {
      EXPECT_EQ(*label_, scan_all.label_);
    }
  }

 private:
  std::optional<memgraph::storage::LabelId> label_;
};

class ExpectScanAllByLabelProperties : public OpChecker<ScanAllByLabelProperties> {
 public:
  ExpectScanAllByLabelProperties(memgraph::storage::LabelId label,
                                 std::vector<memgraph::storage::PropertyPath> properties,
                                 std::vector<ExpressionRange> expression_ranges)
      : label_(label), properties_(std::move(properties)), expression_ranges_(std::move(expression_ranges)) {}

  void ExpectOp(ScanAllByLabelProperties &scan, const SymbolTable &) override {
    EXPECT_EQ(scan.label_, label_);
    EXPECT_EQ(scan.properties_, properties_);
    ASSERT_EQ(scan.expression_ranges_.size(), expression_ranges_.size());

    auto const compare_bound_expression = [](std::optional<utils::Bound<Expression *>> const &lhs,
                                             std::optional<utils::Bound<Expression *>> const &rhs) {
      if (!lhs && !rhs) return true;
      if (!lhs || !rhs) return false;
      if (lhs->type() != rhs->type()) return false;
      // In keeping with the other tests, we are comparing expressions by
      // hash code of their types, rather than performing a full expression
      // comparison.
      if (typeid(*lhs->value()).hash_code() != typeid(*rhs->value()).hash_code()) return false;
      return true;
    };

    auto const compare_expression_range = [&](auto &&lhs, auto &&rhs) {
      if (lhs.type_ != rhs.type_) return false;
      return compare_bound_expression(lhs.lower_, rhs.lower_) && compare_bound_expression(lhs.upper_, rhs.upper_);
    };

    EXPECT_TRUE(ranges::equal(scan.expression_ranges_, expression_ranges_, compare_expression_range));
  }

 private:
  memgraph::storage::LabelId label_;
  std::vector<memgraph::storage::PropertyPath> properties_;
  std::vector<ExpressionRange> expression_ranges_;
};

class ExpectScanAllByEdgeTypePropertyValue : public OpChecker<ScanAllByEdgeTypePropertyValue> {
 public:
  ExpectScanAllByEdgeTypePropertyValue(memgraph::storage::EdgeTypeId edge_type,
                                       const std::pair<std::string, memgraph::storage::PropertyId> &prop_pair,
                                       memgraph::query::Expression *expression)
      : edge_type_(edge_type), property_(prop_pair.second), expression_(expression) {}

  void ExpectOp(ScanAllByEdgeTypePropertyValue &scan_all, const SymbolTable &) override {
    EXPECT_EQ(scan_all.common_.edge_types[0], edge_type_);
    EXPECT_EQ(scan_all.property_, property_);
    // TODO: Proper expression equality
    EXPECT_EQ(typeid(scan_all.expression_).hash_code(), typeid(expression_).hash_code());
  }

 private:
  memgraph::storage::EdgeTypeId edge_type_;
  memgraph::storage::PropertyId property_;
  memgraph::query::Expression *expression_;
};

class ExpectScanAllByEdgeTypePropertyRange : public OpChecker<ScanAllByEdgeTypePropertyRange> {
 public:
  ExpectScanAllByEdgeTypePropertyRange(memgraph::storage::EdgeTypeId edge_type, memgraph::storage::PropertyId property,
                                       std::optional<ScanAllByEdgeTypePropertyRange::Bound> lower_bound,
                                       std::optional<ScanAllByEdgeTypePropertyRange::Bound> upper_bound)
      : edge_type_(edge_type), property_(property), lower_bound_(lower_bound), upper_bound_(upper_bound) {}

  void ExpectOp(ScanAllByEdgeTypePropertyRange &scan_all, const SymbolTable &) override {
    EXPECT_EQ(scan_all.common_.edge_types[0], edge_type_);
    EXPECT_EQ(scan_all.property_, property_);
    if (lower_bound_) {
      ASSERT_TRUE(scan_all.lower_bound_);
      // TODO: Proper expression equality
      EXPECT_EQ(typeid(scan_all.lower_bound_->value()).hash_code(), typeid(lower_bound_->value()).hash_code());
      EXPECT_EQ(scan_all.lower_bound_->type(), lower_bound_->type());
    }
    if (upper_bound_) {
      ASSERT_TRUE(scan_all.upper_bound_);
      // TODO: Proper expression equality
      EXPECT_EQ(typeid(scan_all.upper_bound_->value()).hash_code(), typeid(upper_bound_->value()).hash_code());
      EXPECT_EQ(scan_all.upper_bound_->type(), upper_bound_->type());
    }
  }

 private:
  memgraph::storage::EdgeTypeId edge_type_;
  memgraph::storage::PropertyId property_;
  std::optional<ScanAllByEdgeTypePropertyRange::Bound> lower_bound_;
  std::optional<ScanAllByEdgeTypePropertyRange::Bound> upper_bound_;
};

class ExpectScanAllByEdgeTypeProperty : public OpChecker<ScanAllByEdgeTypeProperty> {
 public:
  ExpectScanAllByEdgeTypeProperty(memgraph::storage::EdgeTypeId edge_type,
                                  const std::pair<std::string, memgraph::storage::PropertyId> &prop_pair)
      : edge_type_(edge_type), property_(prop_pair.second) {}

  void ExpectOp(ScanAllByEdgeTypeProperty &scan_all, const SymbolTable &) override {
    EXPECT_EQ(scan_all.common_.edge_types[0], edge_type_);
    EXPECT_EQ(scan_all.property_, property_);
  }

 private:
  memgraph::storage::EdgeTypeId edge_type_;
  memgraph::storage::PropertyId property_;
};

class ExpectCartesian : public OpChecker<Cartesian> {
 public:
  ExpectCartesian(const std::list<BaseOpChecker *> &left, const std::list<BaseOpChecker *> &right)
      : left_(left), right_(right) {}

  void ExpectOp(Cartesian &op, const SymbolTable &symbol_table) override {
    ASSERT_TRUE(op.left_op_);
    PlanChecker left_checker(left_, symbol_table);
    op.left_op_->Accept(left_checker);
    ASSERT_TRUE(op.right_op_);
    PlanChecker right_checker(right_, symbol_table);
    op.right_op_->Accept(right_checker);
  }

 private:
  const std::list<BaseOpChecker *> &left_;
  const std::list<BaseOpChecker *> &right_;
};

class ExpectHashJoin : public OpChecker<HashJoin> {
 public:
  ExpectHashJoin(const std::list<BaseOpChecker *> &left, const std::list<BaseOpChecker *> &right)
      : left_(left), right_(right) {}

  void ExpectOp(HashJoin &op, const SymbolTable &symbol_table) override {
    ASSERT_TRUE(op.left_op_);
    PlanChecker left_checker(left_, symbol_table);
    op.left_op_->Accept(left_checker);
    ASSERT_TRUE(op.right_op_);
    PlanChecker right_checker(right_, symbol_table);
    op.right_op_->Accept(right_checker);
  }

 private:
  const std::list<BaseOpChecker *> &left_;
  const std::list<BaseOpChecker *> &right_;
};

class ExpectIndexedJoin : public OpChecker<IndexedJoin> {
 public:
  ExpectIndexedJoin(const std::list<BaseOpChecker *> &main_branch, const std::list<BaseOpChecker *> &sub_branch)
      : main_branch_(main_branch), sub_branch_(sub_branch) {}

  void ExpectOp(IndexedJoin &op, const SymbolTable &symbol_table) override {
    ASSERT_TRUE(op.main_branch_);
    PlanChecker main_branch_checker(main_branch_, symbol_table);
    op.main_branch_->Accept(main_branch_checker);
    ASSERT_TRUE(op.sub_branch_);
    PlanChecker sub_branch_checker(sub_branch_, symbol_table);
    op.sub_branch_->Accept(sub_branch_checker);
  }

 private:
  const std::list<BaseOpChecker *> &main_branch_;
  const std::list<BaseOpChecker *> &sub_branch_;
};

class ExpectCallProcedure : public OpChecker<CallProcedure> {
 public:
  ExpectCallProcedure(std::string name, const std::vector<memgraph::query::Expression *> &args,
                      const std::vector<std::string> &fields, const std::vector<Symbol> &result_syms)
      : name_(std::move(name)), args_(args), fields_(fields), result_syms_(result_syms) {}

  void ExpectOp(CallProcedure &op, const SymbolTable &symbol_table) override {
    EXPECT_EQ(op.procedure_name_, name_);
    EXPECT_EQ(op.arguments_.size(), args_.size());
    for (size_t i = 0; i < args_.size(); ++i) {
      const auto *op_arg = op.arguments_[i];
      const auto *expected_arg = args_[i];
      // TODO: Proper expression equality
      EXPECT_EQ(op_arg->GetTypeInfo(), expected_arg->GetTypeInfo());
    }
    EXPECT_EQ(op.result_fields_, fields_);
    EXPECT_EQ(op.result_symbols_, result_syms_);
  }

 private:
  std::string name_;
  std::vector<memgraph::query::Expression *> args_;
  std::vector<std::string> fields_;
  std::vector<Symbol> result_syms_;
};

class ExpectRollUpApply : public OpChecker<RollUpApply> {
 public:
  ExpectRollUpApply(const std::list<std::unique_ptr<BaseOpChecker>> &input,
                    const std::list<std::unique_ptr<BaseOpChecker>> &list_collection_branch)
      : input_(input), list_collection_branch_(list_collection_branch) {}

  void ExpectOp(RollUpApply &op, const SymbolTable &symbol_table) override {
    PlanChecker input_checker(input_, symbol_table);
    op.input_->Accept(input_checker);
    ASSERT_TRUE(op.list_collection_branch_);
    PlanChecker list_collection_branch_checker(list_collection_branch_, symbol_table);
    op.list_collection_branch_->Accept(list_collection_branch_checker);
  }

 private:
  const std::list<std::unique_ptr<BaseOpChecker>> &input_;
  const std::list<std::unique_ptr<BaseOpChecker>> &list_collection_branch_;
};

class ExpectPeriodicSubquery : public OpChecker<PeriodicSubquery> {
 public:
  explicit ExpectPeriodicSubquery(const std::list<BaseOpChecker *> &subquery) : subquery_(subquery) {}

  void ExpectOp(PeriodicSubquery &periodic_subquery, const SymbolTable &symbol_table) override {
    PlanChecker check_subquery(subquery_, symbol_table);
    periodic_subquery.subquery_->Accept(check_subquery);
  }

 private:
  std::list<BaseOpChecker *> subquery_;
};

template <class T>
std::list<std::unique_ptr<BaseOpChecker>> MakeCheckers(T arg) {
  std::list<std::unique_ptr<BaseOpChecker>> l;
  l.emplace_back(std::make_unique<T>(arg));
  return l;
}

template <class T, class... Rest>
std::list<std::unique_ptr<BaseOpChecker>> MakeCheckers(T arg, Rest &&...rest) {
  auto l = MakeCheckers(std::forward<Rest>(rest)...);
  l.emplace_front(std::make_unique<T>(arg));
  return std::move(l);
}

// Helper to build checker lists in natural top-down order (instead of reverse bottom-up order).
// Example:
//   Instead of:  list.push_back(make_unique<Once>()); list.push_back(make_unique<ScanAll>());
//   You can use: auto list = PlanFromTopDown<Produce, Expand, ScanAll, Once>();
//
// This makes test expectations more intuitive by specifying operations in the order you think about them.
template <typename... Checkers>
std::list<std::unique_ptr<BaseOpChecker>> PlanFromTopDown() {
  std::list<std::unique_ptr<BaseOpChecker>> result;
  // Fold expression that creates each checker and adds it at the front of the list
  // This reverses the parameter order, converting top-down to bottom-up
  (result.push_front(std::make_unique<Checkers>()), ...);
  return result;
}

template <class TPlanner, class TDbAccessor>
TPlanner MakePlanner(TDbAccessor *dba, AstStorage &storage, SymbolTable &symbol_table, CypherQuery *query,
                     const std::vector<IndexHint> &index_hints = {}) {
  auto planning_context = MakePlanningContext(&storage, &symbol_table, query, dba);
  auto query_parts = CollectQueryParts(symbol_table, storage, query, false);
  return TPlanner(query_parts, planning_context, index_hints);
}

class FakeDbAccessor {
 public:
  int64_t VerticesCount(memgraph::storage::LabelId label) const {
    auto found = label_index_.find(label);
    if (found != label_index_.end()) return found->second;
    return 0;
  }

  int64_t VerticesCount(memgraph::storage::LabelId label,
                        std::span<memgraph::storage::PropertyPath const> properties) const {
    auto it = std::ranges::find_if(label_properties_index_, [&](auto const &each) {
      return std::get<0>(each) == label && std::ranges::equal(std::get<1>(each), properties);
    });

    if (it != label_properties_index_.end()) {
      return std::get<2>(*it);
    } else {
      return 0;
    }
  }

  bool PointIndexExists(memgraph::storage::LabelId label, memgraph::storage::PropertyId property) const {
    return false;
  }

  std::optional<uint64_t> VerticesPointCount(storage::LabelId label, storage::PropertyId property) const {
    return std::nullopt;
  }

  int64_t EdgesCount(memgraph::storage::EdgeTypeId edge_type) const {
    auto found = edge_type_index_.find(edge_type);
    if (found != edge_type_index_.end()) return found->second;
    return 0;
  }

  int64_t EdgesCount(memgraph::storage::EdgeTypeId edge_type, memgraph::storage::PropertyId property) const {
    for (const auto &index : edge_type_property_index_) {
      if (std::get<0>(index) == edge_type && std::get<1>(index) == property) {
        return std::get<2>(index);
      }
    }
    return 0;
  }

  int64_t EdgesCount(memgraph::storage::PropertyId property) const { return 0; }

  bool LabelIndexReady(memgraph::storage::LabelId label) const {
    return label_index_.find(label) != label_index_.end();
  }

  bool LabelPropertyIndexReady(memgraph::storage::LabelId label,
                               std::span<memgraph::storage::PropertyPath const> properties) const {
    return std::ranges::find_if(label_properties_index_, [&](auto const &each) {
             return std::get<0>(each) == label && std::ranges::equal(std::get<1>(each), properties);
           }) != label_properties_index_.end();
  }

  auto RelevantLabelPropertiesIndicesInfo(std::span<storage::LabelId const> labels,
                                          std::span<storage::PropertyPath const> properties) const
      -> std::vector<storage::LabelPropertiesIndicesInfo> {
    auto res = std::vector<storage::LabelPropertiesIndicesInfo>{};

    for (auto const &[label, props, _] : label_properties_index_) {
      auto label_it = std::ranges::find(labels, label);
      if (label_it == labels.end()) {
        continue;
      }

      std::vector<long> properties_poses;
      properties_poses.reserve(properties.size());
      bool has_matching_property = false;
      for (auto prop : props) {
        auto prop_it = std::ranges::find(properties, prop);
        if (prop_it == properties.end()) {
          properties_poses.emplace_back(-1);
        } else {
          has_matching_property = true;
          auto distance = std::distance(properties.begin(), prop_it);
          // NOLINTNEXTLINE(google-runtime-int)
          properties_poses.emplace_back(static_cast<long>(distance));
        }
      }

      if (has_matching_property) {
        auto l_pos = std::distance(labels.begin(), label_it);
        res.emplace_back(l_pos, std::move(properties_poses), label, props);
      }
    }

    return res;
  }

  bool EdgeTypeIndexReady(memgraph::storage::EdgeTypeId edge_type) const {
    return edge_type_index_.find(edge_type) != edge_type_index_.end();
  }

  bool EdgeTypePropertyIndexReady(memgraph::storage::EdgeTypeId edge_type,
                                  memgraph::storage::PropertyId property) const {
    for (const auto &index : edge_type_property_index_) {
      if (std::get<0>(index) == edge_type && std::get<1>(index) == property) {
        return true;
      }
    }
    return false;
  }

  bool EdgePropertyIndexReady(memgraph::storage::PropertyId property) const { return false; }

  std::optional<memgraph::storage::LabelPropertyIndexStats> GetIndexStats(
      const memgraph::storage::LabelId label, std::span<memgraph::storage::PropertyPath const> properties) const {
    return memgraph::storage::LabelPropertyIndexStats{.statistic = 0, .avg_group_size = 1};  // unique id
  }

  std::optional<memgraph::storage::LabelIndexStats> GetIndexStats(const memgraph::storage::LabelId label) const {
    return memgraph::storage::LabelIndexStats{.count = 0, .avg_degree = 0};  // unique id
  }

  void SetIndexCount(memgraph::storage::LabelId label, int64_t count) { label_index_[label] = count; }

  void SetIndexCount(memgraph::storage::LabelId label, memgraph::storage::PropertyPath const &property, int64_t count) {
    std::vector properties{property};
    for (auto &index : label_properties_index_) {
      if (std::get<0>(index) == label && std::get<1>(index) == properties) {
        std::get<2>(index) = count;
        return;
      }
    }
    label_properties_index_.emplace_back(label, std::move(properties), count);
  }

  void SetIndexCount(memgraph::storage::LabelId label, std::span<memgraph::storage::PropertyPath const> properties,
                     int64_t count) {
    auto it = std::ranges::find_if(label_properties_index_, [&](auto const &each) {
      return std::get<0>(each) == label && std::ranges::equal(std::get<1>(each), properties);
    });

    if (it != label_properties_index_.end()) {
      std::get<2>(*it) = count;
    } else {
      label_properties_index_.emplace_back(label, std::vector(properties.begin(), properties.end()), count);
    }
  }

  void SetIndexCount(memgraph::storage::EdgeTypeId edge_type, int64_t count) { edge_type_index_[edge_type] = count; }

  void SetIndexCount(memgraph::storage::EdgeTypeId edge_type, memgraph::storage::PropertyId property, int64_t count) {
    for (auto &index : edge_type_property_index_) {
      if (std::get<0>(index) == edge_type && std::get<1>(index) == property) {
        std::get<2>(index) = count;
        return;
      }
    }
    edge_type_property_index_.emplace_back(edge_type, property, count);
  }

  memgraph::storage::LabelId NameToLabel(const std::string &name) {
    auto found = labels_.find(name);
    if (found != labels_.end()) return found->second;
    return labels_.emplace(name, memgraph::storage::LabelId::FromUint(labels_.size())).first->second;
  }

  memgraph::storage::LabelId Label(const std::string &name) { return NameToLabel(name); }

  memgraph::storage::EdgeTypeId NameToEdgeType(const std::string &name) {
    auto found = edge_types_.find(name);
    if (found != edge_types_.end()) return found->second;
    return edge_types_.emplace(name, memgraph::storage::EdgeTypeId::FromUint(edge_types_.size())).first->second;
  }

  memgraph::storage::EdgeTypeId EdgeType(const std::string &name) { return NameToEdgeType(name); }

  memgraph::storage::PropertyId NameToProperty(const std::string &name) {
    auto found = properties_.find(name);
    if (found != properties_.end()) return found->second;
    return properties_.emplace(name, memgraph::storage::PropertyId::FromUint(properties_.size())).first->second;
  }

  memgraph::storage::PropertyId Property(const std::string &name) { return NameToProperty(name); }

  std::string PropertyToName(memgraph::storage::PropertyId property) const {
    for (const auto &kv : properties_) {
      if (kv.second == property) return kv.first;
    }
    LOG_FATAL("Unable to find property name");
  }

  std::string PropertyName(memgraph::storage::PropertyId property) const { return PropertyToName(property); }

  auto GetEnumValue(std::string_view name, std::string_view value)
      -> utils::BasicResult<storage::EnumStorageError, storage::Enum> {
    // Does this need to be less fake?
    return memgraph::storage::Enum{memgraph::storage::EnumTypeId{0}, memgraph::storage::EnumValueId{0}};
  }

 private:
  std::unordered_map<std::string, memgraph::storage::LabelId> labels_;
  std::unordered_map<std::string, memgraph::storage::EdgeTypeId> edge_types_;
  std::unordered_map<std::string, memgraph::storage::PropertyId> properties_;

  std::unordered_map<memgraph::storage::LabelId, int64_t> label_index_;
  std::vector<std::tuple<memgraph::storage::LabelId, std::vector<memgraph::storage::PropertyPath>, int64_t>>
      label_properties_index_;
  std::unordered_map<memgraph::storage::EdgeTypeId, int64_t> edge_type_index_;
  std::vector<std::tuple<memgraph::storage::EdgeTypeId, memgraph::storage::PropertyId, int64_t>>
      edge_type_property_index_;
};

}  // namespace memgraph::query::plan
