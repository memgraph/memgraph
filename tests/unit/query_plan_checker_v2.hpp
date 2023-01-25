// Copyright 2023 Memgraph Ltd.
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

#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/v2/plan/operator.hpp"
#include "query/v2/plan/planner.hpp"
#include "query/v2/plan/preprocess.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::query::v2::plan {

class BaseOpChecker {
 public:
  virtual ~BaseOpChecker() {}

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
  PRE_VISIT(ScanAllByLabelPropertyValue);
  PRE_VISIT(ScanAllByLabelPropertyRange);
  PRE_VISIT(ScanAllByLabelProperty);
  PRE_VISIT(ScanByPrimaryKey);
  PRE_VISIT(Expand);
  PRE_VISIT(ExpandVariable);
  PRE_VISIT(Filter);
  PRE_VISIT(ConstructNamedPath);
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

  bool Visit(Once &) override {
    // Ignore checking Once, it is implicitly at the end.
    return true;
  }

  bool PreVisit(Cartesian &op) override {
    CheckOp(op);
    return false;
  }

  PRE_VISIT(CallProcedure);

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

using ExpectCreateNode = OpChecker<CreateNode>;
using ExpectCreateExpand = OpChecker<CreateExpand>;
using ExpectDelete = OpChecker<Delete>;
using ExpectScanAll = OpChecker<ScanAll>;
using ExpectScanAllByLabel = OpChecker<ScanAllByLabel>;
using ExpectExpand = OpChecker<Expand>;
using ExpectFilter = OpChecker<Filter>;
using ExpectConstructNamedPath = OpChecker<ConstructNamedPath>;
using ExpectProduce = OpChecker<Produce>;
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

class ExpectScanAllByLabelPropertyValue : public OpChecker<ScanAllByLabelPropertyValue> {
 public:
  ExpectScanAllByLabelPropertyValue(memgraph::storage::v3::LabelId label,
                                    const std::pair<std::string, memgraph::storage::v3::PropertyId> &prop_pair,
                                    memgraph::query::v2::Expression *expression)
      : label_(label), property_(prop_pair.second), expression_(expression) {}

  void ExpectOp(ScanAllByLabelPropertyValue &scan_all, const SymbolTable &) override {
    EXPECT_EQ(scan_all.label_, label_);
    EXPECT_EQ(scan_all.property_, property_);
    // TODO: Proper expression equality
    EXPECT_EQ(typeid(scan_all.expression_).hash_code(), typeid(expression_).hash_code());
  }

 private:
  memgraph::storage::v3::LabelId label_;
  memgraph::storage::v3::PropertyId property_;
  memgraph::query::v2::Expression *expression_;
};

class ExpectScanByPrimaryKey : public OpChecker<v2::plan::ScanByPrimaryKey> {
 public:
  ExpectScanByPrimaryKey(memgraph::storage::v3::LabelId label, const std::vector<Expression *> &properties)
      : label_(label), properties_(properties) {}

  void ExpectOp(v2::plan::ScanByPrimaryKey &scan_all, const SymbolTable &) override {
    EXPECT_EQ(scan_all.label_, label_);

    bool primary_property_match = true;
    for (const auto &expected_prop : properties_) {
      bool has_match = false;
      for (const auto &prop : scan_all.primary_key_) {
        if (typeid(prop).hash_code() == typeid(expected_prop).hash_code()) {
          has_match = true;
        }
      }
      if (!has_match) {
        primary_property_match = false;
      }
    }

    EXPECT_TRUE(primary_property_match);
  }

 private:
  memgraph::storage::v3::LabelId label_;
  std::vector<Expression *> properties_;
};

class ExpectCartesian : public OpChecker<Cartesian> {
 public:
  ExpectCartesian(const std::list<std::unique_ptr<BaseOpChecker>> &left,
                  const std::list<std::unique_ptr<BaseOpChecker>> &right)
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
  const std::list<std::unique_ptr<BaseOpChecker>> &left_;
  const std::list<std::unique_ptr<BaseOpChecker>> &right_;
};

class ExpectCallProcedure : public OpChecker<CallProcedure> {
 public:
  ExpectCallProcedure(const std::string &name, const std::vector<memgraph::query::Expression *> &args,
                      const std::vector<std::string> &fields, const std::vector<Symbol> &result_syms)
      : name_(name), args_(args), fields_(fields), result_syms_(result_syms) {}

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

template <class TPlanner, class TDbAccessor>
TPlanner MakePlanner(TDbAccessor *dba, AstStorage &storage, SymbolTable &symbol_table, CypherQuery *query) {
  auto planning_context = MakePlanningContext(&storage, &symbol_table, query, dba);
  auto query_parts = CollectQueryParts(symbol_table, storage, query);
  auto single_query_parts = query_parts.query_parts.at(0).single_query_parts;
  return TPlanner(single_query_parts, planning_context);
}

class FakeDistributedDbAccessor {
 public:
  int64_t VerticesCount(memgraph::storage::v3::LabelId label) const {
    auto found = label_index_.find(label);
    if (found != label_index_.end()) return found->second;
    return 0;
  }

  int64_t VerticesCount(memgraph::storage::v3::LabelId label, memgraph::storage::v3::PropertyId property) const {
    for (auto &index : label_property_index_) {
      if (std::get<0>(index) == label && std::get<1>(index) == property) {
        return std::get<2>(index);
      }
    }
    return 0;
  }

  bool LabelIndexExists(memgraph::storage::v3::LabelId label) const {
    throw utils::NotYetImplemented("Label indicies are yet to be implemented.");
  }

  bool LabelPropertyIndexExists(memgraph::storage::v3::LabelId label,
                                memgraph::storage::v3::PropertyId property) const {
    for (auto &index : label_property_index_) {
      if (std::get<0>(index) == label && std::get<1>(index) == property) {
        return true;
      }
    }
    return false;
  }

  bool PrimaryLabelExists(storage::v3::LabelId label) { return label_index_.find(label) != label_index_.end(); }

  void SetIndexCount(memgraph::storage::v3::LabelId label, int64_t count) { label_index_[label] = count; }

  void SetIndexCount(memgraph::storage::v3::LabelId label, memgraph::storage::v3::PropertyId property, int64_t count) {
    for (auto &index : label_property_index_) {
      if (std::get<0>(index) == label && std::get<1>(index) == property) {
        std::get<2>(index) = count;
        return;
      }
    }
    label_property_index_.emplace_back(label, property, count);
  }

  memgraph::storage::v3::LabelId NameToLabel(const std::string &name) {
    auto found = primary_labels_.find(name);
    if (found != primary_labels_.end()) return found->second;
    return primary_labels_.emplace(name, memgraph::storage::v3::LabelId::FromUint(primary_labels_.size()))
        .first->second;
  }

  memgraph::storage::v3::LabelId Label(const std::string &name) { return NameToLabel(name); }

  memgraph::storage::v3::EdgeTypeId NameToEdgeType(const std::string &name) {
    auto found = edge_types_.find(name);
    if (found != edge_types_.end()) return found->second;
    return edge_types_.emplace(name, memgraph::storage::v3::EdgeTypeId::FromUint(edge_types_.size())).first->second;
  }

  memgraph::storage::v3::PropertyId NameToPrimaryProperty(const std::string &name) {
    auto found = primary_properties_.find(name);
    if (found != primary_properties_.end()) return found->second;
    return primary_properties_.emplace(name, memgraph::storage::v3::PropertyId::FromUint(primary_properties_.size()))
        .first->second;
  }

  memgraph::storage::v3::PropertyId NameToSecondaryProperty(const std::string &name) {
    auto found = secondary_properties_.find(name);
    if (found != secondary_properties_.end()) return found->second;
    return secondary_properties_
        .emplace(name, memgraph::storage::v3::PropertyId::FromUint(secondary_properties_.size()))
        .first->second;
  }

  memgraph::storage::v3::PropertyId PrimaryProperty(const std::string &name) { return NameToPrimaryProperty(name); }
  memgraph::storage::v3::PropertyId SecondaryProperty(const std::string &name) { return NameToSecondaryProperty(name); }

  std::string PrimaryPropertyToName(memgraph::storage::v3::PropertyId property) const {
    for (const auto &kv : primary_properties_) {
      if (kv.second == property) return kv.first;
    }
    LOG_FATAL("Unable to find primary property name");
  }

  std::string SecondaryPropertyToName(memgraph::storage::v3::PropertyId property) const {
    for (const auto &kv : secondary_properties_) {
      if (kv.second == property) return kv.first;
    }
    LOG_FATAL("Unable to find secondary property name");
  }

  std::string PrimaryPropertyName(memgraph::storage::v3::PropertyId property) const {
    return PrimaryPropertyToName(property);
  }
  std::string SecondaryPropertyName(memgraph::storage::v3::PropertyId property) const {
    return SecondaryPropertyToName(property);
  }

  memgraph::storage::v3::PropertyId NameToProperty(const std::string &name) {
    auto find_in_prim_properties = primary_properties_.find(name);
    if (find_in_prim_properties != primary_properties_.end()) {
      return find_in_prim_properties->second;
    }
    auto find_in_secondary_properties = secondary_properties_.find(name);
    if (find_in_secondary_properties != secondary_properties_.end()) {
      return find_in_secondary_properties->second;
    }

    LOG_FATAL("The property does not exist as a primary or a secondary property.");
    return memgraph::storage::v3::PropertyId::FromUint(0);
  }

  std::vector<memgraph::storage::v3::SchemaProperty> GetSchemaForLabel(storage::v3::LabelId label) {
    auto schema_properties = schemas_.at(label);
    std::vector<memgraph::storage::v3::SchemaProperty> ret;
    std::transform(schema_properties.begin(), schema_properties.end(), std::back_inserter(ret), [](const auto &prop) {
      memgraph::storage::v3::SchemaProperty schema_prop = {
          .property_id = prop,
          // This should not be hardcoded, but for testing purposes it will suffice.
          .type = memgraph::common::SchemaType::INT};

      return schema_prop;
    });
    return ret;
  }

  std::vector<std::pair<query::v2::Expression *, query::v2::plan::FilterInfo>> ExtractPrimaryKey(
      storage::v3::LabelId label, std::vector<query::v2::plan::FilterInfo> property_filters) {
    MG_ASSERT(schemas_.contains(label),
              "You did not specify the Schema for this label! Use FakeDistributedDbAccessor::CreateSchema(...).");

    std::vector<std::pair<query::v2::Expression *, query::v2::plan::FilterInfo>> pk;
    const auto schema = GetSchemaPropertiesForLabel(label);

    std::vector<storage::v3::PropertyId> schema_properties;
    schema_properties.reserve(schema.size());

    std::transform(schema.begin(), schema.end(), std::back_inserter(schema_properties),
                   [](const auto &schema_elem) { return schema_elem; });

    for (const auto &property_filter : property_filters) {
      const auto &property_id = NameToProperty(property_filter.property_filter->property_.name);
      if (std::find(schema_properties.begin(), schema_properties.end(), property_id) != schema_properties.end()) {
        pk.emplace_back(std::make_pair(property_filter.expression, property_filter));
      }
    }

    return pk.size() == schema_properties.size()
               ? pk
               : std::vector<std::pair<query::v2::Expression *, query::v2::plan::FilterInfo>>{};
  }

  std::vector<memgraph::storage::v3::PropertyId> GetSchemaPropertiesForLabel(storage::v3::LabelId label) {
    return schemas_.at(label);
  }

  void CreateSchema(const memgraph::storage::v3::LabelId primary_label,
                    const std::vector<memgraph::storage::v3::PropertyId> &schemas_types) {
    MG_ASSERT(!schemas_.contains(primary_label), "You already created the schema for this label!");
    schemas_.emplace(primary_label, schemas_types);
  }

 private:
  std::unordered_map<std::string, memgraph::storage::v3::LabelId> primary_labels_;
  std::unordered_map<std::string, memgraph::storage::v3::LabelId> secondary_labels_;
  std::unordered_map<std::string, memgraph::storage::v3::EdgeTypeId> edge_types_;
  std::unordered_map<std::string, memgraph::storage::v3::PropertyId> primary_properties_;
  std::unordered_map<std::string, memgraph::storage::v3::PropertyId> secondary_properties_;

  std::unordered_map<memgraph::storage::v3::LabelId, int64_t> label_index_;
  std::vector<std::tuple<memgraph::storage::v3::LabelId, memgraph::storage::v3::PropertyId, int64_t>>
      label_property_index_;

  std::unordered_map<memgraph::storage::v3::LabelId, std::vector<memgraph::storage::v3::PropertyId>> schemas_;
};

}  // namespace memgraph::query::v2::plan
