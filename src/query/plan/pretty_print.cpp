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

#include "query/plan/pretty_print.hpp"
#include <utility>
#include <variant>

#include <nlohmann/json.hpp>

#include "query/db_accessor.hpp"
#include "query/frontend/ast/pretty_print.hpp"
#include "query/plan/operator.hpp"
#include "utils/string.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::query::plan {

namespace {

std::string ToString(EdgeAtom::Direction dir);

std::string ToString(EdgeAtom::Type type);

std::string ToString(Ordering ord);

nlohmann::json ToJson(Expression *expression, const DbAccessor &dba);

nlohmann::json ToJson(const utils::Bound<Expression *> &bound, const DbAccessor &dba);

nlohmann::json ToJson(const Symbol &symbol);

nlohmann::json ToJson(storage::EdgeTypeId edge_type, const DbAccessor &dba);

nlohmann::json ToJson(storage::LabelId label, const DbAccessor &dba);

nlohmann::json ToJson(storage::PropertyId property, const DbAccessor &dba);

nlohmann::json ToJson(storage::PropertyPath path, const DbAccessor &dba);

nlohmann::json ToJson(NamedExpression *nexpr, const DbAccessor &dba);

nlohmann::json ToJson(const std::vector<std::pair<storage::PropertyId, Expression *>> &properties,
                      const DbAccessor &dba);

nlohmann::json ToJson(const NodeCreationInfo &node_info, const DbAccessor &dba);

nlohmann::json ToJson(const EdgeCreationInfo &edge_info, const DbAccessor &dba);

nlohmann::json ToJson(const Aggregate::Element &elem, const DbAccessor &dba);

nlohmann::json ToJson(const ExpressionRange &expression_range, const DbAccessor &dba);

nlohmann::json ToJson(const std::vector<StorageLabelType> &labels, const DbAccessor &dba);

nlohmann::json ToJson(const StorageEdgeType &edge_type, const DbAccessor &dba);

template <class T, class... Args>
nlohmann::json ToJson(const std::vector<T> &items, Args &&...args);

class PlanToJsonVisitor : public virtual HierarchicalLogicalOperatorVisitor {
 public:
  explicit PlanToJsonVisitor(const DbAccessor *dba);

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool PreVisit(CreateNode & /*op*/) override;
  bool PreVisit(CreateExpand & /*op*/) override;
  bool PreVisit(Delete & /*op*/) override;

  bool PreVisit(SetProperty & /*op*/) override;
  bool PreVisit(SetProperties & /*op*/) override;
  bool PreVisit(SetLabels & /*op*/) override;

  bool PreVisit(RemoveProperty & /*op*/) override;
  bool PreVisit(RemoveLabels & /*op*/) override;

  bool PreVisit(Expand & /*op*/) override;
  bool PreVisit(ExpandVariable & /*op*/) override;

  bool PreVisit(ConstructNamedPath & /*op*/) override;

  bool PreVisit(Merge & /*op*/) override;
  bool PreVisit(Optional & /*op*/) override;

  bool PreVisit(Filter & /*op*/) override;
  bool PreVisit(EvaluatePatternFilter & /*op*/) override;
  bool PreVisit(EdgeUniquenessFilter & /*op*/) override;
  bool PreVisit(Cartesian & /*op*/) override;
  bool PreVisit(Apply & /*unused*/) override;
  bool PreVisit(HashJoin & /*op*/) override;
  bool PreVisit(IndexedJoin & /*unused*/) override;

  bool PreVisit(ScanAll & /*op*/) override;
  bool PreVisit(ScanAllByLabel & /*op*/) override;
  bool PreVisit(ScanAllByLabelProperties & /*op*/) override;
  bool PreVisit(ScanAllById & /*op*/) override;

  bool PreVisit(ScanAllByEdge & /*op*/) override;
  bool PreVisit(ScanAllByEdgeType & /*op*/) override;
  bool PreVisit(ScanAllByEdgeTypeProperty & /*op*/) override;
  bool PreVisit(ScanAllByEdgeTypePropertyValue & /*op*/) override;
  bool PreVisit(ScanAllByEdgeTypePropertyRange & /*op*/) override;
  bool PreVisit(ScanAllByEdgeProperty & /*op*/) override;
  bool PreVisit(ScanAllByEdgePropertyValue & /*op*/) override;
  bool PreVisit(ScanAllByEdgePropertyRange & /*op*/) override;
  bool PreVisit(ScanAllByEdgeId & /*op*/) override;

  bool PreVisit(EmptyResult & /*op*/) override;
  bool PreVisit(Produce & /*op*/) override;
  bool PreVisit(Accumulate & /*op*/) override;
  bool PreVisit(Aggregate & /*op*/) override;
  bool PreVisit(Skip & /*op*/) override;
  bool PreVisit(Limit & /*op*/) override;
  bool PreVisit(OrderBy & /*op*/) override;
  bool PreVisit(Distinct & /*op*/) override;
  bool PreVisit(Union & /*op*/) override;

  bool PreVisit(Unwind & /*op*/) override;
  bool PreVisit(Foreach & /*op*/) override;
  bool PreVisit(CallProcedure & /*op*/) override;
  bool PreVisit(LoadCsv & /*op*/) override;
  bool PreVisit(RollUpApply & /*op*/) override;
  bool PreVisit(PeriodicCommit & /*op*/) override;
  bool PreVisit(PeriodicSubquery & /*op*/) override;
  bool PreVisit(SetNestedProperty & /*op*/) override;
  bool PreVisit(RemoveNestedProperty & /*op*/) override;

  bool Visit(Once & /*unused*/) override;

  nlohmann::json output();

 protected:
  nlohmann::json output_;
  const DbAccessor *dba_;

  nlohmann::json PopOutput() {
    nlohmann::json tmp;
    tmp.swap(output_);
    return tmp;
  }
};

PlanToJsonVisitor::PlanToJsonVisitor(const DbAccessor *dba) : dba_(dba) {}

nlohmann::json PlanToJsonVisitor::output() { return output_; }

///////////////////////////////////////////////////////////////////////////////
//
// PlanToJsonVisitor implementation
//
// The JSON formatted plan is consumed (or will be) by Memgraph Lab, and
// therefore should not be changed before synchronizing with whoever is
// maintaining Memgraph Lab. Hopefully, one day integration tests will exist and
// there will be no need to be super careful.

bool PlanToJsonVisitor::Visit(Once & /*unused*/) {
  nlohmann::json self;
  self["name"] = "Once";

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAll &op) {
  nlohmann::json self;
  self["name"] = "ScanAll";
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByLabel &op) {
  nlohmann::json self;
  self["name"] = "ScanAllByLabel";
  self["label"] = ToJson(op.label_, *dba_);
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByLabelProperties &op) {
  nlohmann::json self;
  self["name"] = "ScanAllByLabelProperties";
  self["label"] = ToJson(op.label_, *dba_);
  self["properties"] = ToJson(op.properties_, *dba_);
  self["expression_ranges"] = ToJson(op.expression_ranges_, *dba_);
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllById &op) {
  nlohmann::json self;
  self["name"] = "ScanAllById";
  self["output_symbol"] = ToJson(op.output_symbol_);
  op.input_->Accept(*this);
  self["input"] = PopOutput();
  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByEdge &op) {
  nlohmann::json self;
  self["name"] = "ScanAllByEdge";
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByEdgeType &op) {
  nlohmann::json self;
  self["name"] = "ScanAllByEdgeType";
  self["edge_type"] = ToJson(op.common_.edge_types[0], *dba_);
  self["output_symbol"] = ToJson(op.common_.edge_symbol);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByEdgeTypeProperty &op) {
  nlohmann::json self;
  self["name"] = "ScanAllByEdgeTypeProperty";
  self["edge_type"] = ToJson(op.common_.edge_types[0], *dba_);
  self["property"] = ToJson(op.property_, *dba_);
  self["output_symbol"] = ToJson(op.common_.edge_symbol);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByEdgeTypePropertyValue &op) {
  nlohmann::json self;
  self["name"] = "ScanAllByEdgeTypePropertyValue";
  self["edge_type"] = ToJson(op.common_.edge_types[0], *dba_);
  self["property"] = ToJson(op.property_, *dba_);
  self["expression"] = ToJson(op.expression_, *dba_);
  self["output_symbol"] = ToJson(op.common_.edge_symbol);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByEdgeTypePropertyRange &op) {
  nlohmann::json self;
  self["name"] = "ScanAllByEdgeTypePropertyRange";
  self["edge_type"] = ToJson(op.common_.edge_types[0], *dba_);
  self["property"] = ToJson(op.property_, *dba_);
  self["lower_bound"] = op.lower_bound_ ? ToJson(*op.lower_bound_, *dba_) : nlohmann::json();
  self["upper_bound"] = op.upper_bound_ ? ToJson(*op.upper_bound_, *dba_) : nlohmann::json();
  self["output_symbol"] = ToJson(op.common_.edge_symbol);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByEdgeProperty &op) {
  nlohmann::json self;
  self["name"] = "ScanAllByEdgeProperty";
  self["property"] = ToJson(op.property_, *dba_);
  self["output_symbol"] = ToJson(op.common_.edge_symbol);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByEdgePropertyValue &op) {
  nlohmann::json self;
  self["name"] = "ScanAllByEdgePropertyValue";
  self["property"] = ToJson(op.property_, *dba_);
  self["expression"] = ToJson(op.expression_, *dba_);
  self["output_symbol"] = ToJson(op.common_.edge_symbol);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByEdgePropertyRange &op) {
  nlohmann::json self;
  self["name"] = "ScanAllByEdgePropertyRange";
  self["property"] = ToJson(op.property_, *dba_);
  self["lower_bound"] = op.lower_bound_ ? ToJson(*op.lower_bound_, *dba_) : nlohmann::json();
  self["upper_bound"] = op.upper_bound_ ? ToJson(*op.upper_bound_, *dba_) : nlohmann::json();
  self["output_symbol"] = ToJson(op.common_.edge_symbol);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByEdgeId &op) {
  nlohmann::json self;
  self["name"] = "ScanAllByEdgeId";
  self["output_symbol"] = ToJson(op.common_.edge_symbol);
  op.input_->Accept(*this);
  self["input"] = PopOutput();
  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(CreateNode &op) {
  nlohmann::json self;
  self["name"] = "CreateNode";
  self["node_info"] = ToJson(op.node_info_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(CreateExpand &op) {
  nlohmann::json self;
  self["name"] = "CreateExpand";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["node_info"] = ToJson(op.node_info_, *dba_);
  self["edge_info"] = ToJson(op.edge_info_, *dba_);
  self["existing_node"] = op.existing_node_;

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Expand &op) {
  nlohmann::json self;
  self["name"] = "Expand";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["node_symbol"] = ToJson(op.common_.node_symbol);
  self["edge_symbol"] = ToJson(op.common_.edge_symbol);
  self["edge_types"] = ToJson(op.common_.edge_types, *dba_);
  self["direction"] = ToString(op.common_.direction);
  self["existing_node"] = op.common_.existing_node;

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ExpandVariable &op) {
  nlohmann::json self;
  self["name"] = "ExpandVariable";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["node_symbol"] = ToJson(op.common_.node_symbol);
  self["edge_symbol"] = ToJson(op.common_.edge_symbol);
  self["edge_types"] = ToJson(op.common_.edge_types, *dba_);
  self["direction"] = ToString(op.common_.direction);
  self["type"] = ToString(op.type_);
  self["is_reverse"] = op.is_reverse_;
  self["lower_bound"] = op.lower_bound_ ? ToJson(op.lower_bound_, *dba_) : nlohmann::json();
  self["upper_bound"] = op.upper_bound_ ? ToJson(op.upper_bound_, *dba_) : nlohmann::json();
  if (op.type_ == EdgeAtom::Type::KSHORTEST) {
    self["limit"] = op.limit_ ? ToJson(op.limit_, *dba_) : nlohmann::json();
  }
  self["existing_node"] = op.common_.existing_node;

  self["filter_lambda"] = op.filter_lambda_.expression ? ToJson(op.filter_lambda_.expression, *dba_) : nlohmann::json();

  if (op.type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH || op.type_ == EdgeAtom::Type::ALL_SHORTEST_PATHS) {
    self["weight_lambda"] = ToJson(op.weight_lambda_->expression, *dba_);
    self["total_weight_symbol"] = ToJson(*op.total_weight_);
  }

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ConstructNamedPath &op) {
  nlohmann::json self;
  self["name"] = "ConstructNamedPath";
  self["path_symbol"] = ToJson(op.path_symbol_);
  self["path_elements"] = ToJson(op.path_elements_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Filter &op) {
  nlohmann::json self;
  self["name"] = "Filter";
  self["expression"] = ToJson(op.expression_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  for (auto pattern_idx = 0; pattern_idx < op.pattern_filters_.size(); pattern_idx++) {
    auto pattern_filter_key = "pattern_filter" + std::to_string(pattern_idx + 1);

    op.pattern_filters_[pattern_idx]->Accept(*this);
    self[pattern_filter_key] = PopOutput();
  }

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Produce &op) {
  nlohmann::json self;
  self["name"] = "Produce";
  self["named_expressions"] = ToJson(op.named_expressions_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Delete &op) {
  nlohmann::json self;
  self["name"] = "Delete";
  self["expressions"] = ToJson(op.expressions_, *dba_);
  self["detach"] = op.detach_;

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(SetProperty &op) {
  nlohmann::json self;
  self["name"] = "SetProperty";
  self["property"] = ToJson(op.property_, *dba_);
  self["lhs"] = ToJson(op.lhs_, *dba_);
  self["rhs"] = ToJson(op.rhs_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(SetNestedProperty &op) {
  nlohmann::json self;
  self["name"] = "SetNestedProperty";
  self["property_path"] = ToJson(op.property_path_, *dba_);
  self["lhs"] = ToJson(op.lhs_, *dba_);
  self["rhs"] = ToJson(op.rhs_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(SetProperties &op) {
  nlohmann::json self;
  self["name"] = "SetProperties";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["rhs"] = ToJson(op.rhs_, *dba_);

  switch (op.op_) {
    case SetProperties::Op::UPDATE:
      self["op"] = "update";
      break;
    case SetProperties::Op::REPLACE:
      self["op"] = "replace";
      break;
  }

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(SetLabels &op) {
  nlohmann::json self;
  self["name"] = "SetLabels";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["labels"] = ToJson(op.labels_, *dba_);
  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(RemoveProperty &op) {
  nlohmann::json self;
  self["name"] = "RemoveProperty";
  self["property"] = ToJson(op.property_, *dba_);
  self["lhs"] = ToJson(op.lhs_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(RemoveNestedProperty &op) {
  nlohmann::json self;
  self["name"] = "RemoveNestedProperty";
  self["property_path"] = ToJson(op.property_path_, *dba_);
  self["lhs"] = ToJson(op.lhs_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(RemoveLabels &op) {
  nlohmann::json self;
  self["name"] = "RemoveLabels";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["labels"] = ToJson(op.labels_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(EdgeUniquenessFilter &op) {
  nlohmann::json self;
  self["name"] = "EdgeUniquenessFilter";
  self["expand_symbol"] = ToJson(op.expand_symbol_);
  self["previous_symbols"] = ToJson(op.previous_symbols_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(EmptyResult &op) {
  nlohmann::json self;
  self["name"] = "EmptyResult";

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Accumulate &op) {
  nlohmann::json self;
  self["name"] = "Accumulate";
  self["symbols"] = ToJson(op.symbols_);
  self["advance_command"] = op.advance_command_;

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Aggregate &op) {
  nlohmann::json self;
  self["name"] = "Aggregate";
  self["aggregations"] = ToJson(op.aggregations_, *dba_);
  self["group_by"] = ToJson(op.group_by_, *dba_);
  self["remember"] = ToJson(op.remember_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Skip &op) {
  nlohmann::json self;
  self["name"] = "Skip";
  self["expression"] = ToJson(op.expression_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Limit &op) {
  nlohmann::json self;
  self["name"] = "Limit";
  self["expression"] = ToJson(op.expression_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(OrderBy &op) {
  nlohmann::json self;
  self["name"] = "OrderBy";

  for (auto i = 0; i < op.order_by_.size(); ++i) {
    nlohmann::json json;
    json["ordering"] = ToString(op.compare_.orderings()[i].ordering());
    json["expression"] = ToJson(op.order_by_[i], *dba_);
    self["order_by"].push_back(json);
  }
  self["output_symbols"] = ToJson(op.output_symbols_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Merge &op) {
  nlohmann::json self;
  self["name"] = "Merge";

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  op.merge_match_->Accept(*this);
  self["merge_match"] = PopOutput();

  op.merge_create_->Accept(*this);
  self["merge_create"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Optional &op) {
  nlohmann::json self;
  self["name"] = "Optional";
  self["optional_symbols"] = ToJson(op.optional_symbols_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  op.optional_->Accept(*this);
  self["optional"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Unwind &op) {
  nlohmann::json self;
  self["name"] = "Unwind";
  self["output_symbol"] = ToJson(op.output_symbol_);
  self["input_expression"] = ToJson(op.input_expression_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(query::plan::CallProcedure &op) {
  nlohmann::json self;
  self["name"] = "CallProcedure";
  self["procedure_name"] = op.procedure_name_;
  self["arguments"] = ToJson(op.arguments_, *dba_);
  self["result_fields"] = op.result_fields_;
  self["result_symbols"] = ToJson(op.result_symbols_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(query::plan::LoadCsv &op) {
  nlohmann::json self;
  self["name"] = "LoadCsv";

  if (op.file_) {
    self["file"] = ToJson(op.file_, *dba_);
  }

  if (op.with_header_) {
    self["with_header"] = op.with_header_;
  }

  if (op.ignore_bad_) {
    self["ignore_bad"] = op.ignore_bad_;
  }

  if (op.delimiter_) {
    self["delimiter"] = ToJson(op.delimiter_, *dba_);
  }

  if (op.quote_) {
    self["quote"] = ToJson(op.quote_, *dba_);
  }

  if (op.nullif_) {
    self["nullif"] = ToJson(op.nullif_, *dba_);
  }

  self["row_variable"] = ToJson(op.row_var_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Distinct &op) {
  nlohmann::json self;
  self["name"] = "Distinct";
  self["value_symbols"] = ToJson(op.value_symbols_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Union &op) {
  nlohmann::json self;
  self["name"] = "Union";
  self["union_symbols"] = ToJson(op.union_symbols_);
  self["left_symbols"] = ToJson(op.left_symbols_);
  self["right_symbols"] = ToJson(op.right_symbols_);

  op.left_op_->Accept(*this);
  self["left_op"] = PopOutput();

  op.right_op_->Accept(*this);
  self["right_op"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Cartesian &op) {
  nlohmann::json self;
  self["name"] = "Cartesian";
  self["left_symbols"] = ToJson(op.left_symbols_);
  self["right_symbols"] = ToJson(op.right_symbols_);

  op.left_op_->Accept(*this);
  self["left_op"] = PopOutput();

  op.right_op_->Accept(*this);
  self["right_op"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(HashJoin &op) {
  nlohmann::json self;
  self["name"] = "HashJoin";

  op.left_op_->Accept(*this);
  self["left_op"] = PopOutput();

  op.right_op_->Accept(*this);
  self["right_op"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Foreach &op) {
  nlohmann::json self;
  self["name"] = "Foreach";
  self["loop_variable_symbol"] = ToJson(op.loop_variable_symbol_);
  self["expression"] = ToJson(op.expression_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  op.update_clauses_->Accept(*this);
  self["update_clauses"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(EvaluatePatternFilter &op) {
  nlohmann::json self;
  self["name"] = "EvaluatePatternFilter";
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Apply &op) {
  nlohmann::json self;
  self["name"] = "Apply";

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  op.subquery_->Accept(*this);
  self["subquery"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(IndexedJoin &op) {
  nlohmann::json self;
  self["name"] = "IndexedJoin";

  op.main_branch_->Accept(*this);
  self["left"] = PopOutput();

  op.sub_branch_->Accept(*this);
  self["right"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(RollUpApply &op) {
  nlohmann::json self;
  self["name"] = "RollUpApply";
  self["output_symbol"] = ToJson(op.result_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  op.list_collection_branch_->Accept(*this);
  self["list_collection_branch"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(PeriodicCommit &op) {
  nlohmann::json self;
  self["name"] = "PeriodicCommit";

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(PeriodicSubquery &op) {
  nlohmann::json self;
  self["name"] = "PeriodicSubquery";

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  op.subquery_->Accept(*this);
  self["subquery"] = PopOutput();

  output_ = std::move(self);
  return false;
}

//////////////////////////// HELPER FUNCTIONS /////////////////////////////////
// TODO: It would be nice to have enum->string functions auto-generated.
std::string ToString(EdgeAtom::Direction dir) {
  switch (dir) {
    case EdgeAtom::Direction::BOTH:
      return "both";
    case EdgeAtom::Direction::IN:
      return "in";
    case EdgeAtom::Direction::OUT:
      return "out";
  }
}

std::string ToString(EdgeAtom::Type type) {
  switch (type) {
    case EdgeAtom::Type::BREADTH_FIRST:
      return "bfs";
    case EdgeAtom::Type::DEPTH_FIRST:
      return "dfs";
    case EdgeAtom::Type::WEIGHTED_SHORTEST_PATH:
      return "wsp";
    case EdgeAtom::Type::ALL_SHORTEST_PATHS:
      return "asp";
    case EdgeAtom::Type::KSHORTEST:
      return "shortest_first";
    case EdgeAtom::Type::SINGLE:
      return "single";
  }
}

std::string ToString(Ordering ord) {
  switch (ord) {
    case Ordering::ASC:
      return "asc";
    case Ordering::DESC:
      return "desc";
  }
}

nlohmann::json ToJson(Expression *expression, const DbAccessor &dba) {
  std::stringstream sstr;
  PrintExpression(expression, &sstr, dba);
  return sstr.str();
}

nlohmann::json ToJson(const utils::Bound<Expression *> &bound, const DbAccessor &dba) {
  nlohmann::json json;
  switch (bound.type()) {
    case utils::BoundType::INCLUSIVE:
      json["type"] = "inclusive";
      break;
    case utils::BoundType::EXCLUSIVE:
      json["type"] = "exclusive";
      break;
  }

  json["value"] = ToJson(bound.value(), dba);

  return json;
}

nlohmann::json ToJson(const Symbol &symbol) { return symbol.name(); }

nlohmann::json ToJson(storage::EdgeTypeId edge_type, const DbAccessor &dba) { return dba.EdgeTypeToName(edge_type); }

nlohmann::json ToJson(storage::LabelId label, const DbAccessor &dba) { return dba.LabelToName(label); }

nlohmann::json ToJson(storage::PropertyId property, const DbAccessor &dba) { return dba.PropertyToName(property); }

nlohmann::json ToJson(storage::PropertyPath path, const DbAccessor &dba) {
  return path | rv::transform([&](auto &&property_id) { return dba.PropertyToName(property_id); }) | rv::join('.') |
         r::to<std::string>;
}

nlohmann::json ToJson(NamedExpression *nexpr, const DbAccessor &dba) {
  nlohmann::json json;
  json["expression"] = ToJson(nexpr->expression_, dba);
  json["name"] = nexpr->name_;
  return json;
}

nlohmann::json ToJson(const PropertiesMapList &properties, const DbAccessor &dba) {
  nlohmann::json json;
  for (const auto &prop_pair : properties) {
    json.emplace(ToJson(prop_pair.first, dba), ToJson(prop_pair.second, dba));
  }
  return json;
}

nlohmann::json ToJson(const std::vector<StorageLabelType> &labels, const DbAccessor &dba) {
  nlohmann::json json;
  for (const auto &label : labels) {
    if (const auto *label_node = std::get_if<Expression *>(&label)) {
      json.emplace_back(ToJson(*label_node, dba));
    } else {
      json.emplace_back(ToJson(std::get<storage::LabelId>(label), dba));
    }
  }
  return json;
}

nlohmann::json ToJson(const StorageEdgeType &edge_type, const DbAccessor &dba) {
  if (const auto *edge_type_expression = std::get_if<Expression *>(&edge_type)) {
    return ToJson(*edge_type_expression, dba);
  }

  return ToJson(std::get<storage::EdgeTypeId>(edge_type), dba);
}

nlohmann::json ToJson(const NodeCreationInfo &node_info, const DbAccessor &dba) {
  nlohmann::json self;
  self["symbol"] = ToJson(node_info.symbol);
  self["labels"] = ToJson(node_info.labels, dba);
  const auto *props = std::get_if<PropertiesMapList>(&node_info.properties);
  self["properties"] = ToJson(props ? *props : PropertiesMapList{}, dba);
  return self;
}

nlohmann::json ToJson(const EdgeCreationInfo &edge_info, const DbAccessor &dba) {
  nlohmann::json self;
  self["symbol"] = ToJson(edge_info.symbol);
  const auto *props = std::get_if<PropertiesMapList>(&edge_info.properties);
  self["properties"] = ToJson(props ? *props : PropertiesMapList{}, dba);
  self["edge_type"] = ToJson(edge_info.edge_type, dba);
  self["direction"] = ToString(edge_info.direction);
  return self;
}

nlohmann::json ToJson(const Aggregate::Element &elem, const DbAccessor &dba) {
  nlohmann::json json;
  if (elem.op == Aggregation::Op::PROJECT_LISTS) {
    if (elem.arg1) {
      json["nodes"] = ToJson(elem.arg1, dba);
    }
    if (elem.arg2) {
      json["relationships"] = ToJson(elem.arg2, dba);
    }
  } else if (elem.op == Aggregation::Op::COLLECT_MAP) {
    if (elem.arg1) {
      json["value"] = ToJson(elem.arg1, dba);
    }
    if (elem.arg2) {
      json["key"] = ToJson(elem.arg2, dba);
    }
  } else {
    if (elem.arg1) {
      json["value"] = ToJson(elem.arg1, dba);
    }
  }

  json["op"] = utils::ToLowerCase(Aggregation::OpToString(elem.op));
  json["output_symbol"] = ToJson(elem.output_sym);
  json["distinct"] = elem.distinct;

  return json;
}

nlohmann::json ToJson(const ExpressionRange &expression_range, const DbAccessor &dba) {
  nlohmann::json result;
  switch (expression_range.type_) {
    case PropertyFilter::Type::EQUAL: {
      result["type"] = "Equal";
      result["expression"] = ToJson(expression_range.lower_->value(), dba);
      break;
    }
    case PropertyFilter::Type::REGEX_MATCH: {
      result["type"] = "Regex";
      break;
    }
    case PropertyFilter::Type::RANGE: {
      result["type"] = "Range";
      result["lower_bound"] = expression_range.lower_ ? ToJson(*expression_range.lower_, dba) : nlohmann::json();
      result["upper_bound"] = expression_range.upper_ ? ToJson(*expression_range.upper_, dba) : nlohmann::json();
      break;
    }
    case PropertyFilter::Type::IN: {
      result["type"] = "In";
      result["expression"] = ToJson(expression_range.lower_->value(), dba);
      break;
    }
    case PropertyFilter::Type::IS_NOT_NULL: {
      result["type"] = "IsNotNull";
      break;
    }
  }
  return result;
}

template <class T, class... Args>
nlohmann::json ToJson(const std::vector<T> &items, Args &&...args) {
  nlohmann::json json;
  for (const auto &item : items) {
    json.emplace_back(ToJson(item, std::forward<Args>(args)...));
  }
  return json;
}

}  // namespace
////////////////////////// END HELPER FUNCTIONS ////////////////////////////////

PlanPrinter::PlanPrinter(const DbAccessor *dba, std::ostream *out) : dba_(dba), out_(out) {}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define PRE_VISIT(TOp)                                                         \
  bool PlanPrinter::PreVisit(TOp &) { /* NOLINT(bugprone-macro-parentheses) */ \
    WithPrintLn([](auto &out) { out << "* " << #TOp; });                       \
    return true;                                                               \
  }

PRE_VISIT(CreateNode);

bool PlanPrinter::PreVisit(CreateExpand &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

PRE_VISIT(Delete);

bool PlanPrinter::PreVisit(query::plan::ScanAll &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByLabel &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByLabelProperties &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(ScanAllById &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByEdge &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByEdgeType &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByEdgeTypeProperty &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByEdgeTypePropertyValue &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByEdgeTypePropertyRange &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByEdgeProperty &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByEdgePropertyValue &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByEdgePropertyRange &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByEdgeId &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByPointDistance &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByPointWithinbbox &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::Expand &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ExpandVariable &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::Produce &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  return true;
}

PRE_VISIT(ConstructNamedPath);
PRE_VISIT(SetProperty);
PRE_VISIT(SetNestedProperty);
PRE_VISIT(RemoveNestedProperty);
PRE_VISIT(SetProperties);
PRE_VISIT(SetLabels);
PRE_VISIT(RemoveProperty);
PRE_VISIT(RemoveLabels);
PRE_VISIT(Accumulate);
PRE_VISIT(EmptyResult);
PRE_VISIT(EvaluatePatternFilter);

bool PlanPrinter::PreVisit(query::plan::Aggregate &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  return true;
}

PRE_VISIT(Skip);
PRE_VISIT(Limit);

bool PlanPrinter::PreVisit(query::plan::OrderBy &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::Merge &op) {
  WithPrintLn([](auto &out) { out << "* Merge"; });
  Branch(*op.merge_match_, "On Match");
  Branch(*op.merge_create_, "On Create");
  op.input_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::Optional &op) {
  WithPrintLn([](auto &out) { out << "* Optional"; });
  Branch(*op.optional_);
  op.input_->Accept(*this);
  return false;
}

PRE_VISIT(Unwind);
PRE_VISIT(Distinct);

bool PlanPrinter::PreVisit(query::plan::Union &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  Branch(*op.right_op_);
  op.left_op_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::RollUpApply &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  Branch(*op.list_collection_branch_);
  op.input_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::PeriodicCommit &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::CallProcedure &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::LoadCsv &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  return true;
}

bool PlanPrinter::Visit(query::plan::Once & /*op*/) {
  WithPrintLn([](auto &out) { out << "* Once"; });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::Cartesian &op) {
  WithPrintLn([&op](auto &out) {
    out << "* Cartesian {";
    utils::PrintIterable(out, op.left_symbols_, ", ", [](auto &out, const auto &sym) { out << sym.name(); });
    out << " : ";
    utils::PrintIterable(out, op.right_symbols_, ", ", [](auto &out, const auto &sym) { out << sym.name(); });
    out << "}";
  });
  Branch(*op.right_op_);
  op.left_op_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::HashJoin &op) {
  WithPrintLn([&](auto &out) { out << "* " << op.ToString(); });
  Branch(*op.right_op_);
  op.left_op_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::Foreach &op) {
  WithPrintLn([](auto &out) { out << "* Foreach"; });
  Branch(*op.update_clauses_);
  op.input_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::Filter &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  for (const auto &pattern_filter : op.pattern_filters_) {
    Branch(*pattern_filter);
  }
  op.input_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::EdgeUniquenessFilter &op) {
  WithPrintLn([&](auto &out) { out << "* " << op.ToString(); });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::Apply &op) {
  WithPrintLn([](auto &out) { out << "* Apply"; });
  Branch(*op.subquery_);
  op.input_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::PeriodicSubquery &op) {
  WithPrintLn([](auto &out) { out << "* PeriodicSubquery"; });
  Branch(*op.subquery_);
  op.input_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::IndexedJoin &op) {
  WithPrintLn([](auto &out) { out << "* IndexedJoin"; });
  Branch(*op.sub_branch_);
  op.main_branch_->Accept(*this);
  return false;
}
#undef PRE_VISIT

bool PlanPrinter::DefaultPreVisit() {
  WithPrintLn([](auto &out) { out << "* Unknown operator!"; });
  return true;
}

void PlanPrinter::Branch(query::plan::LogicalOperator &op, const std::string &branch_name) {
  WithPrintLn([&](auto &out) { out << "|\\ " << branch_name; });
  ++depth_;
  op.Accept(*this);
  --depth_;
}

void PrettyPrint(const DbAccessor &dba, const LogicalOperator *plan_root, std::ostream *out) {
  PlanPrinter printer(&dba, out);
  // FIXME(mtomic): We should make visitors that take const arguments.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  const_cast<LogicalOperator *>(plan_root)->Accept(printer);
}

nlohmann::json PlanToJson(const DbAccessor &dba, const LogicalOperator *plan_root) {
  PlanToJsonVisitor visitor(&dba);
  // FIXME(mtomic): We should make visitors that take const arguments.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  const_cast<LogicalOperator *>(plan_root)->Accept(visitor);
  return visitor.output();
}

}  // namespace memgraph::query::plan
