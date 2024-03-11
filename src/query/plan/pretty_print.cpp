// Copyright 2024 Memgraph Ltd.
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
#include <variant>

#include "query/db_accessor.hpp"
#include "query/frontend/ast/pretty_print.hpp"
#include "query/plan/operator.hpp"
#include "utils/string.hpp"

namespace memgraph::query::plan {

PlanPrinter::PlanPrinter(const DbAccessor *dba, std::ostream *out) : dba_(dba), out_(out) {}

#define PRE_VISIT(TOp)                                   \
  bool PlanPrinter::PreVisit(TOp &) {                    \
    WithPrintLn([](auto &out) { out << "* " << #TOp; }); \
    return true;                                         \
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

bool PlanPrinter::PreVisit(query::plan::ScanAllByLabelPropertyValue &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByLabelPropertyRange &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByLabelProperty &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(ScanAllById &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByEdgeType &op) {
  op.dba_ = dba_;
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
  op.dba_ = nullptr;
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByEdgeId &op) {
  WithPrintLn([&op](auto &out) { out << "* " << op.ToString(); });
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
  const_cast<LogicalOperator *>(plan_root)->Accept(printer);
}

nlohmann::json PlanToJson(const DbAccessor &dba, const LogicalOperator *plan_root) {
  impl::PlanToJsonVisitor visitor(&dba);
  // FIXME(mtomic): We should make visitors that take const arguments.
  const_cast<LogicalOperator *>(plan_root)->Accept(visitor);
  return visitor.output();
}

namespace impl {

///////////////////////////////////////////////////////////////////////////////
//
// PlanToJsonVisitor implementation
//
// The JSON formatted plan is consumed (or will be) by Memgraph Lab, and
// therefore should not be changed before synchronizing with whoever is
// maintaining Memgraph Lab. Hopefully, one day integration tests will exist and
// there will be no need to be super careful.

using nlohmann::json;

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

json ToJson(Expression *expression) {
  std::stringstream sstr;
  PrintExpression(expression, &sstr);
  return sstr.str();
}

json ToJson(const utils::Bound<Expression *> &bound) {
  json json;
  switch (bound.type()) {
    case utils::BoundType::INCLUSIVE:
      json["type"] = "inclusive";
      break;
    case utils::BoundType::EXCLUSIVE:
      json["type"] = "exclusive";
      break;
  }

  json["value"] = ToJson(bound.value());

  return json;
}

json ToJson(const Symbol &symbol) { return symbol.name(); }

json ToJson(storage::EdgeTypeId edge_type, const DbAccessor &dba) { return dba.EdgeTypeToName(edge_type); }

json ToJson(storage::LabelId label, const DbAccessor &dba) { return dba.LabelToName(label); }

json ToJson(storage::PropertyId property, const DbAccessor &dba) { return dba.PropertyToName(property); }

json ToJson(NamedExpression *nexpr) {
  json json;
  json["expression"] = ToJson(nexpr->expression_);
  json["name"] = nexpr->name_;
  return json;
}

json ToJson(const std::vector<std::pair<storage::PropertyId, Expression *>> &properties, const DbAccessor &dba) {
  json json;
  for (const auto &prop_pair : properties) {
    json.emplace(ToJson(prop_pair.first, dba), ToJson(prop_pair.second));
  }
  return json;
}

json ToJson(const NodeCreationInfo &node_info, const DbAccessor &dba) {
  json self;
  self["symbol"] = ToJson(node_info.symbol);
  self["labels"] = ToJson(node_info.labels, dba);
  const auto *props = std::get_if<PropertiesMapList>(&node_info.properties);
  self["properties"] = ToJson(props ? *props : PropertiesMapList{}, dba);
  return self;
}

json ToJson(const EdgeCreationInfo &edge_info, const DbAccessor &dba) {
  json self;
  self["symbol"] = ToJson(edge_info.symbol);
  const auto *props = std::get_if<PropertiesMapList>(&edge_info.properties);
  self["properties"] = ToJson(props ? *props : PropertiesMapList{}, dba);
  self["edge_type"] = ToJson(edge_info.edge_type, dba);
  self["direction"] = ToString(edge_info.direction);
  return self;
}

json ToJson(const Aggregate::Element &elem) {
  json json;
  if (elem.value) {
    json["value"] = ToJson(elem.value);
  }
  if (elem.key) {
    json["key"] = ToJson(elem.key);
  }
  json["op"] = utils::ToLowerCase(Aggregation::OpToString(elem.op));
  json["output_symbol"] = ToJson(elem.output_sym);
  json["distinct"] = elem.distinct;

  return json;
}
////////////////////////// END HELPER FUNCTIONS ////////////////////////////////

bool PlanToJsonVisitor::Visit(Once &) {
  json self;
  self["name"] = "Once";

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAll &op) {
  json self;
  self["name"] = "ScanAll";
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByLabel &op) {
  json self;
  self["name"] = "ScanAllByLabel";
  self["label"] = ToJson(op.label_, *dba_);
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByLabelPropertyRange &op) {
  json self;
  self["name"] = "ScanAllByLabelPropertyRange";
  self["label"] = ToJson(op.label_, *dba_);
  self["property"] = ToJson(op.property_, *dba_);
  self["lower_bound"] = op.lower_bound_ ? ToJson(*op.lower_bound_) : json();
  self["upper_bound"] = op.upper_bound_ ? ToJson(*op.upper_bound_) : json();
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByLabelPropertyValue &op) {
  json self;
  self["name"] = "ScanAllByLabelPropertyValue";
  self["label"] = ToJson(op.label_, *dba_);
  self["property"] = ToJson(op.property_, *dba_);
  self["expression"] = ToJson(op.expression_);
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByLabelProperty &op) {
  json self;
  self["name"] = "ScanAllByLabelProperty";
  self["label"] = ToJson(op.label_, *dba_);
  self["property"] = ToJson(op.property_, *dba_);
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllById &op) {
  json self;
  self["name"] = "ScanAllById";
  self["output_symbol"] = ToJson(op.output_symbol_);
  op.input_->Accept(*this);
  self["input"] = PopOutput();
  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByEdgeType &op) {
  json self;
  self["name"] = "ScanAllByEdgeType";
  self["edge_type"] = ToJson(op.edge_type_, *dba_);
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ScanAllByEdgeId &op) {
  json self;
  self["name"] = "ScanAllByEdgeId";
  self["output_symbol"] = ToJson(op.output_symbol_);
  op.input_->Accept(*this);
  self["input"] = PopOutput();
  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(CreateNode &op) {
  json self;
  self["name"] = "CreateNode";
  self["node_info"] = ToJson(op.node_info_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(CreateExpand &op) {
  json self;
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
  json self;
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
  json self;
  self["name"] = "ExpandVariable";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["node_symbol"] = ToJson(op.common_.node_symbol);
  self["edge_symbol"] = ToJson(op.common_.edge_symbol);
  self["edge_types"] = ToJson(op.common_.edge_types, *dba_);
  self["direction"] = ToString(op.common_.direction);
  self["type"] = ToString(op.type_);
  self["is_reverse"] = op.is_reverse_;
  self["lower_bound"] = op.lower_bound_ ? ToJson(op.lower_bound_) : json();
  self["upper_bound"] = op.upper_bound_ ? ToJson(op.upper_bound_) : json();
  self["existing_node"] = op.common_.existing_node;

  self["filter_lambda"] = op.filter_lambda_.expression ? ToJson(op.filter_lambda_.expression) : json();

  if (op.type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH || op.type_ == EdgeAtom::Type::ALL_SHORTEST_PATHS) {
    self["weight_lambda"] = ToJson(op.weight_lambda_->expression);
    self["total_weight_symbol"] = ToJson(*op.total_weight_);
  }

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(ConstructNamedPath &op) {
  json self;
  self["name"] = "ConstructNamedPath";
  self["path_symbol"] = ToJson(op.path_symbol_);
  self["path_elements"] = ToJson(op.path_elements_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Filter &op) {
  json self;
  self["name"] = "Filter";
  self["expression"] = ToJson(op.expression_);

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
  json self;
  self["name"] = "Produce";
  self["named_expressions"] = ToJson(op.named_expressions_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Delete &op) {
  json self;
  self["name"] = "Delete";
  self["expressions"] = ToJson(op.expressions_);
  self["detach"] = op.detach_;

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(SetProperty &op) {
  json self;
  self["name"] = "SetProperty";
  self["property"] = ToJson(op.property_, *dba_);
  self["lhs"] = ToJson(op.lhs_);
  self["rhs"] = ToJson(op.rhs_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(SetProperties &op) {
  json self;
  self["name"] = "SetProperties";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["rhs"] = ToJson(op.rhs_);

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
  json self;
  self["name"] = "SetLabels";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["labels"] = ToJson(op.labels_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(RemoveProperty &op) {
  json self;
  self["name"] = "RemoveProperty";
  self["property"] = ToJson(op.property_, *dba_);
  self["lhs"] = ToJson(op.lhs_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(RemoveLabels &op) {
  json self;
  self["name"] = "RemoveLabels";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["labels"] = ToJson(op.labels_, *dba_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(EdgeUniquenessFilter &op) {
  json self;
  self["name"] = "EdgeUniquenessFilter";
  self["expand_symbol"] = ToJson(op.expand_symbol_);
  self["previous_symbols"] = ToJson(op.previous_symbols_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(EmptyResult &op) {
  json self;
  self["name"] = "EmptyResult";

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Accumulate &op) {
  json self;
  self["name"] = "Accumulate";
  self["symbols"] = ToJson(op.symbols_);
  self["advance_command"] = op.advance_command_;

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Aggregate &op) {
  json self;
  self["name"] = "Aggregate";
  self["aggregations"] = ToJson(op.aggregations_);
  self["group_by"] = ToJson(op.group_by_);
  self["remember"] = ToJson(op.remember_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Skip &op) {
  json self;
  self["name"] = "Skip";
  self["expression"] = ToJson(op.expression_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Limit &op) {
  json self;
  self["name"] = "Limit";
  self["expression"] = ToJson(op.expression_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(OrderBy &op) {
  json self;
  self["name"] = "OrderBy";

  for (auto i = 0; i < op.order_by_.size(); ++i) {
    json json;
    json["ordering"] = ToString(op.compare_.ordering_[i]);
    json["expression"] = ToJson(op.order_by_[i]);
    self["order_by"].push_back(json);
  }
  self["output_symbols"] = ToJson(op.output_symbols_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Merge &op) {
  json self;
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
  json self;
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
  json self;
  self["name"] = "Unwind";
  self["output_symbol"] = ToJson(op.output_symbol_);
  self["input_expression"] = ToJson(op.input_expression_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(query::plan::CallProcedure &op) {
  json self;
  self["name"] = "CallProcedure";
  self["procedure_name"] = op.procedure_name_;
  self["arguments"] = ToJson(op.arguments_);
  self["result_fields"] = op.result_fields_;
  self["result_symbols"] = ToJson(op.result_symbols_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(query::plan::LoadCsv &op) {
  json self;
  self["name"] = "LoadCsv";

  if (op.file_) {
    self["file"] = ToJson(op.file_);
  }

  if (op.with_header_) {
    self["with_header"] = op.with_header_;
  }

  if (op.ignore_bad_) {
    self["ignore_bad"] = op.ignore_bad_;
  }

  if (op.delimiter_) {
    self["delimiter"] = ToJson(op.delimiter_);
  }

  if (op.quote_) {
    self["quote"] = ToJson(op.quote_);
  }

  if (op.nullif_) {
    self["nullif"] = ToJson(op.nullif_);
  }

  self["row_variable"] = ToJson(op.row_var_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Distinct &op) {
  json self;
  self["name"] = "Distinct";
  self["value_symbols"] = ToJson(op.value_symbols_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Union &op) {
  json self;
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
  json self;
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
  json self;
  self["name"] = "HashJoin";

  op.left_op_->Accept(*this);
  self["left_op"] = PopOutput();

  op.right_op_->Accept(*this);
  self["right_op"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Foreach &op) {
  json self;
  self["name"] = "Foreach";
  self["loop_variable_symbol"] = ToJson(op.loop_variable_symbol_);
  self["expression"] = ToJson(op.expression_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  op.update_clauses_->Accept(*this);
  self["update_clauses"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(EvaluatePatternFilter &op) {
  json self;
  self["name"] = "EvaluatePatternFilter";
  self["output_symbol"] = ToJson(op.output_symbol_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(Apply &op) {
  json self;
  self["name"] = "Apply";

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  op.subquery_->Accept(*this);
  self["subquery"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool PlanToJsonVisitor::PreVisit(IndexedJoin &op) {
  json self;
  self["name"] = "IndexedJoin";

  op.main_branch_->Accept(*this);
  self["left"] = PopOutput();

  op.sub_branch_->Accept(*this);
  self["right"] = PopOutput();

  output_ = std::move(self);
  return false;
}

}  // namespace impl

}  // namespace memgraph::query::plan
