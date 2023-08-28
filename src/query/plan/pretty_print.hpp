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

/// @file
#pragma once

#include <iostream>

#include <json/json.hpp>

#include "query/plan/operator.hpp"

namespace memgraph::query {
class DbAccessor;
class PlanPrinter;

using nlohmann::json;

namespace plan {

class LogicalOperator;

template <class TDbAccessor>
class PlanPrinter : public virtual HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  PlanPrinter(const TDbAccessor *dba, std::ostream *out) : dba_(dba), out_(out) {}

#define PRE_VISIT(TOp)                                   \
  bool PreVisit(TOp &) override {                        \
    WithPrintLn([](auto &out) { out << "* " << #TOp; }); \
    return true;                                         \
  }

  PRE_VISIT(CreateNode);

  bool PreVisit(CreateExpand &op) override {
    WithPrintLn([&](auto &out) {
      out << "* CreateExpand (" << op.input_symbol_.name() << ")"
          << (op.edge_info_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-") << "["
          << op.edge_info_.symbol.name() << ":" << dba_->EdgeTypeToName(op.edge_info_.edge_type) << "]"
          << (op.edge_info_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-") << "("
          << op.node_info_.symbol.name() << ")";
    });
    return true;
  }

  PRE_VISIT(Delete);

  PRE_VISIT(ConstructNamedPath);
  PRE_VISIT(SetProperty);
  PRE_VISIT(SetProperties);
  PRE_VISIT(SetLabels);
  PRE_VISIT(RemoveProperty);
  PRE_VISIT(RemoveLabels);
  PRE_VISIT(EdgeUniquenessFilter);
  PRE_VISIT(Accumulate);
  PRE_VISIT(EmptyResult);
  PRE_VISIT(EvaluatePatternFilter);

  bool PreVisit(query::plan::Aggregate &op) override {
    WithPrintLn([&](auto &out) {
      out << "* Aggregate {";
      utils::PrintIterable(out, op.aggregations_, ", ",
                           [](auto &out, const auto &aggr) { out << aggr.output_sym.name(); });
      out << "} {";
      utils::PrintIterable(out, op.remember_, ", ", [](auto &out, const auto &sym) { out << sym.name(); });
      out << "}";
    });
    return true;
  }

  PRE_VISIT(Skip);
  PRE_VISIT(Limit);

  bool PreVisit(query::plan::OrderBy &op) override {
    WithPrintLn([&op](auto &out) {
      out << "* OrderBy {";
      utils::PrintIterable(out, op.output_symbols_, ", ", [](auto &out, const auto &sym) { out << sym.name(); });
      out << "}";
    });
    return true;
  }

  bool PreVisit(query::plan::Merge &op) override {
    WithPrintLn([](auto &out) { out << "* Merge"; });
    Branch(*op.merge_match_, "On Match");
    Branch(*op.merge_create_, "On Create");
    op.input_->Accept(*this);
    return false;
  }

  bool PreVisit(query::plan::Optional &op) override {
    WithPrintLn([](auto &out) { out << "* Optional"; });
    Branch(*op.optional_);
    op.input_->Accept(*this);
    return false;
  }

  PRE_VISIT(Unwind);
  PRE_VISIT(Distinct);

  bool PreVisit(query::plan::Union &op) override {
    WithPrintLn([&op](auto &out) {
      out << "* Union {";
      utils::PrintIterable(out, op.left_symbols_, ", ", [](auto &out, const auto &sym) { out << sym.name(); });
      out << " : ";
      utils::PrintIterable(out, op.right_symbols_, ", ", [](auto &out, const auto &sym) { out << sym.name(); });
      out << "}";
    });
    Branch(*op.right_op_);
    op.left_op_->Accept(*this);
    return false;
  }

  bool PreVisit(query::plan::CallProcedure &op) override {
    WithPrintLn([&op](auto &out) {
      out << "* CallProcedure<" << op.procedure_name_ << "> {";
      utils::PrintIterable(out, op.result_symbols_, ", ", [](auto &out, const auto &sym) { out << sym.name(); });
      out << "}";
    });
    return true;
  }

  bool PreVisit(query::plan::LoadCsv &op) override {
    WithPrintLn([&op](auto &out) { out << "* LoadCsv {" << op.row_var_.name() << "}"; });
    return true;
  }

  bool Visit(query::plan::Once & /*op*/) override {
    WithPrintLn([](auto &out) { out << "* Once"; });
    return true;
  }

  bool PreVisit(query::plan::Cartesian &op) override {
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

  bool PreVisit(query::plan::Foreach &op) override {
    WithPrintLn([](auto &out) { out << "* Foreach"; });
    Branch(*op.update_clauses_);
    op.input_->Accept(*this);
    return false;
  }

  bool PreVisit(query::plan::Filter &op) override {
    WithPrintLn([](auto &out) { out << "* Filter"; });
    for (const auto &pattern_filter : op.pattern_filters_) {
      Branch(*pattern_filter);
    }
    op.input_->Accept(*this);
    return false;
  }

  bool PreVisit(query::plan::Apply &op) override {
    WithPrintLn([](auto &out) { out << "* Apply"; });
    Branch(*op.subquery_);
    op.input_->Accept(*this);
    return false;
  }
#undef PRE_VISIT

  bool DefaultPreVisit() override {
    WithPrintLn([](auto &out) { out << "* Unknown operator!"; });
    return true;
  }

  bool PreVisit(query::plan::ScanAll &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAll"
          << " (" << op.output_symbol_.name() << ")";
    });
    return true;
  }

  bool PreVisit(query::plan::ScanAllByLabel &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAllByLabel"
          << " (" << op.output_symbol_.name() << " :" << dba_->LabelToName(op.label_) << ")";
    });
    return true;
  }

  bool PreVisit(query::plan::ScanAllByLabelPropertyValue &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAllByLabelPropertyValue"
          << " (" << op.output_symbol_.name() << " :" << dba_->LabelToName(op.label_) << " {"
          << dba_->PropertyToName(op.property_) << "})";
    });
    return true;
  }

  bool PreVisit(query::plan::ScanAllByLabelPropertyRange &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAllByLabelPropertyRange"
          << " (" << op.output_symbol_.name() << " :" << dba_->LabelToName(op.label_) << " {"
          << dba_->PropertyToName(op.property_) << "})";
    });
    return true;
  }

  bool PreVisit(query::plan::ScanAllByLabelProperty &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAllByLabelProperty"
          << " (" << op.output_symbol_.name() << " :" << dba_->LabelToName(op.label_) << " {"
          << dba_->PropertyToName(op.property_) << "})";
    });
    return true;
  }

  bool PreVisit(ScanAllById &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAllById"
          << " (" << op.output_symbol_.name() << ")";
    });
    return true;
  }

  bool PreVisit(query::plan::Expand &op) override {
    WithPrintLn([&](auto &out) {
      *out_ << "* Expand (" << op.input_symbol_.name() << ")"
            << (op.common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-") << "["
            << op.common_.edge_symbol.name();
      utils::PrintIterable(*out_, op.common_.edge_types, "|", [this](auto &stream, const auto &edge_type) {
        stream << ":" << dba_->EdgeTypeToName(edge_type);
      });
      *out_ << "]" << (op.common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-") << "("
            << op.common_.node_symbol.name() << ")";
    });
    return true;
  }

  bool PreVisit(query::plan::ExpandVariable &op) override {
    using Type = query::EdgeAtom::Type;
    WithPrintLn([&](auto &out) {
      *out_ << "* ";
      switch (op.type_) {
        case Type::DEPTH_FIRST:
          *out_ << "ExpandVariable";
          break;
        case Type::BREADTH_FIRST:
          *out_ << (op.common_.existing_node ? "STShortestPath" : "BFSExpand");
          break;
        case Type::WEIGHTED_SHORTEST_PATH:
          *out_ << "WeightedShortestPath";
          break;
        case Type::ALL_SHORTEST_PATHS:
          *out_ << "AllShortestPaths";
          break;
        case Type::SINGLE:
          LOG_FATAL("Unexpected ExpandVariable::type_");
      }
      *out_ << " (" << op.input_symbol_.name() << ")"
            << (op.common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-") << "["
            << op.common_.edge_symbol.name();
      utils::PrintIterable(*out_, op.common_.edge_types, "|", [this](auto &stream, const auto &edge_type) {
        stream << ":" << dba_->EdgeTypeToName(edge_type);
      });
      *out_ << "]" << (op.common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-") << "("
            << op.common_.node_symbol.name() << ")";
    });
    return true;
  }

  bool PreVisit(query::plan::Produce &op) override {
    WithPrintLn([&](auto &out) {
      out << "* Produce {";
      utils::PrintIterable(out, op.named_expressions_, ", ", [](auto &out, const auto &nexpr) { out << nexpr->name_; });
      out << "}";
    });
    return true;
  }

  /// Call fun with output stream. The stream is prefixed with amount of spaces
  /// corresponding to the current depth_.
  template <class TFun>
  void WithPrintLn(TFun fun) {
    *out_ << " ";
    for (int64_t i = 0; i < depth_; ++i) {
      *out_ << "| ";
    }
    fun(*out_);
    *out_ << std::endl;
  }

  /// Forward this printer to another operator branch by incrementing the depth
  /// and printing the branch name.
  void Branch(query::plan::LogicalOperator &op, const std::string &branch_name = "") {
    WithPrintLn([&](auto &out) { out << "|\\ " << branch_name; });
    ++depth_;
    op.Accept(*this);
    --depth_;
  }

  int64_t depth_{0};
  const TDbAccessor *dba_{nullptr};
  std::ostream *out_{nullptr};
};

namespace impl {

std::string ToString(EdgeAtom::Direction dir);

std::string ToString(EdgeAtom::Type type);

std::string ToString(Ordering ord);

nlohmann::json ToJson(Expression *expression);

nlohmann::json ToJson(const utils::Bound<Expression *> &bound);

nlohmann::json ToJson(const Symbol &symbol);

nlohmann::json ToJson(storage::EdgeTypeId edge_type, const DbAccessor &dba);

nlohmann::json ToJson(storage::LabelId label, const DbAccessor &dba);

nlohmann::json ToJson(storage::PropertyId property, const DbAccessor &dba);

nlohmann::json ToJson(NamedExpression *nexpr);

nlohmann::json ToJson(const std::vector<std::pair<storage::PropertyId, Expression *>> &properties,
                      const DbAccessor &dba);

nlohmann::json ToJson(const NodeCreationInfo &node_info, const DbAccessor &dba);

nlohmann::json ToJson(const EdgeCreationInfo &edge_info, const DbAccessor &dba);

nlohmann::json ToJson(const Aggregate::Element &elem);

template <class T, class... Args>
nlohmann::json ToJson(const std::vector<T> &items, Args &&...args) {
  nlohmann::json json;
  for (const auto &item : items) {
    json.emplace_back(ToJson(item, std::forward<Args>(args)...));
  }
  return json;
}

template <class TDbAccessor>
class PlanToJsonVisitor : public virtual HierarchicalLogicalOperatorVisitor {
 public:
  explicit PlanToJsonVisitor(const TDbAccessor *dba) : dba_(dba) {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override {
    json self;
    self["name"] = "Once";

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(ScanAll &op) override {
    json self;
    self["name"] = "ScanAll";
    self["output_symbol"] = ToJson(op.output_symbol_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(ScanAllByLabel &op) override {
    json self;
    self["name"] = "ScanAllByLabel";
    self["label"] = ToJson(op.label_, *dba_);
    self["output_symbol"] = ToJson(op.output_symbol_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(ScanAllByLabelPropertyRange &op) override {
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

  bool PreVisit(ScanAllByLabelPropertyValue &op) override {
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

  bool PreVisit(ScanAllByLabelProperty &op) override {
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

  bool PreVisit(ScanAllById &op) override {
    json self;
    self["name"] = "ScanAllById";
    self["output_symbol"] = ToJson(op.output_symbol_);
    op.input_->Accept(*this);
    self["input"] = PopOutput();
    output_ = std::move(self);
    return false;
  }

  bool PreVisit(CreateNode &op) override {
    json self;
    self["name"] = "CreateNode";
    self["node_info"] = ToJson(op.node_info_, *dba_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(CreateExpand &op) override {
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

  bool PreVisit(Expand &op) override {
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

  bool PreVisit(ExpandVariable &op) override {
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

  bool PreVisit(ConstructNamedPath &op) override {
    json self;
    self["name"] = "ConstructNamedPath";
    self["path_symbol"] = ToJson(op.path_symbol_);
    self["path_elements"] = ToJson(op.path_elements_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(Filter &op) override {
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

  bool PreVisit(Produce &op) override {
    json self;
    self["name"] = "Produce";
    self["named_expressions"] = ToJson(op.named_expressions_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(Delete &op) override {
    json self;
    self["name"] = "Delete";
    self["expressions"] = ToJson(op.expressions_);
    self["detach"] = op.detach_;

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(SetProperty &op) override {
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

  bool PreVisit(SetProperties &op) override {
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

  bool PreVisit(SetLabels &op) override {
    json self;
    self["name"] = "SetLabels";
    self["input_symbol"] = ToJson(op.input_symbol_);
    self["labels"] = ToJson(op.labels_, *dba_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(RemoveProperty &op) override {
    json self;
    self["name"] = "RemoveProperty";
    self["property"] = ToJson(op.property_, *dba_);
    self["lhs"] = ToJson(op.lhs_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(RemoveLabels &op) override {
    json self;
    self["name"] = "RemoveLabels";
    self["input_symbol"] = ToJson(op.input_symbol_);
    self["labels"] = ToJson(op.labels_, *dba_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(EdgeUniquenessFilter &op) override {
    json self;
    self["name"] = "EdgeUniquenessFilter";
    self["expand_symbol"] = ToJson(op.expand_symbol_);
    self["previous_symbols"] = ToJson(op.previous_symbols_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(EmptyResult &op) override {
    json self;
    self["name"] = "EmptyResult";

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(Accumulate &op) override {
    json self;
    self["name"] = "Accumulate";
    self["symbols"] = ToJson(op.symbols_);
    self["advance_command"] = op.advance_command_;

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(Aggregate &op) override {
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

  bool PreVisit(Skip &op) override {
    json self;
    self["name"] = "Skip";
    self["expression"] = ToJson(op.expression_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(Limit &op) override {
    json self;
    self["name"] = "Limit";
    self["expression"] = ToJson(op.expression_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(OrderBy &op) override {
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

  bool PreVisit(Merge &op) override {
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

  bool PreVisit(Optional &op) override {
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

  bool PreVisit(Unwind &op) override {
    json self;
    self["name"] = "Unwind";
    self["output_symbol"] = ToJson(op.output_symbol_);
    self["input_expression"] = ToJson(op.input_expression_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(query::plan::CallProcedure &op) override {
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

  bool PreVisit(query::plan::LoadCsv &op) override {
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

  bool PreVisit(Distinct &op) override {
    json self;
    self["name"] = "Distinct";
    self["value_symbols"] = ToJson(op.value_symbols_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(Union &op) override {
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

  bool PreVisit(Cartesian &op) override {
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

  bool PreVisit(EvaluatePatternFilter &op) override {
    json self;
    self["name"] = "EvaluatePatternFilter";
    self["output_symbol"] = ToJson(op.output_symbol_);

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    output_ = std::move(self);
    return false;
  }
  bool PreVisit(Apply &op) override {
    json self;
    self["name"] = "Apply";

    op.input_->Accept(*this);
    self["input"] = PopOutput();

    op.subquery_->Accept(*this);
    self["subquery"] = PopOutput();

    output_ = std::move(self);
    return false;
  }

  bool PreVisit(Foreach &op) override {
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

  nlohmann::json output() { return output_; }

 protected:
  nlohmann::json output_;
  const TDbAccessor *dba_;

  nlohmann::json PopOutput() {
    nlohmann::json tmp;
    tmp.swap(output_);
    return tmp;
  }
};

}  // namespace impl

/// Pretty print a `LogicalOperator` plan to a `std::ostream`.
/// DbAccessor is needed for resolving label and property names.
/// Note that `plan_root` isn't modified, but we can't take it as a const
/// because we don't have support for visiting a const LogicalOperator.
template <class TDbAccessor>
void PrettyPrint(const TDbAccessor &dba, const LogicalOperator *plan_root, std::ostream *out) {
  PlanPrinter printer(&dba, out);
  // FIXME(mtomic): We should make visitors that take const arguments.
  const_cast<LogicalOperator *>(plan_root)->Accept(printer);
}

/// Overload of `PrettyPrint` which defaults the `std::ostream` to `std::cout`.
template <class TDbAccessor>
inline void PrettyPrint(const TDbAccessor &dba, const LogicalOperator *plan_root) {
  PrettyPrint(dba, plan_root, &std::cout);
}

/// Convert a `LogicalOperator` plan to a JSON representation.
/// DbAccessor is needed for resolving label and property names.
template <class TDbAccessor>
nlohmann::json PlanToJson(const TDbAccessor &dba, const LogicalOperator *plan_root) {
  impl::PlanToJsonVisitor visitor(&dba);
  // FIXME(mtomic): We should make visitors that take const arguments.
  const_cast<LogicalOperator *>(plan_root)->Accept(visitor);
  return visitor.output();
}
}  // namespace plan
}  // namespace memgraph::query
