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

#include "query/plan/pretty_print.hpp"

#include <stdexcept>
#include <string>

#include "query/db_accessor.hpp"
#include "query/parameters.hpp"
#include "query/plan/operator.hpp"
#include "utils/algorithm.hpp"

namespace memgraph::query::plan {

PlanPrinter::PlanPrinter(const DbAccessor *dba, std::ostream *out, Parameters const *parameters)
    : dba_(dba), out_(out), parameters_(parameters) {}

// NOLINTBEGIN(bugprone-macro-parentheses,cppcoreguidelines-macro-usage)
#define PRE_VISIT(TOp)                                                       \
  bool PlanPrinter::PreVisit(TOp &) {                                        \
    WithPrintLn([this](auto &out) { out << StartSymbol() << " " << #TOp; }); \
    return true;                                                             \
  }

#define PRE_VISIT_TS(TOp)                                                                      \
  bool PlanPrinter::PreVisit(TOp &op) {                                                        \
    WithPrintLn([this, &op](auto &out) { out << StartSymbol() << " " << op.ToString(dba_); }); \
    return true;                                                                               \
  }

#define PRE_VISIT_IGNORE(TOp) \
  bool PlanPrinter::PreVisit(TOp &) { return true; }
// NOLINTEND(bugprone-macro-parentheses,cppcoreguidelines-macro-usage)

PRE_VISIT(CreateNode);
PRE_VISIT_TS(CreateExpand);
PRE_VISIT(Delete);

PRE_VISIT_TS(ScanAll);
PRE_VISIT_TS(ScanAllByLabel);
PRE_VISIT_TS(ScanAllByLabelProperties);
PRE_VISIT_TS(ScanAllById);
PRE_VISIT_TS(ScanAllByEdge);
PRE_VISIT_TS(ScanAllByEdgeType);
PRE_VISIT_TS(ScanAllByEdgeTypeProperty);
PRE_VISIT_TS(ScanAllByEdgeProperty);
PRE_VISIT_TS(ScanAllByEdgeId);
PRE_VISIT_TS(ScanAllByVertexProperty);
PRE_VISIT_TS(ScanAllByPointDistance);
PRE_VISIT_TS(ScanAllByPointWithinbbox);

namespace {
std::string ScanChunkToString(const auto &op, const DbAccessor *dba) {
  // ScanChunk is always connected to a ParallelMerge->ScanParallel variant. Combine the two and return the same plan
  // that a single threaded query would produce.
  auto *node = dynamic_cast<ScanParallel *>(op.input_->input().get());
  if (!node) {
    throw std::runtime_error("ScanChunk must be connected to a ScanParallel variant");
  }
  auto name = node->ToString(dba);
  name.replace(name.find("Parallel"), strlen("Parallel"), "All");
  name.insert(name.find('(') + 1, op.output_symbol_.name() + ", ");
  return name;
}
}  // namespace

bool PlanPrinter::PreVisit(ScanChunk &op) {
  WithPrintLn([this, &op](auto &out) { out << StartSymbol() << " " << ScanChunkToString(op, dba_); });
  return true;
}

bool PlanPrinter::PreVisit(ScanChunkByEdge &op) {
  WithPrintLn([this, &op](auto &out) { out << StartSymbol() << " " << ScanChunkToString(op, dba_); });
  return true;
}

PRE_VISIT_IGNORE(ScanParallel);
PRE_VISIT_IGNORE(ScanParallelByLabel);
PRE_VISIT_IGNORE(ScanParallelByLabelProperties);
PRE_VISIT_IGNORE(ScanParallelByEdge);
PRE_VISIT_IGNORE(ScanParallelByEdgeType);
PRE_VISIT_IGNORE(ScanParallelByEdgeTypeProperty);
PRE_VISIT_IGNORE(ScanParallelByEdgeProperty);
PRE_VISIT_IGNORE(ScanParallelByVertexProperty);

bool PlanPrinter::PreVisit(AggregateParallel & /*unused*/) {
  // Hiding in the plan, since it is an implementation detail
  // Next operator is always going to be Aggregate, so no information is lost
  is_parallel_ = true;  // Start of parallel execution
  return true;
}

bool PlanPrinter::PreVisit(OrderByParallel & /*unused*/) {
  // Hiding in the plan, since it is an implementation detail
  // Next operator is always going to be OrderBy, so no information is lost
  is_parallel_ = true;  // Start of parallel execution
  return true;
}

bool PlanPrinter::PreVisit(ParallelMerge & /*unused*/) {
  // Hiding in the plan, since it is a backend connector, not a logical operator
  is_parallel_ = false;  // End of parallel execution
  return true;
}

PRE_VISIT_TS(Expand);
PRE_VISIT_TS(Produce);

bool PlanPrinter::PreVisit(ExpandVariable &op) {
  WithPrintLn([this, &op](auto &out) {
    out << StartSymbol() << " " << (parameters_ ? op.ToStringWithParameters(dba_, *parameters_) : op.ToString(dba_));
  });
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

PRE_VISIT_TS(Aggregate);

PRE_VISIT(Skip);
PRE_VISIT(Limit);

PRE_VISIT_TS(OrderBy);

bool PlanPrinter::PreVisit(query::plan::Merge &op) {
  WithPrintLn([this](auto &out) { out << StartSymbol() << " Merge"; });
  Branch(*op.merge_match_, "On Match");
  Branch(*op.merge_create_, "On Create");
  op.input_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::Optional &op) {
  WithPrintLn([this](auto &out) { out << StartSymbol() << " Optional"; });
  Branch(*op.optional_);
  op.input_->Accept(*this);
  return false;
}

PRE_VISIT(Unwind);
PRE_VISIT(Distinct);

bool PlanPrinter::PreVisit(query::plan::Union &op) {
  WithPrintLn([this, &op](auto &out) { out << StartSymbol() << " " << op.ToString(dba_); });
  Branch(*op.right_op_);
  op.left_op_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::RollUpApply &op) {
  WithPrintLn([this, &op](auto &out) { out << StartSymbol() << " " << op.ToString(dba_); });
  Branch(*op.list_collection_branch_);
  op.input_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::Conditional &op) {
  WithPrintLn([this, &op](auto &out) { out << StartSymbol() << " " << op.ToString(dba_); });
  for (size_t i = 0; i < op.branches_.size(); ++i) {
    const auto &branch = op.branches_[i];
    auto const name = branch.predicate ? fmt::format("WHEN {}", i) : std::string{"ELSE"};
    for (const auto &fold : branch.pattern_filters) Branch(*fold, name);
    Branch(*branch.plan, name);
  }
  op.input_->Accept(*this);
  return false;
}

PRE_VISIT_TS(PeriodicCommit);

PRE_VISIT_TS(CallProcedure);

PRE_VISIT_TS(LoadCsv);

PRE_VISIT_TS(LoadParquet);

bool PlanPrinter::PreVisit(query::plan::LoadJsonl &op) {
  WithPrintLn([this, &op](auto &out) { out << "* " << op.ToString(dba_); });
  return true;
}

bool PlanPrinter::Visit(query::plan::Once & /*op*/) {
  WithPrintLn([this](auto &out) { out << StartSymbol() << " Once"; });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::Cartesian &op) {
  WithPrintLn([this, &op](auto &out) {
    out << StartSymbol() << " Cartesian {";
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
  WithPrintLn([this, &op](auto &out) { out << StartSymbol() << " " << op.ToString(dba_); });
  Branch(*op.right_op_);
  op.left_op_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::Foreach &op) {
  WithPrintLn([this](auto &out) { out << StartSymbol() << " Foreach"; });
  Branch(*op.update_clauses_);
  op.input_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::Filter &op) {
  WithPrintLn([this, &op](auto &out) { out << StartSymbol() << " " << op.ToString(dba_); });
  for (const auto &pattern_filter : op.pattern_filters_) {
    Branch(*pattern_filter);
  }
  op.input_->Accept(*this);
  return false;
}

PRE_VISIT_TS(EdgeUniquenessFilter);

bool PlanPrinter::PreVisit(query::plan::Apply &op) {
  WithPrintLn([this, &op](auto &out) { out << StartSymbol() << " " << op.ToString(dba_); });
  Branch(*op.subquery_);
  op.input_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::PeriodicSubquery &op) {
  WithPrintLn([this, &op](auto &out) { out << StartSymbol() << " " << op.ToString(dba_); });
  Branch(*op.subquery_);
  op.input_->Accept(*this);
  return false;
}

bool PlanPrinter::PreVisit(query::plan::IndexedJoin &op) {
  WithPrintLn([this](auto &out) { out << StartSymbol() << " IndexedJoin"; });
  Branch(*op.sub_branch_);
  op.main_branch_->Accept(*this);
  return false;
}

#undef PRE_VISIT
#undef PRE_VISIT_TS
#undef PRE_VISIT_IGNORE

bool PlanPrinter::DefaultPreVisit() {
  WithPrintLn([this](auto &out) { out << StartSymbol() << " Unknown operator!"; });
  return true;
}

void PlanPrinter::Branch(query::plan::LogicalOperator &op, const std::string &branch_name) {
  WithPrintLn([&](auto &out) { out << "|\\ " << branch_name; });
  ++depth_;
  op.Accept(*this);
  --depth_;
}

void PrettyPrint(const DbAccessor &dba, const LogicalOperator *plan_root, std::ostream *out,
                 Parameters const *parameters) {
  PrettyPrint(&dba, plan_root, out, parameters);
}

void PrettyPrint(const DbAccessor *dba, const LogicalOperator *plan_root, std::ostream *out,
                 Parameters const *parameters) {
  // dba may be null: ToString resolves it only to name a label, property or edge type, and a plan that
  // runs without an accessor contains no operator that names one.
  PlanPrinter printer(dba, out, parameters);
  // FIXME(mtomic): We should make visitors that take const arguments.
  const_cast<LogicalOperator *>(plan_root)->Accept(printer);
}

}  // namespace memgraph::query::plan
