#include "query/distributed/plan/pretty_print.hpp"

namespace query::plan {

bool DistributedPlanPrinter::PreVisit(query::plan::DistributedExpand &op) {
  WithPrintLn([&](auto &out) {
    out << "* DistributedExpand (" << op.input_symbol_.name() << ")"
        << (op.common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-")
        << "[" << op.common_.edge_symbol.name();
    utils::PrintIterable(out, op.common_.edge_types, "|",
                         [this](auto &stream, const auto &edge_type) {
                           stream << ":" << dba_->EdgeTypeName(edge_type);
                         });
    out << "]"
        << (op.common_.direction == query::EdgeAtom::Direction::OUT ? "->"
                                                                    : "-")
        << "(" << op.common_.node_symbol.name() << ")";
  });
  return true;
}

bool DistributedPlanPrinter::PreVisit(query::plan::DistributedExpandBfs &op) {
  WithPrintLn([&](auto &out) {
    out << "* DistributedExpandBfs (" << op.input_symbol_.name() << ")"
        << (op.common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-")
        << "[" << op.common_.edge_symbol.name();
    utils::PrintIterable(out, op.common_.edge_types, "|",
                         [this](auto &stream, const auto &edge_type) {
                           stream << ":" << dba_->EdgeTypeName(edge_type);
                         });
    out << "]"
        << (op.common_.direction == query::EdgeAtom::Direction::OUT ? "->"
                                                                    : "-")
        << "(" << op.common_.node_symbol.name() << ")";
  });
  return true;
}

bool DistributedPlanPrinter::PreVisit(query::plan::PullRemote &op) {
  WithPrintLn([&op](auto &out) {
    out << "* PullRemote [" << op.plan_id_ << "] {";
    utils::PrintIterable(out, op.symbols_, ", ",
                         [](auto &out, const auto &sym) { out << sym.name(); });
    out << "}";
  });
  WithPrintLn([](auto &out) { out << "|\\"; });
  ++depth_;
  WithPrintLn([](auto &out) { out << "* workers"; });
  --depth_;
  return true;
}

bool DistributedPlanPrinter::PreVisit(query::plan::PullRemoteOrderBy &op) {
  WithPrintLn([&op](auto &out) {
    out << "* PullRemoteOrderBy {";
    utils::PrintIterable(out, op.symbols_, ", ",
                         [](auto &out, const auto &sym) { out << sym.name(); });
    out << "}";
  });

  WithPrintLn([](auto &out) { out << "|\\"; });
  ++depth_;
  WithPrintLn([](auto &out) { out << "* workers"; });
  --depth_;
  return true;
}

#define PRE_VISIT(TOp)                                   \
  bool DistributedPlanPrinter::PreVisit(TOp &) {         \
    WithPrintLn([](auto &out) { out << "* " << #TOp; }); \
    return true;                                         \
  }

PRE_VISIT(DistributedCreateNode);

bool DistributedPlanPrinter::PreVisit(DistributedCreateExpand &op) {
  WithPrintLn([&](auto &out) {
    out << "* DistributedCreateExpand (" << op.input_symbol_.name() << ")"
        << (op.edge_info_.direction == query::EdgeAtom::Direction::IN ? "<-"
                                                                      : "-")
        << "[" << op.edge_info_.symbol.name() << ":"
        << dba_->EdgeTypeName(op.edge_info_.edge_type) << "]"
        << (op.edge_info_.direction == query::EdgeAtom::Direction::OUT ? "->"
                                                                       : "-")
        << "(" << op.node_info_.symbol.name() << ")";
  });
  return true;
}

#undef PRE_VISIT

bool DistributedPlanPrinter::PreVisit(query::plan::Synchronize &op) {
  WithPrintLn([&op](auto &out) {
    out << "* Synchronize";
    if (op.advance_command_) out << " (ADV CMD)";
  });
  if (op.pull_remote_) Branch(*op.pull_remote_);
  op.input_->Accept(*this);
  return false;
}

void DistributedPrettyPrint(const database::GraphDbAccessor &dba,
                            const LogicalOperator *plan_root,
                            std::ostream *out) {
  DistributedPlanPrinter printer(&dba, out);
  // FIXME(mtomic): We should make visitors that take const argument.
  const_cast<LogicalOperator *>(plan_root)->Accept(printer);
}

nlohmann::json DistributedPlanToJson(const database::GraphDbAccessor &dba,
                                     const LogicalOperator *plan_root) {
  impl::DistributedPlanToJsonVisitor visitor(&dba);
  const_cast<LogicalOperator *>(plan_root)->Accept(visitor);
  return visitor.output();
}

namespace impl {
///////////////////////////////////////////////////////////////////////////////
//
// DistributedPlanToJsonVisitor implementation
//
// The JSON formatted plan is consumed (or will be) by Memgraph Lab, and
// therefore should not be changed before synchronizing with whoever is
// maintaining Memgraph Lab. Hopefully, one day integration tests will exist and
// there will be no need to be super careful.

using json = nlohmann::json;

bool DistributedPlanToJsonVisitor::PreVisit(DistributedExpand &op) {
  json self;
  self["name"] = "DistributedExpand";
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

bool DistributedPlanToJsonVisitor::PreVisit(DistributedExpandBfs &op) {
  json self;
  self["name"] = "DistributedExpandBfs";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["node_symbol"] = ToJson(op.common_.node_symbol);
  self["edge_symbol"] = ToJson(op.common_.edge_symbol);
  self["edge_types"] = ToJson(op.common_.edge_types, *dba_);
  self["direction"] = ToString(op.common_.direction);
  self["lower_bound"] = op.lower_bound_ ? ToJson(op.lower_bound_) : json();
  self["upper_bound"] = op.upper_bound_ ? ToJson(op.upper_bound_) : json();
  self["existing_node"] = op.common_.existing_node;

  self["filter_lambda"] = op.filter_lambda_.expression
                              ? ToJson(op.filter_lambda_.expression)
                              : json();

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool DistributedPlanToJsonVisitor::PreVisit(PullRemote &op) {
  json self;
  self["name"] = "PullRemote";
  self["symbols"] = ToJson(op.symbols_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool DistributedPlanToJsonVisitor::PreVisit(PullRemoteOrderBy &op) {
  json self;
  self["name"] = "PullRemoteOrderBy";

  for (auto i = 0; i < op.order_by_.size(); ++i) {
    json json;
    json["ordering"] = ToString(op.compare_.ordering_[i]);
    json["expression"] = ToJson(op.order_by_[i]);
    self["order_by"].push_back(json);
  }
  self["symbols"] = ToJson(op.symbols_);

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool DistributedPlanToJsonVisitor::PreVisit(DistributedCreateNode &op) {
  json self;
  self["name"] = "DistributedCreateNode";
  self["node_info"] = ToJson(op.node_info_, *dba_);
  self["on_random_worker"] = op.on_random_worker_;

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool DistributedPlanToJsonVisitor::PreVisit(DistributedCreateExpand &op) {
  json self;
  self["name"] = "DistributedCreateExpand";
  self["input_symbol"] = ToJson(op.input_symbol_);
  self["node_info"] = ToJson(op.node_info_, *dba_);
  self["edge_info"] = ToJson(op.edge_info_, *dba_);
  self["existing_node"] = op.existing_node_;

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  output_ = std::move(self);
  return false;
}

bool DistributedPlanToJsonVisitor::PreVisit(Synchronize &op) {
  json self;
  self["name"] = "Synchronize";
  self["advance_command"] = op.advance_command_;

  op.input_->Accept(*this);
  self["input"] = PopOutput();

  if (op.pull_remote_) {
    op.pull_remote_->Accept(*this);
    self["pull_remote"] = PopOutput();
  } else {
    self["pull_remote"] = json();
  }

  output_ = std::move(self);
  return false;
}

}  // namespace impl

}  // namespace query::plan
