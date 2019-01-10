#include "query/plan/distributed_pretty_print.hpp"

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

}  // namespace query::plan
