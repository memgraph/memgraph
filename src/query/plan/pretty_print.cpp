#include "query/plan/pretty_print.hpp"

#include "database/graph_db_accessor.hpp"

namespace query::plan {

PlanPrinter::PlanPrinter(const database::GraphDbAccessor *dba,
                         std::ostream *out)
    : dba_(dba), out_(out) {}

#define PRE_VISIT(TOp)                                   \
  bool PlanPrinter::PreVisit(TOp &) {                    \
    WithPrintLn([](auto &out) { out << "* " << #TOp; }); \
    return true;                                         \
  }

PRE_VISIT(CreateNode);
PRE_VISIT(CreateExpand);
PRE_VISIT(Delete);

bool PlanPrinter::PreVisit(query::plan::ScanAll &op) {
  WithPrintLn([&](auto &out) {
    out << "* ScanAll"
        << " (" << op.output_symbol_.name() << ")";
  });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByLabel &op) {
  WithPrintLn([&](auto &out) {
    out << "* ScanAllByLabel"
        << " (" << op.output_symbol_.name() << " :"
        << dba_->LabelName(op.label_) << ")";
  });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByLabelPropertyValue &op) {
  WithPrintLn([&](auto &out) {
    out << "* ScanAllByLabelPropertyValue"
        << " (" << op.output_symbol_.name() << " :"
        << dba_->LabelName(op.label_) << " {"
        << dba_->PropertyName(op.property_) << "})";
  });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ScanAllByLabelPropertyRange &op) {
  WithPrintLn([&](auto &out) {
    out << "* ScanAllByLabelPropertyRange"
        << " (" << op.output_symbol_.name() << " :"
        << dba_->LabelName(op.label_) << " {"
        << dba_->PropertyName(op.property_) << "})";
  });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::Expand &op) {
  WithPrintLn([&](auto &out) {
    *out_ << "* Expand (" << op.input_symbol_.name() << ")"
          << (op.common_.direction == query::EdgeAtom::Direction::IN ? "<-"
                                                                     : "-")
          << "[" << op.common_.edge_symbol.name() << "]"
          << (op.common_.direction == query::EdgeAtom::Direction::OUT ? "->"
                                                                      : "-")
          << "(" << op.common_.node_symbol.name() << ")";
  });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::ExpandVariable &op) {
  WithPrintLn([&](auto &out) {
    *out_ << "* ExpandVariable (" << op.input_symbol_.name() << ")"
          << (op.common_.direction == query::EdgeAtom::Direction::IN ? "<-"
                                                                     : "-")
          << "[" << op.common_.edge_symbol.name() << "]"
          << (op.common_.direction == query::EdgeAtom::Direction::OUT ? "->"
                                                                      : "-")
          << "(" << op.common_.node_symbol.name() << ")";
  });
  return true;
}

bool PlanPrinter::PreVisit(query::plan::Produce &op) {
  WithPrintLn([&](auto &out) {
    out << "* Produce {";
    utils::PrintIterable(
        out, op.named_expressions_, ", ",
        [](auto &out, const auto &nexpr) { out << nexpr->name_; });
    out << "}";
  });
  return true;
}

PRE_VISIT(ConstructNamedPath);
PRE_VISIT(Filter);
PRE_VISIT(SetProperty);
PRE_VISIT(SetProperties);
PRE_VISIT(SetLabels);
PRE_VISIT(RemoveProperty);
PRE_VISIT(RemoveLabels);
PRE_VISIT(ExpandUniquenessFilter<VertexAccessor>);
PRE_VISIT(ExpandUniquenessFilter<EdgeAccessor>);
PRE_VISIT(Accumulate);

bool PlanPrinter::PreVisit(query::plan::Aggregate &op) {
  WithPrintLn([&](auto &out) {
    out << "* Aggregate {";
    utils::PrintIterable(
        out, op.aggregations_, ", ",
        [](auto &out, const auto &aggr) { out << aggr.output_sym.name(); });
    out << "} {";
    utils::PrintIterable(out, op.remember_, ", ",
                         [](auto &out, const auto &sym) { out << sym.name(); });
    out << "}";
  });
  return true;
}

PRE_VISIT(Skip);
PRE_VISIT(Limit);

bool PlanPrinter::PreVisit(query::plan::OrderBy &op) {
  WithPrintLn([&op](auto &out) {
    out << "* OrderBy {";
    utils::PrintIterable(out, op.output_symbols_, ", ",
                         [](auto &out, const auto &sym) { out << sym.name(); });
    out << "}";
  });
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

bool PlanPrinter::Visit(query::plan::Once &op) {
  // Ignore checking Once, it is implicitly at the end.
  return true;
}

bool PlanPrinter::PreVisit(query::plan::Cartesian &op) {
  WithPrintLn([&op](auto &out) {
    out << "* Cartesian {";
    utils::PrintIterable(out, op.left_symbols_, ", ",
                         [](auto &out, const auto &sym) { out << sym.name(); });
    out << " : ";
    utils::PrintIterable(out, op.right_symbols_, ", ",
                         [](auto &out, const auto &sym) { out << sym.name(); });
    out << "}";
  });
  Branch(*op.right_op_);
  op.left_op_->Accept(*this);
  return false;
}

#undef PRE_VISIT

bool PlanPrinter::DefaultPreVisit() {
  WithPrintLn([](auto &out) { out << "* Unknown operator!"; });
  return true;
}

void PlanPrinter::Branch(query::plan::LogicalOperator &op,
                         const std::string &branch_name) {
  WithPrintLn([&](auto &out) { out << "|\\ " << branch_name; });
  ++depth_;
  op.Accept(*this);
  --depth_;
}

void PrettyPrint(const database::GraphDbAccessor &dba,
                 const LogicalOperator *plan_root, std::ostream *out) {
  PlanPrinter printer(&dba, out);
  // FIXME(mtomic): We should make visitors that take const arguments.
  const_cast<LogicalOperator *>(plan_root)->Accept(printer);
}

}  // namespace query::plan
