#include "query/plan/pretty_print.hpp"

#include "database/graph_db_accessor.hpp"
#include "query/plan/distributed_ops.hpp"
#include "query/plan/operator.hpp"

namespace query::plan {

namespace {

class PlanPrinter final : public DistributedOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  explicit PlanPrinter(const database::GraphDbAccessor *dba, std::ostream *out)
      : dba_(dba), out_(out) {}

#define PRE_VISIT(TOp)                                   \
  bool PreVisit(query::plan::TOp &) override {           \
    WithPrintLn([](auto &out) { out << "* " << #TOp; }); \
    return true;                                         \
  }

  PRE_VISIT(CreateNode);
  PRE_VISIT(CreateExpand);
  PRE_VISIT(Delete);

  bool PreVisit(query::plan::ScanAll &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAll"
          << " (" << op.output_symbol().name() << ")";
    });
    return true;
  }

  bool PreVisit(query::plan::ScanAllByLabel &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAllByLabel"
          << " (" << op.output_symbol().name() << " :"
          << dba_->LabelName(op.label()) << ")";
    });
    return true;
  }

  bool PreVisit(query::plan::ScanAllByLabelPropertyValue &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAllByLabelPropertyValue"
          << " (" << op.output_symbol().name() << " :"
          << dba_->LabelName(op.label()) << " {"
          << dba_->PropertyName(op.property()) << "})";
    });
    return true;
  }

  bool PreVisit(query::plan::ScanAllByLabelPropertyRange &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAllByLabelPropertyRange"
          << " (" << op.output_symbol().name() << " :"
          << dba_->LabelName(op.label()) << " {"
          << dba_->PropertyName(op.property()) << "})";
    });
    return true;
  }

  bool PreVisit(query::plan::Expand &op) override {
    WithPrintLn([&](auto &out) {
      out << "* Expand";
      PrintExpand(out, op);
    });
    return true;
  }

  bool PreVisit(query::plan::ExpandVariable &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ExpandVariable";
      PrintExpand(out, op);
    });
    return true;
  }

  bool PreVisit(query::plan::DistributedExpandBfs &op) override {
    WithPrintLn([&](auto &out) {
      out << "* DistributedExpandBfs";
      PrintExpand(out, op);
    });
    return true;
  }

  bool PreVisit(query::plan::Produce &op) override {
    WithPrintLn([&](auto &out) {
      out << "* Produce {";
      utils::PrintIterable(
          out, op.named_expressions(), ", ",
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

  bool PreVisit(query::plan::Aggregate &op) override {
    WithPrintLn([&](auto &out) {
      out << "* Aggregate {";
      utils::PrintIterable(
          out, op.aggregations(), ", ",
          [](auto &out, const auto &aggr) { out << aggr.output_sym.name(); });
      out << "} {";
      utils::PrintIterable(
          out, op.remember(), ", ",
          [](auto &out, const auto &sym) { out << sym.name(); });
      out << "}";
    });
    return true;
  }

  PRE_VISIT(Skip);
  PRE_VISIT(Limit);

  bool PreVisit(query::plan::OrderBy &op) override {
    WithPrintLn([&op](auto &out) {
      out << "* OrderBy {";
      utils::PrintIterable(
          out, op.output_symbols(), ", ",
          [](auto &out, const auto &sym) { out << sym.name(); });
      out << "}";
    });
    return true;
  }

  bool PreVisit(query::plan::Merge &op) override {
    WithPrintLn([](auto &out) { out << "* Merge"; });
    Branch(*op.merge_match(), "On Match");
    Branch(*op.merge_create(), "On Create");
    op.input()->Accept(*this);
    return false;
  }

  bool PreVisit(query::plan::Optional &op) override {
    WithPrintLn([](auto &out) { out << "* Optional"; });
    Branch(*op.optional());
    op.input()->Accept(*this);
    return false;
  }

  PRE_VISIT(Unwind);
  PRE_VISIT(Distinct);

  bool Visit(query::plan::Once &op) override {
    // Ignore checking Once, it is implicitly at the end.
    return true;
  }

  bool Visit(query::plan::CreateIndex &op) override {
    WithPrintLn([](auto &out) { out << "* CreateIndex"; });
    return true;
  }

  bool Visit(query::plan::AuthHandler &op) override {
    WithPrintLn([](auto &out) { out << "* AuthHandler"; });
    return true;
  }

  bool Visit(query::plan::CreateStream &op) override {
    WithPrintLn([](auto &out) { out << "* CreateStream"; });
    return true;
  }

  bool Visit(query::plan::DropStream &op) override {
    WithPrintLn([](auto &out) { out << "* DropStream"; });
    return true;
  }

  bool Visit(query::plan::ShowStreams &op) override {
    WithPrintLn([](auto &out) { out << "* ShowStreams"; });
    return true;
  }

  bool Visit(query::plan::StartStopStream &op) override {
    WithPrintLn([](auto &out) { out << "* StartStopStream"; });
    return true;
  }

  bool Visit(query::plan::StartStopAllStreams &op) override {
    WithPrintLn([](auto &out) { out << "* StartStopAllStreams"; });
    return true;
  }

  bool Visit(query::plan::TestStream &op) override {
    WithPrintLn([](auto &out) { out << "* TestStream"; });
    return true;
  }

  bool PreVisit(query::plan::Explain &explain) override {
    WithPrintLn([&explain](auto &out) {
      out << "* Explain {" << explain.output_symbol().name() << "}";
    });
    return true;
  }

  bool PreVisit(query::plan::PullRemote &op) override {
    WithPrintLn([&op](auto &out) {
      out << "* PullRemote [" << op.plan_id() << "] {";
      utils::PrintIterable(
          out, op.symbols(), ", ",
          [](auto &out, const auto &sym) { out << sym.name(); });
      out << "}";
    });
    WithPrintLn([](auto &out) { out << "|\\"; });
    ++depth_;
    WithPrintLn([](auto &out) { out << "* workers"; });
    --depth_;
    return true;
  }

  bool PreVisit(query::plan::Synchronize &op) override {
    WithPrintLn([&op](auto &out) {
      out << "* Synchronize";
      if (op.advance_command()) out << " (ADV CMD)";
    });
    if (op.pull_remote()) Branch(*op.pull_remote());
    op.input()->Accept(*this);
    return false;
  }

  bool PreVisit(query::plan::Cartesian &op) override {
    WithPrintLn([&op](auto &out) {
      out << "* Cartesian {";
      utils::PrintIterable(
          out, op.left_symbols(), ", ",
          [](auto &out, const auto &sym) { out << sym.name(); });
      out << " : ";
      utils::PrintIterable(
          out, op.right_symbols(), ", ",
          [](auto &out, const auto &sym) { out << sym.name(); });
      out << "}";
    });
    Branch(*op.right_op());
    op.left_op()->Accept(*this);
    return false;
  }

  bool PreVisit(query::plan::PullRemoteOrderBy &op) override {
    WithPrintLn([&op](auto &out) {
      out << "* PullRemoteOrderBy {";
      utils::PrintIterable(
          out, op.symbols(), ", ",
          [](auto &out, const auto &sym) { out << sym.name(); });
      out << "}";
    });

    WithPrintLn([](auto &out) { out << "|\\"; });
    ++depth_;
    WithPrintLn([](auto &out) { out << "* workers"; });
    --depth_;
    return true;
  }
#undef PRE_VISIT

 private:
  bool DefaultPreVisit() override {
    WithPrintLn([](auto &out) { out << "* Unknown operator!"; });
    return true;
  }

  // Call fun with output stream. The stream is prefixed with amount of spaces
  // corresponding to the current depth_.
  template <class TFun>
  void WithPrintLn(TFun fun) {
    *out_ << " ";
    for (int i = 0; i < depth_; ++i) {
      *out_ << "|  ";
    }
    fun(*out_);
    *out_ << std::endl;
  }

  // Forward this printer to another operator branch by incrementing the depth
  // and printing the branch name.
  void Branch(query::plan::LogicalOperator &op,
              const std::string &branch_name = "") {
    WithPrintLn([&](auto &out) { out << "|\\ " << branch_name; });
    ++depth_;
    op.Accept(*this);
    --depth_;
  }

  void PrintExpand(std::ostream &out, const query::plan::ExpandCommon &op) {
    out << " (" << op.input_symbol().name() << ")"
        << (op.direction() == query::EdgeAtom::Direction::IN ? "<-" : "-")
        << "[" << op.edge_symbol().name() << "]"
        << (op.direction() == query::EdgeAtom::Direction::OUT ? "->" : "-")
        << "(" << op.node_symbol().name() << ")";
  }

  int depth_ = 0;
  const database::GraphDbAccessor *dba_{nullptr};
  std::ostream *out_{nullptr};
};

}  // namespace

void PrettyPrint(const database::GraphDbAccessor &dba,
                 LogicalOperator *plan_root, std::ostream *out) {
  PlanPrinter printer(&dba, out);
  plan_root->Accept(printer);
}

}  // namespace query::plan
