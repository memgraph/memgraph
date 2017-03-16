#include "query/frontend/logical/planner.hpp"

#include "query/frontend/ast/ast.hpp"
#include "utils/exceptions/not_yet_implemented.hpp"

namespace query {

namespace {

static LogicalOperator *GenCreate(
    Create& create, std::shared_ptr<LogicalOperator> input_op)
{
  if (input_op) {
    throw NotYetImplemented();
  }
  if (create.patterns_.size() != 1) {
    throw NotYetImplemented();
  }
  auto &pattern = create.patterns_[0];
  if (pattern->atoms_.size() != 1) {
    throw NotYetImplemented();
  }
  auto *node_atom = dynamic_cast<NodeAtom*>(pattern->atoms_[0]);
  return new CreateOp(node_atom);
}

LogicalOperator *GenMatch(
    Match& match,
    std::shared_ptr<LogicalOperator> input_op,
    const SymbolTable &symbol_table)
{
  if (input_op) {
    throw NotYetImplemented();
  }
  if (match.patterns_.size() != 1) {
    throw NotYetImplemented();
  }
  auto &pattern = match.patterns_[0];
  if (pattern->atoms_.size() != 1) {
    throw NotYetImplemented();
  }
  auto *node_atom = dynamic_cast<NodeAtom*>(pattern->atoms_[0]);
  auto *scan_all = new ScanAll(node_atom);
  if (!node_atom->labels_.empty() || !node_atom->properties_.empty()) {
    auto &input_symbol = symbol_table.at(*node_atom->identifier_);
    return new NodeFilter(std::shared_ptr<LogicalOperator>(scan_all), input_symbol,
                          node_atom->labels_, node_atom->properties_);
  }
  return scan_all;
}

Produce *GenReturn(Return& ret, std::shared_ptr<LogicalOperator> input_op)
{
  if (!input_op) {
    throw NotYetImplemented();
  }
  return new Produce(input_op, ret.named_expressions_);
}
}

std::unique_ptr<LogicalOperator> MakeLogicalPlan(
    Query& query, const SymbolTable &symbol_table)
{
  LogicalOperator *input_op = nullptr;
  for (auto &clause : query.clauses_) {
    auto *clause_ptr = clause;
    if (auto *match = dynamic_cast<Match*>(clause_ptr)) {
      input_op = GenMatch(*match, std::shared_ptr<LogicalOperator>(input_op),
                          symbol_table);
    } else if (auto *ret = dynamic_cast<Return*>(clause_ptr)) {
      input_op = GenReturn(*ret, std::shared_ptr<LogicalOperator>(input_op));
    } else if (auto *create = dynamic_cast<Create*>(clause_ptr)) {
      input_op = GenCreate(*create, std::shared_ptr<LogicalOperator>(input_op));
    } else {
      throw NotYetImplemented();
    }
  }
  return std::unique_ptr<LogicalOperator>(input_op);
}

}
