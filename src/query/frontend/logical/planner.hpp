#pragma once

#include <memory>
#include <stdexcept>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/logical/operator.hpp"

namespace query {

std::shared_ptr<LogicalOperator> GenMatch(
    Match& match, std::shared_ptr<LogicalOperator> current_op)
{
  if (current_op) {
    throw std::runtime_error("Not implemented");
  }
  if (match.patterns_.size() != 1) {
    throw std::runtime_error("Not implemented");
  }
  auto& pattern = match.patterns_[0];
  if (pattern->node_parts_.size() != 1) {
    throw std::runtime_error("Not implemented");
  }
  auto& node_part = pattern->node_parts_[0];
  return std::shared_ptr<LogicalOperator>(new ScanAll(node_part));
}

std::shared_ptr<LogicalOperator> GenReturn(
    Return& ret, std::shared_ptr<LogicalOperator> current_op)
{
  if (!current_op) {
    throw std::runtime_error("Not implemented");
  }
  return std::shared_ptr<LogicalOperator>(new Produce(current_op, ret.exprs_));
}

std::shared_ptr<LogicalOperator> Apply(Query& query)
{
  std::shared_ptr<LogicalOperator> current_op;
  for (auto& clause : query.clauses_) {
    auto* clause_ptr = clause.get();
    auto* match = dynamic_cast<Match*>(clause_ptr);
    auto* ret = dynamic_cast<Return*>(clause_ptr);
    if (match) {
      current_op = GenMatch(*match, current_op);
    } else if (ret) {
      return GenReturn(*ret, current_op);
    } else {
      throw std::runtime_error("Not implemented");
    }
  }
  return current_op;
}

}
