#pragma once

#include <memory>

#include "query/frontend/logical/operator.hpp"

namespace query {

class Query;
class SymbolTable;

namespace plan {

// Returns the root of LogicalOperator tree. The tree is constructed by
// traversing the given AST Query node. SymbolTable is used to determine inputs
// and outputs of certain operators.
std::unique_ptr<LogicalOperator> MakeLogicalPlan(
    query::Query &query, const query::SymbolTable &symbol_table);
}
}
