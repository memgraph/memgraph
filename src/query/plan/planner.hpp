#pragma once

#include <memory>

#include "query/plan/operator.hpp"

namespace query {

class AstTreeStorage;
class SymbolTable;

namespace plan {

/// @brief Generates the LogicalOperator tree and returns the root operation.
///
/// The tree is constructed by traversing the @c Query node from given
/// @c AstTreeStorage. The storage may also be used to create new AST nodes for
/// use in operators. @c SymbolTable is used to determine inputs and outputs of
/// certain operators.
std::unique_ptr<LogicalOperator> MakeLogicalPlan(
    AstTreeStorage &storage, query::SymbolTable &symbol_table);
}

}  // namespace plan
