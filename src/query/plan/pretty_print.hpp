/// @file
#pragma once

#include <iostream>

namespace database {
class GraphDbAccessor;
}

namespace query::plan {

class LogicalOperator;

/// Pretty print a `LogicalOperator` plan to a `std::ostream`.
/// GraphDbAccessor is needed for resolving label and property names.
/// Note that `plan_root` isn't modified, but we can't take it as a const
/// because we don't have support for visiting a const LogicalOperator.
void PrettyPrint(const database::GraphDbAccessor &dba,
                 LogicalOperator *plan_root, std::ostream *out);

/// Overload of `PrettyPrint` which defaults the `std::ostream` to `std::cout`.
inline void PrettyPrint(const database::GraphDbAccessor &dba,
                        LogicalOperator *plan_root) {
  PrettyPrint(dba, plan_root, &std::cout);
}

}  // namespace query::plan
