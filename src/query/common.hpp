#pragma once

#include <cstdint>
#include <string>

#include "query/typed_value.hpp"

namespace query {

// These are the functions for parsing literals and parameter names from
// opencypher query.
int64_t ParseIntegerLiteral(const std::string &s);
std::string ParseStringLiteral(const std::string &s);
double ParseDoubleLiteral(const std::string &s);
std::string ParseParameter(const std::string &s);

/**
 * Indicates that some part of query execution should
 * see the OLD graph state (the latest state before the
 * current transaction+command), or NEW (state as
 * changed by the current transaction+command).
 *
 * Also some part of query execution could leave
 * the graph state AS_IS, that is as it was left
 * by some previous part of execution.
 */
enum class GraphView { AS_IS, OLD, NEW };

/**
 * Helper function for recursively reconstructing all the accessors in the
 * given TypedValue.
 *
 * @returns - If the reconstruction succeeded.
 */
void ReconstructTypedValue(TypedValue &value);
}
