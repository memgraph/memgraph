#pragma once

#include <ostream>

#include "database/graph_db_accessor.hpp"

namespace database {

/// Dumps database state to output stream as openCypher queries.
///
/// Currently, it only dumps vertices and edges of the graph. In the future,
/// it should also dump indexes, constraints, roles, etc.
///
/// @param os Output stream
/// @param dba Database accessor
void DumpToCypher(std::ostream *os, GraphDbAccessor *dba);

}  // namespace database
