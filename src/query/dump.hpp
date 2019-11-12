#pragma once

#include <ostream>

#include "query/db_accessor.hpp"
#include "query/stream.hpp"

namespace query {

void DumpDatabaseToCypherQueries(query::DbAccessor *dba, AnyStream *stream);

}  // namespace query
