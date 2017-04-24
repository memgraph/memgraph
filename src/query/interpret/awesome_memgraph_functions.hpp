#pragma once

#include <vector>

#include "database/graph_db_accessor.hpp"
#include "query/typed_value.hpp"

namespace query {

std::function<TypedValue(const std::vector<TypedValue> &, GraphDbAccessor &)>
NameToFunction(const std::string &function_name);
}
