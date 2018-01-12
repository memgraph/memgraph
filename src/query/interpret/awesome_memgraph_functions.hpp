#pragma once

#include <vector>

#include "database/graph_db_accessor.hpp"
#include "query/typed_value.hpp"

namespace query {

namespace {
const char kStartsWith[] = "STARTSWITH";
const char kEndsWith[] = "ENDSWITH";
const char kContains[] = "CONTAINS";
}

std::function<TypedValue(const std::vector<TypedValue> &,
                         database::GraphDbAccessor &)>
NameToFunction(const std::string &function_name);
}
