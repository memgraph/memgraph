#pragma once

#include <vector>

#include "query/typed_value.hpp"

namespace query {

class Context;

namespace {
const char kStartsWith[] = "STARTSWITH";
const char kEndsWith[] = "ENDSWITH";
const char kContains[] = "CONTAINS";
}  // namespace

std::function<TypedValue(const std::vector<TypedValue> &, Context *)>
NameToFunction(const std::string &function_name);
}  // namespace query
