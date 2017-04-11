#pragma once

#include <vector>

#include "query/typed_value.hpp"

namespace query {

std::function<TypedValue(const std::vector<TypedValue> &)> NameToFunction(
    const std::string &function_name);
}
