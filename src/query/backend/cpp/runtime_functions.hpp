#pragma once

#include "query/backend/cpp/typed_value.hpp"

namespace backend {
namespace cpp {

TypedValue GetProperty(const TypedValue &t, const std::string &key);
}
}
