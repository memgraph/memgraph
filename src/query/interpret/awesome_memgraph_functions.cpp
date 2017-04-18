#include "query/interpret/awesome_memgraph_functions.hpp"

#include <cmath>
#include <cstdlib>

#include "query/exceptions.hpp"

namespace query {
namespace {

TypedValue Abs(const std::vector<TypedValue> &args) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("ABS requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Int:
      return static_cast<int64_t>(
          std::abs(static_cast<long long>(args[0].Value<int64_t>())));
    case TypedValue::Type::Double:
      return std::abs(args[0].Value<double>());
    default:
      throw QueryRuntimeException("ABS called with incompatible type");
  }
}
}

std::function<TypedValue(const std::vector<TypedValue> &)> NameToFunction(
    const std::string &function_name) {
  if (function_name == "ABS") {
    return Abs;
  }
  return nullptr;
}
}
