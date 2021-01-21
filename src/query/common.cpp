#include "query/common.hpp"

namespace query {

namespace impl {

bool TypedValueCompare(const TypedValue &a, const TypedValue &b) {
  // in ordering null comes after everything else
  // at the same time Null is not less that null
  // first deal with Null < Whatever case
  if (a.IsNull()) return false;
  // now deal with NotNull < Null case
  if (b.IsNull()) return true;

  // comparisons are from this point legal only between values of
  // the  same type, or int+float combinations
  if ((a.type() != b.type() && !(a.IsNumeric() && b.IsNumeric())))
    throw QueryRuntimeException(
        "Can't compare value of type {} to value of type {}.", a.type(),
        b.type());

  switch (a.type()) {
    case TypedValue::Type::Bool:
      return !a.ValueBool() && b.ValueBool();
    case TypedValue::Type::Int:
      if (b.type() == TypedValue::Type::Double)
        return a.ValueInt() < b.ValueDouble();
      else
        return a.ValueInt() < b.ValueInt();
    case TypedValue::Type::Double:
      if (b.type() == TypedValue::Type::Int)
        return a.ValueDouble() < b.ValueInt();
      else
        return a.ValueDouble() < b.ValueDouble();
    case TypedValue::Type::String:
      return a.ValueString() < b.ValueString();
    case TypedValue::Type::List:
    case TypedValue::Type::Map:
    case TypedValue::Type::Vertex:
    case TypedValue::Type::Edge:
    case TypedValue::Type::Path:
      throw QueryRuntimeException(
          "Comparison is not defined for values of type {}.", a.type());
    default:
      LOG_FATAL("Unhandled comparison for types");
  }
}

}  // namespace impl

}  // namespace query
