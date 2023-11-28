// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/common.hpp"

namespace memgraph::query {

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
    throw QueryRuntimeException("Can't compare value of type {} to value of type {}.", a.type(), b.type());

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
      // NOLINTNEXTLINE(modernize-use-nullptr)
      return a.ValueString() < b.ValueString();
    case TypedValue::Type::Date:
      // NOLINTNEXTLINE(modernize-use-nullptr)
      return a.ValueDate() < b.ValueDate();
    case TypedValue::Type::LocalTime:
      // NOLINTNEXTLINE(modernize-use-nullptr)
      return a.ValueLocalTime() < b.ValueLocalTime();
    case TypedValue::Type::LocalDateTime:
      // NOLINTNEXTLINE(modernize-use-nullptr)
      return a.ValueLocalDateTime() < b.ValueLocalDateTime();
    case TypedValue::Type::Duration:
      // NOLINTNEXTLINE(modernize-use-nullptr)
      return a.ValueDuration() < b.ValueDuration();
    case TypedValue::Type::List:
    case TypedValue::Type::Map:
    case TypedValue::Type::Vertex:
    case TypedValue::Type::Edge:
    case TypedValue::Type::Path:
    case TypedValue::Type::Graph:
    case TypedValue::Type::Function:
      throw QueryRuntimeException("Comparison is not defined for values of type {}.", a.type());
    case TypedValue::Type::Null:
      LOG_FATAL("Invalid type");
  }
}

}  // namespace impl

int64_t QueryTimestamp() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}
}  // namespace memgraph::query
