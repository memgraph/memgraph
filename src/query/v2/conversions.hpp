// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once
#include "bindings/typed_value.hpp"
#include "query/v2/accessors.hpp"
#include "query/v2/requests.hpp"

namespace memgraph::query::v2 {

using requests::Value;

inline TypedValue ValueToTypedValue(const Value &value) {
  switch (value.type) {
    case Value::NILL:
      return {};
    case Value::BOOL:
      return TypedValue(value.bool_v);
    case Value::INT64:
      return TypedValue(value.int_v);
    case Value::DOUBLE:
      return TypedValue(value.double_v);
    case Value::STRING:
      return TypedValue(value.string_v);
    case Value::LIST:
      // return TypedValue(value.list_v);
    case Value::MAP:
      // return TypedValue(value.map_v);
    case Value::VERTEX:
      //      return TypedValue(accessors::VertexAccessor(value.vertex_v, {}));
    case Value::EDGE:
      //      return TypedValue(accessors::EdgeAccessor(value.edge_v, {}));
    case Value::PATH:
      break;
  }
  throw std::runtime_error("Incorrect type in conversion");
}

}  // namespace memgraph::query::v2
