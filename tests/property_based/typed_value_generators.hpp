// Copyright 2026 Memgraph Ltd.
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

#include <rapidcheck.h>

#include <vector>

#include "query/typed_value.hpp"

namespace memgraph::test::generators {

/// The query-side counterpart of the stored generator, drawn the same way: one
/// generator per type, mixing that type's shapes with freshly composed values.
///
/// A vertex, an edge, a path and a graph are left out. Making one needs an
/// accessor, and a property over the four relations needs no database, so
/// requiring one here would put a storage instance behind every property.

/// Every type these generators draw, which is every type but the four holding a
/// piece of the graph and the four no shape is made of.
auto GraphFreeTypes() -> std::vector<query::TypedValue::Type>;

/// Draws a value of one type. A container's elements are drawn at `depth - 1`.
auto TypedValueOfType(query::TypedValue::Type type, int depth) -> rc::Gen<query::TypedValue>;

/// Draws a value whose type is chosen uniformly from `GraphFreeTypes`.
auto AnyTypedValue(int depth = 3) -> rc::Gen<query::TypedValue>;

}  // namespace memgraph::test::generators
