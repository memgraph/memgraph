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

#include "query/v2/bindings/bindings.hpp"

#include "expr/typed_value.hpp"
#include "query/v2/db_accessor.hpp"
#include "query/v2/path.hpp"

namespace memgraph::expr {
namespace v2 = memgraph::query::v2;
extern template class memgraph::expr::TypedValueT<v2::VertexAccessor, v2::EdgeAccessor, v2::Path>;
}  // namespace memgraph::expr
namespace memgraph::query::v2 {
using TypedValue = memgraph::expr::TypedValueT<VertexAccessor, EdgeAccessor, Path>;
}  // namespace memgraph::query::v2
