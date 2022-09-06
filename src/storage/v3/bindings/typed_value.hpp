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

#include "storage/v3/bindings/bindings.hpp"

#include "expr/typed_value.hpp"
#include "storage/v3/edge_accessor.hpp"
#include "storage/v3/path.hpp"
#include "storage/v3/vertex_accessor.hpp"

namespace memgraph::expr {
namespace v3 = memgraph::storage::v3;
extern template class memgraph::expr::TypedValueT<v3::VertexAccessor, v3::EdgeAccessor, v3::Path>;
}  // namespace memgraph::expr

namespace memgraph::storage::v3 {
using TypedValue = memgraph::expr::TypedValueT<VertexAccessor, EdgeAccessor, Path>;
}  // namespace memgraph::storage::v3
