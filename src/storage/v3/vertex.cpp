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

#include "storage/v3/vertex.hpp"

#include <limits>
#include <tuple>
#include <type_traits>
#include <vector>

#include "storage/v3/delta.hpp"

namespace memgraph::storage::v3 {

VertexData::VertexData(Delta *delta) : delta{delta} {
  MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT,
            "Vertex must be created with an initial DELETE_OBJECT delta!");
}

bool VertexHasLabel(const Vertex &vertex, const LabelId label) { return utils::Contains(vertex.second.labels, label); }

}  // namespace memgraph::storage::v3
