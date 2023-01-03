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

#include <limits>
#include <tuple>
#include <type_traits>
#include <vector>

#include "storage/v3/edge_ref.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/vertex_id.hpp"
#include "utils/algorithm.hpp"
#include "utils/spin_lock.hpp"

namespace memgraph::storage::v3 {

struct Delta;

struct VertexData {
  using EdgeLink = std::tuple<EdgeTypeId, VertexId, EdgeRef>;

  explicit VertexData(Delta *delta);

  std::vector<LabelId> labels;
  PropertyStore properties;
  std::vector<EdgeLink> in_edges;
  std::vector<EdgeLink> out_edges;

  bool deleted{false};
  // uint8_t PAD;
  // uint16_t PAD;

  Delta *delta;
};

static_assert(alignof(VertexData) >= 8, "The Vertex should be aligned to at least 8!");

using VertexContainer = std::map<PrimaryKey, VertexData>;
using Vertex = VertexContainer::value_type;

bool VertexHasLabel(const Vertex &vertex, LabelId label);

}  // namespace memgraph::storage::v3
