// Copyright 2024 Memgraph Ltd.
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

#include <unordered_map>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"

namespace memgraph::storage {

struct ModifiedEdgeInfo {
  ModifiedEdgeInfo(Delta::Action delta, Gid from_vertex, Gid to_vertex, EdgeTypeId edge_type, const EdgeRef &edge)
      : delta_action(delta),
        src_vertex_gid(from_vertex),
        dest_vertex_gid(to_vertex),
        edge_type_id(edge_type),
        edge_ref(edge) {}

  Delta::Action delta_action;
  Gid src_vertex_gid;
  Gid dest_vertex_gid;
  EdgeTypeId edge_type_id;
  EdgeRef edge_ref;
};

static_assert(std::is_trivially_copyable_v<ModifiedEdgeInfo>, "storage::ModifiedEdgeInfo must be trivially copyable!");

using ModifiedEdgesMap = std::unordered_map<Gid, ModifiedEdgeInfo>;

}  // namespace memgraph::storage
