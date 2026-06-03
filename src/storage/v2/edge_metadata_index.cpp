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

#include "storage/v2/edge_metadata_index.hpp"

#include "storage/v2/indices/indices_utils.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

void EdgeMetadataIndex::RebuildFrom(
    utils::SkipListDb<Vertex> &vertices,
    std::optional<durability::ParallelizedSchemaCreationInfo> const &parallel_exec_info) {
  data_.clear();
  auto vertex_acc = vertices.access();
  PopulateIndexDispatch(
      vertex_acc,
      [this]() { return data_.access(); },
      [](Vertex &vertex, auto &metadata_acc) {
        for (auto const &[edge_type, to_vertex, edge_ref] : vertex.out_edges) {
          auto *edge = edge_ref.ptr;
          // NOLINTBEGIN(clang-analyzer-core.NullDereference)
          auto [_, inserted] = metadata_acc.insert(EdgeMetadata{edge->gid, &vertex});
          MG_ASSERT(inserted, "Edge metadata entry already existed for gid {}!", edge->gid.AsUint());
          // NOLINTEND(clang-analyzer-core.NullDereference)
        }
      },
      []() { return false; },
      parallel_exec_info);
}

}  // namespace memgraph::storage
