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

#include <optional>
#include <ranges>

#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

// Denormalised gid -> from-vertex map used to answer edge-by-gid lookups in
// O(log N) without scanning vertex adjacency. Derived from vertex out_edges;
// never loaded from durability files. Presence on `InMemoryStorage` is gated by
// the salient flag `enable_edges_metadata` (which requires `properties_on_edges`).
//
// Invariant: when the index exists, `size() == edges_.size()` at every
// quiescent point (post-recovery, post-GC). The index is populated during
// recovery by a single post-recovery rebuild from the final adjacency, not
// inlined into any particular recovery route.
class EdgeMetadataIndex {
 public:
  EdgeMetadataIndex() = default;
  EdgeMetadataIndex(EdgeMetadataIndex const &) = delete;
  EdgeMetadataIndex &operator=(EdgeMetadataIndex const &) = delete;
  EdgeMetadataIndex(EdgeMetadataIndex &&) = delete;
  EdgeMetadataIndex &operator=(EdgeMetadataIndex &&) = delete;
  ~EdgeMetadataIndex() = default;

  void OnEdgeCreated(Gid gid, Vertex *from_vertex) {
    auto acc = data_.access();
    auto [_, inserted] = acc.insert(EdgeMetadata{gid, from_vertex});
    MG_ASSERT(inserted, "Edge metadata entry already existed for gid {}!", gid.AsUint());
  }

  void OnEdgeDeleted(Gid gid) {
    auto acc = data_.access();
    MG_ASSERT(acc.remove(gid), "Edge metadata entry missing for deleted edge {}!", gid.AsUint());
  }

  template <std::ranges::input_range R>
    requires std::same_as<std::ranges::range_value_t<R>, Gid>
  void OnEdgesDeleted(R const &gids) {
    auto acc = data_.access();
    for (auto gid : gids) {
      MG_ASSERT(acc.remove(gid), "Edge metadata entry missing for deleted edge {}!", gid.AsUint());
    }
  }

  // Returns the `from_vertex` recorded for `edge_gid`. Hard-asserts on miss:
  // every Edge in `edges_` has a metadata entry when the index is enabled.
  Vertex *FromVertexOf(Gid edge_gid) const {
    auto acc = data_.access();
    auto it = acc.find(edge_gid);
    MG_ASSERT(it != acc.end(), "Edge metadata missing for gid {}!", edge_gid.AsUint());
    return it->from_vertex;
  }

  // The only path that populates the index during recovery.
  void RebuildFrom(utils::SkipListDb<Vertex> &vertices,
                   std::optional<durability::ParallelizedSchemaCreationInfo> const &parallel_exec_info);

  void Clear() { data_.clear(); }

  void RunGc() { data_.run_gc(); }

  std::uint64_t size() const { return data_.size(); }

 private:
  utils::SkipListDb<EdgeMetadata> data_;
};

}  // namespace memgraph::storage
