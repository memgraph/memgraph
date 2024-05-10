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

#include <optional>
#include <tuple>
#include <vector>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::storage {

struct Vertex {
 public:
  using EdgeTuple = std::tuple<EdgeTypeId, Vertex *, EdgeRef>;
  using Edges = std::vector<EdgeTuple>;

  Vertex(Gid gid, Delta *delta) : gid(gid), delta(delta) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT ||
                  delta->action == Delta::Action::DELETE_DESERIALIZED_OBJECT,
              "Vertex must be created with an initial DELETE_OBJECT delta!");
  }

  // inline Edges InEdges() { return in_edges; }
  // inline Edges OutEdges() { return out_edges; }

  Edges::iterator InEdgesBegin();
  Edges::iterator InEdgesEnd();
  Edges::iterator OutEdgesBegin();
  Edges::iterator OutEdgesEnd();

  inline Edges::size_type InEdgesSize() const { return in_edges.size(); }
  inline Edges::size_type OutEdgesSize() const { return out_edges.size(); }

  inline void ReserveInEdges(Edges::size_type size) { in_edges.reserve(size); }
  inline void ReserveOutEdges(Edges::size_type size) { out_edges.reserve(size); }

  void AddInEdge(EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge_ref);
  void AddOutEdge(EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge_ref);
  void AddInEdge(EdgeTuple &&edge);
  void AddOutEdge(EdgeTuple &&edge);

  inline bool HasEdges() const { return !in_edges.empty() || !out_edges.empty(); }

  bool HasInEdge(const EdgeTuple &edge) const;
  bool HasOutEdge(const EdgeTuple &edge) const;

  bool HasInEdge(const EdgeRef &edge) const;
  bool HasOutEdge(const EdgeRef &edge) const;

  bool RemoveInEdge(const EdgeTuple &edge);
  bool RemoveOutEdge(const EdgeTuple &edge);

  std::optional<EdgeTuple> PopBackInEdge();
  std::optional<EdgeTuple> PopBackOutEdge();

  Edges::size_type MoveInEdgesToEraseToEnd(const std::unordered_set<Gid> &set_for_erasure, bool properties_on_edges);
  Edges::size_type MoveOutEdgesToEraseToEnd(const std::unordered_set<Gid> &set_for_erasure, bool properties_on_edges);

  bool ChangeInEdgeType(const EdgeTuple &edge, EdgeTypeId new_type);
  bool ChangeOutEdgeType(const EdgeTuple &edge, EdgeTypeId new_type);

  std::optional<EdgeTuple> GetInEdge(const Gid &edge_gid, bool properties_on_edges) const;
  std::optional<EdgeTuple> GetOutEdge(const Gid &edge_gid, bool properties_on_edges) const;

  std::optional<EdgeTuple> FindInEdge(const Edge *edge_ptr) const;
  std::optional<EdgeTuple> FindOutEdge(const Edge *edge_ptr) const;

  const Gid gid;

  std::vector<LabelId> labels;
  PropertyStore properties;

  mutable utils::RWSpinLock lock;
  bool deleted = false;
  // uint8_t PAD;
  // uint16_t PAD;

  Delta *delta;

 private:
  Edges in_edges;
  Edges out_edges;
};

static_assert(alignof(Vertex) >= 8, "The Vertex should be aligned to at least 8!");

inline bool operator==(const Vertex &first, const Vertex &second) { return first.gid == second.gid; }
inline bool operator<(const Vertex &first, const Vertex &second) { return first.gid < second.gid; }
inline bool operator==(const Vertex &first, const Gid &second) { return first.gid == second; }
inline bool operator<(const Vertex &first, const Gid &second) { return first.gid < second; }

}  // namespace memgraph::storage
