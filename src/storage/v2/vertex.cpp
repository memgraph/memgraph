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

#include "storage/v2/vertex.hpp"

#include <algorithm>
#include <optional>

#include "storage/v2/edge.hpp"
#include "storage/v2/edge_ref.hpp"

namespace memgraph::storage {

namespace {
constexpr std::size_t kEdgeRefPos = 2U;

Gid GetEdgeRefGid(const EdgeRef &edge_ref, bool properties_on_edges) {
  if (properties_on_edges) {
    return edge_ref.ptr->gid;
  }
  return edge_ref.gid;
}

}  // namespace

Vertex::Edges::iterator Vertex::InEdgesBegin() { return in_edges.begin(); }

Vertex::Edges::iterator Vertex::InEdgesEnd() { return in_edges.end(); }

Vertex::Edges::iterator Vertex::OutEdgesBegin() { return out_edges.begin(); }

Vertex::Edges::iterator Vertex::OutEdgesEnd() { return out_edges.end(); }

void Vertex::AddInEdge(EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge_ref) {
  in_edges.emplace_back(edge_type, vertex, edge_ref);
}

void Vertex::AddOutEdge(EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge_ref) {
  out_edges.emplace_back(edge_type, vertex, edge_ref);
}

void Vertex::AddInEdge(Vertex::EdgeTuple &&edge) { in_edges.emplace_back(std::move(edge)); }

void Vertex::AddOutEdge(Vertex::EdgeTuple &&edge) { out_edges.emplace_back(std::move(edge)); }

bool Vertex::HasInEdge(const Vertex::EdgeTuple &edge) const {
  return std::find(in_edges.begin(), in_edges.end(), edge) != in_edges.end();
}

bool Vertex::HasOutEdge(const Vertex::EdgeTuple &edge) const {
  return std::find(out_edges.begin(), out_edges.end(), edge) != out_edges.end();
}

bool Vertex::HasInEdge(const EdgeRef &edge) const {
  return std::find_if(in_edges.begin(), in_edges.end(),
                      [&](const auto &e) { return std::get<kEdgeRefPos>(e) == edge; }) != in_edges.end();
}

bool Vertex::HasOutEdge(const EdgeRef &edge) const {
  return std::find_if(out_edges.begin(), out_edges.end(),
                      [&](const auto &e) { return std::get<kEdgeRefPos>(e) == edge; }) != out_edges.end();
}

bool Vertex::RemoveInEdge(const Vertex::EdgeTuple &edge) {
  auto it = std::find(in_edges.begin(), in_edges.end(), edge);
  if (it != in_edges.end()) {
    in_edges.erase(it);
    return true;
  }
  return false;
}

bool Vertex::RemoveOutEdge(const Vertex::EdgeTuple &edge) {
  auto it = std::find(out_edges.begin(), out_edges.end(), edge);
  if (it != out_edges.end()) {
    out_edges.erase(it);
    return true;
  }
  return false;
}

std::optional<Vertex::EdgeTuple> Vertex::PopBackInEdge() {
  if (in_edges.empty()) {
    return std::nullopt;
  }
  auto tup = std::move(in_edges.back());
  in_edges.pop_back();
  return tup;
}

std::optional<Vertex::EdgeTuple> Vertex::PopBackOutEdge() {
  if (out_edges.empty()) {
    return std::nullopt;
  }
  auto tup = std::move(out_edges.back());
  out_edges.pop_back();
  return tup;
}

Vertex::Edges::size_type Vertex::MoveInEdgesToEraseToEnd(const std::unordered_set<Gid> &set_for_erasure,
                                                         bool properties_on_edges) {
  auto it = std::partition(in_edges.begin(), in_edges.end(), [&set_for_erasure, &properties_on_edges](auto &edge) {
    auto const &[edge_type, opposing_vertex, edge_ref] = edge;
    auto const edge_gid = GetEdgeRefGid(edge_ref, properties_on_edges);
    return !set_for_erasure.contains(edge_gid);
  });
  return in_edges.end() - it;
}

Vertex::Edges::size_type Vertex::MoveOutEdgesToEraseToEnd(const std::unordered_set<Gid> &set_for_erasure,
                                                          bool properties_on_edges) {
  auto it = std::partition(out_edges.begin(), out_edges.end(), [&set_for_erasure, &properties_on_edges](auto &edge) {
    auto const &[edge_type, opposing_vertex, edge_ref] = edge;
    auto const edge_gid = GetEdgeRefGid(edge_ref, properties_on_edges);
    return !set_for_erasure.contains(edge_gid);
  });
  return out_edges.end() - it;
}

bool Vertex::ChangeInEdgeType(const Vertex::EdgeTuple &edge, EdgeTypeId new_type) {
  auto it = std::find(in_edges.begin(), in_edges.end(), edge);
  if (it != in_edges.end()) {
    *it = std::tuple<EdgeTypeId, Vertex *, EdgeRef>{new_type, std::get<1>(*it), std::get<2>(*it)};
    return true;
  }
  return false;
}

bool Vertex::ChangeOutEdgeType(const Vertex::EdgeTuple &edge, EdgeTypeId new_type) {
  auto it = std::find(out_edges.begin(), out_edges.end(), edge);
  if (it != out_edges.end()) {
    *it = std::tuple<EdgeTypeId, Vertex *, EdgeRef>{new_type, std::get<1>(*it), std::get<2>(*it)};
    return true;
  }
  return false;
}

std::optional<Vertex::EdgeTuple> Vertex::GetInEdge(const Gid &edge_gid, bool properties_on_edges) const {
  auto it = std::find_if(in_edges.begin(), in_edges.end(), [&](const auto &edge) {
    return GetEdgeRefGid(std::get<kEdgeRefPos>(edge), properties_on_edges) == edge_gid;
  });

  if (it != in_edges.end()) {
    return *it;
  }
  return std::nullopt;
}

std::optional<Vertex::EdgeTuple> Vertex::GetOutEdge(const Gid &edge_gid, bool properties_on_edges) const {
  auto it = std::find_if(out_edges.begin(), out_edges.end(), [&](const auto &edge) {
    return GetEdgeRefGid(std::get<kEdgeRefPos>(edge), properties_on_edges) == edge_gid;
  });

  if (it != out_edges.end()) {
    return *it;
  }
  return std::nullopt;
}

std::optional<Vertex::EdgeTuple> Vertex::FindInEdge(const Edge *edge_ptr) const {
  auto it = std::find_if(in_edges.begin(), in_edges.end(),
                         [&](const auto &edge) { return std::get<kEdgeRefPos>(edge).ptr == edge_ptr; });

  if (it != in_edges.end()) {
    return *it;
  }
  return std::nullopt;
}

std::optional<Vertex::EdgeTuple> Vertex::FindOutEdge(const Edge *edge_ptr) const {
  auto it = std::find_if(out_edges.begin(), out_edges.end(),
                         [&](const auto &edge) { return std::get<kEdgeRefPos>(edge).ptr == edge_ptr; });

  if (it != out_edges.end()) {
    return *it;
  }
  return std::nullopt;
}

}  // namespace memgraph::storage
