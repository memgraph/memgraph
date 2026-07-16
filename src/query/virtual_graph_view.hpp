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

#include <algorithm>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "query/db_accessor.hpp"
#include "query/graph_view.hpp"
#include "query/virtual_edge.hpp"
#include "query/virtual_graph.hpp"
#include "storage/v2/id_types.hpp"

namespace memgraph::query {

// The projection seen as a graph to scan: iterate its nodes, iterate a node's
// out-edges and in-edges, and map names to/from ids. Topology comes from the
// VirtualGraph; names share the real accessor's namespace, since a projection
// mints no label/property/edge-type ids of its own.
//
// It is the projection GraphView: a scan over it yields the projection's nodes
// as the common scan element, and OutEdges/InEdges expand a node's edges, so the
// read operators run over a projection the same way they run over the real graph.
// A real vertex reaching this view (an outer-scope vertex used inside a USE block)
// is not a projection node and has no edges here. The span-returning OutEdges/
// InEdges below remain for the mgp procedure path.
class VirtualGraphView final : public GraphView {
  VirtualGraph *graph_;

 public:
  VirtualGraphView(VirtualGraph *graph, DbAccessor *names) : GraphView(names), graph_(graph) {}

  VirtualNodeRange Nodes() const { return VirtualNodeRange{graph_->nodes()}; }

  // The borrowed projection, for wrapping in a VirtualGraphDbAccessor when this
  // view is the ambient graph of a procedure call.
  [[nodiscard]] VirtualGraph *graph() const { return graph_; }

  VertexRange Vertices(storage::View /*view*/) override { return VertexRange{Nodes()}; }

  std::span<const VirtualEdge *const> OutEdges(storage::Gid node) const { return graph_->OutEdges(node); }

  std::span<const VirtualEdge *const> InEdges(storage::Gid node) const { return graph_->InEdges(node); }

  EdgeRange OutEdges(const ScanVertex &from, storage::View /*view*/, const std::vector<storage::EdgeTypeId> &edge_types,
                     const std::optional<ScanVertex> &existing_dest, HopsLimit * /*hops*/) override {
    const auto *node = std::get_if<VirtualNode>(&from);
    if (node == nullptr)
      return EdgeRange{std::vector<const VirtualEdge *>{}, 0};  // a real vertex is no projection node
    return FilterEdges(graph_->OutEdges(node->Gid()), edge_types, existing_dest, /*neighbor_is_to=*/true);
  }

  EdgeRange InEdges(const ScanVertex &from, storage::View /*view*/, const std::vector<storage::EdgeTypeId> &edge_types,
                    const std::optional<ScanVertex> &existing_dest, HopsLimit * /*hops*/) override {
    const auto *node = std::get_if<VirtualNode>(&from);
    if (node == nullptr)
      return EdgeRange{std::vector<const VirtualEdge *>{}, 0};  // a real vertex is no projection node
    return FilterEdges(graph_->InEdges(node->Gid()), edge_types, existing_dest, /*neighbor_is_to=*/false);
  }

  // The projection's edge counts for a projection node; a real vertex is no projection node and has
  // no edges here.
  std::pair<int64_t, int64_t> Degree(const ScanVertex &from, storage::View /*view*/) override {
    const auto *node = std::get_if<VirtualNode>(&from);
    if (node == nullptr) return {0, 0};
    return {static_cast<int64_t>(graph_->InEdges(node->Gid()).size()),
            static_cast<int64_t>(graph_->OutEdges(node->Gid()).size())};
  }

 private:
  // Keep the projection edges whose type is in edge_types (empty = any) and, when a
  // destination endpoint is already bound, only the edge reaching it. neighbor_is_to
  // picks the far end for the current direction: the target for an out-edge, the
  // source for an in-edge. Projection edges carry type names, so the requested type
  // ids are resolved to names once per call.
  EdgeRange FilterEdges(std::span<const VirtualEdge *const> edges, const std::vector<storage::EdgeTypeId> &edge_types,
                        const std::optional<ScanVertex> &existing_dest, bool neighbor_is_to) const {
    std::vector<std::string_view> allowed;
    allowed.reserve(edge_types.size());
    for (const auto type : edge_types) allowed.emplace_back(names_->EdgeTypeToName(type));

    std::optional<storage::Gid> dest_gid;
    if (existing_dest) dest_gid = std::get<VirtualNode>(*existing_dest).Gid();

    // Every incident edge is examined; the count is taken before filtering so a
    // projection accounts the same traversal as the real graph (EdgeRange comment).
    const auto examined = static_cast<int64_t>(edges.size());
    std::vector<const VirtualEdge *> kept;
    kept.reserve(edges.size());
    for (const auto *edge : edges) {
      if (!allowed.empty() && std::ranges::find(allowed, std::string_view{edge->EdgeTypeName()}) == allowed.end()) {
        continue;
      }
      if (dest_gid) {
        const auto neighbor = neighbor_is_to ? edge->ToGid() : edge->FromGid();
        if (neighbor != *dest_gid) continue;
      }
      kept.push_back(edge);
    }
    return EdgeRange{std::move(kept), examined};
  }
};

}  // namespace memgraph::query
