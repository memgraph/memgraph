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

#include <span>
#include <string>
#include <string_view>

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
// as the common scan element, so the read operators run over a projection the
// same way they run over the real graph. Edge expansion stays on the element, so
// OutEdges/InEdges are exposed here for the projection's expand surface rather
// than on GraphView.
class VirtualGraphView final : public GraphView {
  VirtualGraph *graph_;
  DbAccessor *names_;

 public:
  VirtualGraphView(VirtualGraph *graph, DbAccessor *names) : graph_(graph), names_(names) {}

  VirtualNodeRange Nodes() const { return VirtualNodeRange{graph_->nodes()}; }

  // The borrowed projection, for wrapping in a VirtualGraphDbAccessor when this
  // view is the ambient graph of a procedure call.
  [[nodiscard]] VirtualGraph *graph() const { return graph_; }

  VertexRange Vertices(storage::View /*view*/) override { return VertexRange{Nodes()}; }

  std::span<const VirtualEdge *const> OutEdges(storage::Gid node) const { return graph_->OutEdges(node); }

  std::span<const VirtualEdge *const> InEdges(storage::Gid node) const { return graph_->InEdges(node); }

  storage::LabelId NameToLabel(std::string_view name) override { return names_->NameToLabel(name); }

  const std::string &LabelToName(storage::LabelId label) const override { return names_->LabelToName(label); }

  storage::PropertyId NameToProperty(std::string_view name) override { return names_->NameToProperty(name); }

  const std::string &PropertyToName(storage::PropertyId prop) const override { return names_->PropertyToName(prop); }

  storage::EdgeTypeId NameToEdgeType(std::string_view name) override { return names_->NameToEdgeType(name); }

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const override { return names_->EdgeTypeToName(type); }
};

}  // namespace memgraph::query
