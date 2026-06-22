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

#include <string>
#include <string_view>

#include "query/db_accessor.hpp"
#include "query/graph.hpp"
#include "query/graph_view.hpp"
#include "storage/v2/id_types.hpp"

namespace memgraph::query {

// A project() subgraph seen as a graph to scan: a scan yields its member real
// vertices, and expansion filters a real vertex's edges to the subgraph's edge
// membership. Topology is the membership sets held on Graph; names share the real
// accessor's namespace, since a subgraph mints no ids of its own.
//
// It is a view layered over the real graph by membership: a scan over it yields a
// member vertex as the common scan element, so the read operators run over a
// subgraph the same way they run over the real graph. Edge membership is exposed
// here for the expand path rather than on GraphView.
class SubgraphGraphView final : public GraphView {
  Graph *graph_;
  DbAccessor *names_;

 public:
  SubgraphGraphView(Graph *graph, DbAccessor *names) : graph_(graph), names_(names) {}

  // True if the edge is a member of the subgraph. Expansion drops non-member
  // edges so a match stays within the subgraph.
  [[nodiscard]] bool ContainsEdge(const EdgeAccessor &edge) const { return graph_->edges().contains(edge); }

  VertexRange Vertices(storage::View /*view*/) override { return VertexRange{VerticesIterable(&graph_->vertices())}; }

  storage::LabelId NameToLabel(std::string_view name) override { return names_->NameToLabel(name); }

  const std::string &LabelToName(storage::LabelId label) const override { return names_->LabelToName(label); }

  storage::PropertyId NameToProperty(std::string_view name) override { return names_->NameToProperty(name); }

  const std::string &PropertyToName(storage::PropertyId prop) const override { return names_->PropertyToName(prop); }

  storage::EdgeTypeId NameToEdgeType(std::string_view name) override { return names_->NameToEdgeType(name); }

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const override { return names_->EdgeTypeToName(type); }
};

}  // namespace memgraph::query
