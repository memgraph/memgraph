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

#include "query/graph.hpp"

namespace memgraph::query {
SubgraphDbAccessor::SubgraphDbAccessor(query::DbAccessor *db_accessor, Graph *graph)
    : db_accessor_(db_accessor), graph_(graph) {}

SubgraphDbAccessor *SubgraphDbAccessor::MakeSubgraphDbAccessor(DbAccessor *db_accessor, Graph *graph) {
  return new SubgraphDbAccessor(db_accessor, graph);
}

storage::PropertyId SubgraphDbAccessor::SubgraphDBNameToProperty(const std::string_view name) {
  return db_accessor_->NameToProperty(name);
}

storage::LabelId SubgraphDbAccessor::NameToLabel(const std::string_view name) {
  return db_accessor_->NameToLabel(name);
}

storage::EdgeTypeId SubgraphDbAccessor::NameToEdgeType(const std::string_view name) {
  return db_accessor_->NameToEdgeType(name);
}

const std::string &SubgraphDbAccessor::PropertyToName(storage::PropertyId prop) const {
  return db_accessor_->PropertyToName(prop);
}

const std::string &SubgraphDbAccessor::LabelToName(storage::LabelId label) const {
  return db_accessor_->LabelToName(label);
}

const std::string &SubgraphDbAccessor::EdgeTypeToName(storage::EdgeTypeId type) const {
  return db_accessor_->EdgeTypeToName(type);
}

storage::Result<std::optional<EdgeAccessor>> SubgraphDbAccessor::RemoveEdge(EdgeAccessor *edge) {
  auto result = db_accessor_->RemoveEdge(edge);
  // todo antoniofilipovic remove edge from subgraph
  return result;
}

storage::Result<EdgeAccessor> SubgraphDbAccessor::InsertEdge(VertexAccessor *from, VertexAccessor *to,
                                                             const storage::EdgeTypeId &edge_type) {
  auto result = db_accessor_->InsertEdge(from, to, edge_type);
  // todo antoniofilipovic add edge to subgraph
  return result;
}

storage::Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>>
SubgraphDbAccessor::DetachRemoveVertex(VertexAccessor *vertex_accessor) {
  auto result = db_accessor_->DetachRemoveVertex(vertex_accessor);
  // todo antoniofilipovic remove vertex and edges from subgraph
  return result;
}

storage::Result<std::optional<VertexAccessor>> SubgraphDbAccessor::RemoveVertex(VertexAccessor *vertex_accessor) {
  auto result = db_accessor_->RemoveVertex(vertex_accessor);
  // todo antoniofilipovic remove vertex from subgraph
  return result;
}

VertexAccessor SubgraphDbAccessor::InsertVertex() {
  auto result = db_accessor_->InsertVertex();
  // todo antoniofilipovic add vertex to subgraph
  return result;
}

VerticesIterable SubgraphDbAccessor::Vertices(storage::View view) {
  // todo antoniofilipovic change to get vertices from subgraph
  return VerticesIterable(graph_->vertices());
  // return db_accessor_->Vertices(view);
}

std::optional<VertexAccessor> SubgraphDbAccessor::FindVertex(storage::Gid gid, storage::View view) {
  // todo antoniofilipovic change to return SubgraphVertexAccessor && add check that vertex exists in subgraph
  return db_accessor_->FindVertex(gid, view);
}

query::Graph *SubgraphDbAccessor::getGraph() { return graph_; }
}  // namespace memgraph::query
