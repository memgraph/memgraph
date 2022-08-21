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

#include "query/db_accessor.hpp"

#include "query/graph.hpp"

#include <cppitertools/filter.hpp>
#include <cppitertools/imap.hpp>
#include "utils/pmr/unordered_set.hpp"

namespace memgraph::query {
SubgraphDbAccessor::SubgraphDbAccessor(query::DbAccessor *db_accessor, Graph *graph)
    : db_accessor_(db_accessor), graph_(graph) {}

SubgraphDbAccessor *SubgraphDbAccessor::MakeSubgraphDbAccessor(DbAccessor *db_accessor, Graph *graph) {
  return new SubgraphDbAccessor(db_accessor, graph);
}

storage::PropertyId SubgraphDbAccessor::NameToProperty(const std::string_view name) {
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
  if (result.HasError() || !*result) {
    return result;
  }
  return this->graph_->RemoveEdge(*edge);
}

storage::Result<EdgeAccessor> SubgraphDbAccessor::InsertEdge(SubgraphVertexAccessor *from, SubgraphVertexAccessor *to,
                                                             const storage::EdgeTypeId &edge_type) {
  VertexAccessor *from_impl = &from->impl_;
  VertexAccessor *to_impl = &to->impl_;

  auto result = db_accessor_->InsertEdge(from_impl, to_impl, edge_type);
  if (result.HasError()) {
    return result;
  }
  this->graph_->InsertEdge(*result);
  return result;
}

storage::Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>>
SubgraphDbAccessor::DetachRemoveVertex(
    VertexAccessor *) {  // NOLINT(hicpp-named-parameter, readability-convert-member-functions-to-static)
  throw std::logic_error{"Such operation not possible on subgraph"};
}

storage::Result<std::optional<VertexAccessor>> SubgraphDbAccessor::RemoveVertex(
    SubgraphVertexAccessor *subgraphvertex_accessor) {
  VertexAccessor *vertex_accessor = &subgraphvertex_accessor->impl_;
  auto result = db_accessor_->RemoveVertex(vertex_accessor);
  if (result.HasError() || !*result) {
    return result;
  }
  return this->graph_->RemoveVertex(*vertex_accessor);
}

SubgraphVertexAccessor SubgraphDbAccessor::InsertVertex() {
  VertexAccessor vertex = db_accessor_->InsertVertex();
  this->graph_->InsertVertex(vertex);
  return SubgraphVertexAccessor(vertex, this->getGraph());
}

VerticesIterable SubgraphDbAccessor::Vertices(storage::View) {
  return VerticesIterable(graph_->vertices());
}  // NOLINT(hicpp-named-parameter)

std::optional<VertexAccessor> SubgraphDbAccessor::FindVertex(storage::Gid gid, storage::View view) {
  std::optional<VertexAccessor> maybe_vertex = db_accessor_->FindVertex(gid, view);
  if (maybe_vertex && this->graph_->ContainsVertex(*maybe_vertex)) {
    return *maybe_vertex;
  }
  return std::nullopt;
}

query::Graph *SubgraphDbAccessor::getGraph() { return graph_; }

VertexAccessor SubgraphVertexAccessor::GetVertexAccessor() const { return impl_; }

auto SubgraphVertexAccessor::OutEdges(storage::View view) const -> decltype(impl_.OutEdges(view)) {
  auto maybe_edges = impl_.impl_.OutEdges(view, {});
  if (maybe_edges.HasError()) return maybe_edges.GetError();
  auto edges = std::move(*maybe_edges);
  auto graph_edges = graph_->edges();

  std::unordered_set<storage::EdgeAccessor> graph_edges_storage;

  for (auto e : graph_edges) {
    graph_edges_storage.insert(e.impl_);
  }

  std::vector<storage::EdgeAccessor> filteredOutEdges;
  for (auto &edge : edges) {
    if (std::find(begin(graph_edges_storage), end(graph_edges_storage), edge) != std::end(graph_edges_storage)) {
      filteredOutEdges.push_back(edge);
    }
  }

  return iter::imap(VertexAccessor::MakeEdgeAccessor, std::move(filteredOutEdges));
}

auto SubgraphVertexAccessor::InEdges(storage::View view) const -> decltype(impl_.InEdges(view)) {
  auto maybe_edges = impl_.impl_.InEdges(view, {});
  if (maybe_edges.HasError()) return maybe_edges.GetError();
  auto edges = std::move(*maybe_edges);
  auto graph_edges = graph_->edges();

  std::unordered_set<storage::EdgeAccessor> graph_edges_storage;

  for (auto e : graph_edges) {
    graph_edges_storage.insert(e.impl_);
  }

  std::vector<storage::EdgeAccessor> filteredOutEdges;
  for (auto &edge : edges) {
    if (std::find(begin(graph_edges_storage), end(graph_edges_storage), edge) != std::end(graph_edges_storage)) {
      filteredOutEdges.push_back(edge);
    }
  }

  return iter::imap(VertexAccessor::MakeEdgeAccessor, std::move(filteredOutEdges));
}

}  // namespace memgraph::query
