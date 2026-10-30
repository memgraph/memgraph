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

#include "query/db_accessor.hpp"
#ifdef MG_ENTERPRISE
#include <algorithm>
#include <unordered_set>
#endif
#include <cppitertools/filter.hpp>
#include <cppitertools/imap.hpp>
#include "query/graph.hpp"
#include "query/virtual_graph.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/indices/text_index.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/storage_mode.hpp"

namespace memgraph::query {
SubgraphDbAccessor::SubgraphDbAccessor(query::DbAccessor db_accessor, Graph *graph)
    : db_accessor_(db_accessor), graph_(graph) {}

void SubgraphDbAccessor::TrackCurrentThreadAllocations() { return db_accessor_.TrackCurrentThreadAllocations(); }

void SubgraphDbAccessor::UntrackCurrentThreadAllocations() { return db_accessor_.UntrackCurrentThreadAllocations(); }

storage::PropertyId SubgraphDbAccessor::NameToProperty(const std::string_view name) {
  return db_accessor_.NameToProperty(name);
}

storage::LabelId SubgraphDbAccessor::NameToLabel(const std::string_view name) { return db_accessor_.NameToLabel(name); }

storage::EdgeTypeId SubgraphDbAccessor::NameToEdgeType(const std::string_view name) {
  return db_accessor_.NameToEdgeType(name);
}

const std::string &SubgraphDbAccessor::PropertyToName(storage::PropertyId prop) const {
  return db_accessor_.PropertyToName(prop);
}

const std::string &SubgraphDbAccessor::LabelToName(storage::LabelId label) const {
  return db_accessor_.LabelToName(label);
}

const std::string &SubgraphDbAccessor::EdgeTypeToName(storage::EdgeTypeId type) const {
  return db_accessor_.EdgeTypeToName(type);
}

storage::Result<std::optional<EdgeAccessor>> SubgraphDbAccessor::RemoveEdge(EdgeAccessor *edge) {
  if (!this->graph_->ContainsEdge(*edge)) {
    throw std::logic_error{"Projected graph must contain edge!"};
  }
  auto result = db_accessor_.RemoveEdge(edge);
  if (!result || !*result) {
    return result;
  }
  return this->graph_->RemoveEdge(*edge);
}

storage::Result<EdgeAccessor> SubgraphDbAccessor::InsertEdge(SubgraphVertexAccessor *from, SubgraphVertexAccessor *to,
                                                             const storage::EdgeTypeId &edge_type) {
  VertexAccessor *from_impl = &from->impl_;
  VertexAccessor *to_impl = &to->impl_;
  if (!this->graph_->ContainsVertex(*from_impl) || !this->graph_->ContainsVertex(*to_impl)) {
    throw std::logic_error{"Projected graph must contain both vertices to insert edge!"};
  }
  auto result = db_accessor_.InsertEdge(from_impl, to_impl, edge_type);
  if (!result) {
    return result;
  }
  this->graph_->InsertEdge(*result);
  return result;
}

storage::Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>>
SubgraphDbAccessor::DetachRemoveVertex(  // NOLINT(readability-convert-member-functions-to-static)
    SubgraphVertexAccessor *) {          // NOLINT(hicpp-named-parameter)
  throw std::logic_error{
      "Vertex holds only partial information about edges. Cannot detach delete safely while using projected graph."};
}

storage::Result<std::optional<VertexAccessor>> SubgraphDbAccessor::RemoveVertex(
    SubgraphVertexAccessor *subgraphvertex_accessor) {
  VertexAccessor *vertex_accessor = &subgraphvertex_accessor->impl_;
  if (!this->graph_->ContainsVertex(*vertex_accessor)) {
    throw std::logic_error{"Projected graph must contain vertex!"};
  }
  auto result = db_accessor_.RemoveVertex(vertex_accessor);
  if (!result || !*result) {
    return result;
  }
  return this->graph_->RemoveVertex(*vertex_accessor);
}

SubgraphVertexAccessor SubgraphDbAccessor::InsertVertex() {
  VertexAccessor vertex = db_accessor_.InsertVertex();
  this->graph_->InsertVertex(vertex);
  return SubgraphVertexAccessor(vertex, this->getGraph());
}

VerticesIterable SubgraphDbAccessor::Vertices(storage::View) {  // NOLINT(hicpp-named-parameter)
  return VerticesIterable(&graph_->vertices());
}

std::optional<VertexAccessor> SubgraphDbAccessor::FindVertex(storage::Gid gid, storage::View view) {
  std::optional<VertexAccessor> maybe_vertex = db_accessor_.FindVertex(gid, view);
  if (maybe_vertex && this->graph_->ContainsVertex(*maybe_vertex)) {
    return *maybe_vertex;
  }
  return std::nullopt;
}

std::optional<EdgeAccessor> SubgraphDbAccessor::FindEdge(storage::Gid edge_gid, storage::Gid from_vertex_gid,
                                                         storage::View view) {
  std::optional<EdgeAccessor> maybe_edge = db_accessor_.FindEdge(edge_gid, from_vertex_gid, view);
  if (maybe_edge && this->graph_->ContainsEdge(*maybe_edge)) {
    return maybe_edge;
  }
  return std::nullopt;
}

query::Graph *SubgraphDbAccessor::getGraph() { return graph_; }

storage::StorageMode SubgraphDbAccessor::GetStorageMode() const noexcept { return db_accessor_.GetStorageMode(); }

DbAccessor *SubgraphDbAccessor::GetAccessor() { return &db_accessor_; }

VirtualGraphDbAccessor::VirtualGraphDbAccessor(query::DbAccessor db_accessor, VirtualGraph *graph)
    : db_accessor_(db_accessor), graph_(graph) {}

void VirtualGraphDbAccessor::TrackCurrentThreadAllocations() { db_accessor_.TrackCurrentThreadAllocations(); }

void VirtualGraphDbAccessor::
    UntrackCurrentThreadAllocations() {  // NOLINT(readability-convert-member-functions-to-static)
  query::DbAccessor::UntrackCurrentThreadAllocations();
}

storage::PropertyId VirtualGraphDbAccessor::NameToProperty(const std::string_view name) {
  return db_accessor_.NameToProperty(name);
}

storage::LabelId VirtualGraphDbAccessor::NameToLabel(const std::string_view name) {
  return db_accessor_.NameToLabel(name);
}

storage::EdgeTypeId VirtualGraphDbAccessor::NameToEdgeType(const std::string_view name) {
  return db_accessor_.NameToEdgeType(name);
}

const std::string &VirtualGraphDbAccessor::PropertyToName(storage::PropertyId prop) const {
  return db_accessor_.PropertyToName(prop);
}

const std::string &VirtualGraphDbAccessor::LabelToName(storage::LabelId label) const {
  return db_accessor_.LabelToName(label);
}

const std::string &VirtualGraphDbAccessor::EdgeTypeToName(storage::EdgeTypeId type) const {
  return db_accessor_.EdgeTypeToName(type);
}

std::shared_ptr<const VirtualNode> VirtualGraphDbAccessor::FindNode(storage::Gid synthetic_gid) const {
  return graph_->FindNode(synthetic_gid);
}

VirtualGraph *VirtualGraphDbAccessor::getGraph() const { return graph_; }

storage::StorageMode VirtualGraphDbAccessor::GetStorageMode() const noexcept { return db_accessor_.GetStorageMode(); }

DbAccessor *VirtualGraphDbAccessor::GetAccessor() { return &db_accessor_; }

VertexAccessor SubgraphVertexAccessor::GetVertexAccessor() const { return impl_; }

storage::Result<storage::PropertyValue> SubgraphVertexAccessor::GetProperty(storage::View view,
                                                                            storage::PropertyId key) const {
  return impl_.GetProperty(view, key);
}

namespace {

/// Narrows an expansion to the edges `graph` holds, keeping it a range that builds its accessors
/// when they are looked at.
auto KeptIn(Graph const *graph) {
  return [graph](storage::EdgesVertexAccessorResult &&result) -> ExpandedEdgesResult {
    auto const &graph_edges = graph->edges();
    auto kept = result.edges.Filtered(
        [&graph_edges](storage::EdgeAccessor const &edge) { return graph_edges.contains(EdgeAccessor{edge}); });
    return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(kept)}, .expanded_count = result.expanded_count};
  };
}
#ifdef MG_ENTERPRISE

/// On a branch the storage adjacency is one-sided and holds raw stubs, so the expansion goes through the branch-aware
/// query accessor and only membership is filtered here (by gid, so stub-backed and fork copies of an edge match).
auto KeptInBranch(Graph const *graph) {
  return [graph](ExpandedEdgesResult &&result) -> ExpandedEdgesResult {
    auto const &graph_edges = graph->edges();
    std::vector<EdgeAccessor> kept;
    for (auto const &edge : result.edges) {
      if (graph_edges.contains(edge)) kept.push_back(edge);
    }
    return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(kept)}, .expanded_count = result.expanded_count};
  };
}
#endif

}  // namespace

storage::Result<ExpandedEdgesResult> SubgraphVertexAccessor::OutEdges(storage::View view) const {
#ifdef MG_ENTERPRISE
  if (impl_.branch_ctx_ != nullptr) [[unlikely]]
    return impl_.OutEdges(view).transform(KeptInBranch(graph_));
#endif
  return impl_.impl_.OutEdges(view, {}).transform(KeptIn(graph_));
}

storage::Result<ExpandedEdgesResult> SubgraphVertexAccessor::InEdges(storage::View view) const {
#ifdef MG_ENTERPRISE
  if (impl_.branch_ctx_ != nullptr) [[unlikely]]
    return impl_.InEdges(view).transform(KeptInBranch(graph_));
#endif
  return impl_.impl_.InEdges(view, {}).transform(KeptIn(graph_));
}

#ifdef MG_ENTERPRISE
std::optional<VertexAccessor> DbAccessor::BranchFindVertex(storage::Gid gid, storage::View view) {
  auto maybe_vertex = branch_ctx_->ResolveVertex(gid, view);
  if (maybe_vertex) return VertexAccessor(*maybe_vertex, branch_ctx_);
  return std::nullopt;
}

std::optional<EdgeAccessor> DbAccessor::BranchFindEdge(storage::Gid gid, storage::View view) {
  if (auto diff_edge = branch_ctx_->FindDiffEdge(gid, view)) return EdgeAccessor(*diff_edge, branch_ctx_);
  for (auto vertex : branch_ctx_->Vertices(view)) {
    for (auto &edge : branch_ctx_->ResolveEdges(vertex.Gid(), storage::EdgeDirection::OUT, view)) {
      if (edge.Gid() == gid) return EdgeAccessor(edge, branch_ctx_);
    }
  }
  return std::nullopt;
}

std::optional<EdgeAccessor> DbAccessor::BranchFindEdge(storage::Gid edge_gid, storage::Gid from_vertex_gid,
                                                       storage::View view) {
  for (auto &edge : branch_ctx_->ResolveEdges(from_vertex_gid, storage::EdgeDirection::OUT, view)) {
    if (edge.Gid() == edge_gid) return EdgeAccessor(edge, branch_ctx_);
  }
  return std::nullopt;
}

VerticesIterable DbAccessor::BranchVertices(storage::View view) {
  // No deletes and no branch-native vertex (the skiplist size counts uncommitted and un-GC'd ones too, so 0 means none
  // visible to anyone): the union is exactly main@fork's stream, yielded without the per-vertex merge. OLD only: a NEW
  // scan must see deletes the same query makes while it iterates, which the union checks per vertex.
  if (view == storage::View::OLD && !branch_ctx_->MayHaveAnyVertexDeleted() &&
      accessor_->ApproximateVertexCount() == 0) {
    return {branch_ctx_->historical().Vertices(storage::View::OLD), branch_ctx_};
  }
  return {branch_ctx_->Vertices(view), branch_ctx_};
}

namespace {
// The diff engine's own adjacency check sees only branch-created edges (for a fork vertex, those on its anchor), so the
// branch's full adjacency is consulted. Edges the same statement deletes (`co_deleted`) do not count.
bool BranchVertexHasUncountedEdges(versioning::BranchContext &ctx, storage::Gid vertex_gid,
                                   const std::unordered_set<storage::Gid> &co_deleted) {
  auto has_uncounted = [&](storage::EdgeDirection direction) {
    auto edges = ctx.ResolveEdges(vertex_gid, direction, storage::View::NEW);
    return std::ranges::any_of(edges, [&](auto const &e) { return !co_deleted.contains(e.Gid()); });
  };
  return has_uncounted(storage::EdgeDirection::OUT) || has_uncounted(storage::EdgeDirection::IN);
}
}  // namespace

storage::Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>>
DbAccessor::BranchDetachDelete(std::vector<VertexAccessor> nodes, std::vector<EdgeAccessor> edges, bool detach) {
  using ReturnType = std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>;
  auto &ctx = *branch_ctx_;
  constexpr auto kView = storage::View::NEW;

  // Already deleted (by this statement or an earlier one): a no-op, as on main.
  std::erase_if(nodes, [&](const VertexAccessor &node) {
    return ctx.IsForkVertexGid(node.Gid()) ? ctx.IsVertexDeleted(node.Gid(), kView)
                                           : !accessor_->FindVertex(node.Gid(), kView).has_value();
  });
  std::erase_if(edges, [&](const EdgeAccessor &edge) {
    return ctx.IsForkEdgeGid(edge.Gid()) ? ctx.IsForkEdgeHidden(edge.impl_, kView)
                                         : !ctx.FindDiffEdge(edge.Gid(), edge.impl_.from_vertex_->gid, kView);
  });

  // A plain DELETE fails like main's while a visible edge remains; checked before any write.
  if (!detach) {
    std::unordered_set<storage::Gid> co_deleted;
    for (const auto &edge : edges) co_deleted.insert(edge.Gid());
    for (const auto &node : nodes) {
      if (BranchVertexHasUncountedEdges(ctx, node.Gid(), co_deleted)) {
        return std::unexpected{storage::Error::VERTEX_HAS_EDGES};
      }
    }
  }

  std::vector<VertexAccessor> deleted_vertices;
  std::vector<EdgeAccessor> deleted_edges;
  std::vector<EdgeAccessor> diff_edges;  // handed to the diff engine's delete
  std::unordered_set<storage::Gid> seen_edges;

  for (auto &edge : edges) {
    if (!seen_edges.insert(edge.Gid()).second) continue;
    if (!ctx.IsForkEdgeGid(edge.Gid())) {
      diff_edges.push_back(edge);
      continue;
    }
    auto deleted = ctx.DeleteForkEdge(edge.impl_);
    if (!deleted) return std::unexpected{deleted.error()};
    if (*deleted) deleted_edges.push_back(edge);
  }

  std::vector<VertexAccessor *> diff_nodes;
  std::vector<VertexAccessor *> fork_nodes;
  for (auto &node : nodes) {
    if (!ctx.IsForkVertexGid(node.Gid())) {
      auto cowed = ctx.CowVertex(node.Gid());
      if (!cowed) throw QueryRuntimeException(cowed.error().message);
      node.impl_ = *cowed;
      diff_nodes.push_back(&node);
      continue;
    }
    fork_nodes.push_back(&node);
    if (!detach) continue;
    // Every incident edge is listed before any delete flag is written, so an edge between two deleted vertices is seen
    // from both ends and counted once.
    for (auto direction : {storage::EdgeDirection::OUT, storage::EdgeDirection::IN}) {
      for (auto &raw : ctx.ResolveEdges(node.Gid(), direction, kView)) {
        if (!seen_edges.insert(raw.Gid()).second) continue;
        EdgeAccessor incident(raw, branch_ctx_);
        if (raw.storage_ == &ctx.diff_engine()) {
          diff_edges.push_back(std::move(incident));
        } else {
          if (!ctx.CanDeleteForkEdge(raw.Gid())) return std::unexpected{storage::Error::SERIALIZATION_ERROR};
          deleted_edges.push_back(std::move(incident));
        }
      }
    }
  }

  if (!diff_nodes.empty() || !diff_edges.empty()) {
    std::vector<storage::VertexAccessor *> nodes_impl;
    std::vector<storage::EdgeAccessor *> edges_impl;
    nodes_impl.reserve(diff_nodes.size());
    edges_impl.reserve(diff_edges.size());
    for (auto *node : diff_nodes) nodes_impl.push_back(&node->impl_);
    for (auto &edge : diff_edges) edges_impl.push_back(&edge.impl_);
    auto res = accessor_->DetachDelete(std::move(nodes_impl), std::move(edges_impl), detach);
    if (!res) return std::unexpected{res.error()};
    if (*res) {
      for (const auto &v : (*res)->first) deleted_vertices.emplace_back(v, branch_ctx_);
      for (const auto &e : (*res)->second) deleted_edges.emplace_back(e, branch_ctx_);
    }
  }

  for (auto *node : fork_nodes) {
    auto marked = ctx.OverlayMarkDeleted(node->Gid());
    if (!marked) return std::unexpected{marked.error()};
    if (*marked) deleted_vertices.push_back(*node);
  }

  return std::make_optional<ReturnType>(std::move(deleted_vertices), std::move(deleted_edges));
}

#endif
bool DbAccessor::TextIndexExists(const std::string &index_name) const { return accessor_->TextIndexExists(index_name); }

std::vector<storage::TextSearchResult> DbAccessor::TextIndexSearch(const std::string &index_name,
                                                                   const std::string &search_query,
                                                                   text_search_mode search_mode,
                                                                   const storage::TextSearchConfig &config) const {
  return accessor_->TextIndexSearch(index_name, search_query, search_mode, config);
}

std::string DbAccessor::TextIndexAggregate(const std::string &index_name, const std::string &search_query,
                                           const std::string &aggregation_query) const {
  return accessor_->TextIndexAggregate(index_name, search_query, aggregation_query);
}

std::string DbAccessor::TextEdgeIndexAggregate(const std::string &index_name, const std::string &search_query,
                                               const std::string &aggregation_query) {
  return accessor_->TextEdgeIndexAggregate(index_name, search_query, aggregation_query);
}

std::vector<storage::TextEdgeSearchResult> DbAccessor::SearchEdgeTextIndex(
    const std::string &index_name, const std::string &search_query, text_search_mode search_mode,
    const storage::TextSearchConfig &config) const {
  return accessor_->SearchEdgeTextIndex(index_name, search_query, search_mode, config);
}

bool DbAccessor::PointIndexExists(storage::LabelId label, storage::PropertyId prop) const {
  return accessor_->PointIndexExists(label, prop);
}

std::vector<std::tuple<storage::VertexAccessor, double, double>> DbAccessor::VectorIndexSearchOnNodes(
    const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) {
  return accessor_->VectorIndexSearchOnNodes(index_name, number_of_results, vector);
}

std::vector<std::tuple<storage::EdgeAccessor, double, double>> DbAccessor::VectorIndexSearchOnEdges(
    const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) {
  return accessor_->VectorIndexSearchOnEdges(index_name, number_of_results, vector);
}

std::vector<storage::VectorIndexInfo> DbAccessor::ListAllVectorIndices() const {
  return accessor_->ListAllVectorIndices();
}

std::vector<storage::VectorEdgeIndexInfo> DbAccessor::ListAllVectorEdgeIndices() const {
  return accessor_->ListAllVectorEdgeIndices();
}

auto DbAccessor::PointVertices(storage::LabelId label, storage::PropertyId property,
                               storage::CoordinateReferenceSystem crs, TypedValue const &point_value,
                               TypedValue const &boundary_value, plan::PointDistanceCondition condition)
    -> PointIterable {
  return PointIterable(accessor_->PointVertices(label,
                                                property,
                                                crs,
                                                point_value.ToPropertyValue(accessor_->GetNameIdMapper()),
                                                boundary_value.ToPropertyValue(accessor_->GetNameIdMapper()),
                                                condition));
}

auto DbAccessor::PointVertices(storage::LabelId label, storage::PropertyId property,
                               storage::CoordinateReferenceSystem crs, TypedValue const &bottom_left,
                               TypedValue const &top_right, plan::WithinBBoxCondition condition) -> PointIterable {
  return PointIterable(accessor_->PointVertices(label,
                                                property,
                                                crs,
                                                bottom_left.ToPropertyValue(accessor_->GetNameIdMapper()),
                                                top_right.ToPropertyValue(accessor_->GetNameIdMapper()),
                                                condition));
}
}  // namespace memgraph::query
