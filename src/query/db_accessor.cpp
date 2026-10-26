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
#include <algorithm>
#include <cppitertools/filter.hpp>
#include <cppitertools/imap.hpp>
#include <vector>
#include "query/graph.hpp"
#include "query/virtual_graph.hpp"
#include "storage/v2/constraints/active_constraints.hpp"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/indices/text_index.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/transaction_constants.hpp"
#include "versioning/main_constraints.hpp"

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
// Storage-level adjacency on a branch is one-sided and exposes raw stubs, so the branch-aware query-level
// {In,Out}Edges is used and only membership is filtered; membership is by gid, so stamped edges match.
storage::Result<EdgeVertexAccessorResult> FilterBranchEdgesBySubgraph(storage::Result<EdgeVertexAccessorResult> res,
                                                                      const Graph &graph) {
  if (!res) return res;
  const auto &graph_edges = graph.edges();
  std::erase_if(res->edges, [&](const EdgeAccessor &edge) { return !graph_edges.contains(edge); });
  return res;
}
}  // namespace

storage::Result<EdgeVertexAccessorResult> SubgraphVertexAccessor::OutEdges(storage::View view) const {
  if (impl_.branch_ctx_ != nullptr) {
    return FilterBranchEdgesBySubgraph(impl_.OutEdges(view), *graph_);
  }
  auto maybe_edges = impl_.impl_.OutEdges(view, {});
  if (!maybe_edges) return std::unexpected{maybe_edges.error()};
  auto edges = std::move(maybe_edges->edges);
  const auto &graph_edges = graph_->edges();

  std::vector<storage::EdgeAccessor> filteredOutEdges;
  for (auto &edge : edges) {
    auto edge_q = EdgeAccessor(edge);
    if (graph_edges.contains(edge_q)) {
      filteredOutEdges.push_back(edge);
    }
  }

  std::vector<EdgeAccessor> resulting_edges;
  resulting_edges.reserve(filteredOutEdges.size());
  std::ranges::transform(
      filteredOutEdges, std::back_inserter(resulting_edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = std::move(resulting_edges), .expanded_count = maybe_edges->expanded_count};
}

storage::Result<EdgeVertexAccessorResult> SubgraphVertexAccessor::InEdges(storage::View view) const {
  if (impl_.branch_ctx_ != nullptr) {
    return FilterBranchEdgesBySubgraph(impl_.InEdges(view), *graph_);
  }
  auto maybe_edges = impl_.impl_.InEdges(view, {});
  if (!maybe_edges) return std::unexpected{maybe_edges.error()};
  auto edges = std::move(maybe_edges->edges);
  const auto &graph_edges = graph_->edges();

  std::vector<storage::EdgeAccessor> filteredOutEdges;
  for (auto &edge : edges) {
    auto edge_q = EdgeAccessor(edge);
    if (graph_edges.contains(edge_q)) {
      filteredOutEdges.push_back(edge);
    }
  }

  std::vector<EdgeAccessor> resulting_edges;
  resulting_edges.reserve(filteredOutEdges.size());
  std::ranges::transform(
      filteredOutEdges, std::back_inserter(resulting_edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = std::move(resulting_edges), .expanded_count = maybe_edges->expanded_count};
}

bool DbAccessor::TextIndexExists(const std::string &index_name) const { return accessor_->TextIndexExists(index_name); }

std::vector<storage::TextSearchResult> DbAccessor::TextIndexSearch(const std::string &index_name,
                                                                   const std::string &search_query,
                                                                   text_search_mode search_mode,
                                                                   const storage::TextSearchConfig &config) const {
  // Text indices are not mirrored into the diff engine, which would report a misleading "index does not exist".
  if (branch_ctx_ != nullptr) {
    throw storage::TextSearchException("Text search is not supported on a checked-out branch.");
  }
  return accessor_->TextIndexSearch(index_name, search_query, search_mode, config);
}

std::string DbAccessor::TextIndexAggregate(const std::string &index_name, const std::string &search_query,
                                           const std::string &aggregation_query) const {
  if (branch_ctx_ != nullptr) {
    throw storage::TextSearchException("Text search is not supported on a checked-out branch.");
  }
  return accessor_->TextIndexAggregate(index_name, search_query, aggregation_query);
}

std::string DbAccessor::TextEdgeIndexAggregate(const std::string &index_name, const std::string &search_query,
                                               const std::string &aggregation_query) {
  if (branch_ctx_ != nullptr) {
    throw storage::TextSearchException("Text search is not supported on a checked-out branch.");
  }
  return accessor_->TextEdgeIndexAggregate(index_name, search_query, aggregation_query);
}

std::vector<storage::TextEdgeSearchResult> DbAccessor::SearchEdgeTextIndex(
    const std::string &index_name, const std::string &search_query, text_search_mode search_mode,
    const storage::TextSearchConfig &config) const {
  if (branch_ctx_ != nullptr) {
    throw storage::TextSearchException("Text search is not supported on a checked-out branch.");
  }
  return accessor_->SearchEdgeTextIndex(index_name, search_query, search_mode, config);
}

bool DbAccessor::PointIndexExists(storage::LabelId label, storage::PropertyId prop) const {
  return accessor_->PointIndexExists(label, prop);
}

std::vector<std::tuple<storage::VertexAccessor, double, double>> DbAccessor::VectorIndexSearchOnNodes(
    const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) {
  // Vector indices are likewise not mirrored into the diff engine.
  if (branch_ctx_ != nullptr) {
    throw storage::VectorSearchException("Vector search is not supported on a checked-out branch.");
  }
  return accessor_->VectorIndexSearchOnNodes(index_name, number_of_results, vector);
}

std::vector<std::tuple<storage::EdgeAccessor, double, double>> DbAccessor::VectorIndexSearchOnEdges(
    const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) {
  if (branch_ctx_ != nullptr) {
    throw storage::VectorSearchException("Vector search is not supported on a checked-out branch.");
  }
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
  // Point indices are not mirrored into the diff engine; delegating would silently miss historical vertices.
  if (branch_ctx_ != nullptr) {
    throw NotYetImplemented("point index scans are not yet supported on a checked-out version/branch");
  }
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
  if (branch_ctx_ != nullptr) {
    throw NotYetImplemented("point index scans are not yet supported on a checked-out version/branch");
  }
  return PointIterable(accessor_->PointVertices(label,
                                                property,
                                                crs,
                                                bottom_left.ToPropertyValue(accessor_->GetNameIdMapper()),
                                                top_right.ToPropertyValue(accessor_->GetNameIdMapper()),
                                                condition));
}

std::expected<void, storage::ConstraintViolation> DbAccessor::ValidateBranchConstraints() {
  if (branch_ctx_ == nullptr) return {};
  auto &ctx = *branch_ctx_;

  // The diff engine mirrors no constraints: main's current set is checked against each touched vertex's reconstructed
  // view. Unique constraints are not enforced on a branch.
  versioning::MainConstraints const main_constraints{ctx};
  auto const existence = main_constraints.Existence();
  auto const types = main_constraints.Types();
  if (existence.empty() && types.empty()) return {};

  // Branch-native vertices: the newest delta of every object this transaction wrote is that object's chain head (a
  // concurrent writer would have hit a serialization error), so the head deltas enumerate them exactly as main's
  // commit does. Fork vertices: the overlay transaction's write set (an anchor's deltas name its fork gid too, which
  // the write set already holds).
  std::vector<uint64_t> touched = ctx.OverlayWrittenGids();
  for (auto const &delta : accessor_->GetTransaction()->deltas) {
    auto const prev = delta.prev.Get();
    if (prev.type == storage::PreviousPtr::Type::VERTEX && !ctx.IsForkVertexGid(prev.vertex->gid)) {
      touched.push_back(prev.vertex->gid.AsUint());
    }
  }
  std::ranges::sort(touched);
  touched.erase(std::ranges::unique(touched).begin(), touched.end());

  for (auto const raw_gid : touched) {
    auto const gid = storage::Gid::FromUint(raw_gid);
    auto resolved =
        ctx.IsForkVertexGid(gid) ? ctx.FindForkVertex(gid) : ctx.FindStubOrDiffVertex(gid, storage::View::NEW);
    if (!resolved) continue;
    VertexAccessor const vertex{*resolved, &ctx};

    auto labels = vertex.Labels(storage::View::NEW);
    if (!labels) {
      if (labels.error() == storage::Error::DELETED_OBJECT) continue;
      throw QueryRuntimeException("Unable to validate constraints on a branch: failed to read vertex labels.");
    }
    auto const constrained = [&](storage::LabelId label) { return std::ranges::contains(*labels, label); };
    bool const relevant = std::ranges::any_of(existence, [&](auto const &c) { return constrained(c.first); }) ||
                          std::ranges::any_of(types, [&](auto const &c) { return constrained(std::get<0>(c)); });
    if (!relevant) continue;

    auto props = vertex.Properties(storage::View::NEW);
    if (!props) {
      if (props.error() == storage::Error::DELETED_OBJECT) continue;
      throw QueryRuntimeException("Unable to validate constraints on a branch: failed to read vertex properties.");
    }

    for (auto const &[label, property] : existence) {
      if (!constrained(label)) continue;
      auto const it = props->find(property);
      if (it == props->end() || it->second.IsNull()) {
        return std::unexpected{
            storage::ConstraintViolation{storage::ConstraintViolation::Type::EXISTENCE, label, std::set{property}}};
      }
    }
    for (auto const &[label, property, kind] : types) {
      if (!constrained(label)) continue;
      auto const it = props->find(property);
      // Type constraints don't enforce existence.
      if (it == props->end() || it->second.IsNull()) continue;
      if (!storage::PropertyValueMatchesTypeConstraint(it->second, kind)) {
        return std::unexpected{storage::ConstraintViolation{
            storage::ConstraintViolation::Type::TYPE, label, kind, std::set<storage::PropertyId>{property}}};
      }
    }
  }
  return {};
}
}  // namespace memgraph::query
