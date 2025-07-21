// Copyright 2025 Memgraph Ltd.
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

#include "memory/query_memory_control.hpp"
#include "plan/point_distance_condition.hpp"
#include "query/edge_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/hops_limit.hpp"
#include "query/typed_value.hpp"
#include "query/vertex_accessor.hpp"
#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "storage/v2/view.hpp"
#include "utils/bound.hpp"
#include "utils/exceptions.hpp"
#include "utils/pmr/unordered_set.hpp"
#include "utils/result.hpp"
#include "utils/variant_helpers.hpp"

#include <cstdint>
#include <optional>
#include <ranges>

#include <cppitertools/filter.hpp>
#include <cppitertools/imap.hpp>

namespace memgraph::query {

class Graph;

class SubgraphVertexAccessor final {
 public:
  query::VertexAccessor impl_;
  query::Graph *graph_;

  explicit SubgraphVertexAccessor(query::VertexAccessor impl, query::Graph *graph_) : impl_(impl), graph_(graph_) {}

  bool operator==(const SubgraphVertexAccessor &v) const noexcept {
    static_assert(noexcept(impl_ == v.impl_));
    return impl_ == v.impl_;
  }

  auto InEdges(storage::View view) const -> decltype(impl_.InEdges(view));

  auto OutEdges(storage::View view) const -> decltype(impl_.OutEdges(view));

  auto Labels(storage::View view) const { return impl_.Labels(view); }

  storage::Result<bool> AddLabel(storage::LabelId label) { return impl_.AddLabel(label); }

  storage::Result<bool> RemoveLabel(storage::LabelId label) { return impl_.RemoveLabel(label); }

  storage::Result<bool> HasLabel(storage::View view, storage::LabelId label) const {
    return impl_.HasLabel(view, label);
  }

  auto Properties(storage::View view) const { return impl_.Properties(view); }

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const {
    return impl_.GetProperty(view, key);
  }

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const {
    return impl_.GetPropertySize(key, view);
  }

  storage::Gid Gid() const noexcept { return impl_.Gid(); }

  storage::Result<size_t> InDegree(storage::View view) const { return impl_.InDegree(view); }

  storage::Result<size_t> OutDegree(storage::View view) const { return impl_.OutDegree(view); }

  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    return impl_.SetProperty(key, value);
  }

  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
    return impl_.UpdateProperties(properties);
  }

  VertexAccessor GetVertexAccessor() const;
};
}  // namespace memgraph::query

namespace memgraph::query {

class VerticesIterable final {
  std::variant<storage::VerticesIterable, std::unordered_set<VertexAccessor, std::hash<VertexAccessor>,
                                                             std::equal_to<void>, utils::Allocator<VertexAccessor>> *>
      iterable_;

 public:
  class Iterator final {
    std::variant<storage::VerticesIterable::Iterator,
                 std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                    utils::Allocator<VertexAccessor>>::iterator>
        it_;

   public:
    explicit Iterator(storage::VerticesIterable::Iterator it) : it_(std::move(it)) {}
    explicit Iterator(std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                         utils::Allocator<VertexAccessor>>::iterator it)
        : it_(it) {}

    VertexAccessor operator*() const {
      return std::visit([](auto &it_) { return VertexAccessor(*it_); }, it_);
    }

    Iterator &operator++() {
      std::visit([](auto &it_) { ++it_; }, it_);
      return *this;
    }

    bool operator==(const Iterator &other) const { return it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return !(other == *this); }
  };

  explicit VerticesIterable(storage::VerticesIterable iterable) : iterable_(std::move(iterable)) {}
  explicit VerticesIterable(std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                               utils::Allocator<VertexAccessor>> *vertices)
      : iterable_(vertices) {}

  Iterator begin() {
    return std::visit(memgraph::utils::Overloaded{
                          [](storage::VerticesIterable &iterable_) { return Iterator(iterable_.begin()); },
                          [](std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                                utils::Allocator<VertexAccessor>> *iterable_) {
                            return Iterator(iterable_->begin());
                          }},
                      iterable_);
  }

  Iterator end() {
    return std::visit(
        memgraph::utils::Overloaded{
            [](storage::VerticesIterable &iterable_) { return Iterator(iterable_.end()); },
            [](std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                  utils::Allocator<VertexAccessor>> *iterable_) { return Iterator(iterable_->end()); }},
        iterable_);
  }
};

template <typename storage_iterator>
struct query_vertex_iterator final {
  using value_type = VertexAccessor;
  explicit query_vertex_iterator(storage_iterator it) : it_(std::move(it)) {}
  query_vertex_iterator(query_vertex_iterator const &) = default;
  query_vertex_iterator(query_vertex_iterator &&) = default;
  query_vertex_iterator &operator=(query_vertex_iterator const &) = default;
  query_vertex_iterator &operator=(query_vertex_iterator &&) = default;

  VertexAccessor operator*() const { return VertexAccessor{*it_}; }

  auto operator++() -> query_vertex_iterator & {
    ++it_;
    return *this;
  }

  friend bool operator==(query_vertex_iterator const &, query_vertex_iterator const &) = default;

 private:
  storage_iterator it_;
};

template <typename storage_iterable>
struct query_iterable final {
  using iterator = query_vertex_iterator<typename storage_iterable::iterator>;

  explicit query_iterable(storage_iterable iterable) : iterable_(std::move(iterable)) {}
  query_iterable(query_iterable const &) = default;
  query_iterable(query_iterable &&) = default;
  query_iterable &operator=(query_iterable const &) = default;
  query_iterable &operator=(query_iterable &&) = default;
  ~query_iterable() = default;

  iterator begin() { return iterator{iterable_.begin()}; }

  iterator end() { return iterator{iterable_.end()}; }

 private:
  storage_iterable iterable_;
};

class EdgesIterable final {
  std::variant<storage::EdgesIterable, std::unordered_set<EdgeAccessor, std::hash<EdgeAccessor>, std::equal_to<void>,
                                                          utils::Allocator<EdgeAccessor>> *>
      iterable_;

 public:
  class Iterator final {
    std::variant<storage::EdgesIterable::Iterator,
                 std::unordered_set<EdgeAccessor, std::hash<EdgeAccessor>, std::equal_to<void>,
                                    utils::Allocator<EdgeAccessor>>::iterator>
        it_;

   public:
    explicit Iterator(storage::EdgesIterable::Iterator it) : it_(std::move(it)) {}
    explicit Iterator(std::unordered_set<EdgeAccessor, std::hash<EdgeAccessor>, std::equal_to<void>,
                                         utils::Allocator<EdgeAccessor>>::iterator it)
        : it_(it) {}

    EdgeAccessor operator*() const {
      return std::visit([](auto &it_) { return EdgeAccessor(*it_); }, it_);
    }

    Iterator &operator++() {
      std::visit([](auto &it_) { ++it_; }, it_);
      return *this;
    }

    bool operator==(const Iterator &other) const { return it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return !(other == *this); }
  };

  explicit EdgesIterable(storage::EdgesIterable iterable) : iterable_(std::move(iterable)) {}
  explicit EdgesIterable(std::unordered_set<EdgeAccessor, std::hash<EdgeAccessor>, std::equal_to<void>,
                                            utils::Allocator<EdgeAccessor>> *edges)
      : iterable_(edges) {}

  Iterator begin() {
    return std::visit(
        memgraph::utils::Overloaded{
            [](storage::EdgesIterable &iterable_) { return Iterator(iterable_.begin()); },
            [](std::unordered_set<EdgeAccessor, std::hash<EdgeAccessor>, std::equal_to<void>,
                                  utils::Allocator<EdgeAccessor>> *iterable_) { return Iterator(iterable_->begin()); }},
        iterable_);
  }

  Iterator end() {
    return std::visit(
        memgraph::utils::Overloaded{
            [](storage::EdgesIterable &iterable_) { return Iterator(iterable_.end()); },
            [](std::unordered_set<EdgeAccessor, std::hash<EdgeAccessor>, std::equal_to<void>,
                                  utils::Allocator<EdgeAccessor>> *iterable_) { return Iterator(iterable_->end()); }},
        iterable_);
  }
};

using PointIterable = query_iterable<storage::PointIterable>;

class DbAccessor final {
  storage::Storage::Accessor *accessor_;

 public:
  explicit DbAccessor(storage::Storage::Accessor *accessor) : accessor_(accessor) {}

  bool CheckIndicesAreReady(storage::IndicesCollection const &required_indices) const {
    return accessor_->CheckIndicesAreReady(required_indices);
  }

  auto type() const { return accessor_->type(); }

  std::optional<VertexAccessor> FindVertex(storage::Gid gid, storage::View view) {
    auto maybe_vertex = accessor_->FindVertex(gid, view);
    if (maybe_vertex) return VertexAccessor(*maybe_vertex);
    return std::nullopt;
  }

  std::optional<EdgeAccessor> FindEdge(storage::Gid gid, storage::View view) {
    auto maybe_edge = accessor_->FindEdge(gid, view);
    if (maybe_edge) return EdgeAccessor(*maybe_edge);
    return std::nullopt;
  }

  void FinalizeTransaction() { accessor_->FinalizeTransaction(); }

  void TrackCurrentThreadAllocations() {
    memgraph::memory::StartTrackingCurrentThread(accessor_->GetQueryMemoryTracker().get());
  }

  void UntrackCurrentThreadAllocations() { memgraph::memory::StopTrackingCurrentThread(); }

  auto &GetQueryMemoryTracker() { return accessor_->GetQueryMemoryTracker(); }

  auto GetTransactionId() { return accessor_->GetTransactionId(); }

  VerticesIterable Vertices(storage::View view) { return VerticesIterable(accessor_->Vertices(view)); }

  VerticesIterable Vertices(storage::View view, storage::LabelId label) {
    return VerticesIterable(accessor_->Vertices(label, view));
  }

  VerticesIterable Vertices(storage::View view, storage::LabelId label,
                            std::span<storage::PropertyPath const> properties,
                            std::span<storage::PropertyValueRange const> property_ranges) {
    return VerticesIterable(accessor_->Vertices(label, properties, property_ranges, view));
  }

  auto PointVertices(storage::LabelId label, storage::PropertyId property, storage::CoordinateReferenceSystem crs,
                     TypedValue const &point_value, TypedValue const &boundary_value,
                     plan::PointDistanceCondition condition) -> PointIterable;

  auto PointVertices(storage::LabelId label, storage::PropertyId property, storage::CoordinateReferenceSystem crs,
                     TypedValue const &bottom_left, TypedValue const &top_right, plan::WithinBBoxCondition condition)
      -> PointIterable;

  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type) {
    return EdgesIterable(accessor_->Edges(edge_type, view));
  }

  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property) {
    return EdgesIterable(accessor_->Edges(edge_type, property, view));
  }

  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property,
                      const storage::PropertyValue value) {
    return EdgesIterable(accessor_->Edges(edge_type, property, value, view));
  }

  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property,
                      const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                      const std::optional<utils::Bound<storage::PropertyValue>> &upper) {
    return EdgesIterable(accessor_->Edges(edge_type, property, lower, upper, view));
  }

  EdgesIterable Edges(storage::View view, storage::PropertyId property) {
    return EdgesIterable(accessor_->Edges(property, view));
  }

  EdgesIterable Edges(storage::View view, storage::PropertyId property, const storage::PropertyValue value) {
    return EdgesIterable(accessor_->Edges(property, value, view));
  }

  EdgesIterable Edges(storage::View view, storage::PropertyId property,
                      const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                      const std::optional<utils::Bound<storage::PropertyValue>> &upper) {
    return EdgesIterable(accessor_->Edges(property, lower, upper, view));
  }

  VertexAccessor InsertVertex() { return VertexAccessor(accessor_->CreateVertex()); }

  storage::Result<EdgeAccessor> InsertEdge(VertexAccessor *from, VertexAccessor *to,
                                           const storage::EdgeTypeId &edge_type) {
    auto maybe_edge = accessor_->CreateEdge(&from->impl_, &to->impl_, edge_type);
    if (maybe_edge.HasError()) return storage::Result<EdgeAccessor>(maybe_edge.GetError());
    return EdgeAccessor(*maybe_edge);
  }

  storage::Result<std::optional<EdgeAccessor>> RemoveEdge(EdgeAccessor *edge) {
    auto res = accessor_->DeleteEdge(&edge->impl_);
    if (res.HasError()) {
      return res.GetError();
    }

    const auto &value = res.GetValue();
    if (!value) {
      return std::optional<EdgeAccessor>{};
    }

    return std::make_optional<EdgeAccessor>(*value);
  }

  storage::Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachRemoveVertex(
      VertexAccessor *vertex_accessor) {
    using ReturnType = std::pair<VertexAccessor, std::vector<EdgeAccessor>>;

    auto res = accessor_->DetachDeleteVertex(&vertex_accessor->impl_);
    if (res.HasError()) {
      return res.GetError();
    }

    const auto &value = res.GetValue();
    if (!value) {
      return std::optional<ReturnType>{};
    }

    const auto &[vertex, edges] = *value;

    std::vector<EdgeAccessor> deleted_edges;
    deleted_edges.reserve(edges.size());
    std::ranges::transform(edges, std::back_inserter(deleted_edges),
                           [](const auto &deleted_edge) { return EdgeAccessor{deleted_edge}; });

    return std::make_optional<ReturnType>(vertex, std::move(deleted_edges));
  }

  storage::Result<std::optional<VertexAccessor>> RemoveVertex(VertexAccessor *vertex_accessor) {
    auto res = accessor_->DeleteVertex(&vertex_accessor->impl_);
    if (res.HasError()) {
      return res.GetError();
    }

    const auto &value = res.GetValue();
    if (!value) {
      return std::optional<VertexAccessor>{};
    }

    return std::make_optional<VertexAccessor>(*value);
  }

  storage::Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>> DetachDelete(
      std::vector<VertexAccessor> nodes, std::vector<EdgeAccessor> edges, bool detach) {
    using ReturnType = std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>;

    std::vector<storage::VertexAccessor *> nodes_impl;
    std::vector<storage::EdgeAccessor *> edges_impl;

    nodes_impl.reserve(nodes.size());
    edges_impl.reserve(edges.size());

    for (auto &vertex_accessor : nodes) {
      nodes_impl.push_back(&vertex_accessor.impl_);
    }

    for (auto &edge_accessor : edges) {
      edges_impl.push_back(&edge_accessor.impl_);
    }

    auto res = accessor_->DetachDelete(std::move(nodes_impl), std::move(edges_impl), detach);
    if (res.HasError()) {
      return res.GetError();
    }

    const auto &value = res.GetValue();
    if (!value) {
      return std::optional<ReturnType>{};
    }

    const auto &[val_vertices, val_edges] = *value;

    std::vector<VertexAccessor> deleted_vertices;
    std::vector<EdgeAccessor> deleted_edges;

    deleted_vertices.reserve(val_vertices.size());
    deleted_edges.reserve(val_edges.size());

    std::ranges::transform(val_vertices, std::back_inserter(deleted_vertices),
                           [](const auto &deleted_vertex) { return VertexAccessor{deleted_vertex}; });
    std::ranges::transform(val_edges, std::back_inserter(deleted_edges),
                           [](const auto &deleted_edge) { return EdgeAccessor{deleted_edge}; });

    return std::make_optional<ReturnType>(std::move(deleted_vertices), std::move(deleted_edges));
  }

  storage::PropertyId NameToProperty(const std::string_view name) { return accessor_->NameToProperty(name); }

  std::optional<storage::PropertyId> NameToPropertyIfExists(std::string_view name) const {
    return accessor_->NameToPropertyIfExists(name);
  }

  storage::LabelId NameToLabel(const std::string_view name) { return accessor_->NameToLabel(name); }

  storage::EdgeTypeId NameToEdgeType(const std::string_view name) { return accessor_->NameToEdgeType(name); }

  const std::string &PropertyToName(storage::PropertyId prop) const { return accessor_->PropertyToName(prop); }

  const std::string &LabelToName(storage::LabelId label) const { return accessor_->LabelToName(label); }

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const { return accessor_->EdgeTypeToName(type); }

  const std::string &DatabaseName() const { return accessor_->id(); }

  void AdvanceCommand() { accessor_->AdvanceCommand(); }

  utils::BasicResult<storage::StorageManipulationError, void> Commit(storage::CommitReplicationArgs reparg = {},
                                                                     storage::DatabaseAccessProtector db_acc = {}) {
    return accessor_->PrepareForCommitPhase(std::move(reparg), std::move(db_acc));
  }

  utils::BasicResult<storage::StorageManipulationError, void> PeriodicCommit(
      storage::CommitReplicationArgs reparg = {}, storage::DatabaseAccessProtector db_acc = {}) {
    return accessor_->PeriodicCommit(std::move(reparg), std::move(db_acc));
  }

  void Abort() { accessor_->Abort(); }

  storage::StorageMode GetStorageMode() const noexcept { return accessor_->GetCreationStorageMode(); }

  bool LabelIndexReady(storage::LabelId label) const { return accessor_->LabelIndexReady(label); }

  bool LabelPropertyIndexReady(storage::LabelId label, std::span<storage::PropertyPath const> properties) const {
    return accessor_->LabelPropertyIndexReady(label, properties);
  }

  auto RelevantLabelPropertiesIndicesInfo(std::span<storage::LabelId const> labels,
                                          std::span<storage::PropertyPath const> properties) const
      -> std::vector<storage::LabelPropertiesIndicesInfo> {
    return accessor_->RelevantLabelPropertiesIndicesInfo(labels, properties);
  }

  bool EdgeTypeIndexReady(storage::EdgeTypeId edge_type) const { return accessor_->EdgeTypeIndexReady(edge_type); }

  bool EdgeTypePropertyIndexReady(storage::EdgeTypeId edge_type, storage::PropertyId property) const {
    return accessor_->EdgeTypePropertyIndexReady(edge_type, property);
  }

  bool EdgePropertyIndexReady(storage::PropertyId property) const {
    return accessor_->EdgePropertyIndexReady(property);
  }

  bool TextIndexExists(const std::string &index_name) const { return accessor_->TextIndexExists(index_name); }

  void TextIndexAddVertex(const VertexAccessor &vertex) { accessor_->TextIndexAddVertex(vertex.impl_); }

  void TextIndexUpdateVertex(const VertexAccessor &vertex, const std::vector<storage::LabelId> &removed_labels = {}) {
    accessor_->TextIndexUpdateVertex(vertex.impl_, removed_labels);
  }

  std::vector<storage::Gid> TextIndexSearch(const std::string &index_name, const std::string &search_query,
                                            text_search_mode search_mode) const {
    return accessor_->TextIndexSearch(index_name, search_query, search_mode);
  }

  std::string TextIndexAggregate(const std::string &index_name, const std::string &search_query,
                                 const std::string &aggregation_query) const {
    return accessor_->TextIndexAggregate(index_name, search_query, aggregation_query);
  }

  bool PointIndexExists(storage::LabelId label, storage::PropertyId prop) const {
    return accessor_->PointIndexExists(label, prop);
  }

  std::vector<std::tuple<storage::VertexAccessor, double, double>> VectorIndexSearchOnNodes(
      const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) {
    return accessor_->VectorIndexSearchOnNodes(index_name, number_of_results, vector);
  }

  std::vector<std::tuple<storage::EdgeAccessor, double, double>> VectorIndexSearchOnEdges(
      const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) {
    return accessor_->VectorIndexSearchOnEdges(index_name, number_of_results, vector);
  }

  std::vector<storage::VectorIndexInfo> ListAllVectorIndices() const { return accessor_->ListAllVectorIndices(); }

  std::vector<storage::VectorEdgeIndexInfo> ListAllVectorEdgeIndices() const {
    return accessor_->ListAllVectorEdgeIndices();
  }

  std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const {
    return accessor_->GetIndexStats(label);
  }

  std::optional<storage::LabelPropertyIndexStats> GetIndexStats(
      const storage::LabelId &label, std::span<storage::PropertyPath const> properties) const {
    return accessor_->GetIndexStats(label, properties);
  }

  std::vector<std::pair<storage::LabelId, std::vector<storage::PropertyPath>>> DeleteLabelPropertyIndexStats(
      const storage::LabelId &label) {
    return accessor_->DeleteLabelPropertyIndexStats(label);
  }

  bool DeleteLabelIndexStats(const storage::LabelId &label) { return accessor_->DeleteLabelIndexStats(label); }

  void SetIndexStats(const storage::LabelId &label, const storage::LabelIndexStats &stats) {
    accessor_->SetIndexStats(label, stats);
  }

  void SetIndexStats(const storage::LabelId &label, std::span<storage::PropertyPath const> properties,
                     const storage::LabelPropertyIndexStats &stats) {
    accessor_->SetIndexStats(label, properties, stats);
  }

  int64_t VerticesCount() const { return accessor_->ApproximateVertexCount(); }

  int64_t VerticesCount(storage::LabelId label) const { return accessor_->ApproximateVertexCount(label); }

  int64_t VerticesCount(storage::LabelId label, std::span<storage::PropertyPath const> properties) const {
    return accessor_->ApproximateVertexCount(label, properties);
  }

  int64_t VerticesCount(storage::LabelId label, std::span<storage::PropertyPath const> properties,
                        std::span<storage::PropertyValue const> values) const {
    return accessor_->ApproximateVertexCount(label, properties, values);
  }

  // TODO: rename to ApproximateVertexCount?
  int64_t VerticesCount(storage::LabelId label, std::span<storage::PropertyPath const> properties,
                        std::span<storage::PropertyValueRange const> bounds) const {
    return accessor_->ApproximateVertexCount(label, properties, bounds);
  }
  std::optional<uint64_t> VerticesPointCount(storage::LabelId label, storage::PropertyId property) const {
    return accessor_->ApproximateVerticesPointCount(label, property);
  }

  std::optional<uint64_t> VerticesVectorCount(storage::LabelId label, storage::PropertyId property) const {
    return accessor_->ApproximateVerticesVectorCount(label, property);
  }

  int64_t EdgesCount() const { return accessor_->ApproximateEdgeCount(); }

  int64_t EdgesCount(storage::EdgeTypeId edge_type) const { return accessor_->ApproximateEdgeCount(edge_type); }

  int64_t EdgesCount(storage::EdgeTypeId edge_type, storage::PropertyId property) const {
    return accessor_->ApproximateEdgeCount(edge_type, property);
  }

  int64_t EdgesCount(storage::EdgeTypeId edge_type, storage::PropertyId property,
                     const storage::PropertyValue &value) const {
    return accessor_->ApproximateEdgeCount(edge_type, property, value);
  }

  int64_t EdgesCount(storage::EdgeTypeId edge_type, storage::PropertyId property,
                     const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                     const std::optional<utils::Bound<storage::PropertyValue>> &upper) const {
    return accessor_->ApproximateEdgeCount(edge_type, property, lower, upper);
  }

  int64_t EdgesCount(storage::PropertyId property) const { return accessor_->ApproximateEdgeCount(property); }

  int64_t EdgesCount(storage::PropertyId property, const storage::PropertyValue &value) const {
    return accessor_->ApproximateEdgeCount(property, value);
  }

  int64_t EdgesCount(storage::PropertyId property, const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                     const std::optional<utils::Bound<storage::PropertyValue>> &upper) const {
    return accessor_->ApproximateEdgeCount(property, lower, upper);
  }

  std::vector<storage::LabelId> ListAllPossiblyPresentVertexLabels() const {
    return accessor_->ListAllPossiblyPresentVertexLabels();
  }
  std::vector<storage::EdgeTypeId> ListAllPossiblyPresentEdgeTypes() const {
    return accessor_->ListAllPossiblyPresentEdgeTypes();
  }

  storage::IndicesInfo ListAllIndices() const { return accessor_->ListAllIndices(); }

  storage::ConstraintsInfo ListAllConstraints() const { return accessor_->ListAllConstraints(); }

  const std::string &id() const { return accessor_->id(); }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateIndex(
      storage::LabelId label, storage::CheckCancelFunction cancel_check = storage::neverCancel,
      storage::PublishIndexWrapper wrapper = storage::publish_no_wrap) {
    return accessor_->CreateIndex(label, cancel_check, std::move(wrapper));
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateIndex(
      storage::LabelId label, std::vector<storage::PropertyPath> &&properties,
      storage::CheckCancelFunction cancel_check = storage::neverCancel,
      storage::PublishIndexWrapper wrapper = storage::publish_no_wrap) {
    return accessor_->CreateIndex(label, std::move(properties), std::move(cancel_check), std::move(wrapper));
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateIndex(
      storage::EdgeTypeId edge_type, storage::CheckCancelFunction cancel_check = storage::neverCancel,
      storage::PublishIndexWrapper wrapper = storage::publish_no_wrap) {
    return accessor_->CreateIndex(edge_type, std::move(cancel_check), std::move(wrapper));
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateIndex(
      storage::EdgeTypeId edge_type, storage::PropertyId property,
      storage::CheckCancelFunction cancel_check = storage::neverCancel,
      storage::PublishIndexWrapper wrapper = storage::publish_no_wrap) {
    return accessor_->CreateIndex(edge_type, property, std::move(cancel_check), std::move(wrapper));
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateGlobalEdgeIndex(
      storage::PropertyId property, storage::CheckCancelFunction cancel_check = storage::neverCancel,
      storage::PublishIndexWrapper wrapper = storage::publish_no_wrap) {
    return accessor_->CreateGlobalEdgeIndex(property, std::move(cancel_check), std::move(wrapper));
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> DropIndex(
      storage::LabelId label, storage::DropIndexWrapper wrapper = storage::drop_no_wrap) {
    return accessor_->DropIndex(label, std::move(wrapper));
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> DropIndex(
      storage::LabelId label, std::vector<storage::PropertyPath> &&properties,
      storage::DropIndexWrapper wrapper = storage::drop_no_wrap) {
    return accessor_->DropIndex(label, std::move(properties), std::move(wrapper));
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> DropIndex(
      storage::EdgeTypeId edge_type, storage::DropIndexWrapper wrapper = storage::drop_no_wrap) {
    return accessor_->DropIndex(edge_type, std::move(wrapper));
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> DropIndex(
      storage::EdgeTypeId edge_type, storage::PropertyId property,
      storage::DropIndexWrapper wrapper = storage::drop_no_wrap) {
    return accessor_->DropIndex(edge_type, property, std::move(wrapper));
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> DropGlobalEdgeIndex(
      storage::PropertyId property, storage::DropIndexWrapper wrapper = storage::drop_no_wrap) {
    return accessor_->DropGlobalEdgeIndex(property, std::move(wrapper));
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreatePointIndex(storage::LabelId label,
                                                                                  storage::PropertyId property) {
    return accessor_->CreatePointIndex(label, property);
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> DropPointIndex(storage::LabelId label,
                                                                                storage::PropertyId property) {
    return accessor_->DropPointIndex(label, property);
  }

  void CreateTextIndex(const std::string &index_name, storage::LabelId label) {
    accessor_->CreateTextIndex(index_name, label);
  }

  void DropTextIndex(const std::string &index_name) { accessor_->DropTextIndex(index_name); }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateVectorIndex(storage::VectorIndexSpec spec) {
    return accessor_->CreateVectorIndex(std::move(spec));
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> DropVectorIndex(std::string_view index_name) {
    return accessor_->DropVectorIndex(index_name);
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateVectorEdgeIndex(
      storage::VectorEdgeIndexSpec spec) {
    return accessor_->CreateVectorEdgeIndex(std::move(spec));
  }

  utils::BasicResult<storage::StorageExistenceConstraintDefinitionError, void> CreateExistenceConstraint(
      storage::LabelId label, storage::PropertyId property) {
    return accessor_->CreateExistenceConstraint(label, property);
  }

  utils::BasicResult<storage::StorageExistenceConstraintDroppingError, void> DropExistenceConstraint(
      storage::LabelId label, storage::PropertyId property) {
    return accessor_->DropExistenceConstraint(label, property);
  }

  utils::BasicResult<storage::StorageUniqueConstraintDefinitionError, storage::UniqueConstraints::CreationStatus>
  CreateUniqueConstraint(storage::LabelId label, const std::set<storage::PropertyId> &properties) {
    return accessor_->CreateUniqueConstraint(label, properties);
  }

  storage::UniqueConstraints::DeletionStatus DropUniqueConstraint(storage::LabelId label,
                                                                  const std::set<storage::PropertyId> &properties) {
    return accessor_->DropUniqueConstraint(label, properties);
  }

  utils::BasicResult<storage::StorageExistenceConstraintDefinitionError, void> CreateTypeConstraint(
      storage::LabelId label, storage::PropertyId property, storage::TypeConstraintKind type) {
    return accessor_->CreateTypeConstraint(label, property, type);
  }

  utils::BasicResult<storage::StorageExistenceConstraintDroppingError, void> DropTypeConstraint(
      storage::LabelId label, storage::PropertyId property, storage::TypeConstraintKind type) {
    return accessor_->DropTypeConstraint(label, property, type);
  }

  void DropGraph() { return accessor_->DropGraph(); }

  auto CreateEnum(std::string_view name, std::span<std::string const> values)
      -> utils::BasicResult<storage::EnumStorageError, storage::EnumTypeId> {
    return accessor_->CreateEnum(name, values);
  }

  auto ShowEnums() { return accessor_->ShowEnums(); }

  auto GetEnumValue(std::string_view name, std::string_view value) const
      -> utils::BasicResult<storage::EnumStorageError, storage::Enum> {
    return accessor_->GetEnumValue(name, value);
  }
  auto GetEnumValue(std::string_view enum_str) -> utils::BasicResult<storage::EnumStorageError, storage::Enum> {
    return accessor_->GetEnumValue(enum_str);
  }

  auto EnumToName(storage::Enum value) const -> memgraph::utils::BasicResult<storage::EnumStorageError, std::string> {
    return accessor_->GetEnumStoreShared().ToString(value);
  }

  auto EnumAlterAdd(std::string_view name, std::string_view value)
      -> utils::BasicResult<storage::EnumStorageError, storage::Enum> {
    return accessor_->EnumAlterAdd(name, value);
  }

  auto EnumAlterUpdate(std::string_view name, std::string_view old_value, std::string_view new_value)
      -> utils::BasicResult<storage::EnumStorageError, storage::Enum> {
    return accessor_->EnumAlterUpdate(name, old_value, new_value);
  }

  auto GetStorageAccessor() const -> storage::Storage::Accessor * { return accessor_; }
};

class SubgraphDbAccessor final {
  DbAccessor db_accessor_;
  Graph *graph_;

 public:
  explicit SubgraphDbAccessor(DbAccessor db_accessor, Graph *graph);

  static SubgraphDbAccessor *MakeSubgraphDbAccessor(DbAccessor *db_accessor, Graph *graph);

  void TrackThreadAllocations(const char *thread_id);

  void TrackCurrentThreadAllocations();

  void UntrackThreadAllocations(const char *thread_id);

  void UntrackCurrentThreadAllocations();

  storage::PropertyId NameToProperty(std::string_view name);

  storage::LabelId NameToLabel(std::string_view name);

  storage::EdgeTypeId NameToEdgeType(std::string_view name);

  const std::string &PropertyToName(storage::PropertyId prop) const;

  const std::string &LabelToName(storage::LabelId label) const;

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const;

  storage::Result<std::optional<EdgeAccessor>> RemoveEdge(EdgeAccessor *edge);

  storage::Result<EdgeAccessor> InsertEdge(SubgraphVertexAccessor *from, SubgraphVertexAccessor *to,
                                           const storage::EdgeTypeId &edge_type);

  storage::Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachRemoveVertex(
      SubgraphVertexAccessor *vertex_accessor);

  storage::Result<std::optional<VertexAccessor>> RemoveVertex(SubgraphVertexAccessor *vertex_accessor);

  SubgraphVertexAccessor InsertVertex();

  VerticesIterable Vertices(storage::View view);

  std::optional<VertexAccessor> FindVertex(storage::Gid gid, storage::View view);

  std::optional<EdgeAccessor> FindEdge(storage::Gid gid, storage::View view);

  Graph *getGraph();

  storage::StorageMode GetStorageMode() const noexcept;

  DbAccessor *GetAccessor();
};

}  // namespace memgraph::query
