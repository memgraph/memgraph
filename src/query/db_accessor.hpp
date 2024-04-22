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
#include <ranges>

#include <cppitertools/filter.hpp>
#include <cppitertools/imap.hpp>

#include "memory/query_memory_control.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/view.hpp"
#include "utils/bound.hpp"
#include "utils/exceptions.hpp"
#include "utils/pmr/unordered_set.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::query {

class Graph;
class VertexAccessor;

class EdgeAccessor final {
 public:
  storage::EdgeAccessor impl_;

  explicit EdgeAccessor(storage::EdgeAccessor impl) : impl_(std::move(impl)) {}

  bool IsDeleted() const { return impl_.IsDeleted(); }

  bool IsVisible(storage::View view) const { return impl_.IsVisible(view); }

  storage::EdgeTypeId EdgeType() const { return impl_.EdgeType(); }

  auto Properties(storage::View view) const { return impl_.Properties(view); }

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const {
    return impl_.GetProperty(key, view);
  }

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const {
    return impl_.GetPropertySize(key, view);
  }

  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    return impl_.InitProperties(properties);
  }

  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
    return impl_.UpdateProperties(properties);
  }

  storage::Result<storage::PropertyValue> RemoveProperty(storage::PropertyId key) {
    return SetProperty(key, storage::PropertyValue());
  }

  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearProperties() {
    return impl_.ClearProperties();
  }

  VertexAccessor To() const;

  VertexAccessor From() const;

  /// When edge is deleted and you are accessing To vertex
  /// for_deleted_ flag will in this case be updated properly
  VertexAccessor DeletedEdgeToVertex() const;

  /// When edge is deleted and you are accessing From vertex
  /// for_deleted_ flag will in this case be updated properly
  VertexAccessor DeletedEdgeFromVertex() const;

  bool IsCycle() const;

  int64_t CypherId() const { return impl_.Gid().AsInt(); }

  storage::Gid Gid() const noexcept { return impl_.Gid(); }

  bool operator==(const EdgeAccessor &e) const noexcept { return impl_ == e.impl_; }

  bool operator!=(const EdgeAccessor &e) const noexcept { return !(*this == e); }
};

struct EdgeVertexAccessorResult {
  std::vector<EdgeAccessor> edges;
  int64_t expanded_count;
};

class VertexAccessor final {
 public:
  storage::VertexAccessor impl_;

  static EdgeAccessor MakeEdgeAccessor(const storage::EdgeAccessor impl) { return EdgeAccessor(impl); }

  explicit VertexAccessor(storage::VertexAccessor impl) : impl_(impl) {}

  bool IsVisible(storage::View view) const { return impl_.IsVisible(view); }

  auto Labels(storage::View view) const { return impl_.Labels(view); }

  storage::Result<bool> AddLabel(storage::LabelId label) { return impl_.AddLabel(label); }

  storage::Result<bool> RemoveLabel(storage::LabelId label) { return impl_.RemoveLabel(label); }

  storage::Result<bool> HasLabel(storage::View view, storage::LabelId label) const {
    return impl_.HasLabel(label, view);
  }

  auto Properties(storage::View view) const { return impl_.Properties(view); }

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const {
    return impl_.GetProperty(key, view);
  }

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const {
    return impl_.GetPropertySize(key, view);
  }

  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    return impl_.InitProperties(properties);
  }

  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
    return impl_.UpdateProperties(properties);
  }

  storage::Result<storage::PropertyValue> RemoveProperty(storage::PropertyId key) {
    return SetProperty(key, storage::PropertyValue());
  }

  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearProperties() {
    return impl_.ClearProperties();
  }

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view,
                                                    const std::vector<storage::EdgeTypeId> &edge_types) const {
    auto maybe_result = impl_.InEdges(view, edge_types);
    if (maybe_result.HasError()) return maybe_result.GetError();

    std::vector<EdgeAccessor> edges;
    edges.reserve((*maybe_result).edges.size());
    std::ranges::transform((*maybe_result).edges, std::back_inserter(edges),
                           [](auto const &edge) { return EdgeAccessor(edge); });

    return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
  }

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view) const { return InEdges(view, {}); }

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view,
                                                    const std::vector<storage::EdgeTypeId> &edge_types,
                                                    const VertexAccessor &dest) const {
    auto maybe_result = impl_.InEdges(view, edge_types, &dest.impl_);
    if (maybe_result.HasError()) return maybe_result.GetError();

    std::vector<EdgeAccessor> edges;
    edges.reserve((*maybe_result).edges.size());
    std::ranges::transform((*maybe_result).edges, std::back_inserter(edges),
                           [](auto const &edge) { return EdgeAccessor(edge); });

    return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
  }

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view,
                                                     const std::vector<storage::EdgeTypeId> &edge_types) const {
    auto maybe_result = impl_.OutEdges(view, edge_types);
    if (maybe_result.HasError()) return maybe_result.GetError();

    std::vector<EdgeAccessor> edges;
    edges.reserve((*maybe_result).edges.size());
    std::ranges::transform((*maybe_result).edges, std::back_inserter(edges),
                           [](auto const &edge) { return EdgeAccessor(edge); });

    return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
  }

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view) const { return OutEdges(view, {}); }

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view,
                                                     const std::vector<storage::EdgeTypeId> &edge_types,
                                                     const VertexAccessor &dest) const {
    auto maybe_result = impl_.OutEdges(view, edge_types, &dest.impl_);
    if (maybe_result.HasError()) return maybe_result.GetError();

    std::vector<EdgeAccessor> edges;
    edges.reserve((*maybe_result).edges.size());
    std::ranges::transform((*maybe_result).edges, std::back_inserter(edges),
                           [](auto const &edge) { return EdgeAccessor(edge); });

    return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
  }

  storage::Result<size_t> InDegree(storage::View view) const { return impl_.InDegree(view); }

  storage::Result<size_t> OutDegree(storage::View view) const { return impl_.OutDegree(view); }

  int64_t CypherId() const { return impl_.Gid().AsInt(); }

  storage::Gid Gid() const noexcept { return impl_.Gid(); }

  bool operator==(const VertexAccessor &v) const noexcept {
    static_assert(noexcept(impl_ == v.impl_));
    return impl_ == v.impl_;
  }

  bool operator!=(const VertexAccessor &v) const noexcept { return !(*this == v); }
};

inline VertexAccessor EdgeAccessor::To() const { return VertexAccessor(impl_.ToVertex()); }

inline VertexAccessor EdgeAccessor::From() const { return VertexAccessor(impl_.FromVertex()); }

inline VertexAccessor EdgeAccessor::DeletedEdgeToVertex() const { return VertexAccessor(impl_.DeletedEdgeToVertex()); }

inline VertexAccessor EdgeAccessor::DeletedEdgeFromVertex() const {
  return VertexAccessor(impl_.DeletedEdgeFromVertex());
}

inline bool EdgeAccessor::IsCycle() const { return To() == From(); }

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

namespace std {
template <>
struct hash<memgraph::query::VertexAccessor> {
  size_t operator()(const memgraph::query::VertexAccessor &v) const { return std::hash<decltype(v.impl_)>{}(v.impl_); }
};

template <>
struct hash<memgraph::query::EdgeAccessor> {
  size_t operator()(const memgraph::query::EdgeAccessor &e) const { return std::hash<decltype(e.impl_)>{}(e.impl_); }
};

}  // namespace std

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

class DbAccessor final {
  storage::Storage::Accessor *accessor_;

 public:
  explicit DbAccessor(storage::Storage::Accessor *accessor) : accessor_(accessor) {}

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
    memgraph::memory::StartTrackingCurrentThreadTransaction(*accessor_->GetTransactionId());
  }

  void UntrackCurrentThreadAllocations() {
    memgraph::memory::StopTrackingCurrentThreadTransaction(*accessor_->GetTransactionId());
  }

  std::optional<uint64_t> GetTransactionId() { return accessor_->GetTransactionId(); }

  VerticesIterable Vertices(storage::View view) { return VerticesIterable(accessor_->Vertices(view)); }

  VerticesIterable Vertices(storage::View view, storage::LabelId label) {
    return VerticesIterable(accessor_->Vertices(label, view));
  }

  VerticesIterable Vertices(storage::View view, storage::LabelId label, storage::PropertyId property) {
    return VerticesIterable(accessor_->Vertices(label, property, view));
  }

  VerticesIterable Vertices(storage::View view, storage::LabelId label, storage::PropertyId property,
                            const storage::PropertyValue &value) {
    return VerticesIterable(accessor_->Vertices(label, property, value, view));
  }

  VerticesIterable Vertices(storage::View view, storage::LabelId label, storage::PropertyId property,
                            const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                            const std::optional<utils::Bound<storage::PropertyValue>> &upper) {
    return VerticesIterable(accessor_->Vertices(label, property, lower, upper, view));
  }

  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type) {
    return EdgesIterable(accessor_->Edges(edge_type, view));
  }

  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property) {
    return EdgesIterable(accessor_->Edges(edge_type, property, view));
  }

  VertexAccessor InsertVertex() { return VertexAccessor(accessor_->CreateVertex()); }

  storage::Result<EdgeAccessor> InsertEdge(VertexAccessor *from, VertexAccessor *to,
                                           const storage::EdgeTypeId &edge_type) {
    auto maybe_edge = accessor_->CreateEdge(&from->impl_, &to->impl_, edge_type);
    if (maybe_edge.HasError()) return storage::Result<EdgeAccessor>(maybe_edge.GetError());
    return EdgeAccessor(*maybe_edge);
  }

  storage::Result<EdgeAccessor> EdgeSetFrom(EdgeAccessor *edge, VertexAccessor *new_from) {
    auto changed_edge = accessor_->EdgeSetFrom(&edge->impl_, &new_from->impl_);
    if (changed_edge.HasError()) return storage::Result<EdgeAccessor>(changed_edge.GetError());
    return EdgeAccessor(*changed_edge);
  }

  storage::Result<EdgeAccessor> EdgeSetTo(EdgeAccessor *edge, VertexAccessor *new_to) {
    auto changed_edge = accessor_->EdgeSetTo(&edge->impl_, &new_to->impl_);
    if (changed_edge.HasError()) return storage::Result<EdgeAccessor>(changed_edge.GetError());
    return EdgeAccessor(*changed_edge);
  }

  storage::Result<EdgeAccessor> EdgeChangeType(EdgeAccessor *edge, storage::EdgeTypeId new_edge_type) {
    auto changed_edge = accessor_->EdgeChangeType(&edge->impl_, new_edge_type);
    if (changed_edge.HasError()) return storage::Result<EdgeAccessor>{changed_edge.GetError()};
    return EdgeAccessor(*changed_edge);
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

  utils::BasicResult<storage::StorageManipulationError, void> Commit(storage::CommitReplArgs reparg = {},
                                                                     storage::DatabaseAccessProtector db_acc = {}) {
    return accessor_->Commit(std::move(reparg), std::move(db_acc));
  }

  void Abort() { accessor_->Abort(); }

  storage::StorageMode GetStorageMode() const noexcept { return accessor_->GetCreationStorageMode(); }

  bool LabelIndexExists(storage::LabelId label) const { return accessor_->LabelIndexExists(label); }

  bool LabelPropertyIndexExists(storage::LabelId label, storage::PropertyId prop) const {
    return accessor_->LabelPropertyIndexExists(label, prop);
  }

  bool EdgeTypeIndexExists(storage::EdgeTypeId edge_type) const { return accessor_->EdgeTypeIndexExists(edge_type); }

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

  std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const {
    return accessor_->GetIndexStats(label);
  }

  std::optional<storage::LabelPropertyIndexStats> GetIndexStats(const storage::LabelId &label,
                                                                const storage::PropertyId &property) const {
    return accessor_->GetIndexStats(label, property);
  }

  std::vector<std::pair<storage::LabelId, storage::PropertyId>> DeleteLabelPropertyIndexStats(
      const storage::LabelId &label) {
    return accessor_->DeleteLabelPropertyIndexStats(label);
  }

  bool DeleteLabelIndexStats(const storage::LabelId &label) { return accessor_->DeleteLabelIndexStats(label); }

  void SetIndexStats(const storage::LabelId &label, const storage::LabelIndexStats &stats) {
    accessor_->SetIndexStats(label, stats);
  }

  void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                     const storage::LabelPropertyIndexStats &stats) {
    accessor_->SetIndexStats(label, property, stats);
  }

  int64_t VerticesCount() const { return accessor_->ApproximateVertexCount(); }

  int64_t VerticesCount(storage::LabelId label) const { return accessor_->ApproximateVertexCount(label); }

  int64_t VerticesCount(storage::LabelId label, storage::PropertyId property) const {
    return accessor_->ApproximateVertexCount(label, property);
  }

  int64_t VerticesCount(storage::LabelId label, storage::PropertyId property,
                        const storage::PropertyValue &value) const {
    return accessor_->ApproximateVertexCount(label, property, value);
  }

  int64_t VerticesCount(storage::LabelId label, storage::PropertyId property,
                        const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                        const std::optional<utils::Bound<storage::PropertyValue>> &upper) const {
    return accessor_->ApproximateVertexCount(label, property, lower, upper);
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

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateIndex(storage::LabelId label) {
    return accessor_->CreateIndex(label);
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateIndex(storage::LabelId label,
                                                                             storage::PropertyId property) {
    return accessor_->CreateIndex(label, property);
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateIndex(storage::EdgeTypeId edge_type) {
    return accessor_->CreateIndex(edge_type);
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> CreateIndex(storage::EdgeTypeId edge_type,
                                                                             storage::PropertyId property) {
    return accessor_->CreateIndex(edge_type, property);
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> DropIndex(storage::LabelId label) {
    return accessor_->DropIndex(label);
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> DropIndex(storage::LabelId label,
                                                                           storage::PropertyId property) {
    return accessor_->DropIndex(label, property);
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> DropIndex(storage::EdgeTypeId edge_type) {
    return accessor_->DropIndex(edge_type);
  }

  utils::BasicResult<storage::StorageIndexDefinitionError, void> DropIndex(storage::EdgeTypeId edge_type,
                                                                           storage::PropertyId property) {
    return accessor_->DropIndex(edge_type, property);
  }

  void CreateTextIndex(const std::string &index_name, storage::LabelId label) {
    accessor_->CreateTextIndex(index_name, label, this);
  }

  void DropTextIndex(const std::string &index_name) { accessor_->DropTextIndex(index_name); }

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

  void DropGraph() { return accessor_->DropGraph(); }
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

  storage::Result<EdgeAccessor> EdgeSetFrom(EdgeAccessor *edge, SubgraphVertexAccessor *new_from);

  storage::Result<EdgeAccessor> EdgeSetTo(EdgeAccessor *edge, SubgraphVertexAccessor *new_to);

  storage::Result<EdgeAccessor> EdgeChangeType(EdgeAccessor *edge, storage::EdgeTypeId new_edge_type);

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
