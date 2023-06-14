// Copyright 2023 Memgraph Ltd.
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

#include <cppitertools/filter.hpp>
#include <cppitertools/imap.hpp>

#include "query/exceptions.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "utils/pmr/unordered_set.hpp"
#include "utils/variant_helpers.hpp"

///////////////////////////////////////////////////////////
// Our communication layer and query engine don't mix
// very well on Centos because OpenSSL version available
// on Centos 7 include  libkrb5 which has brilliant macros
// called TRUE and FALSE. For more detailed explanation go
// to memgraph.cpp.
//
// Because of the replication storage now uses some form of
// communication so we have some unwanted macros.
// This cannot be avoided by simple include orderings so we
// simply undefine those macros as we're sure that libkrb5
// won't and can't be used anywhere in the query engine.
#include "storage/v2/storage.hpp"

#undef FALSE
#undef TRUE
///////////////////////////////////////////////////////////

#include "storage/v2/view.hpp"
#include "utils/bound.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::query {

class Graph;
class VertexAccessor;

class EdgeAccessor final {
 public:
  storage::EdgeAccessor impl_;

 public:
  explicit EdgeAccessor(storage::EdgeAccessor impl) : impl_(std::move(impl)) {}

  bool IsVisible(storage::View view) const { return impl_.IsVisible(view); }

  storage::EdgeTypeId EdgeType() const { return impl_.EdgeType(); }

  auto Properties(storage::View view) const { return impl_.Properties(view); }

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const {
    return impl_.GetProperty(key, view);
  }

  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    return impl_.InitProperties(properties);
  }

  storage::Result<storage::PropertyValue> RemoveProperty(storage::PropertyId key) {
    return SetProperty(key, storage::PropertyValue());
  }

  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearProperties() {
    return impl_.ClearProperties();
  }

  VertexAccessor To() const;

  VertexAccessor From() const;

  bool IsCycle() const;

  int64_t CypherId() const { return impl_.Gid().AsInt(); }

  storage::Gid Gid() const noexcept { return impl_.Gid(); }

  bool operator==(const EdgeAccessor &e) const noexcept { return impl_ == e.impl_; }

  bool operator!=(const EdgeAccessor &e) const noexcept { return !(*this == e); }
};

class VertexAccessor final {
 public:
  storage::VertexAccessor impl_;

  static EdgeAccessor MakeEdgeAccessor(const storage::EdgeAccessor impl) { return EdgeAccessor(impl); }

 public:
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

  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    return impl_.InitProperties(properties);
  }

  storage::Result<storage::PropertyValue> RemoveProperty(storage::PropertyId key) {
    return SetProperty(key, storage::PropertyValue());
  }

  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearProperties() {
    return impl_.ClearProperties();
  }

  auto InEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types) const
      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.InEdges(view)))> {
    auto maybe_edges = impl_.InEdges(view, edge_types);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  }

  auto InEdges(storage::View view) const { return InEdges(view, {}); }

  auto InEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types, const VertexAccessor &dest) const
      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.InEdges(view)))> {
    auto maybe_edges = impl_.InEdges(view, edge_types, &dest.impl_);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  }

  auto OutEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types) const
      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.OutEdges(view)))> {
    auto maybe_edges = impl_.OutEdges(view, edge_types);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  }

  auto OutEdges(storage::View view) const { return OutEdges(view, {}); }

  auto OutEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types,
                const VertexAccessor &dest) const
      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.OutEdges(view)))> {
    auto maybe_edges = impl_.OutEdges(view, edge_types, &dest.impl_);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
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

  storage::Gid Gid() const noexcept { return impl_.Gid(); }

  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    return impl_.SetProperty(key, value);
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
    explicit Iterator(storage::VerticesIterable::Iterator it) : it_(it) {}
    explicit Iterator(std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                         utils::Allocator<VertexAccessor>>::iterator it)
        : it_(it) {}

    VertexAccessor operator*() const {
      return std::visit([](auto it_) { return VertexAccessor(*it_); }, it_);
    }

    Iterator &operator++() {
      std::visit([this](auto it_) { this->it_ = ++it_; }, it_);
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

class DbAccessor final {
  storage::Storage::Accessor *accessor_;

 public:
  explicit DbAccessor(storage::Storage::Accessor *accessor) : accessor_(accessor) {}

  std::optional<VertexAccessor> FindVertex(storage::Gid gid, storage::View view) {
    auto maybe_vertex = accessor_->FindVertex(gid, view);
    if (maybe_vertex) return VertexAccessor(*maybe_vertex);
    return std::nullopt;
  }

  void FinalizeTransaction() { accessor_->FinalizeTransaction(); }

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
    std::transform(edges.begin(), edges.end(), std::back_inserter(deleted_edges),
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

  storage::PropertyId NameToProperty(const std::string_view name) { return accessor_->NameToProperty(name); }

  storage::LabelId NameToLabel(const std::string_view name) { return accessor_->NameToLabel(name); }

  storage::EdgeTypeId NameToEdgeType(const std::string_view name) { return accessor_->NameToEdgeType(name); }

  const std::string &PropertyToName(storage::PropertyId prop) const { return accessor_->PropertyToName(prop); }

  const std::string &LabelToName(storage::LabelId label) const { return accessor_->LabelToName(label); }

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const { return accessor_->EdgeTypeToName(type); }

  void AdvanceCommand() { accessor_->AdvanceCommand(); }

  utils::BasicResult<storage::StorageDataManipulationError, void> Commit() { return accessor_->Commit(); }

  void Abort() { accessor_->Abort(); }

  bool LabelIndexExists(storage::LabelId label) const { return accessor_->LabelIndexExists(label); }

  bool LabelPropertyIndexExists(storage::LabelId label, storage::PropertyId prop) const {
    return accessor_->LabelPropertyIndexExists(label, prop);
  }

  std::optional<storage::IndexStats> GetIndexStats(const storage::LabelId &label,
                                                   const storage::PropertyId &property) const {
    return accessor_->GetIndexStats(label, property);
  }

  std::vector<std::pair<storage::LabelId, storage::PropertyId>> ClearIndexStats() {
    return accessor_->ClearIndexStats();
  }

  std::vector<std::pair<storage::LabelId, storage::PropertyId>> DeleteIndexStatsForLabels(
      const std::span<std::string> labels) {
    return accessor_->DeleteIndexStatsForLabels(labels);
  }

  void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                     const storage::IndexStats &stats) {
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

  storage::IndicesInfo ListAllIndices() const { return accessor_->ListAllIndices(); }

  storage::ConstraintsInfo ListAllConstraints() const { return accessor_->ListAllConstraints(); }
};

class SubgraphDbAccessor final {
  DbAccessor db_accessor_;
  Graph *graph_;

 public:
  explicit SubgraphDbAccessor(DbAccessor db_accessor, Graph *graph);

  static SubgraphDbAccessor *MakeSubgraphDbAccessor(DbAccessor *db_accessor, Graph *graph);

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

  Graph *getGraph();
};

}  // namespace memgraph::query
