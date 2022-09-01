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

#include <cstdint>
#include <optional>
#include <vector>

#include <cppitertools/filter.hpp>
#include <cppitertools/imap.hpp>

#include "query/v2/exceptions.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"

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
#include "storage/v3/storage.hpp"
#include "utils/logging.hpp"
#include "utils/result.hpp"

#undef FALSE
#undef TRUE
///////////////////////////////////////////////////////////

#include "storage/v3/view.hpp"
#include "utils/bound.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::query::v2 {

class VertexAccessor;

class EdgeAccessor final {
 public:
  storage::v3::EdgeAccessor impl_;

  explicit EdgeAccessor(storage::v3::EdgeAccessor impl) : impl_(std::move(impl)) {}

  bool IsVisible(storage::v3::View view) const { return impl_.IsVisible(view); }

  storage::v3::EdgeTypeId EdgeType() const { return impl_.EdgeType(); }

  auto Properties(storage::v3::View view) const { return impl_.Properties(view); }

  storage::v3::Result<storage::v3::PropertyValue> GetProperty(storage::v3::View view,
                                                              storage::v3::PropertyId key) const {
    return impl_.GetProperty(key, view);
  }

  storage::v3::Result<storage::v3::PropertyValue> SetProperty(storage::v3::PropertyId key,
                                                              const storage::v3::PropertyValue &value) {
    return impl_.SetProperty(key, value);
  }

  storage::v3::Result<storage::v3::PropertyValue> RemoveProperty(storage::v3::PropertyId key) {
    return SetProperty(key, storage::v3::PropertyValue());
  }

  storage::v3::Result<std::map<storage::v3::PropertyId, storage::v3::PropertyValue>> ClearProperties() {
    return impl_.ClearProperties();
  }

  VertexAccessor To() const;

  VertexAccessor From() const;

  bool IsCycle() const;

  int64_t CypherId() const { return impl_.Gid().AsInt(); }

  storage::v3::Gid Gid() const noexcept { return impl_.Gid(); }

  bool operator==(const EdgeAccessor &e) const noexcept { return impl_ == e.impl_; }

  bool operator!=(const EdgeAccessor &e) const noexcept { return !(*this == e); }
};

class VertexAccessor final {
 public:
  storage::v3::VertexAccessor impl_;

  static EdgeAccessor MakeEdgeAccessor(const storage::v3::EdgeAccessor impl) { return EdgeAccessor(impl); }

  explicit VertexAccessor(storage::v3::VertexAccessor impl) : impl_(impl) {}

  bool IsVisible(storage::v3::View view) const { return impl_.IsVisible(view); }

  auto Labels(storage::v3::View view) const { return impl_.Labels(view); }

  auto PrimaryLabel(storage::v3::View view) const { return impl_.PrimaryLabel(view); }

  auto PrimaryKey(storage::v3::View view) const { return impl_.PrimaryKey(view); }

  storage::v3::ResultSchema<bool> AddLabel(storage::v3::LabelId label) { return impl_.AddLabelAndValidate(label); }

  storage::v3::ResultSchema<bool> AddLabelAndValidate(storage::v3::LabelId label) {
    return impl_.AddLabelAndValidate(label);
  }

  storage::v3::ResultSchema<bool> RemoveLabel(storage::v3::LabelId label) {
    return impl_.RemoveLabelAndValidate(label);
  }

  storage::v3::ResultSchema<bool> RemoveLabelAndValidate(storage::v3::LabelId label) {
    return impl_.RemoveLabelAndValidate(label);
  }

  storage::v3::Result<bool> HasLabel(storage::v3::View view, storage::v3::LabelId label) const {
    return impl_.HasLabel(label, view);
  }

  auto Properties(storage::v3::View view) const { return impl_.Properties(view); }

  storage::v3::Result<storage::v3::PropertyValue> GetProperty(storage::v3::View view,
                                                              storage::v3::PropertyId key) const {
    return impl_.GetProperty(key, view);
  }

  storage::v3::ResultSchema<storage::v3::PropertyValue> SetProperty(storage::v3::PropertyId key,
                                                                    const storage::v3::PropertyValue &value) {
    return impl_.SetPropertyAndValidate(key, value);
  }

  storage::v3::ResultSchema<storage::v3::PropertyValue> SetPropertyAndValidate(
      storage::v3::PropertyId key, const storage::v3::PropertyValue &value) {
    return impl_.SetPropertyAndValidate(key, value);
  }

  storage::v3::ResultSchema<storage::v3::PropertyValue> RemovePropertyAndValidate(storage::v3::PropertyId key) {
    return SetPropertyAndValidate(key, storage::v3::PropertyValue{});
  }

  storage::v3::Result<std::map<storage::v3::PropertyId, storage::v3::PropertyValue>> ClearProperties() {
    return impl_.ClearProperties();
  }

  auto InEdges(storage::v3::View view, const std::vector<storage::v3::EdgeTypeId> &edge_types) const
      -> storage::v3::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.InEdges(view)))> {
    auto maybe_edges = impl_.InEdges(view, edge_types);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  }

  auto InEdges(storage::v3::View view) const { return InEdges(view, {}); }

  auto InEdges(storage::v3::View view, const std::vector<storage::v3::EdgeTypeId> &edge_types,
               const VertexAccessor &dest) const
      -> storage::v3::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.InEdges(view)))> {
    auto maybe_edges = impl_.InEdges(view, edge_types, &dest.impl_);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  }

  auto OutEdges(storage::v3::View view, const std::vector<storage::v3::EdgeTypeId> &edge_types) const
      -> storage::v3::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.OutEdges(view)))> {
    auto maybe_edges = impl_.OutEdges(view, edge_types);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  }

  auto OutEdges(storage::v3::View view) const { return OutEdges(view, {}); }

  auto OutEdges(storage::v3::View view, const std::vector<storage::v3::EdgeTypeId> &edge_types,
                const VertexAccessor &dest) const
      -> storage::v3::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.OutEdges(view)))> {
    auto maybe_edges = impl_.OutEdges(view, edge_types, &dest.impl_);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  }

  storage::v3::Result<size_t> InDegree(storage::v3::View view) const { return impl_.InDegree(view); }

  storage::v3::Result<size_t> OutDegree(storage::v3::View view) const { return impl_.OutDegree(view); }

  // TODO(jbajic) Fix Remove Gid
  static int64_t CypherId() { return 1; }

  bool operator==(const VertexAccessor &v) const noexcept {
    static_assert(noexcept(impl_ == v.impl_));
    return impl_ == v.impl_;
  }

  bool operator!=(const VertexAccessor &v) const noexcept { return !(*this == v); }
};

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnull-dereference"
inline VertexAccessor EdgeAccessor::To() const { return *static_cast<VertexAccessor *>(nullptr); }

inline VertexAccessor EdgeAccessor::From() const { return *static_cast<VertexAccessor *>(nullptr); }
#pragma clang diagnostic pop

inline bool EdgeAccessor::IsCycle() const { return To() == From(); }

class DbAccessor final {
  storage::v3::Shard::Accessor *accessor_;

  class VerticesIterable final {
    storage::v3::VerticesIterable iterable_;

   public:
    class Iterator final {
      storage::v3::VerticesIterable::Iterator it_;

     public:
      explicit Iterator(storage::v3::VerticesIterable::Iterator it) : it_(it) {}

      VertexAccessor operator*() const { return VertexAccessor(*it_); }

      Iterator &operator++() {
        ++it_;
        return *this;
      }

      bool operator==(const Iterator &other) const { return it_ == other.it_; }

      bool operator!=(const Iterator &other) const { return !(other == *this); }
    };

    explicit VerticesIterable(storage::v3::VerticesIterable iterable) : iterable_(std::move(iterable)) {}

    Iterator begin() { return Iterator(iterable_.begin()); }

    Iterator end() { return Iterator(iterable_.end()); }
  };

 public:
  explicit DbAccessor(storage::v3::Shard::Accessor *accessor) : accessor_(accessor) {}

  // TODO(jbajic) Fix Remove Gid
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  std::optional<VertexAccessor> FindVertex(uint64_t /*unused*/) { return std::nullopt; }

  std::optional<VertexAccessor> FindVertex(storage::v3::PrimaryKey &primary_key, storage::v3::View view) {
    auto maybe_vertex = accessor_->FindVertex(primary_key, view);
    if (maybe_vertex) return VertexAccessor(*maybe_vertex);
    return std::nullopt;
  }

  void FinalizeTransaction() { accessor_->FinalizeTransaction(); }

  VerticesIterable Vertices(storage::v3::View view) { return VerticesIterable(accessor_->Vertices(view)); }

  VerticesIterable Vertices(storage::v3::View view, storage::v3::LabelId label) {
    return VerticesIterable(accessor_->Vertices(label, view));
  }

  VerticesIterable Vertices(storage::v3::View view, storage::v3::LabelId label, storage::v3::PropertyId property) {
    return VerticesIterable(accessor_->Vertices(label, property, view));
  }

  VerticesIterable Vertices(storage::v3::View view, storage::v3::LabelId label, storage::v3::PropertyId property,
                            const storage::v3::PropertyValue &value) {
    return VerticesIterable(accessor_->Vertices(label, property, value, view));
  }

  VerticesIterable Vertices(storage::v3::View view, storage::v3::LabelId label, storage::v3::PropertyId property,
                            const std::optional<utils::Bound<storage::v3::PropertyValue>> &lower,
                            const std::optional<utils::Bound<storage::v3::PropertyValue>> &upper) {
    return VerticesIterable(accessor_->Vertices(label, property, lower, upper, view));
  }

  storage::v3::ResultSchema<VertexAccessor> InsertVertexAndValidate(
      const storage::v3::LabelId primary_label, const std::vector<storage::v3::LabelId> &labels,
      const std::vector<std::pair<storage::v3::PropertyId, storage::v3::PropertyValue>> &properties) {
    auto maybe_vertex_acc = accessor_->CreateVertexAndValidate(primary_label, labels, properties);
    if (maybe_vertex_acc.HasError()) {
      return {std::move(maybe_vertex_acc.GetError())};
    }
    return VertexAccessor{maybe_vertex_acc.GetValue()};
  }

  storage::v3::Result<EdgeAccessor> InsertEdge(VertexAccessor *from, VertexAccessor *to,
                                               const storage::v3::EdgeTypeId &edge_type) {
    auto maybe_edge = accessor_->CreateEdge(&from->impl_, &to->impl_, edge_type);
    if (maybe_edge.HasError()) return storage::v3::Result<EdgeAccessor>(maybe_edge.GetError());
    return EdgeAccessor(*maybe_edge);
  }

  storage::v3::Result<std::optional<EdgeAccessor>> RemoveEdge(EdgeAccessor *edge) {
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

  storage::v3::Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachRemoveVertex(
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

  storage::v3::Result<std::optional<VertexAccessor>> RemoveVertex(VertexAccessor *vertex_accessor) {
    auto res = accessor_->DeleteVertex(&vertex_accessor->impl_);
    if (res.HasError()) {
      return res.GetError();
    }

    const auto &value = res.GetValue();
    if (!value) {
      return std::optional<VertexAccessor>{};
    }

    return {std::make_optional<VertexAccessor>(*value)};
  }

  // TODO(jbajic) Query engine should have a map of labels, properties and edge
  // types
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  storage::v3::PropertyId NameToProperty(const std::string_view /*name*/) {
    return storage::v3::PropertyId::FromUint(0);
  }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  storage::v3::LabelId NameToLabel(const std::string_view /*name*/) { return storage::v3::LabelId::FromUint(0); }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  storage::v3::EdgeTypeId NameToEdgeType(const std::string_view /*name*/) {
    return storage::v3::EdgeTypeId::FromUint(0);
  }

  const std::string &PropertyToName(storage::v3::PropertyId prop) const { return accessor_->PropertyToName(prop); }

  const std::string &LabelToName(storage::v3::LabelId label) const { return accessor_->LabelToName(label); }

  const std::string &EdgeTypeToName(storage::v3::EdgeTypeId type) const { return accessor_->EdgeTypeToName(type); }

  void AdvanceCommand() { accessor_->AdvanceCommand(); }

  utils::BasicResult<storage::v3::ConstraintViolation, void> Commit() { return accessor_->Commit(); }

  void Abort() { accessor_->Abort(); }

  bool LabelIndexExists(storage::v3::LabelId label) const { return accessor_->LabelIndexExists(label); }

  bool LabelPropertyIndexExists(storage::v3::LabelId label, storage::v3::PropertyId prop) const {
    return accessor_->LabelPropertyIndexExists(label, prop);
  }

  int64_t VerticesCount() const { return accessor_->ApproximateVertexCount(); }

  int64_t VerticesCount(storage::v3::LabelId label) const { return accessor_->ApproximateVertexCount(label); }

  int64_t VerticesCount(storage::v3::LabelId label, storage::v3::PropertyId property) const {
    return accessor_->ApproximateVertexCount(label, property);
  }

  int64_t VerticesCount(storage::v3::LabelId label, storage::v3::PropertyId property,
                        const storage::v3::PropertyValue &value) const {
    return accessor_->ApproximateVertexCount(label, property, value);
  }

  int64_t VerticesCount(storage::v3::LabelId label, storage::v3::PropertyId property,
                        const std::optional<utils::Bound<storage::v3::PropertyValue>> &lower,
                        const std::optional<utils::Bound<storage::v3::PropertyValue>> &upper) const {
    return accessor_->ApproximateVertexCount(label, property, lower, upper);
  }

  storage::v3::IndicesInfo ListAllIndices() const { return accessor_->ListAllIndices(); }

  storage::v3::ConstraintsInfo ListAllConstraints() const { return accessor_->ListAllConstraints(); }

  const storage::v3::SchemaValidator &GetSchemaValidator() const { return accessor_->GetSchemaValidator(); }

  storage::v3::SchemasInfo ListAllSchemas() const { return accessor_->ListAllSchemas(); }
};

}  // namespace memgraph::query::v2

namespace std {

template <>
struct hash<memgraph::query::v2::VertexAccessor> {
  size_t operator()(const memgraph::query::v2::VertexAccessor &v) const {
    return std::hash<decltype(v.impl_)>{}(v.impl_);
  }
};

template <>
struct hash<memgraph::query::v2::EdgeAccessor> {
  size_t operator()(const memgraph::query::v2::EdgeAccessor &e) const {
    return std::hash<decltype(e.impl_)>{}(e.impl_);
  }
};

}  // namespace std
