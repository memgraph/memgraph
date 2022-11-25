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

#include "storage/v3/result.hpp"
#include "storage/v3/shard.hpp"

namespace memgraph::storage::v3 {

class DbAccessor final {
  Shard::Accessor *accessor_;

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

  std::optional<VertexAccessor> FindVertex(storage::v3::PrimaryKey &primary_key, storage::v3::View view) {
    auto maybe_vertex = accessor_->FindVertex(primary_key, view);
    if (maybe_vertex) return VertexAccessor(*maybe_vertex);
    return std::nullopt;
  }

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

  storage::v3::ShardResult<EdgeAccessor> InsertEdge(VertexAccessor *from, VertexAccessor *to,
                                                    const storage::v3::EdgeTypeId &edge_type) {
    static constexpr auto kDummyGid = storage::v3::Gid::FromUint(0);
    auto maybe_edge = accessor_->CreateEdge(from->Id(storage::v3::View::NEW).GetValue(),
                                            to->Id(storage::v3::View::NEW).GetValue(), edge_type, kDummyGid);
    if (maybe_edge.HasError()) return {maybe_edge.GetError()};
    return EdgeAccessor(*maybe_edge);
  }

  storage::v3::ShardResult<std::optional<EdgeAccessor>> RemoveEdge(EdgeAccessor *edge) {
    auto res = accessor_->DeleteEdge(edge->FromVertex(), edge->ToVertex(), edge->Gid());
    if (res.HasError()) {
      return res.GetError();
    }

    const auto &value = res.GetValue();
    if (!value) {
      return std::optional<EdgeAccessor>{};
    }

    return std::make_optional<EdgeAccessor>(*value);
  }

  storage::v3::ShardResult<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachRemoveVertex(
      VertexAccessor *vertex_accessor) {
    using ReturnType = std::pair<VertexAccessor, std::vector<EdgeAccessor>>;

    auto res = accessor_->DetachDeleteVertex(vertex_accessor);
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

  storage::v3::ShardResult<std::optional<VertexAccessor>> RemoveVertex(VertexAccessor *vertex_accessor) {
    auto res = accessor_->DeleteVertex(vertex_accessor);
    if (res.HasError()) {
      return res.GetError();
    }

    const auto &value = res.GetValue();
    if (!value) {
      return std::optional<VertexAccessor>{};
    }

    return {std::make_optional<VertexAccessor>(*value)};
  }

  storage::v3::LabelId NameToLabel(const std::string_view name) { return accessor_->NameToLabel(name); }

  storage::v3::PropertyId NameToProperty(const std::string_view name) { return accessor_->NameToProperty(name); }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  storage::v3::EdgeTypeId NameToEdgeType(const std::string_view name) { return accessor_->NameToEdgeType(name); }

  const std::string &PropertyToName(storage::v3::PropertyId prop) const { return accessor_->PropertyToName(prop); }

  const std::string &LabelToName(storage::v3::LabelId label) const { return accessor_->LabelToName(label); }

  const std::string &EdgeTypeToName(storage::v3::EdgeTypeId type) const { return accessor_->EdgeTypeToName(type); }

  void AdvanceCommand() { accessor_->AdvanceCommand(); }

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

  const storage::v3::SchemaValidator &GetSchemaValidator() const { return accessor_->GetSchemaValidator(); }

  storage::v3::SchemasInfo ListAllSchemas() const { return accessor_->ListAllSchemas(); }
};
}  // namespace memgraph::storage::v3
