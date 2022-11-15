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

  storage::v3::ShardOperationResult<bool> AddLabel(storage::v3::LabelId label) {
    return impl_.AddLabelAndValidate(label);
  }

  storage::v3::ShardOperationResult<bool> AddLabelAndValidate(storage::v3::LabelId label) {
    return impl_.AddLabelAndValidate(label);
  }

  storage::v3::ShardOperationResult<bool> RemoveLabel(storage::v3::LabelId label) {
    return impl_.RemoveLabelAndValidate(label);
  }

  storage::v3::ShardOperationResult<bool> RemoveLabelAndValidate(storage::v3::LabelId label) {
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

  storage::v3::ShardOperationResult<storage::v3::PropertyValue> SetProperty(storage::v3::PropertyId key,
                                                                            const storage::v3::PropertyValue &value) {
    return impl_.SetPropertyAndValidate(key, value);
  }

  storage::v3::ShardOperationResult<storage::v3::PropertyValue> SetPropertyAndValidate(
      storage::v3::PropertyId key, const storage::v3::PropertyValue &value) {
    return impl_.SetPropertyAndValidate(key, value);
  }

  storage::v3::ShardOperationResult<storage::v3::PropertyValue> RemovePropertyAndValidate(storage::v3::PropertyId key) {
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
    const auto dest_id = dest.impl_.Id(view).GetValue();
    auto maybe_edges = impl_.InEdges(view, edge_types, &dest_id);
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
    const auto dest_id = dest.impl_.Id(view).GetValue();
    auto maybe_edges = impl_.OutEdges(view, edge_types, &dest_id);
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
// NOLINTNEXTLINE(readability-convert-member-functions-to-static,clang-analyzer-core.NonNullParamChecker)
inline VertexAccessor EdgeAccessor::To() const { return *static_cast<VertexAccessor *>(nullptr); }

// NOLINTNEXTLINE(readability-convert-member-functions-to-static,clang-analyzer-core.NonNullParamChecker)
inline VertexAccessor EdgeAccessor::From() const { return *static_cast<VertexAccessor *>(nullptr); }
#pragma clang diagnostic pop

inline bool EdgeAccessor::IsCycle() const { return To() == From(); }

class DbAccessor final {
 public:
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
