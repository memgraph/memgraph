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

#include <cstdint>
#include <vector>

#include "storage/v2/vertex_accessor.hpp"

namespace memgraph::query {

class EdgeAccessor;

struct EdgeVertexAccessorResult {
  std::vector<EdgeAccessor> edges;
  int64_t expanded_count;
};

class VertexAccessor final {
 public:
  storage::VertexAccessor impl_;

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

  storage::Result<storage::PropertyValue> SetProperty(const std::vector<storage::PropertyId> &key,
                                                      const storage::PropertyValue &value) {
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

  storage::Result<storage::PropertyValue> RemoveProperty(const std::vector<storage::PropertyId> &key) {
    return SetProperty(key, storage::PropertyValue());
  }

  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearProperties() {
    return impl_.ClearProperties();
  }

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view,
                                                    const std::vector<storage::EdgeTypeId> &edge_types,
                                                    query::HopsLimit *hops_limit = nullptr) const;

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view) const;

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view,
                                                    const std::vector<storage::EdgeTypeId> &edge_types,
                                                    const VertexAccessor &dest,
                                                    query::HopsLimit *hops_limit = nullptr) const;

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view,
                                                     const std::vector<storage::EdgeTypeId> &edge_types,
                                                     query::HopsLimit *hops_limit = nullptr) const;

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view) const;

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view,
                                                     const std::vector<storage::EdgeTypeId> &edge_types,
                                                     const VertexAccessor &dest,
                                                     query::HopsLimit *hops_limit = nullptr) const;

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

}  // namespace memgraph::query

namespace std {
template <>
struct hash<memgraph::query::VertexAccessor> {
  size_t operator()(const memgraph::query::VertexAccessor &v) const { return std::hash<decltype(v.impl_)>{}(v.impl_); }
};

}  // namespace std
