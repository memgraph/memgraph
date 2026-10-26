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

#pragma once

#include <map>
#include <tuple>
#include <vector>

#include "storage/v2/edge_accessor.hpp"

namespace memgraph::versioning {
class BranchContext;
}  // namespace memgraph::versioning

namespace memgraph::query {

class VertexAccessor;

class EdgeAccessor final {
 public:
  storage::EdgeAccessor impl_;
  // Non-null iff minted while checked out on a branch; non-owning.
  versioning::BranchContext *branch_ctx_{nullptr};

  explicit EdgeAccessor(storage::EdgeAccessor impl) : impl_(std::move(impl)) {}

  EdgeAccessor(storage::EdgeAccessor impl, versioning::BranchContext *branch_ctx)
      : impl_(std::move(impl)), branch_ctx_(branch_ctx) {}

  bool IsDeleted() const { return impl_.IsDeleted(); }

  bool IsVisible(storage::View view) const { return impl_.IsVisible(view); }

  storage::EdgeTypeId EdgeType() const { return impl_.EdgeType(); }

  auto Properties(storage::View view) const -> decltype(impl_.Properties(view));

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const;

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const;

  // Edge vector indexes live in main storage and are global, so no per-branch resolution is needed.
  std::vector<storage::PropertyId> VectorIndexedProperties() const { return impl_.VectorIndexedProperties(); }

  // On a branch, a write to a fork edge is an overlay version keyed by its gid (BranchContext::EdgeOverlay*); reads
  // merge it over the fork edge. `impl_` keeps pointing at the fork edge. Out-of-line (BranchContext incomplete).
  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value);

  storage::Result<bool> InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);

  // Non-const (unlike storage's UpdateProperties) to match VertexAccessor::UpdateProperties.
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);

  storage::Result<storage::PropertyValue> RemoveProperty(storage::PropertyId key) {
    return SetProperty(key, storage::PropertyValue());
  }

  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearProperties();

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

  // On a branch, compare by gid like VertexAccessor (hash is gid-based too).
  bool operator==(const EdgeAccessor &e) const noexcept {
    if (branch_ctx_ != nullptr || e.branch_ctx_ != nullptr) {
      return Gid() == e.Gid();
    }
    return impl_ == e.impl_;
  }

  bool operator!=(const EdgeAccessor &e) const noexcept { return !(*this == e); }

 private:
  // Branch-mode mutators (edge_accessor.cpp); only reached when branch_ctx_ != nullptr.
  storage::Result<storage::PropertyValue> BranchSetProperty(storage::PropertyId key,
                                                            const storage::PropertyValue &value);
  storage::Result<bool> BranchInitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  BranchUpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchClearProperties();
};

}  // namespace memgraph::query

namespace std {

template <>
struct hash<memgraph::query::EdgeAccessor> {
  size_t operator()(const memgraph::query::EdgeAccessor &e) const { return std::hash<decltype(e.impl_)>{}(e.impl_); }
};

}  // namespace std
