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

#include "storage/v2/edge_accessor.hpp"
#ifdef MG_ENTERPRISE
#include <cstdint>
#include <span>

#include "storage/v2/transaction.hpp"
#include "utils/logging.hpp"
#endif

#ifdef MG_ENTERPRISE
namespace memgraph::versioning {
class BranchContext;
}  // namespace memgraph::versioning

#endif
namespace memgraph::query {

class VertexAccessor;

class EdgeAccessor final {
 public:
  storage::EdgeAccessor impl_;

  explicit EdgeAccessor(storage::EdgeAccessor impl) : impl_(std::move(impl)) {}

#ifdef MG_ENTERPRISE
  EdgeAccessor(storage::EdgeAccessor impl, [[maybe_unused]] versioning::BranchContext *branch_ctx)
      : impl_(std::move(impl)) {
    DMG_ASSERT(impl_.transaction_->branch_ctx == branch_ctx, "The accessor's transaction is not the branch's.");
  }

  // Non-null iff impl_'s transaction is a branch session's (storage::Transaction::branch_ctx). A fork edge keeps its
  // main@fork_ts accessor in impl_.
  versioning::BranchContext *branch_ctx() const { return impl_.transaction_->branch_ctx; }

#endif
#ifdef MG_ENTERPRISE
  bool IsDeleted() const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchIsDeleted();
    return impl_.IsDeleted();
  }
#else
  bool IsDeleted() const { return impl_.IsDeleted(); }
#endif

  bool IsVisible(storage::View view) const { return impl_.IsVisible(view); }

  storage::EdgeTypeId EdgeType() const { return impl_.EdgeType(); }

#ifdef MG_ENTERPRISE
  auto Properties(storage::View view) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchProperties(view);
    return impl_.Properties(view);
  }

  // A branch read decodes every value, so there is no compact form to keep.
  auto PropertiesCompact(storage::View view) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchProperties(view);
    return impl_.PropertiesCompact(view);
  }

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchGetProperty(view, key);
    return impl_.GetProperty(key, view);
  }

  storage::Result<storage::PropertyValue> GetPropertyCompact(storage::View view, storage::PropertyId key) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchGetProperty(view, key);
    return impl_.GetPropertyCompact(key, view);
  }

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchGetPropertySize(key, view);
    return impl_.GetPropertySize(key, view);
  }

  storage::Result<void> ReadPropertyValues(storage::OrderedPropertyPaths const &ordered_properties,
                                           std::span<storage::PropertyValue> out, storage::View view) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchReadPropertyValues(ordered_properties, out, view);
    return impl_.ReadPropertyValues(ordered_properties, out, view);
  }
#else
  auto Properties(storage::View view) const { return impl_.Properties(view); }

  auto PropertiesCompact(storage::View view) const { return impl_.PropertiesCompact(view); }

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const {
    return impl_.GetProperty(key, view);
  }

  storage::Result<storage::PropertyValue> GetPropertyCompact(storage::View view, storage::PropertyId key) const {
    return impl_.GetPropertyCompact(key, view);
  }

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const {
    return impl_.GetPropertySize(key, view);
  }

#endif

  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
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

  // Enterprise: inline at the end of query/vertex_accessor.hpp; community: edge_accessor.cpp.
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
#ifdef MG_ENTERPRISE

 private:
  enum class Endpoint : uint8_t { kTo, kFrom, kDeletedTo, kDeletedFrom };
  // The endpoint as the query layer holds it: a diff-engine stub surfaces as its fork vertex (ForkIdentity).
  [[gnu::cold]] VertexAccessor BranchEndpoint(Endpoint endpoint) const;

  // Branch reads (edge_accessor.cpp); require branch_ctx() != nullptr.
  bool BranchIsDeleted() const;
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchProperties(storage::View view) const;
  storage::Result<storage::PropertyValue> BranchGetProperty(storage::View view, storage::PropertyId key) const;
  storage::Result<uint64_t> BranchGetPropertySize(storage::PropertyId key, storage::View view) const;
  storage::Result<void> BranchReadPropertyValues(storage::OrderedPropertyPaths const &ordered_properties,
                                                 std::span<storage::PropertyValue> out, storage::View view) const;
#endif
};

}  // namespace memgraph::query

#ifdef MG_ENTERPRISE
#include "query/vertex_accessor.hpp"  // IWYU pragma: keep
#endif

namespace std {

template <>
struct hash<memgraph::query::EdgeAccessor> {
  size_t operator()(const memgraph::query::EdgeAccessor &e) const { return std::hash<decltype(e.impl_)>{}(e.impl_); }
};

}  // namespace std
