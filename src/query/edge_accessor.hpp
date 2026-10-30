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

#include <cstdint>
#include <span>

#include "storage/v2/edge_accessor.hpp"
#ifdef MG_ENTERPRISE
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
  // For a call site that holds the context: `branch_ctx` is the one `impl`'s transaction belongs to.
  EdgeAccessor(storage::EdgeAccessor impl, [[maybe_unused]] versioning::BranchContext *branch_ctx)
      : impl_(std::move(impl)) {
    DMG_ASSERT(impl_.transaction_->branch_ctx == branch_ctx, "The accessor's transaction is not the branch's.");
  }

  // Non-null iff the accessor belongs to a branch's transaction (storage::Transaction::branch_ctx). A fork edge keeps
  // its main@fork_ts accessor in impl_ and its branch changes are read by gid.
  versioning::BranchContext *branch_ctx() const { return impl_.transaction_->branch_ctx; }

#endif
#ifdef MG_ENTERPRISE
  // On a branch: whether the branch deleted or hid the edge.
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

  // One slot per ordered path; on a branch each key is read as the branch sees it, not as main at the fork holds it.
  storage::Result<void> ReadPropertyValues(storage::OrderedPropertyPaths const &ordered_properties,
                                           std::span<storage::PropertyValue> out, storage::View view) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchReadPropertyValues(ordered_properties, out, view);
    return impl_.ReadPropertyValues(ordered_properties, out, view);
  }

  // Read from impl_'s storage; the Bolt encoder omits these properties when asked to.
  std::vector<storage::PropertyId> VectorIndexedProperties() const { return impl_.VectorIndexedProperties(); }
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

  storage::Result<void> ReadPropertyValues(storage::OrderedPropertyPaths const &ordered_properties,
                                           std::span<storage::PropertyValue> out, storage::View view) const {
    return impl_.ReadPropertyValues(ordered_properties, out, view);
  }
#endif

  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
#ifdef MG_ENTERPRISE
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchSetProperty(key, value);
#endif
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
#ifdef MG_ENTERPRISE
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchInitProperties(properties);
#endif
    return impl_.InitProperties(properties);
  }

  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
#ifdef MG_ENTERPRISE
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchUpdateProperties(properties);
#endif
    return impl_.UpdateProperties(properties);
  }

  storage::Result<storage::PropertyValue> RemoveProperty(storage::PropertyId key) {
    return SetProperty(key, storage::PropertyValue());
  }

  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearProperties() {
#ifdef MG_ENTERPRISE
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchClearProperties();
#endif
    return impl_.ClearProperties();
  }

  // Defined inline at the end of query/vertex_accessor.hpp, which this header includes last.
  inline VertexAccessor To() const;

  inline VertexAccessor From() const;

  /// When edge is deleted and you are accessing To vertex
  /// for_deleted_ flag will in this case be updated properly
  inline VertexAccessor DeletedEdgeToVertex() const;

  /// When edge is deleted and you are accessing From vertex
  /// for_deleted_ flag will in this case be updated properly
  inline VertexAccessor DeletedEdgeFromVertex() const;

  bool IsCycle() const;

  int64_t CypherId() const { return impl_.Gid().AsInt(); }

  storage::Gid Gid() const noexcept { return impl_.Gid(); }

  bool operator==(const EdgeAccessor &e) const noexcept { return impl_ == e.impl_; }

  bool operator!=(const EdgeAccessor &e) const noexcept { return !(*this == e); }
#ifdef MG_ENTERPRISE

 private:
  // An endpoint of this branch edge as the query layer holds it: the fork vertex, not a diff-engine stub
  // (edge_accessor.cpp).
  enum class Endpoint : uint8_t { kTo, kFrom, kDeletedTo, kDeletedFrom };
  [[gnu::cold]] VertexAccessor BranchEndpoint(Endpoint endpoint) const;

  // Branch reads (edge_accessor.cpp); reached only when branch_ctx() != nullptr. Each forwards to the BranchContext
  // read it is named after.
  bool BranchIsDeleted() const;
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchProperties(storage::View view) const;
  storage::Result<storage::PropertyValue> BranchGetProperty(storage::View view, storage::PropertyId key) const;
  storage::Result<uint64_t> BranchGetPropertySize(storage::PropertyId key, storage::View view) const;
  storage::Result<void> BranchReadPropertyValues(storage::OrderedPropertyPaths const &ordered_properties,
                                                 std::span<storage::PropertyValue> out, storage::View view) const;

  // Branch writes: forwarders to the BranchContext edge write resolver, which writes a fork edge's stub and a native
  // edge directly. A write to an edge the branch deleted fails with DELETED_OBJECT, and with PROPERTIES_DISABLED if
  // MAIN has no edge properties.
  storage::Result<storage::PropertyValue> BranchSetProperty(storage::PropertyId key,
                                                            const storage::PropertyValue &value);
  storage::Result<bool> BranchInitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  BranchUpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) const;
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchClearProperties();
#endif
};

}  // namespace memgraph::query

#include "query/vertex_accessor.hpp"  // IWYU pragma: keep

namespace std {

template <>
struct hash<memgraph::query::EdgeAccessor> {
  size_t operator()(const memgraph::query::EdgeAccessor &e) const { return std::hash<decltype(e.impl_)>{}(e.impl_); }
};

}  // namespace std
