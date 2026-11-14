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
#include <type_traits>
#include <vector>

#include "query/expanded_edges.hpp"
#include "storage/v2/vertex_accessor.hpp"
#ifdef MG_ENTERPRISE
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

class EdgeAccessor;

class VertexAccessor final {
 public:
  storage::VertexAccessor impl_;

  explicit VertexAccessor(storage::VertexAccessor impl) : impl_(impl) {}

#ifdef MG_ENTERPRISE
  VertexAccessor(storage::VertexAccessor impl, [[maybe_unused]] versioning::BranchContext *branch_ctx) : impl_(impl) {
    DMG_ASSERT(impl.transaction_->branch_ctx == branch_ctx, "The accessor's transaction is not the branch's.");
  }

  // Non-null iff impl_'s transaction is a branch's (storage::Transaction::branch_ctx); a fork vertex's impl_ is still
  // its main@fork_ts accessor.
  versioning::BranchContext *branch_ctx() const { return impl_.transaction_->branch_ctx; }

#endif
  bool IsVisible(storage::View view) const { return impl_.IsVisible(view); }

#ifdef MG_ENTERPRISE
  bool IsDeleted() const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchIsDeleted();
    return impl_.vertex_->deleted();
  }

  auto Labels(storage::View view) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchLabels(view);
    return impl_.Labels(view);
  }
#else
  auto Labels(storage::View view) const { return impl_.Labels(view); }
#endif

  storage::Result<bool> AddLabel(storage::LabelId label) { return impl_.AddLabel(label); }

  storage::Result<bool> RemoveLabel(storage::LabelId label) { return impl_.RemoveLabel(label); }

#ifdef MG_ENTERPRISE
  storage::Result<bool> HasLabel(storage::View view, storage::LabelId label) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchHasLabel(view, label);
    return impl_.HasLabel(label, view);
  }

  storage::Result<bool> HasAnyLabel(storage::View view) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchHasAnyLabel(view);
    return impl_.HasAnyLabel(view);
  }

  auto Properties(storage::View view) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchProperties(view);
    return impl_.Properties(view);
  }

  // A branch read decodes every value, so there is no compact form.
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
  storage::Result<bool> HasLabel(storage::View view, storage::LabelId label) const {
    return impl_.HasLabel(label, view);
  }

  storage::Result<bool> HasAnyLabel(storage::View view) const { return impl_.HasAnyLabel(view); }

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

  /// The edges of this vertex, as a range that builds each accessor when it is looked at. See
  /// `ExpandedEdges`.
  ///
  /// The read itself is not deferred. The edges are read and the vertex's lock released before any
  /// of these returns, so what a concurrent writer can do to the result is what it could do to an
  /// array of accessors.
  storage::Result<ExpandedEdgesResult> InEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types,
                                               storage::HopsLimit *hops_limit = nullptr) const;

  storage::Result<ExpandedEdgesResult> InEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types,
                                               const VertexAccessor &dest,
                                               storage::HopsLimit *hops_limit = nullptr) const;

  storage::Result<ExpandedEdgesResult> OutEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types,
                                                storage::HopsLimit *hops_limit = nullptr) const;

  storage::Result<ExpandedEdgesResult> OutEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types,
                                                const VertexAccessor &dest,
                                                storage::HopsLimit *hops_limit = nullptr) const;

  storage::Result<ExpandedEdgesResult> InEdges(storage::View view) const;

  storage::Result<ExpandedEdgesResult> OutEdges(storage::View view) const;

#ifdef MG_ENTERPRISE
  storage::Result<size_t> InDegree(storage::View view) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchDegree(storage::EdgeDirection::IN, view);
    return impl_.InDegree(view);
  }

  storage::Result<size_t> OutDegree(storage::View view) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchDegree(storage::EdgeDirection::OUT, view);
    return impl_.OutDegree(view);
  }
#else
  storage::Result<size_t> InDegree(storage::View view) const { return impl_.InDegree(view); }

  storage::Result<size_t> OutDegree(storage::View view) const { return impl_.OutDegree(view); }
#endif

  int64_t CypherId() const { return impl_.Gid().AsInt(); }

  storage::Gid Gid() const noexcept { return impl_.Gid(); }

  bool operator==(const VertexAccessor &v) const noexcept {
    static_assert(noexcept(impl_ == v.impl_));
    return impl_ == v.impl_;
  }

  bool operator!=(const VertexAccessor &v) const noexcept { return !(*this == v); }
#ifdef MG_ENTERPRISE

 private:
  // Out-of-line branch reads; reached only when branch_ctx() != nullptr.
  storage::Result<storage::VertexKey> BranchLabels(storage::View view) const;
  storage::Result<bool> BranchHasLabel(storage::View view, storage::LabelId label) const;
  storage::Result<bool> BranchHasAnyLabel(storage::View view) const;
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchProperties(storage::View view) const;
  storage::Result<storage::PropertyValue> BranchGetProperty(storage::View view, storage::PropertyId key) const;
  storage::Result<uint64_t> BranchGetPropertySize(storage::PropertyId key, storage::View view) const;
  storage::Result<void> BranchReadPropertyValues(storage::OrderedPropertyPaths const &ordered_properties,
                                                 std::span<storage::PropertyValue> out, storage::View view) const;
  storage::Result<size_t> BranchDegree(storage::EdgeDirection direction, storage::View view) const;

  bool BranchIsDeleted() const;

  // `dest` null means no destination filter.
  storage::Result<ExpandedEdgesResult> BranchEdges(storage::EdgeDirection direction, storage::View view,
                                                   const std::vector<storage::EdgeTypeId> &edge_types,
                                                   const VertexAccessor *dest, storage::HopsLimit *hops_limit) const;
#endif
};

static_assert(std::is_trivially_copyable<VertexAccessor>::value,
              "VertexAccessor must be trivially copyable to be used in hash tables");

#ifdef MG_ENTERPRISE
// The branch context is reached through the transaction, so the accessors stay the storage accessors' size.
static_assert(sizeof(VertexAccessor) == sizeof(storage::VertexAccessor));
static_assert(sizeof(EdgeAccessor) == sizeof(storage::EdgeAccessor));
static_assert(sizeof(ExpandedEdges) == sizeof(storage::ExpandedEdges));

// One predictable test, on branch_diff_ctx rather than branch_ctx: only the diff engine's edges can have a diff-engine
// stub as an endpoint, so To()/From() on other branch edges stay on the storage path. Branch work is one cold call.
inline VertexAccessor EdgeAccessor::To() const {
  if (impl_.transaction_->branch_diff_ctx != nullptr) [[unlikely]]
    return BranchEndpoint(Endpoint::kTo);
  return VertexAccessor(impl_.ToVertex());
}

inline VertexAccessor EdgeAccessor::From() const {
  if (impl_.transaction_->branch_diff_ctx != nullptr) [[unlikely]]
    return BranchEndpoint(Endpoint::kFrom);
  return VertexAccessor(impl_.FromVertex());
}

inline VertexAccessor EdgeAccessor::DeletedEdgeToVertex() const {
  if (impl_.transaction_->branch_diff_ctx != nullptr) [[unlikely]]
    return BranchEndpoint(Endpoint::kDeletedTo);
  return VertexAccessor(impl_.DeletedEdgeToVertex());
}

inline VertexAccessor EdgeAccessor::DeletedEdgeFromVertex() const {
  if (impl_.transaction_->branch_diff_ctx != nullptr) [[unlikely]]
    return BranchEndpoint(Endpoint::kDeletedFrom);
  return VertexAccessor(impl_.DeletedEdgeFromVertex());
}
#endif

}  // namespace memgraph::query

namespace std {
template <>
struct hash<memgraph::query::VertexAccessor> {
  size_t operator()(const memgraph::query::VertexAccessor &v) const { return std::hash<decltype(v.impl_)>{}(v.impl_); }
};

}  // namespace std
