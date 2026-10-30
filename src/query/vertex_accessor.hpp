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
#include <type_traits>
#include <vector>

#include "query/expanded_edges.hpp"
#include "storage/v2/vertex_accessor.hpp"
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

class EdgeAccessor;

class VertexAccessor final {
 public:
  storage::VertexAccessor impl_;

  explicit VertexAccessor(storage::VertexAccessor impl) : impl_(impl) {}

#ifdef MG_ENTERPRISE
  // For a call site that holds the context: `branch_ctx` is the one `impl`'s transaction belongs to.
  VertexAccessor(storage::VertexAccessor impl, [[maybe_unused]] versioning::BranchContext *branch_ctx) : impl_(impl) {
    DMG_ASSERT(impl.transaction_->branch_ctx == branch_ctx, "The accessor's transaction is not the branch's.");
  }

  // Non-null iff the accessor belongs to a branch's transaction (storage::Transaction::branch_ctx). A fork vertex keeps
  // its main@fork_ts accessor in impl_ and its branch changes are read by gid. Every read checks this once and leaves
  // the branch work out of line.
  versioning::BranchContext *branch_ctx() const { return impl_.transaction_->branch_ctx; }

#endif
  bool IsVisible(storage::View view) const { return impl_.IsVisible(view); }

  // The deleted flag of the underlying object; on a branch, whether the branch deleted the vertex.
  bool IsDeleted() const;

#ifdef MG_ENTERPRISE
  auto Labels(storage::View view) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchLabels(view);
    return impl_.Labels(view);
  }
#else
  auto Labels(storage::View view) const { return impl_.Labels(view); }
#endif

#ifdef MG_ENTERPRISE
  storage::Result<bool> AddLabel(storage::LabelId label) {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchAddLabel(label);
    return impl_.AddLabel(label);
  }

  storage::Result<bool> RemoveLabel(storage::LabelId label) {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchRemoveLabel(label);
    return impl_.RemoveLabel(label);
  }
#else
  storage::Result<bool> AddLabel(storage::LabelId label) { return impl_.AddLabel(label); }

  storage::Result<bool> RemoveLabel(storage::LabelId label) { return impl_.RemoveLabel(label); }
#endif

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
  std::vector<storage::PropertyId> VectorIndexedProperties(std::span<storage::LabelId const> labels) const {
    return impl_.VectorIndexedProperties(labels);
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

  storage::Result<void> ReadPropertyValues(storage::OrderedPropertyPaths const &ordered_properties,
                                           std::span<storage::PropertyValue> out, storage::View view) const {
    return impl_.ReadPropertyValues(ordered_properties, out, view);
  }
#endif

#ifdef MG_ENTERPRISE
  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchSetProperty(key, value);
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchInitProperties(properties);
    return impl_.InitProperties(properties);
  }

  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
    if (branch_ctx() != nullptr) [[unlikely]]
      return BranchUpdateProperties(properties);
    return impl_.UpdateProperties(properties);
  }
#else
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
#endif

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
  // Branch reads (vertex_accessor.cpp); reached only when branch_ctx() != nullptr. Each forwards to the BranchContext
  // read it is named after, which merges the branch's changes over the fork vertex.
  storage::Result<storage::VertexKey> BranchLabels(storage::View view) const;
  storage::Result<bool> BranchHasLabel(storage::View view, storage::LabelId label) const;
  storage::Result<bool> BranchHasAnyLabel(storage::View view) const;
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchProperties(storage::View view) const;
  storage::Result<storage::PropertyValue> BranchGetProperty(storage::View view, storage::PropertyId key) const;
  storage::Result<uint64_t> BranchGetPropertySize(storage::PropertyId key, storage::View view) const;
  storage::Result<void> BranchReadPropertyValues(storage::OrderedPropertyPaths const &ordered_properties,
                                                 std::span<storage::PropertyValue> out, storage::View view) const;
  storage::Result<size_t> BranchDegree(storage::EdgeDirection direction, storage::View view) const;

  // Branch writes: forwarders to the BranchContext write resolver, which writes a fork vertex's stub and a native
  // vertex directly. A write to a vertex the branch deleted fails with DELETED_OBJECT.
  storage::Result<bool> BranchAddLabel(storage::LabelId label);
  storage::Result<bool> BranchRemoveLabel(storage::LabelId label);
  storage::Result<storage::PropertyValue> BranchSetProperty(storage::PropertyId key,
                                                            const storage::PropertyValue &value);
  storage::Result<bool> BranchInitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  BranchUpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) const;
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchClearProperties();

  bool BranchIsDeleted() const;

  // `dest` null means no destination filter.
  storage::Result<ExpandedEdgesResult> BranchEdges(storage::EdgeDirection direction, storage::View view,
                                                   const std::vector<storage::EdgeTypeId> &edge_types,
                                                   const VertexAccessor *dest, storage::HopsLimit *hops_limit) const;
#endif
};

static_assert(std::is_trivially_copyable<VertexAccessor>::value,
              "VertexAccessor must be trivially copyable to be used in hash tables");
// The branch context is reached through the transaction, so the accessors stay the storage accessors' size.
static_assert(sizeof(VertexAccessor) == sizeof(storage::VertexAccessor));
static_assert(sizeof(EdgeAccessor) == sizeof(storage::EdgeAccessor));
static_assert(sizeof(ExpandedEdges) == sizeof(storage::ExpandedEdges));

#ifdef MG_ENTERPRISE
// On main each is the storage call behind one predictable test, with the branch work in a single out-of-line call that
// leaves the result in the caller's slot. The test is on the diff engine's transaction alone: a fork edge's endpoints
// are fork vertices, so reading them stays as cheap on a branch as on main.
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

/// When edge is deleted and you are accessing To vertex
/// for_deleted_ flag will in this case be updated properly
inline VertexAccessor EdgeAccessor::DeletedEdgeToVertex() const {
  if (impl_.transaction_->branch_diff_ctx != nullptr) [[unlikely]]
    return BranchEndpoint(Endpoint::kDeletedTo);
  return VertexAccessor(impl_.DeletedEdgeToVertex());
}

/// When edge is deleted and you are accessing From vertex
/// for_deleted_ flag will in this case be updated properly
inline VertexAccessor EdgeAccessor::DeletedEdgeFromVertex() const {
  if (impl_.transaction_->branch_diff_ctx != nullptr) [[unlikely]]
    return BranchEndpoint(Endpoint::kDeletedFrom);
  return VertexAccessor(impl_.DeletedEdgeFromVertex());
}
#else
inline VertexAccessor EdgeAccessor::To() const { return VertexAccessor(impl_.ToVertex()); }

inline VertexAccessor EdgeAccessor::From() const { return VertexAccessor(impl_.FromVertex()); }

/// When edge is deleted and you are accessing To vertex
/// for_deleted_ flag will in this case be updated properly
inline VertexAccessor EdgeAccessor::DeletedEdgeToVertex() const { return VertexAccessor(impl_.DeletedEdgeToVertex()); }

/// When edge is deleted and you are accessing From vertex
/// for_deleted_ flag will in this case be updated properly
inline VertexAccessor EdgeAccessor::DeletedEdgeFromVertex() const {
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
