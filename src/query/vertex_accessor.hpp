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
namespace memgraph::versioning {
class BranchContext;
}  // namespace memgraph::versioning

#endif
namespace memgraph::query {

class EdgeAccessor;

class VertexAccessor final {
 public:
  storage::VertexAccessor impl_;
#ifdef MG_ENTERPRISE
  // Non-null iff minted on a branch; non-owning. A fork vertex keeps its main@fork_ts accessor in impl_ and its branch
  // changes are read by gid. Every read checks this pointer once and leaves the branch work out of line.
  versioning::BranchContext *branch_ctx_{nullptr};
#endif

  explicit VertexAccessor(storage::VertexAccessor impl) : impl_(impl) {}

#ifdef MG_ENTERPRISE
  VertexAccessor(storage::VertexAccessor impl, versioning::BranchContext *branch_ctx)
      : impl_(impl), branch_ctx_(branch_ctx) {}

#endif
  bool IsVisible(storage::View view) const { return impl_.IsVisible(view); }

#ifdef MG_ENTERPRISE
  auto Labels(storage::View view) const {
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchLabels(view);
    return impl_.Labels(view);
  }
#else
  auto Labels(storage::View view) const { return impl_.Labels(view); }
#endif

#ifdef MG_ENTERPRISE
  storage::Result<bool> AddLabel(storage::LabelId label) {
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchAddLabel(label);
    return impl_.AddLabel(label);
  }

  storage::Result<bool> RemoveLabel(storage::LabelId label) {
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchRemoveLabel(label);
    return impl_.RemoveLabel(label);
  }
#else
  storage::Result<bool> AddLabel(storage::LabelId label) { return impl_.AddLabel(label); }

  storage::Result<bool> RemoveLabel(storage::LabelId label) { return impl_.RemoveLabel(label); }
#endif

#ifdef MG_ENTERPRISE
  storage::Result<bool> HasLabel(storage::View view, storage::LabelId label) const {
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchHasLabel(view, label);
    return impl_.HasLabel(label, view);
  }

  storage::Result<bool> HasAnyLabel(storage::View view) const {
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchHasAnyLabel(view);
    return impl_.HasAnyLabel(view);
  }

  auto Properties(storage::View view) const {
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchProperties(view);
    return impl_.Properties(view);
  }

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const {
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchGetProperty(view, key);
    return impl_.GetProperty(key, view);
  }

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const {
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchGetPropertySize(key, view);
    return impl_.GetPropertySize(key, view);
  }

  // The Bolt encoder omits these properties when asked to.
  std::vector<storage::PropertyId> VectorIndexedProperties(std::span<storage::LabelId const> labels) const {
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchVectorIndexedProperties(labels);
    return impl_.VectorIndexedProperties(labels);
  }
#else
  storage::Result<bool> HasLabel(storage::View view, storage::LabelId label) const {
    return impl_.HasLabel(label, view);
  }

  storage::Result<bool> HasAnyLabel(storage::View view) const { return impl_.HasAnyLabel(view); }

  auto Properties(storage::View view) const { return impl_.Properties(view); }

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const {
    return impl_.GetProperty(key, view);
  }

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const {
    return impl_.GetPropertySize(key, view);
  }
#endif

#ifdef MG_ENTERPRISE
  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchSetProperty(key, value);
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchInitProperties(properties);
    return impl_.InitProperties(properties);
  }

  // Non-const, unlike storage's: a branch write of a branch-native vertex re-points impl_ at the transaction's vertex.
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    if (branch_ctx_ != nullptr) [[unlikely]]
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
    if (branch_ctx_ != nullptr) [[unlikely]]
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
    if (branch_ctx_ != nullptr) [[unlikely]]
      return BranchDegree(storage::EdgeDirection::IN, view);
    return impl_.InDegree(view);
  }

  storage::Result<size_t> OutDegree(storage::View view) const {
    if (branch_ctx_ != nullptr) [[unlikely]]
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
#ifdef MG_ENTERPRISE
    // A fork vertex and its diff-engine stub have distinct impl_ but are one vertex (the hash is gid-based too).
    if (branch_ctx_ != nullptr || v.branch_ctx_ != nullptr) [[unlikely]]
      return Gid() == v.Gid();
#endif
    static_assert(noexcept(impl_ == v.impl_));
    return impl_ == v.impl_;
  }

  bool operator!=(const VertexAccessor &v) const noexcept { return !(*this == v); }
#ifdef MG_ENTERPRISE

 private:
  // Branch reads (vertex_accessor.cpp); reached only when branch_ctx_ != nullptr. A touched fork vertex reads as its
  // overlay version merged over impl_; a deleted one fails with DELETED_OBJECT.
  storage::Result<storage::VertexKey> BranchLabels(storage::View view) const;
  storage::Result<bool> BranchHasLabel(storage::View view, storage::LabelId label) const;
  storage::Result<bool> BranchHasAnyLabel(storage::View view) const;
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchProperties(storage::View view) const;
  storage::Result<storage::PropertyValue> BranchGetProperty(storage::View view, storage::PropertyId key) const;
  storage::Result<uint64_t> BranchGetPropertySize(storage::PropertyId key, storage::View view) const;
  storage::Result<size_t> BranchDegree(storage::EdgeDirection direction, storage::View view) const;
  // A vertex in the diff engine (branch-native or a stub) reads main's vector indexes: the diff engine mirrors none.
  std::vector<storage::PropertyId> BranchVectorIndexedProperties(std::span<storage::LabelId const> labels) const;

  // Branch writes: a fork vertex is written in the overlay by gid (impl_ stays the fork accessor), a branch-native one
  // in the diff engine. A write to a vertex this branch deleted throws QueryRuntimeException.
  storage::Result<bool> BranchAddLabel(storage::LabelId label);
  storage::Result<bool> BranchRemoveLabel(storage::LabelId label);
  storage::Result<storage::PropertyValue> BranchSetProperty(storage::PropertyId key,
                                                            const storage::PropertyValue &value);
  storage::Result<bool> BranchInitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  BranchUpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchClearProperties();

  // True when impl_'s own adjacency is the branch's (a fork vertex the branch gave no edge change).
  bool BranchEdgesUnchanged() const;
  // `dest` null means no destination filter.
  storage::Result<ExpandedEdgesResult> BranchEdges(storage::EdgeDirection direction, storage::View view,
                                                   const std::vector<storage::EdgeTypeId> &edge_types,
                                                   const VertexAccessor *dest, storage::HopsLimit *hops_limit) const;
#endif
};

static_assert(std::is_trivially_copyable<VertexAccessor>::value,
              "VertexAccessor must be trivially copyable to be used in hash tables");

}  // namespace memgraph::query

namespace std {
template <>
struct hash<memgraph::query::VertexAccessor> {
  size_t operator()(const memgraph::query::VertexAccessor &v) const { return std::hash<decltype(v.impl_)>{}(v.impl_); }
};

}  // namespace std
