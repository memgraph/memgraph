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
#include <map>
#include <span>
#include <tuple>
#include <type_traits>
#include <vector>

#include "storage/v2/vertex_accessor.hpp"
#include "versioning/branch_change_kind.hpp"

namespace memgraph::versioning {
class BranchContext;
}  // namespace memgraph::versioning

namespace memgraph::query {

class EdgeAccessor;

struct EdgeVertexAccessorResult {
  std::vector<EdgeAccessor> edges;
  int64_t expanded_count;
};

class VertexAccessor final {
 public:
  // On a branch a fork vertex keeps its main@fork_ts accessor here even once changed; changes are reached by gid
  // (BranchContext::ReadOverlayVersion, Overlay* writes).
  storage::VertexAccessor impl_;
  // Non-null iff minted while checked out on a branch. A single raw pointer keeps this trivially copyable
  // and within mgp_vertex's kMaxMgpVertexSize budget; non-const because CowIfNeeded calls BranchContext::CowVertex.
  versioning::BranchContext *branch_ctx_{nullptr};

  explicit VertexAccessor(storage::VertexAccessor impl) : impl_(impl) {}

  VertexAccessor(storage::VertexAccessor impl, versioning::BranchContext *branch_ctx)
      : impl_(impl), branch_ctx_(branch_ctx) {}

  bool IsVisible(storage::View view) const { return impl_.IsVisible(view); }

  // On a branch a fork vertex is deleted only if the branch deleted it; main's flag on the fork accessor is irrelevant.
  bool IsDeleted() const;

  // On a branch, reads re-resolve by gid: a copy taken before a same-statement write (e.g. the Frame's `n` in
  // `MATCH (n) SET n.x=2 RETURN n.x`) still points at the pre-COW object. Out-of-line: BranchContext is only
  // forward-declared.
  auto Labels(storage::View view) const -> decltype(impl_.Labels(view));

  storage::Result<bool> AddLabel(storage::LabelId label) {
    if (branch_ctx_ != nullptr) return BranchAddLabel(label);
    return impl_.AddLabel(label);
  }

  storage::Result<bool> RemoveLabel(storage::LabelId label) {
    if (branch_ctx_ != nullptr) return BranchRemoveLabel(label);
    return impl_.RemoveLabel(label);
  }

  storage::Result<bool> HasLabel(storage::View view, storage::LabelId label) const;

  auto Properties(storage::View view) const -> decltype(impl_.Properties(view));

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const;

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const;

  // Vector indexes are global (main storage), so delegating to impl_ is correct on a branch.
  std::vector<storage::PropertyId> VectorIndexedProperties(std::span<storage::LabelId const> labels) const {
    return impl_.VectorIndexedProperties(labels);
  }

  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    if (branch_ctx_ != nullptr) return BranchSetProperty(key, value);
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    if (branch_ctx_ != nullptr) return BranchInitProperties(properties);
    return impl_.InitProperties(properties);
  }

  // Non-const (unlike storage's UpdateProperties) because the branch path reassigns impl_ via CowIfNeeded.
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    if (branch_ctx_ != nullptr) return BranchUpdateProperties(properties);
    return impl_.UpdateProperties(properties);
  }

  storage::Result<storage::PropertyValue> RemoveProperty(storage::PropertyId key) {
    return SetProperty(key, storage::PropertyValue());
  }

  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearProperties() {
    if (branch_ctx_ != nullptr) return BranchClearProperties();
    return impl_.ClearProperties();
  }

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view,
                                                    const std::vector<storage::EdgeTypeId> &edge_types,
                                                    storage::HopsLimit *hops_limit = nullptr) const;

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view) const;

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view,
                                                    const std::vector<storage::EdgeTypeId> &edge_types,
                                                    const VertexAccessor &dest,
                                                    storage::HopsLimit *hops_limit = nullptr) const;

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view,
                                                     const std::vector<storage::EdgeTypeId> &edge_types,
                                                     storage::HopsLimit *hops_limit = nullptr) const;

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view) const;

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view,
                                                     const std::vector<storage::EdgeTypeId> &edge_types,
                                                     const VertexAccessor &dest,
                                                     storage::HopsLimit *hops_limit = nullptr) const;

  // On a branch the degree counts the union of historical and diff-engine edges (via ResolveEdges), not a cheaper
  // count. Out-of-line: BranchContext is only forward-declared.
  storage::Result<size_t> InDegree(storage::View view) const;

  storage::Result<size_t> OutDegree(storage::View view) const;

  int64_t CypherId() const { return impl_.Gid().AsInt(); }

  storage::Gid Gid() const noexcept { return impl_.Gid(); }

  // On a branch, compare by gid: a COW'd vertex and its historical original have distinct impl_ (Vertex*/Transaction*).
  bool operator==(const VertexAccessor &v) const noexcept {
    if (branch_ctx_ != nullptr || v.branch_ctx_ != nullptr) {
      return Gid() == v.Gid();
    }
    static_assert(noexcept(impl_ == v.impl_));
    return impl_ == v.impl_;
  }

  bool operator!=(const VertexAccessor &v) const noexcept { return !(*this == v); }

 private:
  // Fork vertex: materializes the overlay (impl_ unchanged); native: redirects impl_ to its diff-engine vertex.
  // Idempotent; throws QueryRuntimeException on COW error. `kind` is recorded on every call, not only the first.
  void CowIfNeeded(versioning::BranchChangeKind kind);

  // Records gid + pid in the fine property filter so GetProperty(pid) can skip the resolve for unchanged pids.
  void RecordPropertyFieldChange(storage::PropertyId pid);
  void RecordPropertyFieldChanges(const std::map<storage::PropertyId, storage::PropertyValue> &properties);

  // Branch-mode mutators: native vertices are written in the diff engine, fork vertices through BranchContext's
  // Overlay* writes. Writes of reserved names are rejected.
  storage::Result<bool> BranchAddLabel(storage::LabelId label);
  storage::Result<bool> BranchRemoveLabel(storage::LabelId label);
  storage::Result<storage::PropertyValue> BranchSetProperty(storage::PropertyId key,
                                                            const storage::PropertyValue &value);
  storage::Result<bool> BranchInitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  BranchUpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);
  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchClearProperties();
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
