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

#include "query/edge_accessor.hpp"

#include "query/exceptions.hpp"
#include "query/vertex_accessor.hpp"
#include "versioning/branch_engine.hpp"

namespace memgraph::query {

// Graph Versioning v1 (lazy diff-context, slice E-2a): in branch mode, re-Resolve the endpoint by
// gid (View::NEW, so this sees the branch's own prior writes -- mirrors
// query::VertexAccessor::CowIfNeeded's own View::NEW-on-the-diff-engine convention) rather than
// trusting `impl_.ToVertex()`/`impl_.FromVertex()` verbatim -- the storage-level edge's endpoint
// Vertex* was fixed at whatever engine CREATED the edge, but a LATER statement in the same branch
// session could have COW'd that same vertex (e.g. a subsequent `SET` on it) into the diff engine --
// `ResolveVertex` picks up that COW'd copy; the raw `impl_.ToVertex()`/`FromVertex()` would keep
// pointing at the (now-superseded) object the endpoint resolved to at CREATE/EXPAND time. Falls back
// to the raw endpoint if ResolveVertex somehow misses (defensive; every endpoint gid is guaranteed
// to exist in one store or the other by construction, mirroring DbAccessor::FindVertex's own
// fallback shape).
VertexAccessor EdgeAccessor::To() const {
  if (branch_ctx_ != nullptr) {
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.ToVertex().Gid(), storage::View::NEW)) {
      return VertexAccessor(*resolved, branch_ctx_);
    }
  }
  return VertexAccessor(impl_.ToVertex(), branch_ctx_);
}

VertexAccessor EdgeAccessor::From() const {
  if (branch_ctx_ != nullptr) {
    if (auto resolved = branch_ctx_->ResolveVertex(impl_.FromVertex().Gid(), storage::View::NEW)) {
      return VertexAccessor(*resolved, branch_ctx_);
    }
  }
  return VertexAccessor(impl_.FromVertex(), branch_ctx_);
}

/// When edge is deleted and you are accessing To vertex
/// for_deleted_ flag will in this case be updated properly
VertexAccessor EdgeAccessor::DeletedEdgeToVertex() const {
  return VertexAccessor(impl_.DeletedEdgeToVertex(), branch_ctx_);
}

/// When edge is deleted and you are accessing From vertex
/// for_deleted_ flag will in this case be updated properly
VertexAccessor EdgeAccessor::DeletedEdgeFromVertex() const {
  return VertexAccessor(impl_.DeletedEdgeFromVertex(), branch_ctx_);
}

bool EdgeAccessor::IsCycle() const { return To() == From(); }

// Graph Versioning v1 (lazy diff-context, slice E-2c) HIGH-2-equivalent FIX: see each declaration's
// own doc-comment in edge_accessor.hpp for WHY these are self-correcting (re-resolve via
// FindDiffEdge, diff-engine-only, before reading) and WHY they're out-of-line (BranchContext is
// only forward-declared in the header). Not a mutation -- FindDiffEdge never COWs -- so this cannot
// double-apply a COW; it's a plain diff-engine-only FindEdge, same cost class as the vertex-side
// equivalent already on this hot path.
auto EdgeAccessor::Properties(storage::View view) const -> decltype(impl_.Properties(view)) {
  if (branch_ctx_ != nullptr) {
    if (auto diff_edge = branch_ctx_->FindDiffEdge(impl_.Gid(), view)) {
      return diff_edge->Properties(view);
    }
  }
  return impl_.Properties(view);
}

storage::Result<storage::PropertyValue> EdgeAccessor::GetProperty(storage::View view, storage::PropertyId key) const {
  if (branch_ctx_ != nullptr) {
    if (auto diff_edge = branch_ctx_->FindDiffEdge(impl_.Gid(), view)) {
      return diff_edge->GetProperty(key, view);
    }
  }
  return impl_.GetProperty(key, view);
}

storage::Result<uint64_t> EdgeAccessor::GetPropertySize(storage::PropertyId key, storage::View view) const {
  if (branch_ctx_ != nullptr) {
    if (auto diff_edge = branch_ctx_->FindDiffEdge(impl_.Gid(), view)) {
      return diff_edge->GetPropertySize(key, view);
    }
  }
  return impl_.GetPropertySize(key, view);
}

// Graph Versioning v1 (lazy diff-context, slice E-2c) -- see the header's own doc-comment. Every
// mutator call site (SetProperty/InitProperties/UpdateProperties/ClearProperties) already checks
// branch_ctx_ != nullptr before calling this, so branch_ctx_ is guaranteed non-null here.
void EdgeAccessor::CowEdgeIfNeeded() {
  // CowEdge reads `impl_` (whichever side it currently resolves through, historical or diff) and
  // the diff-engine accessor from branch_ctx_->current_diff_txn() itself (a single per-query slot,
  // branch_engine.hpp) rather than taking one as a parameter here.
  auto cowed = branch_ctx_->CowEdge(impl_);
  if (!cowed) {
    throw QueryRuntimeException(cowed.error().message);
  }
  impl_ = *cowed;
}

// Graph Versioning v1 (lazy diff-context, slice E-2c): edge mutator COW -- see the header's own
// doc-comment for why this now routes through CowEdgeIfNeeded instead of rejecting with
// NotYetImplemented (E-2a's guard, superseded by this slice).
storage::Result<storage::PropertyValue> EdgeAccessor::SetProperty(storage::PropertyId key,
                                                                  const storage::PropertyValue &value) {
  if (branch_ctx_ != nullptr) CowEdgeIfNeeded();
  return impl_.SetProperty(key, value);
}

storage::Result<bool> EdgeAccessor::InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  if (branch_ctx_ != nullptr) CowEdgeIfNeeded();
  return impl_.InitProperties(properties);
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
EdgeAccessor::UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  if (branch_ctx_ != nullptr) CowEdgeIfNeeded();
  return impl_.UpdateProperties(properties);
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> EdgeAccessor::ClearProperties() {
  if (branch_ctx_ != nullptr) CowEdgeIfNeeded();
  return impl_.ClearProperties();
}

}  // namespace memgraph::query
