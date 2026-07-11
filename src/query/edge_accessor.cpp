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

// Graph Versioning v1 (lazy diff-context, slice E-2a): see the declaration's own doc-comment
// (edge_accessor.hpp) -- edge mutator COW is E-2c, not this slice; reject cleanly rather than risk
// writing through a historical (read-only) edge.
storage::Result<storage::PropertyValue> EdgeAccessor::SetProperty(storage::PropertyId key,
                                                                  const storage::PropertyValue &value) {
  if (branch_ctx_ != nullptr) {
    throw NotYetImplemented("Edge property mutation on a versioned branch");
  }
  return impl_.SetProperty(key, value);
}

storage::Result<bool> EdgeAccessor::InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  if (branch_ctx_ != nullptr) {
    throw NotYetImplemented("Edge property mutation on a versioned branch");
  }
  return impl_.InitProperties(properties);
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
EdgeAccessor::UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) const {
  if (branch_ctx_ != nullptr) {
    throw NotYetImplemented("Edge property mutation on a versioned branch");
  }
  return impl_.UpdateProperties(properties);
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> EdgeAccessor::ClearProperties() {
  if (branch_ctx_ != nullptr) {
    throw NotYetImplemented("Edge property mutation on a versioned branch");
  }
  return impl_.ClearProperties();
}

}  // namespace memgraph::query
