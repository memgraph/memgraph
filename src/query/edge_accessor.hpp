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

namespace memgraph::versioning {
class BranchContext;
}  // namespace memgraph::versioning

namespace memgraph::query {

class VertexAccessor;

class EdgeAccessor final {
 public:
  storage::EdgeAccessor impl_;
  // Graph Versioning v1 (lazy diff-context, slice E-2a): non-null iff this accessor was minted
  // while checked out on a branch -- mirrors query::VertexAccessor::branch_ctx_ (vertex_accessor.hpp,
  // slice E-1) exactly: a single, non-owning raw pointer (the diff-engine accessor an edge would
  // need lives on `branch_ctx_` itself, `BranchContext::current_diff_txn()`, not duplicated here) so
  // EdgeAccessor stays trivially copyable and cheap on the non-branch path (one extra default-null
  // member, R6).
  versioning::BranchContext *branch_ctx_{nullptr};

  explicit EdgeAccessor(storage::EdgeAccessor impl) : impl_(std::move(impl)) {}

  EdgeAccessor(storage::EdgeAccessor impl, versioning::BranchContext *branch_ctx)
      : impl_(std::move(impl)), branch_ctx_(branch_ctx) {}

  bool IsDeleted() const { return impl_.IsDeleted(); }

  bool IsVisible(storage::View view) const { return impl_.IsVisible(view); }

  storage::EdgeTypeId EdgeType() const { return impl_.EdgeType(); }

  // Graph Versioning v1 (lazy diff-context, slice E-2c) HIGH-2-equivalent FIX: edge property reads
  // are now self-correcting, mirroring query::VertexAccessor's own HIGH-2 fix (vertex_accessor.hpp)
  // -- E-2c adds edge mutator COW below, so the same mid-statement stale-copy hazard vertices had
  // now exists for edges too (`MATCH ()-[r]->() SET r.x=2 RETURN r.x` would otherwise read the
  // Frame's own, pre-COW copy of `r`). DIFF-SIDE ONLY, unlike VertexAccessor's ResolveVertex-backed
  // fix: re-resolving via `BranchContext::FindDiffEdge` (diff engine only, no historical_ fallback)
  // -- ResolveEdge's own doc-comment (branch_engine.hpp) documents its historical_ half as
  // unreliable for edges, so this deliberately does NOT route through it. A not-yet-COW'd
  // historical edge's `impl_` needs no re-resolve: it's already whatever ResolveEdges (or
  // InsertEdge) handed out, and reading it directly is correct as long as it hasn't been COW'd out
  // from under it -- exactly what FindDiffEdge's presence/absence check distinguishes. Out-of-line
  // (edge_accessor.cpp): FindDiffEdge is a member function needing BranchContext's complete type,
  // only forward-declared here.
  auto Properties(storage::View view) const -> decltype(impl_.Properties(view));

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const;

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const;

  // Graph Versioning v1 (lazy diff-context, slice E-2c): edge mutator COW. A query::EdgeAccessor
  // reachable through a checked-out branch can wrap a historical (read-only) storage::EdgeAccessor
  // (any not-yet-touched fork-state edge reached via VertexAccessor::In/OutEdges) -- writing
  // straight through it would trip transaction.hpp's `is_historical_` MG_ASSERT, the exact crash
  // class HIGH-1 fixed for vertices (CowVertex's own history, branch_engine.cpp). CowEdgeIfNeeded()
  // (mirrors VertexAccessor::CowIfNeeded exactly) COWs `impl_` into the diff engine first --
  // idempotent, so calling it unconditionally on every mutator call is safe and cheap (a plain
  // diff-engine FindEdge, BranchContext::CowEdge's own doc-comment). Out-of-line (edge_accessor.cpp):
  // CowEdgeIfNeeded needs BranchContext's complete type, only forward-declared here.
  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value);

  storage::Result<bool> InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);

  // NON-const, unlike storage::EdgeAccessor::UpdateProperties (which is `const` -- it mutates via an
  // internal delta/version-chain mechanism, not through a non-const `this`): CowEdgeIfNeeded()
  // reassigns `impl_`, which a `const` member function cannot do. Mirrors
  // query::VertexAccessor::UpdateProperties's own identical HIGH-1 "dropped const" precedent
  // (vertex_accessor.hpp) -- every existing call site already passes a non-const pointer/lvalue.
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

  // Graph Versioning v1 (lazy diff-context, slice E-2a) IDENTITY HARDENING: mirrors
  // query::VertexAccessor::operator== exactly (vertex_accessor.hpp's own "IDENTITY HARDENING" doc-
  // comment) -- `impl_ == e.impl_` (storage::EdgeAccessor::operator==) compares the underlying
  // EdgeRef AND Transaction* pointers, both of which differ between a not-yet-COW'd historical edge
  // and its diff-engine COW'd copy (E-2c, CowEdge below). Gated on branch_ctx_ != nullptr so the
  // non-branch path is byte-identical.
  bool operator==(const EdgeAccessor &e) const noexcept {
    if (branch_ctx_ != nullptr || e.branch_ctx_ != nullptr) {
      return Gid() == e.Gid();
    }
    return impl_ == e.impl_;
  }

  bool operator!=(const EdgeAccessor &e) const noexcept { return !(*this == e); }

 private:
  // Graph Versioning v1 (lazy diff-context, slice E-2c): out-of-line (edge_accessor.cpp), mirrors
  // query::VertexAccessor::CowIfNeeded exactly -- if `impl_` is not yet resident in the branch's
  // diff engine, COWs it in (`branch_ctx_->CowEdge`) and redirects `impl_` to point at the copy.
  // Idempotent (CowEdge itself is), so a no-op the second time a given EdgeAccessor value is
  // mutated. Throws QueryRuntimeException on an unsupported (Enum) property -- see
  // BranchContext::CowError. Only ever called when branch_ctx_ != nullptr (every mutator call site
  // already checks).
  void CowEdgeIfNeeded();
};

}  // namespace memgraph::query

namespace std {

template <>
struct hash<memgraph::query::EdgeAccessor> {
  size_t operator()(const memgraph::query::EdgeAccessor &e) const { return std::hash<decltype(e.impl_)>{}(e.impl_); }
};

}  // namespace std
