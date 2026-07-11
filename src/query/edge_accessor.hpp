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

  // Graph Versioning v1 (lazy diff-context, slice E-2a): edge property READS are NOT made
  // self-correcting the way VertexAccessor's are (HIGH-2 fix, vertex_accessor.hpp) -- this slice has
  // no edge mutator COW (E-2c, below), so there is no mid-statement stale-copy hazard yet for edges:
  // every query::EdgeAccessor this slice hands out (InsertEdge, VertexAccessor::In/OutEdges) already
  // wraps whichever engine's copy is currently authoritative, and nothing can re-COW it out from
  // under a live copy within the same statement. Revisit when E-2c adds edge mutator COW.
  auto Properties(storage::View view) const { return impl_.Properties(view); }

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const {
    return impl_.GetProperty(key, view);
  }

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const {
    return impl_.GetPropertySize(key, view);
  }

  // Graph Versioning v1 (lazy diff-context, slice E-2a): edge mutator COW is E-2c, not this slice --
  // but a query::EdgeAccessor reachable through a checked-out branch CAN already wrap a historical
  // (read-only) storage::EdgeAccessor this slice (any not-yet-touched fork-state edge reached via
  // VertexAccessor::In/OutEdges). Writing straight through it would trip transaction.hpp's
  // `is_historical_` MG_ASSERT -- the exact crash class HIGH-1 fixed for vertices (CowVertex's own
  // history, branch_engine.cpp). Reject cleanly and loudly instead (mirrors
  // DbAccessor::RemoveEdge/RemoveVertex's own NotYetImplemented idiom for the analogous DELETE gap,
  // db_accessor.hpp) rather than leave a crash reachable through the very expansion path this slice
  // adds. Out-of-line (edge_accessor.cpp): throwing needs query::NotYetImplemented
  // (query/exceptions.hpp), which this header deliberately does not pull in.
  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value);

  storage::Result<bool> InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties);

  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) const;

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
  // and a hypothetical diff-engine copy of it (E-2c). Gated on branch_ctx_ != nullptr so the
  // non-branch path is byte-identical.
  bool operator==(const EdgeAccessor &e) const noexcept {
    if (branch_ctx_ != nullptr || e.branch_ctx_ != nullptr) {
      return Gid() == e.Gid();
    }
    return impl_ == e.impl_;
  }

  bool operator!=(const EdgeAccessor &e) const noexcept { return !(*this == e); }
};

}  // namespace memgraph::query

namespace std {

template <>
struct hash<memgraph::query::EdgeAccessor> {
  size_t operator()(const memgraph::query::EdgeAccessor &e) const { return std::hash<decltype(e.impl_)>{}(e.impl_); }
};

}  // namespace std
