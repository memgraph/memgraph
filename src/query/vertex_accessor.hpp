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

#include "storage/v2/vertex_accessor.hpp"

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
  storage::VertexAccessor impl_;
  // Graph Versioning v1 (lazy diff-context, slice E-1): non-null iff this accessor was minted
  // while checked out on a branch (see DbAccessor::FindVertex/Vertices/InsertVertex,
  // db_accessor.hpp). A raw, non-owning pointer so VertexAccessor stays trivially copyable (see
  // the static_assert below) -- costs nothing on the non-branch path beyond one extra default-null
  // member (R6).
  //
  // SINGLE-POINTER COLLAPSE (mg_procedure_impl.hpp's kMaxMgpVertexSize budget): this used to be
  // TWO pointers (branch_ctx_ + a duplicated diff_txn_) -- that overflowed mgp_vertex's 64-byte
  // C-API size budget (VertexAccessor sits inside mgp_vertex's variant) and grew this hot,
  // ubiquitous accessor for EVERY caller, branch or not, violating R6. The diff-engine accessor a
  // mutator needs (CowIfNeeded, below) now lives on `branch_ctx_` itself
  // (`BranchContext::current_diff_txn()`, branch_engine.hpp) as a single per-query slot -- safe
  // because a checked-out branch is exclusive single-writer, so at most one query ever runs
  // against a given BranchContext at a time. `branch_ctx_` is therefore the ONLY extra pointer
  // this class needs.
  //
  // NON-const: CowVertex (via CowIfNeeded) opens accessors against the diff engine and is a
  // non-const member function of BranchContext -- a const pointer here could not call it.
  versioning::BranchContext *branch_ctx_{nullptr};

  explicit VertexAccessor(storage::VertexAccessor impl) : impl_(impl) {}

  VertexAccessor(storage::VertexAccessor impl, versioning::BranchContext *branch_ctx)
      : impl_(impl), branch_ctx_(branch_ctx) {}

  bool IsVisible(storage::View view) const { return impl_.IsVisible(view); }

  // Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-2 FIX (adversarial-review
  // stale-copy-read bug): reads DO need branch-aware logic after all -- the ORIGINAL reasoning
  // above ("FindVertex/Vertices/CowVertex already resolved impl_ before construction") was true at
  // CONSTRUCTION time, but a VALUE COPY of this accessor taken before a same-statement write (e.g.
  // the Frame's copy of `n` in `MATCH (n) SET n.x=2 RETURN n.x`) keeps pointing at the pre-COW
  // historical object forever -- CowIfNeeded() only redirects the ONE accessor it's called through
  // (SetProperty's `this`), not every outstanding copy. Making every read self-correcting (re-
  // resolve by gid, through the SAME view the caller asked for, before reading) fixes this
  // transparently: ResolveVertex is diff-engine-first (cheap FindVertex, O(log) on the skip list)
  // so once ANY copy has triggered the COW, every copy's reads immediately pick up the diff-engine
  // object again. Not a mutation (no CowIfNeeded call here) so this cannot double-apply a COW, and
  // it costs nothing on the non-branch path beyond the existing branch_ctx_ != nullptr check.
  //
  // OUT-OF-LINE (vertex_accessor.cpp), like CowIfNeeded below: `branch_ctx_` is only
  // FORWARD-declared in this header (`namespace memgraph::versioning { class BranchContext; }`
  // above) -- calling BranchContext::ResolveVertex (a member function, needing the complete type)
  // inline here would not compile. The trailing `decltype(impl_.Labels(view))`/
  // `decltype(impl_.Properties(view))` return types (mirroring db_accessor.hpp's
  // SubgraphVertexAccessor::InEdges, which uses the identical `-> decltype(impl_....)` idiom for
  // the same reason) let these stay declaration-only here without spelling out the storage-layer's
  // concrete Result<...> type by hand.
  auto Labels(storage::View view) const -> decltype(impl_.Labels(view));

  storage::Result<bool> AddLabel(storage::LabelId label) {
    if (branch_ctx_ != nullptr) CowIfNeeded();
    return impl_.AddLabel(label);
  }

  storage::Result<bool> RemoveLabel(storage::LabelId label) {
    if (branch_ctx_ != nullptr) CowIfNeeded();
    return impl_.RemoveLabel(label);
  }

  // HIGH-2 FIX, out-of-line -- see Labels' own doc-comment above.
  storage::Result<bool> HasLabel(storage::View view, storage::LabelId label) const;

  // HIGH-2 FIX, out-of-line -- see Labels' own doc-comment above.
  auto Properties(storage::View view) const -> decltype(impl_.Properties(view));

  // HIGH-2 FIX, out-of-line -- see Labels' own doc-comment above.
  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const;

  // HIGH-2 FIX, out-of-line -- see Labels' own doc-comment above.
  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const;

  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    if (branch_ctx_ != nullptr) CowIfNeeded();
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    if (branch_ctx_ != nullptr) CowIfNeeded();
    return impl_.InitProperties(properties);
  }

  // Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-1 FIX (adversarial-review): this and
  // ClearProperties() are effectively mutators (`SET n += {...}` / `SET n = {...}`) just like
  // SetProperty/AddLabel above, but were missing the COW guard -- on a branch they wrote straight
  // through the READ-ONLY historical accessor, tripping transaction.hpp's `is_historical_`
  // MG_ASSERT. Storage-layer UpdateProperties happens to be declared `const` (it mutates via an
  // internal delta/version-chain mechanism, not through a non-const `this`), so it was possible to
  // keep this method `const` too -- but CowIfNeeded() reassigns `impl_`, which a `const` member
  // function cannot do. Dropped `const` here to mirror SetProperty (also "effectively mutating" but
  // already non-const); every existing call site already passes a non-const pointer/lvalue (see
  // plan/operator.cpp's SetPropertiesOnRecord, which takes `TRecordAccessor *record`, and
  // mg_procedure_impl.cpp's `v->getImpl().UpdateProperties(...)`, which calls through a fresh
  // by-value temporary -- calling a non-const method on either is fine).
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    if (branch_ctx_ != nullptr) CowIfNeeded();
    return impl_.UpdateProperties(properties);
  }

  storage::Result<storage::PropertyValue> RemoveProperty(storage::PropertyId key) {
    return SetProperty(key, storage::PropertyValue());
  }

  storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> ClearProperties() {
    if (branch_ctx_ != nullptr) CowIfNeeded();
    return impl_.ClearProperties();
  }

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view,
                                                    const std::vector<storage::EdgeTypeId> &edge_types,
                                                    query::HopsLimit *hops_limit = nullptr) const;

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view) const;

  storage::Result<EdgeVertexAccessorResult> InEdges(storage::View view,
                                                    const std::vector<storage::EdgeTypeId> &edge_types,
                                                    const VertexAccessor &dest,
                                                    query::HopsLimit *hops_limit = nullptr) const;

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view,
                                                     const std::vector<storage::EdgeTypeId> &edge_types,
                                                     query::HopsLimit *hops_limit = nullptr) const;

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view) const;

  storage::Result<EdgeVertexAccessorResult> OutEdges(storage::View view,
                                                     const std::vector<storage::EdgeTypeId> &edge_types,
                                                     const VertexAccessor &dest,
                                                     query::HopsLimit *hops_limit = nullptr) const;

  // Graph Versioning v1 (lazy diff-context, slice E-2a): in branch mode, the degree must count the
  // UNION (historical_ + diff engine), not just whatever `impl_` happens to point at -- same
  // historical-vs-diff union hazard InEdges/OutEdges below have, just collapsed to a count. Falls
  // back to a plain `ResolveEdges(...).size()` rather than a cheaper dedicated counting path (out of
  // scope for this slice -- E-2d, mirrors the "correct over fast" tradeoff DbAccessor::Vertices(view,
  // label)'s MaterializeFilteredBranchScan already made for the analogous vertex-side gap). Out-of-line
  // for the same forward-declaration reason as Labels/Properties/etc above.
  storage::Result<size_t> InDegree(storage::View view) const;

  storage::Result<size_t> OutDegree(storage::View view) const;

  int64_t CypherId() const { return impl_.Gid().AsInt(); }

  storage::Gid Gid() const noexcept { return impl_.Gid(); }

  // Graph Versioning v1 (lazy diff-context, slice E-1) IDENTITY HARDENING: `impl_ == v.impl_`
  // (storage::VertexAccessor::operator==, storage/v2/vertex_accessor.hpp) compares the underlying
  // Vertex* AND Transaction* pointers -- both differ between a not-yet-COW'd historical copy and
  // its diff-engine COW'd counterpart (two physically distinct objects, in two distinct storage
  // engines, each with its own Transaction). Without this override, the SAME logical vertex would
  // split into two non-equal identities the moment it gets COW'd mid-query (e.g. a Cypher
  // MERGE/dedup keyed on vertex identity would treat "historical-A" and "diff-A'" as different
  // nodes). Gated on branch_ctx_ != nullptr so the non-branch path is byte-identical (one extra
  // pointer compare).
  bool operator==(const VertexAccessor &v) const noexcept {
    if (branch_ctx_ != nullptr || v.branch_ctx_ != nullptr) {
      return Gid() == v.Gid();
    }
    static_assert(noexcept(impl_ == v.impl_));
    return impl_ == v.impl_;
  }

  bool operator!=(const VertexAccessor &v) const noexcept { return !(*this == v); }

 private:
  // Out-of-line (vertex_accessor.cpp): if `impl_` is not yet resident in the branch's diff engine,
  // COWs it in (`branch_ctx_->CowVertex`) and redirects `impl_` to point at the copy -- idempotent,
  // and a no-op the second time a given VertexAccessor value is mutated. Throws QueryRuntimeException
  // on an unsupported (Enum) property -- see BranchContext::CowError. Only ever called when
  // branch_ctx_ != nullptr (every call site above already checks).
  void CowIfNeeded();
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
