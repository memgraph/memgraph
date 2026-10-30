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

#include "query/vertex_accessor.hpp"
#ifdef MG_ENTERPRISE
#include <map>
#include <optional>

#include "query/exceptions.hpp"
#include "storage/v2/indices/active_indices.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/vertex_overlay.hpp"
#endif

namespace memgraph::query {

#ifdef MG_ENTERPRISE
namespace {

// A branch-native edge on a fork vertex has the vertex's diff-engine stub as its endpoint; a fork vertex always
// surfaces as its fork accessor. Not resolved further: the vertex's reads resolve by gid themselves.
VertexAccessor Endpoint(const storage::VertexAccessor &impl, versioning::BranchContext *branch_ctx) {
  return VertexAccessor(branch_ctx->ForkIdentity(impl), branch_ctx);
}

// What a branch read of a fork edge resolves to. Branch-native edges (diff-engine accessors) are complete: kDirect.
struct ReadTarget {
  enum class Kind : uint8_t { kDirect, kHidden, kOverlay };
  Kind kind{Kind::kDirect};
  const versioning::OverlayVersion *version{nullptr};
};

// A clear branched bit proves the branch never wrote the fork edge, but a deleted endpoint hides it regardless, so the
// bit short-circuits only while the branch has deleted no vertex. `field` narrows the property gate to one property.
ReadTarget ResolveReadTarget(versioning::BranchContext &ctx, const storage::EdgeAccessor &impl, storage::View view,
                             std::optional<storage::PropertyId> field) {
  if (impl.storage_ == &ctx.diff_engine()) return {};
  bool const hint = impl.MaybeBranchedHint();
  if (!hint && !ctx.MayHaveAnyVertexDeleted()) return {};
  if (ctx.IsForkEdgeHidden(impl, view)) return {.kind = ReadTarget::Kind::kHidden};
  if (!hint) return {};
  auto const gid = impl.Gid();
  bool const may_have =
      field ? ctx.MayHaveForkEdgePropertyFieldChange(gid, *field) : ctx.MayHaveForkEdgePropertyChange(gid);
  if (!may_have) return {};
  if (auto const *version = ctx.ReadEdgeOverlayVersion(gid, view)) {
    return {.kind = ReadTarget::Kind::kOverlay, .version = version};
  }
  return {};
}

using PropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;

// False for a branch-native edge, which is written in the diff engine as is. A fork edge gets its branched bit; one
// hidden on the branch (or any edge with edge properties disabled) throws.
bool PrepareBranchEdgeWrite(versioning::BranchContext &ctx, const storage::EdgeAccessor &impl) {
  if (impl.storage_ == &ctx.diff_engine()) return false;
  if (auto prepared = ctx.PrepareForkEdgeWrite(impl); !prepared) throw QueryRuntimeException(prepared.error().message);
  return true;
}

// The fork edge's change filter and the edge-property scans' touched set, recorded before the overlay write.
void RecordForkEdgeWrite(versioning::BranchContext &ctx, const storage::EdgeAccessor &fork_edge,
                         storage::PropertyId pid) {
  auto &core = *ctx.core();
  core.RecordForkEdgeProperty(fork_edge.Gid(), pid);
  core.RecordEdgePropertyTouched(pid, fork_edge.Gid(), fork_edge.from_vertex_->gid);
}

void RecordForkEdgeWrites(versioning::BranchContext &ctx, const storage::EdgeAccessor &fork_edge,
                          const PropertyMap &properties) {
  for (const auto &[pid, _] : properties) RecordForkEdgeWrite(ctx, fork_edge, pid);
}

}  // namespace

#endif
#ifdef MG_ENTERPRISE
VertexAccessor EdgeAccessor::To() const {
  if (branch_ctx_ != nullptr) [[unlikely]]
    return Endpoint(impl_.ToVertex(), branch_ctx_);
  return VertexAccessor(impl_.ToVertex());
}

VertexAccessor EdgeAccessor::From() const {
  if (branch_ctx_ != nullptr) [[unlikely]]
    return Endpoint(impl_.FromVertex(), branch_ctx_);
  return VertexAccessor(impl_.FromVertex());
}
#else
VertexAccessor EdgeAccessor::To() const { return VertexAccessor(impl_.ToVertex()); }

VertexAccessor EdgeAccessor::From() const { return VertexAccessor(impl_.FromVertex()); }
#endif

/// When edge is deleted and you are accessing To vertex
/// for_deleted_ flag will in this case be updated properly
#ifdef MG_ENTERPRISE
VertexAccessor EdgeAccessor::DeletedEdgeToVertex() const {
  if (branch_ctx_ != nullptr) [[unlikely]]
    return Endpoint(impl_.DeletedEdgeToVertex(), branch_ctx_);
  return VertexAccessor(impl_.DeletedEdgeToVertex());
}
#else
VertexAccessor EdgeAccessor::DeletedEdgeToVertex() const { return VertexAccessor(impl_.DeletedEdgeToVertex()); }
#endif

/// When edge is deleted and you are accessing From vertex
/// for_deleted_ flag will in this case be updated properly
#ifdef MG_ENTERPRISE
VertexAccessor EdgeAccessor::DeletedEdgeFromVertex() const {
  if (branch_ctx_ != nullptr) [[unlikely]]
    return Endpoint(impl_.DeletedEdgeFromVertex(), branch_ctx_);
  return VertexAccessor(impl_.DeletedEdgeFromVertex());
}
#else
VertexAccessor EdgeAccessor::DeletedEdgeFromVertex() const { return VertexAccessor(impl_.DeletedEdgeFromVertex()); }
#endif

bool EdgeAccessor::IsCycle() const { return To() == From(); }

#ifdef MG_ENTERPRISE
storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> EdgeAccessor::BranchProperties(
    storage::View view) const {
  auto const target = ResolveReadTarget(*branch_ctx_, impl_, view, std::nullopt);
  if (target.kind == ReadTarget::Kind::kHidden) return std::unexpected{storage::Error::DELETED_OBJECT};
  if (target.kind == ReadTarget::Kind::kOverlay) return versioning::MergeProperties(*target.version, impl_);
  return impl_.Properties(view);
}

storage::Result<storage::PropertyValue> EdgeAccessor::BranchGetProperty(storage::View view,
                                                                        storage::PropertyId key) const {
  auto const target = ResolveReadTarget(*branch_ctx_, impl_, view, key);
  if (target.kind == ReadTarget::Kind::kHidden) return std::unexpected{storage::Error::DELETED_OBJECT};
  if (target.kind == ReadTarget::Kind::kOverlay) return versioning::MergeGetProperty(*target.version, impl_, key);
  return impl_.GetProperty(key, view);
}

storage::Result<uint64_t> EdgeAccessor::BranchGetPropertySize(storage::PropertyId key, storage::View view) const {
  auto const target = ResolveReadTarget(*branch_ctx_, impl_, view, key);
  if (target.kind == ReadTarget::Kind::kHidden) return std::unexpected{storage::Error::DELETED_OBJECT};
  if (target.kind == ReadTarget::Kind::kOverlay) return versioning::MergeGetPropertySize(*target.version, impl_, key);
  return impl_.GetPropertySize(key, view);
}

std::vector<storage::PropertyId> EdgeAccessor::BranchVectorIndexedProperties() const {
  if (impl_.storage_ != &branch_ctx_->diff_engine()) return impl_.VectorIndexedProperties();
  return branch_ctx_->historical().GetTransaction()->active_indices_->vector_edge_->IndexedProperties(impl_.EdgeType());
}

storage::Result<storage::PropertyValue> EdgeAccessor::BranchSetProperty(storage::PropertyId key,
                                                                        const storage::PropertyValue &value) {
  auto &ctx = *branch_ctx_;
  ctx.BumpScanEpoch(versioning::BranchContext::ScanInput::kProperty, key.AsUint());
  if (!PrepareBranchEdgeWrite(ctx, impl_)) return impl_.SetProperty(key, value);
  RecordForkEdgeWrite(ctx, impl_, key);
  return ctx.EdgeOverlaySetProperty(impl_, key, value);
}

storage::Result<bool> EdgeAccessor::BranchInitProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  if (!PrepareBranchEdgeWrite(ctx, impl_)) return impl_.InitProperties(properties);
  RecordForkEdgeWrites(ctx, impl_, properties);
  return ctx.EdgeOverlayInitProperties(impl_, properties);
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
EdgeAccessor::BranchUpdateProperties(PropertyMap &properties) const {
  auto &ctx = *branch_ctx_;
  for (const auto &[pid, _] : properties) {
    ctx.BumpScanEpoch(versioning::BranchContext::ScanInput::kProperty, pid.AsUint());
  }
  if (!PrepareBranchEdgeWrite(ctx, impl_)) return impl_.UpdateProperties(properties);
  RecordForkEdgeWrites(ctx, impl_, properties);
  return ctx.EdgeOverlayUpdateProperties(impl_, properties);
}

// The removed keys are known only after the write.
storage::Result<PropertyMap> EdgeAccessor::BranchClearProperties() {
  auto &ctx = *branch_ctx_;
  if (!PrepareBranchEdgeWrite(ctx, impl_)) return impl_.ClearProperties();
  auto result = ctx.EdgeOverlayClearProperties(impl_);
  if (result) RecordForkEdgeWrites(ctx, impl_, *result);
  return result;
}

#endif
}  // namespace memgraph::query
