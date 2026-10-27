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

#include <algorithm>
#include <optional>

#include "query/exceptions.hpp"
#include "query/vertex_accessor.hpp"
#include "utils/logging.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/vertex_overlay.hpp"

namespace memgraph::query {

namespace {

// A branch-native edge between a fork vertex and anything else has the fork vertex's diff-engine stub as its endpoint
// (the diff engine needs a Vertex there); a fork vertex always surfaces as its fork accessor.
VertexAccessor Endpoint(const storage::VertexAccessor &impl, versioning::BranchContext *branch_ctx) {
  if (branch_ctx == nullptr) return VertexAccessor(impl);
  return VertexAccessor(branch_ctx->ForkIdentity(impl), branch_ctx);
}

}  // namespace

// Endpoint is deliberately not resolved further: branch-aware VertexAccessor reads resolve by gid themselves, and eager
// per-edge resolution dominated deep branch traversals.
VertexAccessor EdgeAccessor::To() const { return Endpoint(impl_.ToVertex(), branch_ctx_); }

VertexAccessor EdgeAccessor::From() const { return Endpoint(impl_.FromVertex(), branch_ctx_); }

/// When edge is deleted and you are accessing To vertex
/// for_deleted_ flag will in this case be updated properly
VertexAccessor EdgeAccessor::DeletedEdgeToVertex() const { return Endpoint(impl_.DeletedEdgeToVertex(), branch_ctx_); }

/// When edge is deleted and you are accessing From vertex
/// for_deleted_ flag will in this case be updated properly
VertexAccessor EdgeAccessor::DeletedEdgeFromVertex() const {
  return Endpoint(impl_.DeletedEdgeFromVertex(), branch_ctx_);
}

bool EdgeAccessor::IsCycle() const { return To() == From(); }

namespace {

using PropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;

// A fork edge's branch-side changes are its overlay version, merged over it on read. Branch-native edges (diff-engine
// accessors) are complete and never merged.
struct ReadTarget {
  enum class Kind { kDirect, kHidden, kOverlay };
  Kind kind{Kind::kDirect};
  const versioning::OverlayVersion *version{nullptr};
};

// A clear branched bit proves the branch never wrote the fork edge, but a deleted endpoint hides it regardless, so
// the bit short-circuits only while the branch has deleted no vertex. A deleted fork edge is kHidden (DELETED_OBJECT);
// `field` narrows the property gate to one property.
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

storage::Result<uint64_t> MergedGetPropertySize(const versioning::OverlayVersion &version,
                                                const storage::EdgeAccessor &fork, storage::PropertyId key) {
  return versioning::MergeGetPropertySize(version, fork, key);
}

void PrepareForkEdge(versioning::BranchContext &ctx, const storage::EdgeAccessor &fork_edge) {
  if (auto prepared = ctx.PrepareForkEdgeWrite(fork_edge); !prepared) {
    throw QueryRuntimeException(prepared.error().message);
  }
}

void RecordForkEdgeWrite(versioning::BranchContext &ctx, const storage::EdgeAccessor &fork_edge,
                         storage::PropertyId pid) {
  auto const gid = fork_edge.Gid();
  ctx.RecordForkEdgeProperty(gid, pid);
  ctx.RecordEdgePropertyTouched(pid, gid, fork_edge.from_vertex_->gid);
}

void RecordForkEdgeWrites(versioning::BranchContext &ctx, const storage::EdgeAccessor &fork_edge,
                          const PropertyMap &properties) {
  for (const auto &[pid, _] : properties) RecordForkEdgeWrite(ctx, fork_edge, pid);
}

}  // namespace

// Out-of-line because BranchContext is only forward-declared in the header. Reads never write and never redirect impl_.
auto EdgeAccessor::Properties(storage::View view) const -> decltype(impl_.Properties(view)) {
  if (branch_ctx_ != nullptr) {
    auto target = ResolveReadTarget(*branch_ctx_, impl_, view, std::nullopt);
    if (target.kind == ReadTarget::Kind::kHidden) return std::unexpected{storage::Error::DELETED_OBJECT};
    if (target.kind == ReadTarget::Kind::kOverlay) return versioning::MergeProperties(*target.version, impl_);
  }
  return impl_.Properties(view);
}

storage::Result<storage::PropertyValue> EdgeAccessor::GetProperty(storage::View view, storage::PropertyId key) const {
  if (branch_ctx_ != nullptr) {
    auto target = ResolveReadTarget(*branch_ctx_, impl_, view, key);
    if (target.kind == ReadTarget::Kind::kHidden) return std::unexpected{storage::Error::DELETED_OBJECT};
    if (target.kind == ReadTarget::Kind::kOverlay) return versioning::MergeGetProperty(*target.version, impl_, key);
  }
  return impl_.GetProperty(key, view);
}

storage::Result<uint64_t> EdgeAccessor::GetPropertySize(storage::PropertyId key, storage::View view) const {
  if (branch_ctx_ != nullptr) {
    auto target = ResolveReadTarget(*branch_ctx_, impl_, view, key);
    if (target.kind == ReadTarget::Kind::kHidden) return std::unexpected{storage::Error::DELETED_OBJECT};
    if (target.kind == ReadTarget::Kind::kOverlay) return MergedGetPropertySize(*target.version, impl_, key);
  }
  return impl_.GetPropertySize(key, view);
}

storage::Result<storage::PropertyValue> EdgeAccessor::SetProperty(storage::PropertyId key,
                                                                  const storage::PropertyValue &value) {
  if (branch_ctx_ == nullptr) return impl_.SetProperty(key, value);
  return BranchSetProperty(key, value);
}

storage::Result<bool> EdgeAccessor::InitProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  if (branch_ctx_ == nullptr) return impl_.InitProperties(properties);
  return BranchInitProperties(properties);
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
EdgeAccessor::UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  if (branch_ctx_ == nullptr) return impl_.UpdateProperties(properties);
  return BranchUpdateProperties(properties);
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> EdgeAccessor::ClearProperties() {
  if (branch_ctx_ == nullptr) return impl_.ClearProperties();
  return BranchClearProperties();
}

storage::Result<storage::PropertyValue> EdgeAccessor::BranchSetProperty(storage::PropertyId key,
                                                                        const storage::PropertyValue &value) {
  auto &ctx = *branch_ctx_;
  if (impl_.storage_ == &ctx.diff_engine()) return impl_.SetProperty(key, value);
  PrepareForkEdge(ctx, impl_);
  RecordForkEdgeWrite(ctx, impl_, key);
  return ctx.EdgeOverlaySetProperty(impl_, key, value);
}

storage::Result<bool> EdgeAccessor::BranchInitProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  if (impl_.storage_ == &ctx.diff_engine()) return impl_.InitProperties(properties);
  PrepareForkEdge(ctx, impl_);
  RecordForkEdgeWrites(ctx, impl_, properties);
  return ctx.EdgeOverlayInitProperties(impl_, properties);
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
EdgeAccessor::BranchUpdateProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  if (impl_.storage_ == &ctx.diff_engine()) return impl_.UpdateProperties(properties);
  PrepareForkEdge(ctx, impl_);
  RecordForkEdgeWrites(ctx, impl_, properties);
  return ctx.EdgeOverlayUpdateProperties(impl_, properties);
}

storage::Result<PropertyMap> EdgeAccessor::BranchClearProperties() {
  auto &ctx = *branch_ctx_;
  if (impl_.storage_ == &ctx.diff_engine()) return impl_.ClearProperties();
  PrepareForkEdge(ctx, impl_);
  auto result = ctx.EdgeOverlayClearProperties(impl_);
  if (result.has_value()) RecordForkEdgeWrites(ctx, impl_, *result);
  return result;
}

}  // namespace memgraph::query
