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

#include "query/branch_reserved.hpp"
#include "query/exceptions.hpp"
#include "query/vertex_accessor.hpp"
#include "utils/logging.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/stub_properties.hpp"

namespace memgraph::query {

// Endpoint is deliberately not resolved to its diff-engine copy: branch-aware VertexAccessor reads resolve by gid
// themselves, and eager per-edge resolution dominated deep branch traversals.
VertexAccessor EdgeAccessor::To() const { return VertexAccessor(impl_.ToVertex(), branch_ctx_); }

VertexAccessor EdgeAccessor::From() const { return VertexAccessor(impl_.FromVertex(), branch_ctx_); }

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

namespace {

using PropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;

// A fork edge changed on the branch is an EMPTY diff-engine stub with the same gid (BranchContext::CowEdge); a read
// overlays the stub on the fork edge. Branch-native edges (gid >= 2^62) are complete and never merged.
struct ReadTarget {
  enum class Kind { kDirect, kHidden, kOverlay };
  Kind kind{Kind::kDirect};
  std::optional<storage::EdgeAccessor> stub;
  std::optional<storage::EdgeAccessor> fork;
};

// `impl` is either the fork edge (historical_) or a diff-engine edge (stub or native). A clear branched bit on the
// fork edge proves no stub exists, skipping the lookup; a deleted fork edge reads as DELETED_OBJECT (kHidden).
ReadTarget ResolveReadTarget(versioning::BranchContext &ctx, const storage::EdgeAccessor &impl, storage::View view) {
  if (impl.storage_ == &ctx.diff_engine()) {
    if (!ctx.IsForkGid(impl.Gid())) return {};
    if (ctx.IsForkEdgeHidden(impl, view)) return {.kind = ReadTarget::Kind::kHidden};
    auto fork = ctx.FindForkEdge(impl);
    DMG_ASSERT(fork.has_value(), "Fork edge missing for stub {}", impl.Gid().AsUint());
    if (!fork) return {};
    return {.kind = ReadTarget::Kind::kOverlay, .stub = impl, .fork = std::move(fork)};
  }
  if (!impl.MaybeBranchedHint()) return {};
  if (ctx.IsForkEdgeHidden(impl, view)) return {.kind = ReadTarget::Kind::kHidden};
  if (auto stub = ctx.FindDiffEdge(impl.Gid(), impl.from_vertex_->gid, view)) {
    return {.kind = ReadTarget::Kind::kOverlay, .stub = std::move(stub), .fork = impl};
  }
  return {};
}

storage::Result<PropertyMap> MergedProperties(versioning::BranchContext &ctx, const storage::EdgeAccessor &stub,
                                              const storage::EdgeAccessor &fork, storage::View view) {
  auto stub_props = stub.Properties(view);  // one call, so values and removal list are read consistently
  if (!stub_props) return stub_props;
  return versioning::OverlayProperties(ctx, *stub_props, fork);
}

storage::Result<storage::PropertyValue> MergedGetProperty(versioning::BranchContext &ctx,
                                                          const storage::EdgeAccessor &stub,
                                                          const storage::EdgeAccessor &fork, storage::View view,
                                                          storage::PropertyId key) {
  if (key == ctx.RemovedPropsKey() || key == ctx.DeletedEdgesKey()) return storage::PropertyValue{};
  auto in_fork = fork.GetProperty(key, storage::View::OLD);
  if (!in_fork) return in_fork;
  if (in_fork->IsNull()) return stub.GetProperty(key, view);
  return versioning::OverlayOnForkValue(ctx, stub, view, key, std::move(*in_fork));
}

storage::Result<uint64_t> MergedGetPropertySize(versioning::BranchContext &ctx, const storage::EdgeAccessor &stub,
                                                const storage::EdgeAccessor &fork, storage::PropertyId key,
                                                storage::View view) {
  if (key == ctx.RemovedPropsKey() || key == ctx.DeletedEdgesKey()) return uint64_t{0};
  auto in_fork = fork.GetPropertySize(key, storage::View::OLD);
  if (!in_fork) return in_fork;
  if (*in_fork == 0) return stub.GetPropertySize(key, view);
  return versioning::OverlaySizeOnFork(ctx, stub, view, key, *in_fork);
}

// `stub` is the diff-engine edge written; `fork` is the fork edge it overlays, empty for a branch-native edge.
struct WriteTarget {
  storage::EdgeAccessor stub;
  std::optional<storage::EdgeAccessor> fork;
};

WriteTarget ResolveWriteTarget(versioning::BranchContext &ctx, const storage::EdgeAccessor &impl) {
  if (impl.storage_ == &ctx.diff_engine() && !ctx.IsForkGid(impl.Gid())) return {impl, std::nullopt};
  auto stub = ctx.CowEdge(impl);
  if (!stub) {
    throw QueryRuntimeException(stub.error().message);
  }
  auto fork =
      impl.storage_ != &ctx.diff_engine() ? std::optional<storage::EdgeAccessor>{impl} : ctx.FindForkEdge(*stub);
  if (!fork) throw QueryRuntimeException("Cannot write edge {}: its fork edge is missing.", impl.Gid().AsUint());
  return {*stub, std::move(fork)};
}

}  // namespace

// Out-of-line because BranchContext is only forward-declared in the header. Reads never COW and never redirect impl_.
auto EdgeAccessor::Properties(storage::View view) const -> decltype(impl_.Properties(view)) {
  if (branch_ctx_ != nullptr) {
    auto target = ResolveReadTarget(*branch_ctx_, impl_, view);
    if (target.kind == ReadTarget::Kind::kHidden) return std::unexpected{storage::Error::DELETED_OBJECT};
    if (target.kind == ReadTarget::Kind::kOverlay)
      return MergedProperties(*branch_ctx_, *target.stub, *target.fork, view);
  }
  return impl_.Properties(view);
}

storage::Result<storage::PropertyValue> EdgeAccessor::GetProperty(storage::View view, storage::PropertyId key) const {
  if (branch_ctx_ != nullptr) {
    auto target = ResolveReadTarget(*branch_ctx_, impl_, view);
    if (target.kind == ReadTarget::Kind::kHidden) return std::unexpected{storage::Error::DELETED_OBJECT};
    if (target.kind == ReadTarget::Kind::kOverlay) {
      return MergedGetProperty(*branch_ctx_, *target.stub, *target.fork, view, key);
    }
  }
  return impl_.GetProperty(key, view);
}

storage::Result<uint64_t> EdgeAccessor::GetPropertySize(storage::PropertyId key, storage::View view) const {
  if (branch_ctx_ != nullptr) {
    auto target = ResolveReadTarget(*branch_ctx_, impl_, view);
    if (target.kind == ReadTarget::Kind::kHidden) return std::unexpected{storage::Error::DELETED_OBJECT};
    if (target.kind == ReadTarget::Kind::kOverlay) {
      return MergedGetPropertySize(*branch_ctx_, *target.stub, *target.fork, key, view);
    }
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
  if (ctx.IsReservedProperty(key)) ThrowReserved("property");
  auto target = ResolveWriteTarget(ctx, impl_);
  if (!target.fork) return target.stub.SetProperty(key, value);

  auto old_value = MergedGetProperty(ctx, target.stub, *target.fork, storage::View::NEW, key);
  if (!old_value) return old_value;
  auto removed = versioning::ReadRemoved(ctx, target.stub);
  if (!removed) return std::unexpected{removed.error()};
  const PropertyMap changes{{key, value}};
  auto applied = versioning::ApplyPropertyChanges(
      ctx, target.stub, std::move(*removed), changes, versioning::ForkHas(*target.fork));
  if (!applied) return std::unexpected{applied.error()};
  return old_value;
}

storage::Result<bool> EdgeAccessor::BranchInitProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  RejectReservedProperties(ctx, properties);
  auto target = ResolveWriteTarget(ctx, impl_);
  if (!target.fork) return target.stub.InitProperties(properties);

  auto existing = MergedProperties(ctx, target.stub, *target.fork, storage::View::NEW);
  if (!existing) return std::unexpected{existing.error()};
  if (!existing->empty()) return false;
  auto removed = versioning::ReadRemoved(ctx, target.stub);
  if (!removed) return std::unexpected{removed.error()};
  auto applied = versioning::ApplyPropertyChanges(
      ctx, target.stub, std::move(*removed), properties, versioning::ForkHas(*target.fork));
  if (!applied) return std::unexpected{applied.error()};
  return true;
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
EdgeAccessor::BranchUpdateProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  RejectReservedProperties(ctx, properties);
  auto target = ResolveWriteTarget(ctx, impl_);
  if (!target.fork) return target.stub.UpdateProperties(properties);

  auto existing = MergedProperties(ctx, target.stub, *target.fork, storage::View::NEW);
  if (!existing) return std::unexpected{existing.error()};
  auto removed = versioning::ReadRemoved(ctx, target.stub);
  if (!removed) return std::unexpected{removed.error()};

  auto changes = versioning::UpdateChanges(*existing, properties);
  auto applied = versioning::ApplyPropertyChanges(
      ctx, target.stub, std::move(*removed), properties, versioning::ForkHas(*target.fork));
  if (!applied) return std::unexpected{applied.error()};
  versioning::MergeOldBack(*existing, properties);
  return changes;
}

storage::Result<PropertyMap> EdgeAccessor::BranchClearProperties() {
  auto &ctx = *branch_ctx_;
  auto target = ResolveWriteTarget(ctx, impl_);
  if (!target.fork) return target.stub.ClearProperties();

  auto existing = MergedProperties(ctx, target.stub, *target.fork, storage::View::NEW);
  if (!existing) return existing;
  auto fork_props = target.fork->Properties(storage::View::OLD);
  if (!fork_props) return std::unexpected{fork_props.error()};
  if (auto cleared = target.stub.ClearProperties(); !cleared) return std::unexpected{cleared.error()};
  if (auto written = versioning::WriteAllForkRemoved(ctx, target.stub, *fork_props); !written) {
    return std::unexpected{written.error()};
  }
  return existing;
}

}  // namespace memgraph::query
