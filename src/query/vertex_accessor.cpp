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

#include "query/vertex_accessor.hpp"

#include <ranges>
#include <utility>

#include "query/edge_accessor.hpp"
#ifdef MG_ENTERPRISE
#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <tuple>
#include <vector>

#include "query/exceptions.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/exceptions.hpp"
#include "storage/v2/indices/active_indices.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "versioning/branch_change_kind.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/main_constraints.hpp"
#include "versioning/vertex_overlay.hpp"
#endif

namespace memgraph::query {

static_assert(std::ranges::forward_range<ExpandedEdges>);
static_assert(std::ranges::sized_range<ExpandedEdges>);

namespace {

/// The same expansion as the accessors this layer deals in. The edges move across whole, so this
/// changes their type and not what was found.
auto AsQueryResult(storage::EdgesVertexAccessorResult &&result) -> ExpandedEdgesResult {
  return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(result.edges)}, .expanded_count = result.expanded_count};
}

#ifdef MG_ENTERPRISE
// Fork vertices the branch never wrote are fork-identical; branch-native vertices are complete and never merged.
bool MayHaveOverlay(versioning::BranchContext &ctx, const storage::VertexAccessor &impl) {
  return impl.storage_ != &ctx.diff_engine() && impl.vertex_->branched();
}

using PropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;

// Readies `vertex` for a branch write of `kind` and records it in the change filters; returns whether it is a fork
// vertex. A fork vertex keeps its fork accessor (the overlay is addressed by gid, no stub is needed); a branch-native
// one is re-pointed at the current diff transaction's vertex. Throws if the branch deleted the vertex.
bool PrepareBranchWrite(VertexAccessor &vertex, versioning::BranchChangeKind kind) {
  auto &ctx = *vertex.branch_ctx_;
  auto const gid = vertex.Gid();
  bool const fork = ctx.IsForkVertexGid(gid);
  if (fork) {
    if (auto prepared = ctx.PrepareForkWrite(gid); !prepared) throw QueryRuntimeException(prepared.error().message);
  } else {
    auto cowed = ctx.CowVertex(gid);
    if (!cowed) throw QueryRuntimeException(cowed.error().message);
    vertex.impl_ = *cowed;
  }
  ctx.RecordVertexChange(gid, kind);
  return fork;
}

// Feeds GetProperty's per-key filter, the scan epochs and, for a fork vertex, the property scans' touched set.
void RecordPropertyWrites(versioning::BranchContext &ctx, storage::Gid gid, bool fork, const PropertyMap &properties) {
  for (auto const &[pid, _] : properties) {
    ctx.core()->RecordPropertyFieldChange(gid, pid);
    ctx.BumpScanEpoch(versioning::BranchContext::ScanInput::kProperty, pid.AsUint());
    if (fork) ctx.core()->RecordPropertyTouched(pid, gid);
  }
}

// The diff engine mirrors no constraints, so a branch write is checked against main's type constraints here, before
// anything is written, with main's message (storage's HandleTypeConstraintViolation).
[[noreturn]] void ThrowTypeViolation(versioning::BranchContext &ctx, storage::LabelId label,
                                     storage::TypeConstraintKind kind, storage::PropertyId property) {
  auto &main = ctx.core()->main();
  throw storage::TypeConstraintViolationException("IS TYPED {} violation on {}({})",
                                                  storage::TypeConstraintKindToString(kind),
                                                  main.LabelToName(label),
                                                  main.PropertyToName(property));
}

// `written(pid)` is the value about to be written for `pid`, or nullptr. Null never violates a type constraint.
// `init`: InitProperties writes nothing on a vertex that has properties, so such a vertex is not checked.
template <typename Written>
std::optional<storage::Error> CheckTypesOnPropertyWrite(const VertexAccessor &vertex, Written &&written,
                                                        bool init = false) {
  auto &ctx = *vertex.branch_ctx_;
  auto const types = versioning::MainConstraints{ctx}.Types();
  auto const checked = [&](storage::PropertyId pid) -> const storage::PropertyValue * {
    auto const *value = written(pid);
    return value == nullptr || value->IsNull() ? nullptr : value;
  };
  if (std::ranges::none_of(types, [&](auto const &c) { return checked(std::get<1>(c)) != nullptr; })) {
    return std::nullopt;
  }
  if (init) {
    auto const props = vertex.Properties(storage::View::NEW);
    if (!props) return props.error();
    if (!props->empty()) return std::nullopt;
  }
  auto const labels = vertex.Labels(storage::View::NEW);
  if (!labels) return labels.error();
  for (auto const &[label, property, kind] : types) {
    auto const *value = checked(property);
    if (value != nullptr && std::ranges::contains(*labels, label) &&
        !storage::PropertyValueMatchesTypeConstraint(*value, kind)) {
      ThrowTypeViolation(ctx, label, kind, property);
    }
  }
  return std::nullopt;
}

std::optional<storage::Error> CheckTypesOnPropertiesWrite(const VertexAccessor &vertex, const PropertyMap &properties,
                                                          bool init = false) {
  return CheckTypesOnPropertyWrite(
      vertex,
      [&](storage::PropertyId pid) -> const storage::PropertyValue * {
        auto const it = properties.find(pid);
        return it == properties.end() ? nullptr : &it->second;
      },
      init);
}

// Checks the vertex's current properties against `label`'s type constraints.
std::optional<storage::Error> CheckTypesOnAddLabel(const VertexAccessor &vertex, storage::LabelId label) {
  auto &ctx = *vertex.branch_ctx_;
  auto const types = versioning::MainConstraints{ctx}.Types();
  if (std::ranges::none_of(types, [&](auto const &c) { return std::get<0>(c) == label; })) return std::nullopt;
  auto const has_label = vertex.HasLabel(storage::View::NEW, label);
  if (!has_label) return has_label.error();
  if (*has_label) return std::nullopt;
  auto const props = vertex.Properties(storage::View::NEW);
  if (!props) return props.error();
  for (auto const &[constrained, property, kind] : types) {
    auto const it = props->find(property);
    if (constrained == label && it != props->end() && !it->second.IsNull() &&
        !storage::PropertyValueMatchesTypeConstraint(it->second, kind)) {
      ThrowTypeViolation(ctx, label, kind, property);
    }
  }
  return std::nullopt;
}

#endif
}  // namespace

#ifdef MG_ENTERPRISE
// A delete records both the label and the property kind, so a deleted vertex always reaches the overlay and fails.
storage::Result<storage::VertexKey> VertexAccessor::BranchLabels(storage::View view) const {
  auto &ctx = *branch_ctx_;
  if (!MayHaveOverlay(ctx, impl_) || !ctx.MayHaveLabelChange(impl_.Gid())) return impl_.Labels(view);
  auto const *version = ctx.ReadOverlayVersion(impl_.Gid(), view);
  if (version == nullptr) return impl_.Labels(view);
  if (version->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  auto fork_labels = impl_.Labels(storage::View::OLD);
  if (!fork_labels) return fork_labels;
  return versioning::MergeLabels(*version, std::move(*fork_labels));
}

// Merges only for a deleted fork vertex (must report DELETED_OBJECT) or one the branch wrote `label` on (exact).
storage::Result<bool> VertexAccessor::BranchHasLabel(storage::View view, storage::LabelId label) const {
  auto &ctx = *branch_ctx_;
  auto const gid = impl_.Gid();
  if (!MayHaveOverlay(ctx, impl_) ||
      !((ctx.MayHaveAnyVertexDeleted() && ctx.MayHaveVertexDeleted(gid)) || ctx.LabelMayHaveChanged(label, gid))) {
    return impl_.HasLabel(label, view);
  }
  auto const *version = ctx.ReadOverlayVersion(gid, view);
  if (version == nullptr) return impl_.HasLabel(label, view);
  if (version->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  if (auto const decided = versioning::OverlayDecidesLabel(*version, label)) return *decided;
  return impl_.HasLabel(label, storage::View::OLD);
}

storage::Result<bool> VertexAccessor::BranchHasAnyLabel(storage::View view) const {
  auto &ctx = *branch_ctx_;
  if (!MayHaveOverlay(ctx, impl_) || !ctx.MayHaveLabelChange(impl_.Gid())) return impl_.HasAnyLabel(view);
  auto labels = BranchLabels(view);
  if (!labels) return std::unexpected{labels.error()};
  return !labels->empty();
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> VertexAccessor::BranchProperties(
    storage::View view) const {
  auto &ctx = *branch_ctx_;
  if (!MayHaveOverlay(ctx, impl_) || !ctx.MayHavePropertyChange(impl_.Gid())) return impl_.Properties(view);
  auto const *version = ctx.ReadOverlayVersion(impl_.Gid(), view);
  if (version == nullptr) return impl_.Properties(view);
  if (version->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  return versioning::MergeProperties(*version, impl_);
}

storage::Result<storage::PropertyValue> VertexAccessor::BranchGetProperty(storage::View view,
                                                                          storage::PropertyId key) const {
  auto &ctx = *branch_ctx_;
  auto const gid = impl_.Gid();
  if (!MayHaveOverlay(ctx, impl_) || !(ctx.MayHavePropertyFieldChange(gid, key) || ctx.MayHaveVertexDeleted(gid))) {
    return impl_.GetProperty(key, view);
  }
  auto const *version = ctx.ReadOverlayVersion(gid, view);
  if (version == nullptr) return impl_.GetProperty(key, view);
  if (version->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  return versioning::MergeGetProperty(*version, impl_, key);
}

storage::Result<uint64_t> VertexAccessor::BranchGetPropertySize(storage::PropertyId key, storage::View view) const {
  auto &ctx = *branch_ctx_;
  auto const gid = impl_.Gid();
  if (!MayHaveOverlay(ctx, impl_) || !(ctx.MayHavePropertyFieldChange(gid, key) || ctx.MayHaveVertexDeleted(gid))) {
    return impl_.GetPropertySize(key, view);
  }
  auto const *version = ctx.ReadOverlayVersion(gid, view);
  if (version == nullptr) return impl_.GetPropertySize(key, view);
  if (version->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  return versioning::MergeGetPropertySize(*version, impl_, key);
}

// A fork vertex's label write is recorded as touched before anything else, so a failed write leaves a superset.
storage::Result<bool> VertexAccessor::BranchAddLabel(storage::LabelId label) {
  auto &ctx = *branch_ctx_;
  if (auto err = CheckTypesOnAddLabel(*this, label)) return std::unexpected{*err};
  if (ctx.IsForkVertexGid(Gid())) ctx.core()->RecordLabelTouched(label, Gid());
  ctx.BumpScanEpoch(versioning::BranchContext::ScanInput::kLabel, label.AsUint());
  if (!PrepareBranchWrite(*this, versioning::BranchChangeKind::kLabel)) return impl_.AddLabel(label);
  return ctx.OverlayAddLabel(Gid(), label);
}

storage::Result<bool> VertexAccessor::BranchRemoveLabel(storage::LabelId label) {
  auto &ctx = *branch_ctx_;
  if (ctx.IsForkVertexGid(Gid())) ctx.core()->RecordLabelTouched(label, Gid());
  if (!PrepareBranchWrite(*this, versioning::BranchChangeKind::kLabel)) return impl_.RemoveLabel(label);
  return ctx.OverlayRemoveLabel(Gid(), label);
}

storage::Result<storage::PropertyValue> VertexAccessor::BranchSetProperty(storage::PropertyId key,
                                                                          const storage::PropertyValue &value) {
  auto &ctx = *branch_ctx_;
  if (auto err =
          CheckTypesOnPropertyWrite(*this, [&](storage::PropertyId pid) { return pid == key ? &value : nullptr; }))
    return std::unexpected{*err};
  bool const fork = PrepareBranchWrite(*this, versioning::BranchChangeKind::kProperty);
  ctx.core()->RecordPropertyFieldChange(Gid(), key);
  ctx.BumpScanEpoch(versioning::BranchContext::ScanInput::kProperty, key.AsUint());
  if (!fork) return impl_.SetProperty(key, value);
  ctx.core()->RecordPropertyTouched(key, Gid());
  return ctx.OverlaySetProperty(Gid(), key, value);
}

storage::Result<bool> VertexAccessor::BranchInitProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  if (auto err = CheckTypesOnPropertiesWrite(*this, properties, /*init=*/true)) return std::unexpected{*err};
  bool const fork = PrepareBranchWrite(*this, versioning::BranchChangeKind::kProperty);
  RecordPropertyWrites(ctx, Gid(), fork, properties);
  if (!fork) return impl_.InitProperties(properties);
  return ctx.OverlayInitProperties(Gid(), properties);
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
VertexAccessor::BranchUpdateProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  if (auto err = CheckTypesOnPropertiesWrite(*this, properties)) return std::unexpected{*err};
  bool const fork = PrepareBranchWrite(*this, versioning::BranchChangeKind::kProperty);
  RecordPropertyWrites(ctx, Gid(), fork, properties);
  if (!fork) return impl_.UpdateProperties(properties);
  return ctx.OverlayUpdateProperties(Gid(), properties);
}

// The cleared keys are known only from the result, so they are recorded after the write (still before the commit).
storage::Result<PropertyMap> VertexAccessor::BranchClearProperties() {
  auto &ctx = *branch_ctx_;
  bool const fork = PrepareBranchWrite(*this, versioning::BranchChangeKind::kProperty);
  auto result = fork ? ctx.OverlayClearProperties(Gid()) : impl_.ClearProperties();
  if (result) RecordPropertyWrites(ctx, Gid(), fork, *result);
  return result;
}

// The union of the fork and diff-engine edges (ResolveEdges), not a cheaper count.
storage::Result<size_t> VertexAccessor::BranchDegree(storage::EdgeDirection direction, storage::View view) const {
  bool vertex_deleted = false;
  auto const count = branch_ctx_->ResolveEdges(Gid(), direction, view, &vertex_deleted).size();
  if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
  return count;
}

std::vector<storage::PropertyId> VertexAccessor::BranchVectorIndexedProperties(
    std::span<storage::LabelId const> labels) const {
  if (impl_.storage_ != &branch_ctx_->diff_engine()) return impl_.VectorIndexedProperties(labels);
  return branch_ctx_->historical().GetTransaction()->active_indices_->vector_->IndexedProperties(labels);
}

// A delete flags the deleted vertex, so it takes ResolveEdges; its neighbours are not flagged, so the fast path filters
// their edges through IsForkVertexDeleted.
bool VertexAccessor::BranchEdgesUnchanged() const {
  return impl_.storage_ != &branch_ctx_->diff_engine() &&
         (!impl_.vertex_->branched() || !branch_ctx_->MayHaveEdgeChange(impl_.Gid()));
}

// ResolveEdges is unfiltered, so hops are charged before the edge_types/dest filters. Never pass `dest` to
// impl_.{In,Out}Edges: a stub dest.impl_ belongs to another transaction (storage DMG_ASSERT).
storage::Result<ExpandedEdgesResult> VertexAccessor::BranchEdges(storage::EdgeDirection direction, storage::View view,
                                                                 const std::vector<storage::EdgeTypeId> &edge_types,
                                                                 const VertexAccessor *dest,
                                                                 storage::HopsLimit *hops_limit) const {
  auto const dest_gid = dest != nullptr ? dest->Gid() : storage::kInvalidGid;
  auto const far_end = [direction](auto const &edge) {
    return direction == storage::EdgeDirection::IN ? edge.FromVertex() : edge.ToVertex();
  };

  std::vector<EdgeAccessor> edges;
  if (BranchEdgesUnchanged()) {
    auto maybe_result = direction == storage::EdgeDirection::IN
                            ? impl_.InEdges(storage::View::OLD, edge_types, nullptr, hops_limit)
                            : impl_.OutEdges(storage::View::OLD, edge_types, nullptr, hops_limit);
    if (!maybe_result) return std::unexpected{maybe_result.error()};

    edges.reserve(maybe_result->edges.size());
    for (auto const &edge : maybe_result->edges) {
      auto const other = far_end(edge);
      if (dest != nullptr && other.Gid() != dest_gid) continue;
      if (branch_ctx_->IsForkVertexDeleted(other, view)) continue;
      edges.emplace_back(edge, branch_ctx_);
    }
    return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(edges)},
                               .expanded_count = maybe_result->expanded_count};
  }

  bool vertex_deleted = false;
  auto resolved = branch_ctx_->ResolveEdges(Gid(), direction, view, &vertex_deleted);
  if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
  int64_t expanded_count = 0;
  for (auto const &edge : resolved) {
    if (hops_limit != nullptr && hops_limit->IsUsed()) {
      if (hops_limit->IncrementHopsCount() == 0) break;
    }
    ++expanded_count;
    if (dest != nullptr && far_end(edge).Gid() != dest_gid) continue;
    if (!edge_types.empty() && !std::ranges::contains(edge_types, edge.EdgeType())) continue;
    edges.emplace_back(edge, branch_ctx_);
  }
  return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(edges)}, .expanded_count = expanded_count};
}

#endif
storage::Result<ExpandedEdgesResult> VertexAccessor::InEdges(storage::View view) const { return InEdges(view, {}); }

storage::Result<ExpandedEdgesResult> VertexAccessor::InEdges(storage::View view,
                                                             const std::vector<storage::EdgeTypeId> &edge_types,
                                                             storage::HopsLimit *hops_limit) const {
#ifdef MG_ENTERPRISE
  if (branch_ctx_ != nullptr) [[unlikely]]
    return BranchEdges(storage::EdgeDirection::IN, view, edge_types, nullptr, hops_limit);
#endif
  return impl_.InEdges(view, edge_types, nullptr, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::InEdges(storage::View view,
                                                             const std::vector<storage::EdgeTypeId> &edge_types,
                                                             const VertexAccessor &dest,
                                                             storage::HopsLimit *hops_limit) const {
#ifdef MG_ENTERPRISE
  if (branch_ctx_ != nullptr) [[unlikely]]
    return BranchEdges(storage::EdgeDirection::IN, view, edge_types, &dest, hops_limit);
#endif
  return impl_.InEdges(view, edge_types, &dest.impl_, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::OutEdges(storage::View view,
                                                              const std::vector<storage::EdgeTypeId> &edge_types,
                                                              storage::HopsLimit *hops_limit) const {
#ifdef MG_ENTERPRISE
  if (branch_ctx_ != nullptr) [[unlikely]]
    return BranchEdges(storage::EdgeDirection::OUT, view, edge_types, nullptr, hops_limit);
#endif
  return impl_.OutEdges(view, edge_types, nullptr, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::OutEdges(storage::View view,
                                                              const std::vector<storage::EdgeTypeId> &edge_types,
                                                              const VertexAccessor &dest,
                                                              storage::HopsLimit *hops_limit) const {
#ifdef MG_ENTERPRISE
  if (branch_ctx_ != nullptr) [[unlikely]]
    return BranchEdges(storage::EdgeDirection::OUT, view, edge_types, &dest, hops_limit);
#endif
  return impl_.OutEdges(view, edge_types, &dest.impl_, hops_limit).transform(AsQueryResult);
}

storage::Result<ExpandedEdgesResult> VertexAccessor::OutEdges(storage::View view) const { return OutEdges(view, {}); }

}  // namespace memgraph::query
