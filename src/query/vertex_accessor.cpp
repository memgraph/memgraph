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

#include <algorithm>
#include <map>
#include <optional>
#include <tuple>
#include <vector>

#include "query/edge_accessor.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/constraints/active_constraints.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/exceptions.hpp"
#include "storage/v2/transaction_constants.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/main_constraints.hpp"

namespace memgraph::query {

namespace {

using PropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;

// A branch-touched fork vertex reads as its overlay version merged over the fork vertex `impl` (main@fork_ts); a
// delete reads as DELETED_OBJECT. Branch-native vertices are complete and never merged.
storage::Result<storage::VertexKey> MergedLabels(versioning::BranchContext &ctx, const storage::VertexAccessor &impl,
                                                 storage::View view) {
  auto const *version = ctx.ReadOverlayVersion(impl.Gid(), view);
  if (version == nullptr) return impl.Labels(view);
  if (version->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  auto fork_labels = impl.Labels(storage::View::OLD);
  if (!fork_labels) return fork_labels;
  return versioning::MergeLabels(*version, std::move(*fork_labels));
}

storage::Result<bool> MergedHasLabel(versioning::BranchContext &ctx, const storage::VertexAccessor &impl,
                                     storage::View view, storage::LabelId label) {
  auto const *version = ctx.ReadOverlayVersion(impl.Gid(), view);
  if (version == nullptr) return impl.HasLabel(label, view);
  if (version->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  if (auto const decided = versioning::OverlayDecidesLabel(*version, label)) return *decided;
  return impl.HasLabel(label, storage::View::OLD);
}

storage::Result<PropertyMap> MergedProperties(versioning::BranchContext &ctx, const storage::VertexAccessor &impl,
                                              storage::View view) {
  auto const *version = ctx.ReadOverlayVersion(impl.Gid(), view);
  if (version == nullptr) return impl.Properties(view);
  if (version->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  return versioning::MergeProperties(*version, impl);
}

storage::Result<storage::PropertyValue> MergedGetProperty(versioning::BranchContext &ctx,
                                                          const storage::VertexAccessor &impl, storage::View view,
                                                          storage::PropertyId key) {
  auto const *version = ctx.ReadOverlayVersion(impl.Gid(), view);
  if (version == nullptr) return impl.GetProperty(key, view);
  if (version->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  return versioning::MergeGetProperty(*version, impl, key);
}

storage::Result<uint64_t> MergedGetPropertySize(versioning::BranchContext &ctx, const storage::VertexAccessor &impl,
                                                storage::PropertyId key, storage::View view) {
  auto const *version = ctx.ReadOverlayVersion(impl.Gid(), view);
  if (version == nullptr) return impl.GetPropertySize(key, view);
  if (version->deleted()) return std::unexpected{storage::Error::DELETED_OBJECT};
  return versioning::MergeGetPropertySize(*version, impl, key);
}

bool MayHaveOverlay(versioning::BranchContext &ctx, const storage::VertexAccessor &impl) {
  return impl.storage_ != &ctx.diff_engine() && impl.vertex_->branched();
}

[[noreturn]] void ThrowTypeViolation(versioning::BranchContext &ctx, storage::LabelId label,
                                     storage::TypeConstraintKind kind, storage::PropertyId property) {
  auto const &main = ctx.core()->main();
  // Same text as storage's HandleTypeConstraintViolation (storage/v2/vertex_accessor.cpp).
  throw storage::TypeConstraintViolationException("IS TYPED {} violation on {}({})",
                                                  storage::TypeConstraintKindToString(kind),
                                                  main.LabelToName(label),
                                                  main.PropertyToName(property));
}

// Checks values about to be written (`find` returns the value or nullptr) against main's type constraints on the
// reconstructed labels, BEFORE the overlay is written. `require_no_properties`: skip if properties exist.
template <typename FindWritten>
std::optional<storage::Error> CheckTypesOnPropertyWrite(versioning::BranchContext &ctx, const VertexAccessor &vertex,
                                                        FindWritten &&find, bool require_no_properties = false) {
  auto const types = versioning::MainConstraints{ctx}.Types();
  if (types.empty()) return std::nullopt;
  auto const written = [&](storage::PropertyId pid) -> const storage::PropertyValue * {
    auto const *value = find(pid);
    return (value == nullptr || value->IsNull()) ? nullptr : value;  // type constraints don't enforce existence
  };
  if (!std::ranges::any_of(types, [&](auto const &c) { return written(std::get<1>(c)) != nullptr; })) {
    return std::nullopt;
  }
  if (require_no_properties) {
    auto const props = vertex.Properties(storage::View::NEW);
    if (!props) return props.error();
    if (!props->empty()) return std::nullopt;
  }
  auto const labels = vertex.Labels(storage::View::NEW);
  if (!labels) return labels.error();
  for (auto const &[label, property, kind] : types) {
    auto const *value = written(property);
    if (value == nullptr || !std::ranges::contains(*labels, label)) continue;
    if (!storage::PropertyValueMatchesTypeConstraint(*value, kind)) ThrowTypeViolation(ctx, label, kind, property);
  }
  return std::nullopt;
}

// Validates the vertex's existing properties against `label`'s type constraints BEFORE the overlay is written.
std::optional<storage::Error> CheckTypesOnAddLabel(versioning::BranchContext &ctx, const VertexAccessor &vertex,
                                                   storage::LabelId label) {
  auto const types = versioning::MainConstraints{ctx}.Types();
  if (!std::ranges::any_of(types, [&](auto const &c) { return std::get<0>(c) == label; })) return std::nullopt;
  auto const labels = vertex.Labels(storage::View::NEW);
  if (!labels) return labels.error();
  if (std::ranges::contains(*labels, label)) return std::nullopt;
  auto const props = vertex.Properties(storage::View::NEW);
  if (!props) return props.error();
  for (auto const &[constrained_label, property, kind] : types) {
    if (constrained_label != label) continue;
    auto const it = props->find(property);
    if (it == props->end() || it->second.IsNull()) continue;
    if (!storage::PropertyValueMatchesTypeConstraint(it->second, kind)) ThrowTypeViolation(ctx, label, kind, property);
  }
  return std::nullopt;
}

}  // namespace

bool VertexAccessor::IsDeleted() const {
  if (branch_ctx_ != nullptr && branch_ctx_->IsForkVertexGid(impl_.Gid())) {
    return branch_ctx_->current_diff_txn() != nullptr && branch_ctx_->IsVertexDeleted(impl_.Gid(), storage::View::NEW);
  }
  return impl_.vertex_->deleted();
}

// Un-branched main vertices are fork-identical, so a clear filter bit skips the merge. A delete records both the
// label and property kinds, so deleted vertices always reach the overlay and fail the read.
auto VertexAccessor::Labels(storage::View view) const -> decltype(impl_.Labels(view)) {
  if (branch_ctx_ != nullptr && MayHaveOverlay(*branch_ctx_, impl_) && branch_ctx_->MayHaveLabelChange(impl_.Gid())) {
    return MergedLabels(*branch_ctx_, impl_, view);
  }
  return impl_.Labels(view);
}

storage::Result<bool> VertexAccessor::HasLabel(storage::View view, storage::LabelId label) const {
  // Merge only for a deleted fork vertex (must report DELETED_OBJECT) or one the branch wrote `label` on (exact).
  if (branch_ctx_ != nullptr && MayHaveOverlay(*branch_ctx_, impl_) &&
      ((branch_ctx_->MayHaveAnyVertexDeleted() && branch_ctx_->MayHaveVertexDeleted(impl_.Gid())) ||
       branch_ctx_->LabelMayHaveChanged(label, impl_.Gid()))) {
    return MergedHasLabel(*branch_ctx_, impl_, view, label);
  }
  return impl_.HasLabel(label, view);
}

auto VertexAccessor::Properties(storage::View view) const -> decltype(impl_.Properties(view)) {
  if (branch_ctx_ != nullptr && MayHaveOverlay(*branch_ctx_, impl_) &&
      branch_ctx_->MayHavePropertyChange(impl_.Gid())) {
    return MergedProperties(*branch_ctx_, impl_, view);
  }
  return impl_.Properties(view);
}

storage::Result<storage::PropertyValue> VertexAccessor::GetProperty(storage::View view, storage::PropertyId key) const {
  // A deleted vertex must fail the read, so a recorded delete also forces the merge.
  if (branch_ctx_ != nullptr && MayHaveOverlay(*branch_ctx_, impl_) &&
      (branch_ctx_->MayHavePropertyFieldChange(impl_.Gid(), key) || branch_ctx_->MayHaveVertexDeleted(impl_.Gid()))) {
    return MergedGetProperty(*branch_ctx_, impl_, view, key);
  }
  return impl_.GetProperty(key, view);
}

storage::Result<uint64_t> VertexAccessor::GetPropertySize(storage::PropertyId key, storage::View view) const {
  if (branch_ctx_ != nullptr && MayHaveOverlay(*branch_ctx_, impl_) &&
      (branch_ctx_->MayHavePropertyFieldChange(impl_.Gid(), key) || branch_ctx_->MayHaveVertexDeleted(impl_.Gid()))) {
    return MergedGetPropertySize(*branch_ctx_, impl_, key, view);
  }
  return impl_.GetPropertySize(key, view);
}

// Pre-condition: branch_ctx_ != nullptr.
void VertexAccessor::CowIfNeeded(versioning::BranchChangeKind kind) {
  // A fork vertex keeps its fork accessor: the branch's changes are addressed by gid through the overlay API, so it
  // needs no diff-engine copy.
  if (branch_ctx_->IsForkVertexGid(impl_.Gid())) {
    if (auto prepared = branch_ctx_->PrepareForkWrite(impl_.Gid()); !prepared) {
      throw QueryRuntimeException(prepared.error().message);
    }
  } else {
    auto cowed = branch_ctx_->CowVertex(impl_.Gid());
    if (!cowed) throw QueryRuntimeException(cowed.error().message);
    impl_ = *cowed;
  }
  branch_ctx_->RecordVertexChange(impl_.Gid(), kind);
}

storage::Result<bool> VertexAccessor::BranchAddLabel(storage::LabelId label) {
  auto &ctx = *branch_ctx_;
  if (auto err = CheckTypesOnAddLabel(ctx, *this, label)) return std::unexpected{*err};
  const bool fork = ctx.IsForkVertexGid(impl_.Gid());
  if (fork) ctx.RecordLabelTouched(label, impl_.Gid());
  CowIfNeeded(versioning::BranchChangeKind::kLabel);
  if (!fork) return impl_.AddLabel(label);
  return ctx.OverlayAddLabel(impl_.Gid(), label);
}

storage::Result<bool> VertexAccessor::BranchRemoveLabel(storage::LabelId label) {
  auto &ctx = *branch_ctx_;
  const bool fork = ctx.IsForkVertexGid(impl_.Gid());
  if (fork) ctx.RecordLabelTouched(label, impl_.Gid());
  CowIfNeeded(versioning::BranchChangeKind::kLabel);
  if (!fork) return impl_.RemoveLabel(label);
  return ctx.OverlayRemoveLabel(impl_.Gid(), label);
}

storage::Result<storage::PropertyValue> VertexAccessor::BranchSetProperty(storage::PropertyId key,
                                                                          const storage::PropertyValue &value) {
  auto &ctx = *branch_ctx_;
  if (auto err = CheckTypesOnPropertyWrite(
          ctx, *this, [&](storage::PropertyId pid) { return pid == key ? &value : nullptr; })) {
    return std::unexpected{*err};
  }
  CowIfNeeded(versioning::BranchChangeKind::kProperty);
  RecordPropertyFieldChange(key);
  if (!ctx.IsForkVertexGid(impl_.Gid())) return impl_.SetProperty(key, value);
  return ctx.OverlaySetProperty(impl_.Gid(), key, value);
}

storage::Result<bool> VertexAccessor::BranchInitProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  if (auto err = CheckTypesOnPropertyWrite(
          ctx,
          *this,
          [&](storage::PropertyId pid) {
            auto const it = properties.find(pid);
            return it == properties.end() ? nullptr : &it->second;
          },
          /*require_no_properties=*/true)) {
    return std::unexpected{*err};
  }
  CowIfNeeded(versioning::BranchChangeKind::kProperty);
  RecordPropertyFieldChanges(properties);
  if (!ctx.IsForkVertexGid(impl_.Gid())) return impl_.InitProperties(properties);
  return ctx.OverlayInitProperties(impl_.Gid(), properties);
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
VertexAccessor::BranchUpdateProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  if (auto err = CheckTypesOnPropertyWrite(ctx, *this, [&](storage::PropertyId pid) {
        auto const it = properties.find(pid);
        return it == properties.end() ? nullptr : &it->second;
      }))
    return std::unexpected{*err};
  CowIfNeeded(versioning::BranchChangeKind::kProperty);
  RecordPropertyFieldChanges(properties);
  if (!ctx.IsForkVertexGid(impl_.Gid())) return impl_.UpdateProperties(properties);
  return ctx.OverlayUpdateProperties(impl_.Gid(), properties);
}

storage::Result<PropertyMap> VertexAccessor::BranchClearProperties() {
  auto &ctx = *branch_ctx_;
  CowIfNeeded(versioning::BranchChangeKind::kProperty);
  auto result = ctx.IsForkVertexGid(impl_.Gid()) ? ctx.OverlayClearProperties(impl_.Gid()) : impl_.ClearProperties();
  if (result.has_value()) RecordPropertyFieldChanges(*result);
  return result;
}

// Call after CowIfNeeded.
void VertexAccessor::RecordPropertyFieldChange(storage::PropertyId pid) {
  branch_ctx_->RecordPropertyFieldChange(impl_.Gid(), pid);
  if (branch_ctx_->IsForkVertexGid(impl_.Gid())) branch_ctx_->RecordPropertyTouched(pid, impl_.Gid());
}

void VertexAccessor::RecordPropertyFieldChanges(
    const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  const auto gid = impl_.Gid();
  for (const auto &[pid, _] : properties) {
    branch_ctx_->RecordPropertyFieldChange(gid, pid);
    if (branch_ctx_->IsForkVertexGid(gid)) branch_ctx_->RecordPropertyTouched(pid, gid);
  }
}

// ResolveEdges is unfiltered, so hops are charged before the edge_types filter.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Deleting an edge or vertex flags the edge bit (DeleteForkEdge/OverlayMarkDeleted), sending it to ResolveEdges.
    // A neighbour of a deleted vertex is not flagged; the fast path filters its edges via IsForkVertexDeleted.
    if (impl_.storage_ != &branch_ctx_->diff_engine() &&
        (!impl_.vertex_->branched() || !branch_ctx_->MayHaveEdgeChange(impl_.Gid()))) {
      auto maybe_result = impl_.InEdges(storage::View::OLD, edge_types, nullptr, hops_limit);
      if (!maybe_result) return std::unexpected{maybe_result.error()};

      std::vector<EdgeAccessor> edges;
      edges.reserve((*maybe_result).edges.size());
      for (auto const &edge : (*maybe_result).edges) {
        if (branch_ctx_->IsForkVertexDeleted(edge.FromVertex(), view)) continue;
        edges.emplace_back(edge, branch_ctx_);
      }

      return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = (*maybe_result).expanded_count};
    }

    bool vertex_deleted = false;
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::IN, view, &vertex_deleted);
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;
      }
      ++expanded_count;
      if (!edge_types.empty() && !std::ranges::contains(edge_types, edge.EdgeType())) continue;
      edges.emplace_back(edge, branch_ctx_);
    }
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = expanded_count};
  }

  auto maybe_result = impl_.InEdges(view, edge_types, nullptr, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

// Destination filter is client-side by gid; hops are charged before the dest/type filters.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  const VertexAccessor &dest,
                                                                  storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Never pass `dest` to impl_.InEdges: a COW'd dest.impl_ is in another transaction (storage DMG_ASSERT).
    if (impl_.storage_ != &branch_ctx_->diff_engine() &&
        (!impl_.vertex_->branched() || !branch_ctx_->MayHaveEdgeChange(impl_.Gid()))) {
      auto maybe_result = impl_.InEdges(storage::View::OLD, edge_types, nullptr, hops_limit);
      if (!maybe_result) return std::unexpected{maybe_result.error()};

      auto const dest_gid = dest.Gid();
      std::vector<EdgeAccessor> edges;
      edges.reserve((*maybe_result).edges.size());
      for (auto const &edge : (*maybe_result).edges) {
        if (edge.FromVertex().Gid() != dest_gid) continue;
        if (branch_ctx_->IsForkVertexDeleted(edge.FromVertex(), view)) continue;
        edges.emplace_back(edge, branch_ctx_);
      }
      return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = (*maybe_result).expanded_count};
    }

    bool vertex_deleted = false;
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::IN, view, &vertex_deleted);
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    auto const dest_gid = dest.Gid();
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;
      }
      ++expanded_count;
      if (edge.FromVertex().Gid() != dest_gid) continue;
      if (!edge_types.empty() && !std::ranges::contains(edge_types, edge.EdgeType())) continue;
      edges.emplace_back(edge, branch_ctx_);
    }
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = expanded_count};
  }

  auto maybe_result = impl_.InEdges(view, edge_types, &dest.impl_, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view) const {
  return InEdges(view, {});
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   const std::vector<storage::EdgeTypeId> &edge_types,
                                                                   storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    if (impl_.storage_ != &branch_ctx_->diff_engine() &&
        (!impl_.vertex_->branched() || !branch_ctx_->MayHaveEdgeChange(impl_.Gid()))) {
      auto maybe_result = impl_.OutEdges(storage::View::OLD, edge_types, nullptr, hops_limit);
      if (!maybe_result) return std::unexpected{maybe_result.error()};

      std::vector<EdgeAccessor> edges;
      edges.reserve((*maybe_result).edges.size());
      for (auto const &edge : (*maybe_result).edges) {
        if (branch_ctx_->IsForkVertexDeleted(edge.ToVertex(), view)) continue;
        edges.emplace_back(edge, branch_ctx_);
      }

      return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = (*maybe_result).expanded_count};
    }

    bool vertex_deleted = false;
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::OUT, view, &vertex_deleted);
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;
      }
      ++expanded_count;
      if (!edge_types.empty() && !std::ranges::contains(edge_types, edge.EdgeType())) continue;
      edges.emplace_back(edge, branch_ctx_);
    }
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = expanded_count};
  }

  auto maybe_result = impl_.OutEdges(view, edge_types, nullptr, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   std::vector<storage::EdgeTypeId> const &edge_types,
                                                                   VertexAccessor const &dest,
                                                                   storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    if (impl_.storage_ != &branch_ctx_->diff_engine() &&
        (!impl_.vertex_->branched() || !branch_ctx_->MayHaveEdgeChange(impl_.Gid()))) {
      auto maybe_result = impl_.OutEdges(storage::View::OLD, edge_types, nullptr, hops_limit);
      if (!maybe_result) return std::unexpected{maybe_result.error()};

      auto const dest_gid = dest.Gid();
      std::vector<EdgeAccessor> edges;
      edges.reserve((*maybe_result).edges.size());
      for (auto const &edge : (*maybe_result).edges) {
        if (edge.ToVertex().Gid() != dest_gid) continue;
        if (branch_ctx_->IsForkVertexDeleted(edge.ToVertex(), view)) continue;
        edges.emplace_back(edge, branch_ctx_);
      }
      return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = (*maybe_result).expanded_count};
    }

    bool vertex_deleted = false;
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::OUT, view, &vertex_deleted);
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    auto const dest_gid = dest.Gid();
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;
      }
      ++expanded_count;
      if (edge.ToVertex().Gid() != dest_gid) continue;
      if (!edge_types.empty() && !std::ranges::contains(edge_types, edge.EdgeType())) continue;
      edges.emplace_back(edge, branch_ctx_);
    }
    return EdgeVertexAccessorResult{.edges = std::move(edges), .expanded_count = expanded_count};
  }

  auto maybe_result = impl_.OutEdges(view, edge_types, &dest.impl_, hops_limit);
  if (!maybe_result) return std::unexpected{maybe_result.error()};

  std::vector<EdgeAccessor> edges;
  edges.reserve((*maybe_result).edges.size());
  std::ranges::transform(
      (*maybe_result).edges, std::back_inserter(edges), [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = edges, .expanded_count = (*maybe_result).expanded_count};
}

storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view) const {
  return OutEdges(view, {});
}

storage::Result<size_t> VertexAccessor::InDegree(storage::View view) const {
  if (branch_ctx_ != nullptr) {
    bool vertex_deleted = false;
    auto const count = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::IN, view, &vertex_deleted).size();
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    return count;
  }
  return impl_.InDegree(view);
}

storage::Result<size_t> VertexAccessor::OutDegree(storage::View view) const {
  if (branch_ctx_ != nullptr) {
    bool vertex_deleted = false;
    auto const count = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::OUT, view, &vertex_deleted).size();
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    return count;
  }
  return impl_.OutDegree(view);
}

}  // namespace memgraph::query
