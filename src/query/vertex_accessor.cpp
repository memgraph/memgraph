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

#include "query/branch_reserved.hpp"
#include "query/edge_accessor.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/constraints/active_constraints.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/exceptions.hpp"
#include "storage/v2/transaction_constants.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/main_constraints.hpp"
#include "versioning/stub_properties.hpp"

namespace memgraph::query {

namespace {

using PropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;

// A branch-touched fork vertex is a sparse diff-engine stub; reads overlay it on the fork vertex (main@fork_ts).
// Removals and deletes are reserved-name markers on the stub, and a delete marker reads as DELETED_OBJECT.
// Branch-native vertices are complete and never merged.
storage::Result<storage::VertexKey> MergedLabels(versioning::BranchContext &ctx, const storage::VertexAccessor &stub,
                                                 storage::View view) {
  auto stub_labels = stub.Labels(view);
  if (!stub_labels) return stub_labels;
  std::vector<storage::LabelId> added;
  std::vector<storage::LabelId> removed;
  const auto deleted_label = ctx.DeletedLabel();
  for (auto const label : *stub_labels) {
    if (label == deleted_label) return std::unexpected{storage::Error::DELETED_OBJECT};
    const auto info = ctx.ClassifyLabel(label);
    if (info.removed_label) {
      removed.push_back(*info.removed_label);
    } else if (!info.reserved) {
      added.push_back(label);
    }
  }
  auto fork = ctx.FindForkVertex(stub.Gid());
  DMG_ASSERT(fork.has_value(), "Fork vertex missing for stub {}", stub.Gid().AsUint());
  std::vector<storage::LabelId> merged;
  if (fork) {
    auto fork_labels = fork->Labels(storage::View::OLD);
    if (!fork_labels) return fork_labels;
    for (auto const label : *fork_labels) {
      if (!std::ranges::contains(removed, label)) merged.push_back(label);
    }
    stub_labels = std::move(fork_labels);
  }
  for (auto const label : added) {
    if (!std::ranges::contains(merged, label)) merged.push_back(label);
  }
  stub_labels->clear();
  for (auto const label : merged) stub_labels->push_back(label);
  return stub_labels;
}

storage::Result<bool> MergedHasLabel(versioning::BranchContext &ctx, const storage::VertexAccessor &stub,
                                     storage::View view, storage::LabelId label) {
  if (ctx.IsStubDeleted(stub, view)) return std::unexpected{storage::Error::DELETED_OBJECT};
  auto fork = ctx.FindForkVertex(stub.Gid());
  DMG_ASSERT(fork.has_value(), "Fork vertex missing for stub {}", stub.Gid().AsUint());
  if (!fork) return stub.HasLabel(label, view);
  auto in_fork = fork->HasLabel(label, storage::View::OLD);
  if (!in_fork) return in_fork;
  if (*in_fork) {
    auto removed = stub.HasLabel(ctx.RemovalMarker(label), view);
    if (!removed) return removed;
    return !*removed;
  }
  return stub.HasLabel(label, view);
}

storage::Result<PropertyMap> MergedProperties(versioning::BranchContext &ctx, const storage::VertexAccessor &stub,
                                              storage::View view) {
  auto stub_props = stub.Properties(view);  // single call: no torn read between value and removal list
  if (!stub_props) return stub_props;
  if (ctx.IsStubDeleted(stub, view)) return std::unexpected{storage::Error::DELETED_OBJECT};
  auto fork = ctx.FindForkVertex(stub.Gid());
  DMG_ASSERT(fork.has_value(), "Fork vertex missing for stub {}", stub.Gid().AsUint());
  if (!fork) {
    versioning::TakeRemoved(ctx, *stub_props);
    return stub_props;
  }
  return versioning::OverlayProperties(ctx, *stub_props, *fork);
}

storage::Result<storage::PropertyValue> MergedGetProperty(versioning::BranchContext &ctx,
                                                          const storage::VertexAccessor &stub, storage::View view,
                                                          storage::PropertyId key) {
  if (key == ctx.RemovedPropsKey() || key == ctx.DeletedEdgesKey()) return storage::PropertyValue{};
  if (ctx.IsStubDeleted(stub, view)) return std::unexpected{storage::Error::DELETED_OBJECT};
  auto fork = ctx.FindForkVertex(stub.Gid());
  DMG_ASSERT(fork.has_value(), "Fork vertex missing for stub {}", stub.Gid().AsUint());
  if (!fork) return stub.GetProperty(key, view);
  auto in_fork = fork->GetProperty(key, storage::View::OLD);
  if (!in_fork) return in_fork;
  if (in_fork->IsNull()) return stub.GetProperty(key, view);
  return versioning::OverlayOnForkValue(ctx, stub, view, key, std::move(*in_fork));
}

storage::Result<uint64_t> MergedGetPropertySize(versioning::BranchContext &ctx, const storage::VertexAccessor &stub,
                                                storage::PropertyId key, storage::View view) {
  if (key == ctx.RemovedPropsKey() || key == ctx.DeletedEdgesKey()) return uint64_t{0};
  if (ctx.IsStubDeleted(stub, view)) return std::unexpected{storage::Error::DELETED_OBJECT};
  auto fork = ctx.FindForkVertex(stub.Gid());
  DMG_ASSERT(fork.has_value(), "Fork vertex missing for stub {}", stub.Gid().AsUint());
  if (!fork) return stub.GetPropertySize(key, view);
  auto in_fork = fork->GetPropertySize(key, storage::View::OLD);
  if (!in_fork) return in_fork;
  if (*in_fork == 0) return stub.GetPropertySize(key, view);
  return versioning::OverlaySizeOnFork(ctx, stub, view, key, *in_fork);
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
// reconstructed labels, BEFORE the stub is mutated. `require_no_properties`: skip if properties exist (InitProperties
// no-op).
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

// Validates the vertex's existing properties against `label`'s type constraints BEFORE the stub is mutated.
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

// Un-branched main vertices are fork-identical, so a clear filter bit skips the merge. A delete records both the
// label and property kinds, so deleted vertices always reach the stub and fail the read.
auto VertexAccessor::Labels(storage::View view) const -> decltype(impl_.Labels(view)) {
  if (branch_ctx_ != nullptr) {
    if (branch_ctx_->IsStub(impl_)) return MergedLabels(*branch_ctx_, impl_, view);
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->MayHaveLabelChange(gid)) {
        if (auto stub = branch_ctx_->FindStubOrDiffVertex(gid, view)) {
          if (branch_ctx_->IsStub(*stub)) return MergedLabels(*branch_ctx_, *stub, view);
          return stub->Labels(view);
        }
      }
    }
  }
  return impl_.Labels(view);
}

storage::Result<bool> VertexAccessor::HasLabel(storage::View view, storage::LabelId label) const {
  if (branch_ctx_ != nullptr) {
    if (branch_ctx_->IsStub(impl_)) return MergedHasLabel(*branch_ctx_, impl_, view, label);
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->MayHaveLabelChange(gid)) {
        if (auto stub = branch_ctx_->FindStubOrDiffVertex(gid, view)) {
          if (branch_ctx_->IsStub(*stub)) return MergedHasLabel(*branch_ctx_, *stub, view, label);
          return stub->HasLabel(label, view);
        }
      }
    }
  }
  return impl_.HasLabel(label, view);
}

auto VertexAccessor::Properties(storage::View view) const -> decltype(impl_.Properties(view)) {
  if (branch_ctx_ != nullptr) {
    if (branch_ctx_->IsStub(impl_)) return MergedProperties(*branch_ctx_, impl_, view);
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->MayHavePropertyChange(gid)) {
        if (auto stub = branch_ctx_->FindStubOrDiffVertex(gid, view)) {
          if (branch_ctx_->IsStub(*stub)) return MergedProperties(*branch_ctx_, *stub, view);
          return stub->Properties(view);
        }
      }
    }
  }
  return impl_.Properties(view);
}

storage::Result<storage::PropertyValue> VertexAccessor::GetProperty(storage::View view, storage::PropertyId key) const {
  if (branch_ctx_ != nullptr) {
    if (branch_ctx_->IsStub(impl_)) return MergedGetProperty(*branch_ctx_, impl_, view, key);
    // Label changes also force the merge: a delete is a label marker and reading a deleted vertex must fail.
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->MayHavePropertyFieldChange(gid, key) || branch_ctx_->MayHaveLabelChange(gid)) {
        if (auto stub = branch_ctx_->FindStubOrDiffVertex(gid, view)) {
          if (branch_ctx_->IsStub(*stub)) return MergedGetProperty(*branch_ctx_, *stub, view, key);
          return stub->GetProperty(key, view);
        }
      }
    }
  }
  return impl_.GetProperty(key, view);
}

storage::Result<uint64_t> VertexAccessor::GetPropertySize(storage::PropertyId key, storage::View view) const {
  if (branch_ctx_ != nullptr) {
    if (branch_ctx_->IsStub(impl_)) return MergedGetPropertySize(*branch_ctx_, impl_, key, view);
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->MayHavePropertyFieldChange(gid, key) || branch_ctx_->MayHaveLabelChange(gid)) {
        if (auto stub = branch_ctx_->FindStubOrDiffVertex(gid, view)) {
          if (branch_ctx_->IsStub(*stub)) return MergedGetPropertySize(*branch_ctx_, *stub, key, view);
          return stub->GetPropertySize(key, view);
        }
      }
    }
  }
  return impl_.GetPropertySize(key, view);
}

// Pre-condition: branch_ctx_ != nullptr.
void VertexAccessor::CowIfNeeded(versioning::BranchChangeKind kind) {
  auto cowed = branch_ctx_->CowVertex(impl_.Gid());
  if (!cowed) {
    throw QueryRuntimeException(cowed.error().message);
  }
  impl_ = *cowed;
  branch_ctx_->RecordVertexChange(impl_.Gid(), kind);
}

storage::Result<bool> VertexAccessor::BranchAddLabel(storage::LabelId label) {
  auto &ctx = *branch_ctx_;
  if (ctx.ClassifyLabel(label).reserved) ThrowReserved("label");
  if (auto err = CheckTypesOnAddLabel(ctx, *this, label)) return std::unexpected{*err};
  CowIfNeeded(versioning::BranchChangeKind::kLabel);
  if (!ctx.IsStub(impl_)) return impl_.AddLabel(label);

  auto fork = ctx.FindForkVertex(impl_.Gid());
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  const auto marker = ctx.RemovalMarker(label);
  auto in_fork = fork->HasLabel(label, storage::View::OLD);
  if (!in_fork) return in_fork;
  auto in_stub = impl_.HasLabel(label, storage::View::NEW);
  if (!in_stub) return in_stub;
  auto marked = impl_.HasLabel(marker, storage::View::NEW);
  if (!marked) return marked;

  if (*in_stub || (*in_fork && !*marked)) return false;
  if (*marked) {
    if (auto r = impl_.RemoveLabel(marker); !r) return std::unexpected{r.error()};
  }
  // Re-adding a label the fork vertex has is just un-hiding it; no redundant diff label-index entry.
  if (!*in_fork) {
    if (auto r = impl_.AddLabel(label); !r) return std::unexpected{r.error()};
  }
  return true;
}

storage::Result<bool> VertexAccessor::BranchRemoveLabel(storage::LabelId label) {
  auto &ctx = *branch_ctx_;
  if (ctx.ClassifyLabel(label).reserved) ThrowReserved("label");
  CowIfNeeded(versioning::BranchChangeKind::kLabel);
  if (!ctx.IsStub(impl_)) return impl_.RemoveLabel(label);

  auto fork = ctx.FindForkVertex(impl_.Gid());
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  const auto marker = ctx.RemovalMarker(label);
  auto in_fork = fork->HasLabel(label, storage::View::OLD);
  if (!in_fork) return in_fork;
  auto in_stub = impl_.HasLabel(label, storage::View::NEW);
  if (!in_stub) return in_stub;
  auto marked = impl_.HasLabel(marker, storage::View::NEW);
  if (!marked) return marked;

  if (!*in_stub && !(*in_fork && !*marked)) return false;
  if (*in_stub) {
    if (auto r = impl_.RemoveLabel(label); !r) return std::unexpected{r.error()};
  }
  if (*in_fork && !*marked) {
    if (auto r = impl_.AddLabel(marker); !r) return std::unexpected{r.error()};
  }
  return true;
}

storage::Result<storage::PropertyValue> VertexAccessor::BranchSetProperty(storage::PropertyId key,
                                                                          const storage::PropertyValue &value) {
  auto &ctx = *branch_ctx_;
  if (ctx.IsReservedProperty(key)) ThrowReserved("property");
  if (auto err = CheckTypesOnPropertyWrite(
          ctx, *this, [&](storage::PropertyId pid) { return pid == key ? &value : nullptr; })) {
    return std::unexpected{*err};
  }
  CowIfNeeded(versioning::BranchChangeKind::kProperty);
  RecordPropertyFieldChange(key);
  if (!ctx.IsStub(impl_)) return impl_.SetProperty(key, value);

  auto fork = ctx.FindForkVertex(impl_.Gid());
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto old_value = MergedGetProperty(ctx, impl_, storage::View::NEW, key);
  if (!old_value) return old_value;
  auto removed = versioning::ReadRemoved(ctx, impl_);
  if (!removed) return std::unexpected{removed.error()};

  const PropertyMap changes{{key, value}};
  auto applied = versioning::ApplyPropertyChanges(ctx, impl_, std::move(*removed), changes, versioning::ForkHas(*fork));
  if (!applied) return std::unexpected{applied.error()};
  return old_value;
}

storage::Result<bool> VertexAccessor::BranchInitProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  RejectReservedProperties(ctx, properties);
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
  if (!ctx.IsStub(impl_)) return impl_.InitProperties(properties);

  auto fork = ctx.FindForkVertex(impl_.Gid());
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto existing = MergedProperties(ctx, impl_, storage::View::NEW);
  if (!existing) return std::unexpected{existing.error()};
  if (!existing->empty()) return false;
  auto removed = versioning::ReadRemoved(ctx, impl_);
  if (!removed) return std::unexpected{removed.error()};
  auto applied =
      versioning::ApplyPropertyChanges(ctx, impl_, std::move(*removed), properties, versioning::ForkHas(*fork));
  if (!applied) return std::unexpected{applied.error()};
  return true;
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
VertexAccessor::BranchUpdateProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  RejectReservedProperties(ctx, properties);
  if (auto err = CheckTypesOnPropertyWrite(ctx, *this, [&](storage::PropertyId pid) {
        auto const it = properties.find(pid);
        return it == properties.end() ? nullptr : &it->second;
      }))
    return std::unexpected{*err};
  CowIfNeeded(versioning::BranchChangeKind::kProperty);
  RecordPropertyFieldChanges(properties);
  if (!ctx.IsStub(impl_)) return impl_.UpdateProperties(properties);

  auto fork = ctx.FindForkVertex(impl_.Gid());
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto existing = MergedProperties(ctx, impl_, storage::View::NEW);
  if (!existing) return std::unexpected{existing.error()};
  auto removed = versioning::ReadRemoved(ctx, impl_);
  if (!removed) return std::unexpected{removed.error()};

  auto changes = versioning::UpdateChanges(*existing, properties);
  auto applied =
      versioning::ApplyPropertyChanges(ctx, impl_, std::move(*removed), properties, versioning::ForkHas(*fork));
  if (!applied) return std::unexpected{applied.error()};
  versioning::MergeOldBack(*existing, properties);
  return changes;
}

storage::Result<PropertyMap> VertexAccessor::BranchClearProperties() {
  auto &ctx = *branch_ctx_;
  CowIfNeeded(versioning::BranchChangeKind::kProperty);
  if (!ctx.IsStub(impl_)) {
    auto result = impl_.ClearProperties();
    if (result.has_value()) RecordPropertyFieldChanges(*result);
    return result;
  }

  auto fork = ctx.FindForkVertex(impl_.Gid());
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto existing = MergedProperties(ctx, impl_, storage::View::NEW);
  if (!existing) return existing;
  auto fork_props = fork->Properties(storage::View::OLD);
  if (!fork_props) return std::unexpected{fork_props.error()};
  const auto deleted_edges = ctx.DeletedEdgeGids(impl_, storage::View::NEW);
  if (auto cleared = impl_.ClearProperties(); !cleared) return std::unexpected{cleared.error()};  // incl. the lists
  if (!deleted_edges.empty()) {
    if (auto stored = ctx.StoreDeletedEdgeGids(impl_, deleted_edges); !stored) return std::unexpected{stored.error()};
  }
  if (auto written = versioning::WriteAllForkRemoved(ctx, impl_, *fork_props); !written) {
    return std::unexpected{written.error()};
  }
  RecordPropertyFieldChanges(*existing);
  return existing;
}

// Call after CowIfNeeded.
void VertexAccessor::RecordPropertyFieldChange(storage::PropertyId pid) {
  branch_ctx_->RecordPropertyFieldChange(impl_.Gid(), pid);
}

void VertexAccessor::RecordPropertyFieldChanges(
    const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  const auto gid = impl_.Gid();
  for (const auto &[pid, _] : properties) {
    branch_ctx_->RecordPropertyFieldChange(gid, pid);
  }
}

// ResolveEdges is unfiltered, so hops are charged before the edge_types filter.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Deleting an edge or vertex flags the edge bit (DeleteForkEdge/DeleteForkVertex), sending it to ResolveEdges.
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
