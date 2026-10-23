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
#include <tuple>
#include <vector>

#include "query/edge_accessor.hpp"
#include "query/exceptions.hpp"
#include "versioning/branch_engine.hpp"

namespace memgraph::query {

namespace {

using PropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;

// Slice 1 stubs: a branch-touched fork vertex lives in the diff engine as an EMPTY stub holding only
// what changed, so a read is the fork vertex (main@fork_ts) overlaid with the stub. Additions are plain
// labels/properties on the stub; removals of fork state are reserved-name markers on the stub
// (BranchContext::RemovalMarker / RemovedPropsKey), and so are deletes (DeletedLabel / DeletedEdgesKey): a stub
// carrying the delete marker at the read's view is a deleted vertex (DELETED_OBJECT, like main). Native vertices
// (gid >= 2^62) are complete and never merged.
bool IsStub(versioning::BranchContext &ctx, const storage::VertexAccessor &impl) {
  return impl.storage_ == &ctx.diff_engine() && ctx.IsForkGid(impl.Gid());
}

// Removed pids encoded in a stub's Properties() snapshot; the marker entries are erased so they never leak.
std::vector<storage::PropertyId> TakeRemoved(versioning::BranchContext &ctx, PropertyMap &stub_props) {
  stub_props.erase(ctx.DeletedEdgesKey());
  std::vector<storage::PropertyId> removed;
  auto it = stub_props.find(ctx.RemovedPropsKey());
  if (it == stub_props.end()) return removed;
  versioning::ForEachRemovedPid(it->second, [&](storage::PropertyId pid) { removed.push_back(pid); });
  stub_props.erase(it);
  return removed;
}

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
  // Fork has it: only a removal marker on the stub hides it. Otherwise only the stub can add it.
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
  auto removed = TakeRemoved(ctx, *stub_props);
  auto fork = ctx.FindForkVertex(stub.Gid());
  DMG_ASSERT(fork.has_value(), "Fork vertex missing for stub {}", stub.Gid().AsUint());
  if (!fork) return stub_props;
  auto merged = fork->Properties(storage::View::OLD);
  if (!merged) return merged;
  for (auto const pid : removed) merged->erase(pid);
  for (auto &[key, value] : *stub_props) {
    merged->insert_or_assign(key, std::move(value));
  }
  return merged;
}

// Fork value + stub snapshot -> reconstructed value of `key`. `fork_value` non-null here.
storage::Result<storage::PropertyValue> OverlayOnForkValue(versioning::BranchContext &ctx,
                                                           const storage::VertexAccessor &stub, storage::View view,
                                                           storage::PropertyId key, storage::PropertyValue fork_value) {
  auto stub_props = stub.Properties(view);  // single call: value and removal list must agree
  if (!stub_props) return std::unexpected{stub_props.error()};
  if (auto it = stub_props->find(key); it != stub_props->end()) return std::move(it->second);
  auto removed = TakeRemoved(ctx, *stub_props);
  if (std::ranges::contains(removed, key)) return storage::PropertyValue{};
  return fork_value;
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
  return OverlayOnForkValue(ctx, stub, view, key, std::move(*in_fork));
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
  auto stub_props = stub.Properties(view);
  if (!stub_props) return std::unexpected{stub_props.error()};
  if (stub_props->contains(key)) return stub.GetPropertySize(key, view);
  auto removed = TakeRemoved(ctx, *stub_props);
  if (std::ranges::contains(removed, key)) return uint64_t{0};
  return in_fork;
}

[[noreturn]] void ThrowReserved(const char *what) {
  throw QueryRuntimeException(
      "Names starting with '__mg_' are reserved for versioning and cannot be used as a {} on a branch.", what);
}

void RejectReservedProperties(versioning::BranchContext &ctx, const PropertyMap &properties) {
  for (const auto &[pid, _] : properties) {
    if (ctx.IsReservedProperty(pid)) ThrowReserved("property");
  }
}

storage::Result<std::vector<storage::PropertyId>> ReadRemoved(versioning::BranchContext &ctx,
                                                              const storage::VertexAccessor &stub) {
  auto value = stub.GetProperty(ctx.RemovedPropsKey(), storage::View::NEW);
  if (!value) return std::unexpected{value.error()};
  std::vector<storage::PropertyId> removed;
  versioning::ForEachRemovedPid(*value, [&](storage::PropertyId pid) { removed.push_back(pid); });
  return removed;
}

storage::Result<bool> WriteRemoved(versioning::BranchContext &ctx, storage::VertexAccessor &stub,
                                   const std::vector<storage::PropertyId> &removed) {
  storage::PropertyValue value;
  if (!removed.empty()) {
    storage::PropertyValue::list_t list;
    list.reserve(removed.size());
    for (auto const pid : removed) list.emplace_back(static_cast<int64_t>(pid.AsInt()));
    value = storage::PropertyValue{std::move(list)};
  }
  auto set = stub.SetProperty(ctx.RemovedPropsKey(), value);
  if (!set) return std::unexpected{set.error()};
  return true;
}

// Applies `changes` (Null value = remove) to the stub: values land on the stub, and removing a
// property the fork vertex has is recorded in the removed-ids list (re-setting a value drops it from
// the list again). `removed` is the stub's current list; written back once, only if it changed.
template <typename ForkHas>
storage::Result<bool> ApplyPropertyChanges(versioning::BranchContext &ctx, storage::VertexAccessor &stub,
                                           std::vector<storage::PropertyId> removed, const PropertyMap &changes,
                                           ForkHas &&fork_has) {
  bool dirty = false;
  for (const auto &[pid, value] : changes) {
    auto set = stub.SetProperty(pid, value);
    if (!set) return std::unexpected{set.error()};
    auto it = std::ranges::find(removed, pid);
    if (value.IsNull()) {
      if (it == removed.end() && fork_has(pid)) {
        removed.push_back(pid);
        dirty = true;
      }
    } else if (it != removed.end()) {
      removed.erase(it);
      dirty = true;
    }
  }
  return dirty ? WriteRemoved(ctx, stub, removed) : storage::Result<bool>{true};
}

}  // namespace

// Out-of-line because BranchContext is only forward-declared in the header. Reads never COW.
// Un-branched main vertices are fork-identical (phase-1 invariant); a clear filter bit is a definitive skip, a set
// bit merges the stub over the fork vertex. A delete records the label and property kinds, so deleted vertices
// always reach the stub, where the delete marker turns the read into DELETED_OBJECT.
auto VertexAccessor::Labels(storage::View view) const -> decltype(impl_.Labels(view)) {
  if (branch_ctx_ != nullptr) {
    if (IsStub(*branch_ctx_, impl_)) return MergedLabels(*branch_ctx_, impl_, view);
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->MayHaveLabelChange(gid)) {
        if (auto stub = branch_ctx_->FindStubOrDiffVertex(gid, view)) {
          if (IsStub(*branch_ctx_, *stub)) return MergedLabels(*branch_ctx_, *stub, view);
          return stub->Labels(view);
        }
      }
    }
  }
  return impl_.Labels(view);
}

storage::Result<bool> VertexAccessor::HasLabel(storage::View view, storage::LabelId label) const {
  if (branch_ctx_ != nullptr) {
    if (IsStub(*branch_ctx_, impl_)) return MergedHasLabel(*branch_ctx_, impl_, view, label);
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->MayHaveLabelChange(gid)) {
        if (auto stub = branch_ctx_->FindStubOrDiffVertex(gid, view)) {
          if (IsStub(*branch_ctx_, *stub)) return MergedHasLabel(*branch_ctx_, *stub, view, label);
          return stub->HasLabel(label, view);
        }
      }
    }
  }
  return impl_.HasLabel(label, view);
}

auto VertexAccessor::Properties(storage::View view) const -> decltype(impl_.Properties(view)) {
  if (branch_ctx_ != nullptr) {
    if (IsStub(*branch_ctx_, impl_)) return MergedProperties(*branch_ctx_, impl_, view);
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->MayHavePropertyChange(gid)) {
        if (auto stub = branch_ctx_->FindStubOrDiffVertex(gid, view)) {
          if (IsStub(*branch_ctx_, *stub)) return MergedProperties(*branch_ctx_, *stub, view);
          return stub->Properties(view);
        }
      }
    }
  }
  return impl_.Properties(view);
}

storage::Result<storage::PropertyValue> VertexAccessor::GetProperty(storage::View view, storage::PropertyId key) const {
  if (branch_ctx_ != nullptr) {
    if (IsStub(*branch_ctx_, impl_)) return MergedGetProperty(*branch_ctx_, impl_, view, key);
    // Fine per-property filter: merges only when THIS property changed on the branch, or the vertex's labels did
    // (a delete is a label marker; reading a deleted vertex must fail).
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->MayHavePropertyFieldChange(gid, key) || branch_ctx_->MayHaveLabelChange(gid)) {
        if (auto stub = branch_ctx_->FindStubOrDiffVertex(gid, view)) {
          if (IsStub(*branch_ctx_, *stub)) return MergedGetProperty(*branch_ctx_, *stub, view, key);
          return stub->GetProperty(key, view);
        }
      }
    }
  }
  return impl_.GetProperty(key, view);
}

storage::Result<uint64_t> VertexAccessor::GetPropertySize(storage::PropertyId key, storage::View view) const {
  if (branch_ctx_ != nullptr) {
    if (IsStub(*branch_ctx_, impl_)) return MergedGetPropertySize(*branch_ctx_, impl_, key, view);
    if (impl_.storage_ != &branch_ctx_->diff_engine() && impl_.vertex_->branched()) {
      const auto gid = impl_.Gid();
      if (branch_ctx_->MayHavePropertyFieldChange(gid, key) || branch_ctx_->MayHaveLabelChange(gid)) {
        if (auto stub = branch_ctx_->FindStubOrDiffVertex(gid, view)) {
          if (IsStub(*branch_ctx_, *stub)) return MergedGetPropertySize(*branch_ctx_, *stub, key, view);
          return stub->GetPropertySize(key, view);
        }
      }
    }
  }
  return impl_.GetPropertySize(key, view);
}

// Pre-condition: branch_ctx_ is non-null (all call sites guard on branch_ctx_ != nullptr).
// Out-of-line because BranchContext is only forward-declared in the header.
void VertexAccessor::CowIfNeeded(versioning::BranchChangeKind kind) {
  auto cowed = branch_ctx_->CowVertex(impl_.Gid());
  if (!cowed) {
    throw QueryRuntimeException(cowed.error().message);
  }
  impl_ = *cowed;
  // Always fire after COW: even a vertex already COW'd for a different kind needs this call to flag
  // the new kind. gid is stable across the COW; the filter is a lock-free monotonic bloom, recorded before this
  // transaction commits, so it is safe against concurrent sessions of the branch.
  branch_ctx_->RecordVertexChange(impl_.Gid(), kind);
}

storage::Result<bool> VertexAccessor::BranchAddLabel(storage::LabelId label) {
  auto &ctx = *branch_ctx_;
  if (ctx.ClassifyLabel(label).reserved) ThrowReserved("label");
  CowIfNeeded(versioning::BranchChangeKind::kLabel);
  if (!IsStub(ctx, impl_)) return impl_.AddLabel(label);

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
  if (!IsStub(ctx, impl_)) return impl_.RemoveLabel(label);

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
  CowIfNeeded(versioning::BranchChangeKind::kProperty);
  RecordPropertyFieldChange(key);
  if (!IsStub(ctx, impl_)) return impl_.SetProperty(key, value);

  auto fork = ctx.FindForkVertex(impl_.Gid());
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto old_value = MergedGetProperty(ctx, impl_, storage::View::NEW, key);
  if (!old_value) return old_value;
  auto removed = ReadRemoved(ctx, impl_);
  if (!removed) return std::unexpected{removed.error()};

  const PropertyMap changes{{key, value}};
  auto applied = ApplyPropertyChanges(ctx, impl_, std::move(*removed), changes, [&](storage::PropertyId pid) {
    auto in_fork = fork->GetProperty(pid, storage::View::OLD);
    return in_fork && !in_fork->IsNull();
  });
  if (!applied) return std::unexpected{applied.error()};
  return old_value;
}

storage::Result<bool> VertexAccessor::BranchInitProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  RejectReservedProperties(ctx, properties);
  CowIfNeeded(versioning::BranchChangeKind::kProperty);
  RecordPropertyFieldChanges(properties);
  if (!IsStub(ctx, impl_)) return impl_.InitProperties(properties);

  auto fork = ctx.FindForkVertex(impl_.Gid());
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto existing = MergedProperties(ctx, impl_, storage::View::NEW);
  if (!existing) return std::unexpected{existing.error()};
  if (!existing->empty()) return false;
  auto removed = ReadRemoved(ctx, impl_);
  if (!removed) return std::unexpected{removed.error()};
  auto applied = ApplyPropertyChanges(ctx, impl_, std::move(*removed), properties, [&](storage::PropertyId pid) {
    auto in_fork = fork->GetProperty(pid, storage::View::OLD);
    return in_fork && !in_fork->IsNull();
  });
  if (!applied) return std::unexpected{applied.error()};
  return true;
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
VertexAccessor::BranchUpdateProperties(PropertyMap &properties) {
  auto &ctx = *branch_ctx_;
  RejectReservedProperties(ctx, properties);
  CowIfNeeded(versioning::BranchChangeKind::kProperty);
  RecordPropertyFieldChanges(properties);
  if (!IsStub(ctx, impl_)) return impl_.UpdateProperties(properties);

  auto fork = ctx.FindForkVertex(impl_.Gid());
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto existing = MergedProperties(ctx, impl_, storage::View::NEW);
  if (!existing) return std::unexpected{existing.error()};
  auto removed = ReadRemoved(ctx, impl_);
  if (!removed) return std::unexpected{removed.error()};

  std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>> changes;
  changes.reserve(properties.size());
  for (const auto &[pid, new_value] : properties) {
    auto it = existing->find(pid);
    changes.emplace_back(pid, it != existing->end() ? it->second : storage::PropertyValue{}, new_value);
  }
  auto applied = ApplyPropertyChanges(ctx, impl_, std::move(*removed), properties, [&](storage::PropertyId pid) {
    auto in_fork = fork->GetProperty(pid, storage::View::OLD);
    return in_fork && !in_fork->IsNull();
  });
  if (!applied) return std::unexpected{applied.error()};
  // Storage's UpdateProperties leaves the untouched old properties merged into the caller's map
  // (callers count it for UPDATED_PROPERTIES); keep that contract.
  for (auto &[pid, old_value] : *existing) properties.emplace(pid, std::move(old_value));
  return changes;
}

storage::Result<PropertyMap> VertexAccessor::BranchClearProperties() {
  auto &ctx = *branch_ctx_;
  CowIfNeeded(versioning::BranchChangeKind::kProperty);
  if (!IsStub(ctx, impl_)) {
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
  std::vector<storage::PropertyId> removed;
  removed.reserve(fork_props->size());
  for (const auto &[pid, _] : *fork_props) removed.push_back(pid);
  if (!removed.empty()) {
    if (auto written = WriteRemoved(ctx, impl_, removed); !written) return std::unexpected{written.error()};
  }
  RecordPropertyFieldChanges(*existing);
  return existing;
}

// Must be called after CowIfNeeded by property mutators. Records pid so GetProperty(pid) can skip
// the resolve when only other pids changed on this vertex. gid is stable across the COW.
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

// Branch mode returns the union of historical_ (fork-state) and diff-engine edges; impl_.InEdges
// alone sees only one side. ResolveEdges is unfiltered; hops charged before the edge_types filter.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // View::OLD mirrors ResolveEdges' convention for the frozen historical_ snapshot.
    // DELETE SAFETY: a deleted edge or vertex flags its endpoints' edge bit (DeleteForkEdge/DeleteForkVertex), so
    // this fast path is skipped for them and ResolveEdges runs instead. A neighbour of a deleted vertex is not
    // flagged: its fast-path edges are filtered per edge by the other endpoint's delete marker.
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
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::IN, view, {}, &vertex_deleted);
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;  // quota exhausted -> truncate
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

// Destination filter is client-side by gid (FromVertex for an IN edge); gid is stable across COW
// so no re-resolve is needed. ResolveEdges is unfiltered; hops charged before the dest/type filters.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::InEdges(storage::View view,
                                                                  const std::vector<storage::EdgeTypeId> &edge_types,
                                                                  const VertexAccessor &dest,
                                                                  storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Phase 2 fast path -- see non-dest InEdges above. NEVER pass `dest` to impl_.InEdges:
    // a COW'd dest.impl_ lives in a different transaction, tripping storage's DMG_ASSERT.
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
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::IN, view, {}, &vertex_deleted);
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    auto const dest_gid = dest.Gid();
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;  // quota exhausted -> truncate
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

// Branch-aware -- see InEdges above (identical rationale, OUT direction).
storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   const std::vector<storage::EdgeTypeId> &edge_types,
                                                                   storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Phase 2 fast path -- see InEdges above (identical rationale, OUT direction).
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
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::OUT, view, {}, &vertex_deleted);
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;  // quota exhausted -> truncate
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

// Destination filter uses ToVertex().Gid() (TO vertex for ADD_OUT_EDGE) -- mirrors InEdges(dest)
// above with FromVertex(). ResolveEdges unfiltered; hops charged before dest/type filters.
storage::Result<EdgeVertexAccessorResult> VertexAccessor::OutEdges(storage::View view,
                                                                   std::vector<storage::EdgeTypeId> const &edge_types,
                                                                   VertexAccessor const &dest,
                                                                   storage::HopsLimit *hops_limit) const {
  if (branch_ctx_ != nullptr) {
    // Phase 2 fast path -- see InEdges(dest) above. Client-side filter uses ToVertex() (not FromVertex()).
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
    auto resolved = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::OUT, view, {}, &vertex_deleted);
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    auto const dest_gid = dest.Gid();
    std::vector<EdgeAccessor> edges;
    int64_t expanded_count = 0;
    for (auto const &edge : resolved) {
      if (hops_limit != nullptr && hops_limit->IsUsed()) {
        if (hops_limit->IncrementHopsCount() == 0) break;  // quota exhausted -> truncate
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

// Branch mode uses a full ResolveEdges call for correctness: impl_.InDegree/OutDegree only sees
// one engine side. A dedicated branch-aware degree counter is a future optimization.
storage::Result<size_t> VertexAccessor::InDegree(storage::View view) const {
  if (branch_ctx_ != nullptr) {
    bool vertex_deleted = false;
    auto const count = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::IN, view, {}, &vertex_deleted).size();
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    return count;
  }
  return impl_.InDegree(view);
}

storage::Result<size_t> VertexAccessor::OutDegree(storage::View view) const {
  if (branch_ctx_ != nullptr) {
    bool vertex_deleted = false;
    auto const count = branch_ctx_->ResolveEdges(Gid(), storage::EdgeDirection::OUT, view, {}, &vertex_deleted).size();
    if (vertex_deleted) return std::unexpected{storage::Error::DELETED_OBJECT};
    return count;
  }
  return impl_.OutDegree(view);
}

}  // namespace memgraph::query
