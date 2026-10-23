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

#include <algorithm>
#include <map>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/view.hpp"
#include "versioning/branch_engine.hpp"

// Property-removal bookkeeping shared by vertex and edge stubs: a stub stores changed values as plain properties and
// the ids of fork properties it removed in the reserved list property `__mg_removed`. `Stub` is a
// storage::VertexAccessor or storage::EdgeAccessor.
namespace memgraph::versioning {

// Removed pids encoded in a stub's Properties() snapshot; the marker entries are erased so they never leak.
inline std::vector<storage::PropertyId> TakeRemoved(BranchContext &ctx,
                                                    std::map<storage::PropertyId, storage::PropertyValue> &stub_props) {
  stub_props.erase(ctx.DeletedEdgesKey());
  std::vector<storage::PropertyId> removed;
  auto it = stub_props.find(ctx.RemovedPropsKey());
  if (it == stub_props.end()) return removed;
  ForEachRemovedPid(it->second, [&](storage::PropertyId pid) { removed.push_back(pid); });
  stub_props.erase(it);
  return removed;
}

template <typename Stub>
storage::Result<std::vector<storage::PropertyId>> ReadRemoved(BranchContext &ctx, const Stub &stub) {
  auto value = stub.GetProperty(ctx.RemovedPropsKey(), storage::View::NEW);
  if (!value) return std::unexpected{value.error()};
  std::vector<storage::PropertyId> removed;
  ForEachRemovedPid(*value, [&](storage::PropertyId pid) { removed.push_back(pid); });
  return removed;
}

template <typename Stub>
storage::Result<bool> WriteRemoved(BranchContext &ctx, Stub &stub, const std::vector<storage::PropertyId> &removed) {
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

// Applies `changes` (Null value = remove) to the stub: values land on the stub, and removing a property the fork
// object has is recorded in the removed-ids list (re-setting a value drops it from the list again). `removed` is the
// stub's current list; written back once, only if it changed.
template <typename Stub, typename ForkHas>
storage::Result<bool> ApplyPropertyChanges(BranchContext &ctx, Stub &stub, std::vector<storage::PropertyId> removed,
                                           const std::map<storage::PropertyId, storage::PropertyValue> &changes,
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

}  // namespace memgraph::versioning
