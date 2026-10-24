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
#include <cstdint>
#include <map>
#include <tuple>
#include <type_traits>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/view.hpp"
#include "versioning/branch_engine.hpp"

// A stub stores changed values as plain properties and the ids of fork properties it removed in the reserved list
// property `__mg_removed`. `Stub` is a storage::VertexAccessor or storage::EdgeAccessor.
namespace memgraph::versioning {

template <typename T, typename PV>
void AppendListed(PV const &value, std::vector<T> &out) {
  ForEachListedInt(value, [&](int64_t v) {
    if constexpr (std::is_same_v<T, storage::PropertyId>) {
      out.push_back(storage::PropertyId::FromInt(static_cast<int32_t>(v)));
    } else {
      out.push_back(static_cast<T>(v));
    }
  });
}

template <typename T, typename Stub>
storage::Result<std::vector<T>> ReadIntList(const Stub &stub, storage::PropertyId key, storage::View view) {
  auto value = stub.GetProperty(key, view);
  if (!value) return std::unexpected{value.error()};
  std::vector<T> out;
  AppendListed(*value, out);
  return out;
}

// An empty `items` clears the property instead of storing an empty list.
template <typename T, typename Stub>
storage::Result<bool> WriteIntList(Stub &stub, storage::PropertyId key, const std::vector<T> &items) {
  storage::PropertyValue value;
  if (!items.empty()) {
    storage::PropertyValue::list_t list;
    list.reserve(items.size());
    for (auto const &item : items) {
      if constexpr (std::is_same_v<T, storage::PropertyId>) {
        list.emplace_back(static_cast<int64_t>(item.AsInt()));
      } else {
        list.emplace_back(static_cast<int64_t>(item));
      }
    }
    value = storage::PropertyValue{std::move(list)};
  }
  auto set = stub.SetProperty(key, value);
  if (!set) return std::unexpected{set.error()};
  return true;
}

// Removed pids from a stub's Properties() snapshot; erases the marker entries (also the deleted-edges list) from it.
inline std::vector<storage::PropertyId> TakeRemoved(BranchContext &ctx,
                                                    std::map<storage::PropertyId, storage::PropertyValue> &stub_props) {
  stub_props.erase(ctx.DeletedEdgesKey());
  std::vector<storage::PropertyId> removed;
  auto it = stub_props.find(ctx.RemovedPropsKey());
  if (it == stub_props.end()) return removed;
  AppendListed(it->second, removed);
  stub_props.erase(it);
  return removed;
}

template <typename Stub>
storage::Result<std::vector<storage::PropertyId>> ReadRemoved(BranchContext &ctx, const Stub &stub) {
  return ReadIntList<storage::PropertyId>(stub, ctx.RemovedPropsKey(), storage::View::NEW);
}

template <typename Stub>
storage::Result<bool> WriteRemoved(BranchContext &ctx, Stub &stub, const std::vector<storage::PropertyId> &removed) {
  return WriteIntList(stub, ctx.RemovedPropsKey(), removed);
}

using StubPropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;

// Value of `key` given a non-null `fork_value`: the stub's own, else null if the stub removed it, else the fork's.
template <typename Stub>
storage::Result<storage::PropertyValue> OverlayOnForkValue(BranchContext &ctx, const Stub &stub, storage::View view,
                                                           storage::PropertyId key, storage::PropertyValue fork_value) {
  auto stub_props = stub.Properties(view);  // single call: value and removal list must agree
  if (!stub_props) return std::unexpected{stub_props.error()};
  if (auto it = stub_props->find(key); it != stub_props->end()) return std::move(it->second);
  auto removed = TakeRemoved(ctx, *stub_props);
  if (std::ranges::contains(removed, key)) return storage::PropertyValue{};
  return fork_value;
}

// Size counterpart of OverlayOnForkValue; `fork_size` is non-zero.
template <typename Stub>
storage::Result<uint64_t> OverlaySizeOnFork(BranchContext &ctx, const Stub &stub, storage::View view,
                                            storage::PropertyId key, uint64_t fork_size) {
  auto stub_props = stub.Properties(view);
  if (!stub_props) return std::unexpected{stub_props.error()};
  if (stub_props->contains(key)) return stub.GetPropertySize(key, view);
  auto removed = TakeRemoved(ctx, *stub_props);
  if (std::ranges::contains(removed, key)) return uint64_t{0};
  return fork_size;
}

// Fork properties minus the stub's removals, with the stub's own values laid over them. Consumes the markers in
// `stub_props` (the stub's Properties() snapshot).
template <typename Fork>
storage::Result<StubPropertyMap> OverlayProperties(BranchContext &ctx, StubPropertyMap &stub_props, const Fork &fork) {
  auto removed = TakeRemoved(ctx, stub_props);
  auto merged = fork.Properties(storage::View::OLD);
  if (!merged) return merged;
  for (auto const pid : removed) merged->erase(pid);
  for (auto &[key, value] : stub_props) merged->insert_or_assign(key, std::move(value));
  return merged;
}

template <typename Fork>
auto ForkHas(const Fork &fork) {
  return [&fork](storage::PropertyId pid) {
    auto in_fork = fork.GetProperty(pid, storage::View::OLD);
    return in_fork && !in_fork->IsNull();
  };
}

// (pid, old, new) per entry of `properties`; `existing` supplies the old values.
inline std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>> UpdateChanges(
    const StubPropertyMap &existing, const StubPropertyMap &properties) {
  std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>> changes;
  changes.reserve(properties.size());
  for (const auto &[pid, new_value] : properties) {
    auto it = existing.find(pid);
    changes.emplace_back(pid, it != existing.end() ? it->second : storage::PropertyValue{}, new_value);
  }
  return changes;
}

// Storage's UpdateProperties leaves the untouched old properties merged into the caller's map; mirror that.
inline void MergeOldBack(StubPropertyMap &existing, StubPropertyMap &properties) {
  for (auto &[pid, old_value] : existing) properties.emplace(pid, std::move(old_value));
}

// Records every fork property as removed, after the stub was cleared.
template <typename Stub>
storage::Result<bool> WriteAllForkRemoved(BranchContext &ctx, Stub &stub, const StubPropertyMap &fork_props) {
  if (fork_props.empty()) return true;
  std::vector<storage::PropertyId> removed;
  removed.reserve(fork_props.size());
  for (const auto &[pid, _] : fork_props) removed.push_back(pid);
  return WriteRemoved(ctx, stub, removed);
}

// Applies `changes` (Null = remove) to the stub; removing a fork property adds it to `removed`, re-setting drops it
// again. `removed` (the stub's current list) is written back once, only if it changed.
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
