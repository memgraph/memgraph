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
#include <atomic>
#include <cstdint>
#include <expected>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/logging.hpp"

// A branch keeps what it changed on a fork object as a stub: a sparse stock Vertex or Edge in the branch's diff engine
// that holds only the difference to the object as of the fork. The diff engine's own MVCC then versions, aborts and
// collects the stub; this header is the stub's encoding in labels and properties:
//
//   label added by the branch       -> label L
//   fork label removed              -> label Mark(L)
//   property set by the branch      -> property p = value
//   fork property removed           -> property Mark(p) = true
//   object deleted                  -> label kDeleteLabel (vertex) / property kEdgeDeletePid (edge)
//   fork edge deleted               -> entry of type Mark(T) in both endpoint stubs' adjacency (LinkEdgeMarker)
//
// A marked id has the reserved bit set (id_types.hpp), so it never collides with a user id.
namespace memgraph::versioning {

// What a stub holds, unpacked. The mutators keep it canonical: a label or property is never both added and removed,
// a removed property carries no value, and a deleted object carries nothing else.
struct StubState {
  bool deleted{false};
  std::vector<storage::LabelId> added_labels;     // sorted
  std::vector<storage::LabelId> removed_labels;   // sorted
  storage::PropertyStore properties;              // changed values only
  std::vector<storage::PropertyId> removed_pids;  // sorted

  // @throw std::bad_alloc
  StubState Copy() const {
    StubState copy;
    copy.deleted = deleted;
    copy.added_labels = added_labels;
    copy.removed_labels = removed_labels;
    copy.properties = storage::PropertyStore::CreateFromBuffer(properties.StringBuffer());
    copy.removed_pids = removed_pids;
    return copy;
  }

  void AddLabel(storage::LabelId label) {
    Erase(removed_labels, label);
    Insert(added_labels, label);
  }

  void RemoveLabel(storage::LabelId label) {
    Erase(added_labels, label);
    Insert(removed_labels, label);
  }

  // A fork label the stub hid is shown again.
  void UnhideLabel(storage::LabelId label) { Erase(removed_labels, label); }

  void DropAddedLabel(storage::LabelId label) { Erase(added_labels, label); }

  // A null value removes the property.
  void SetProperty(storage::PropertyId pid, const storage::PropertyValue &value) {
    if (value.IsNull()) {
      RemoveProperty(pid);
      return;
    }
    properties.SetProperty(pid, value);
    Erase(removed_pids, pid);
  }

  void RemoveProperty(storage::PropertyId pid) {
    properties.SetProperty(pid, storage::PropertyValue{});
    Insert(removed_pids, pid);
  }

  // A deleted object carries no other changes.
  void MarkDeleted() {
    deleted = true;
    added_labels.clear();
    removed_labels.clear();
    properties.ClearProperties();
    removed_pids.clear();
  }

 private:
  template <typename T>
  static void Insert(std::vector<T> &sorted, T value) {
    auto const it = std::ranges::lower_bound(sorted, value);
    if (it == sorted.end() || *it != value) sorted.insert(it, value);
  }

  template <typename T>
  static void Erase(std::vector<T> &sorted, T value) {
    auto const it = std::ranges::lower_bound(sorted, value);
    if (it != sorted.end() && *it == value) sorted.erase(it);
  }
};

}  // namespace memgraph::versioning

namespace memgraph::versioning::stub {

// Real ids come from a counter that starts at 0 and stay far below kLastRealId; the delete id sits above every mark.
inline constexpr uint32_t kLastRealId = storage::kReservedIdBit - 3;

// Labels and properties share one name mapper, so the one delete id serves both.
inline constexpr uint32_t kDeleteId = 0xFFFFFFFEU;
inline constexpr storage::LabelId kDeleteLabel = storage::LabelId::FromUint(kDeleteId);
inline constexpr storage::PropertyId kEdgeDeletePid = storage::PropertyId::FromUint(kDeleteId);

inline storage::LabelId Mark(storage::LabelId l) noexcept {
  return storage::LabelId::FromUint(l.AsUint() | storage::kReservedIdBit);
}

inline storage::PropertyId Mark(storage::PropertyId p) noexcept {
  return storage::PropertyId::FromUint(p.AsUint() | storage::kReservedIdBit);
}

inline storage::EdgeTypeId Mark(storage::EdgeTypeId t) noexcept {
  return storage::EdgeTypeId::FromUint(t.AsUint() | storage::kReservedIdBit);
}

inline storage::LabelId Strip(storage::LabelId l) noexcept {
  return storage::LabelId::FromUint(l.AsUint() & ~storage::kReservedIdBit);
}

inline storage::PropertyId Strip(storage::PropertyId p) noexcept {
  return storage::PropertyId::FromUint(p.AsUint() & ~storage::kReservedIdBit);
}

inline storage::EdgeTypeId Strip(storage::EdgeTypeId t) noexcept {
  return storage::EdgeTypeId::FromUint(t.AsUint() & ~storage::kReservedIdBit);
}

static_assert(storage::IsReservedId(kDeleteId));
static_assert((kLastRealId | storage::kReservedIdBit) < kDeleteId,
              "no real id's mark is a delete marker, nor the invalid id 0xFFFFFFFF");

namespace detail {

// The property half of Apply, shared by vertex and edge stubs: single-key stock writes only. Whole-set writes
// (ClearProperties/InitProperties/UpdateProperties) would wipe the markers.
template <typename Accessor>
std::optional<storage::Error> ApplyProperties(Accessor &stub, const StubState &before, const StubState &after) {
  auto const old_props = before.properties.Properties();
  auto const new_props = after.properties.Properties();
  for (auto const &[pid, value] : new_props) {
    auto const it = old_props.find(pid);
    if (it != old_props.end() && it->second == value) continue;
    if (auto r = stub.SetProperty(pid, value); !r) return r.error();
  }
  for (auto const &entry : old_props) {
    if (new_props.contains(entry.first)) continue;
    if (auto r = stub.SetProperty(entry.first, storage::PropertyValue{}); !r) return r.error();
  }
  for (auto const pid : after.removed_pids) {
    if (std::ranges::binary_search(before.removed_pids, pid)) continue;
    if (auto r = stub.SetProperty(Mark(pid), storage::PropertyValue{true}); !r) return r.error();
  }
  for (auto const pid : before.removed_pids) {
    if (std::ranges::binary_search(after.removed_pids, pid)) continue;
    if (auto r = stub.SetProperty(Mark(pid), storage::PropertyValue{}); !r) return r.error();
  }
  return std::nullopt;
}

inline void DecodeProperty(StubState &state, storage::PropertyId pid, const storage::PropertyValue &value) {
  if (storage::IsReservedId(pid)) {
    state.removed_pids.push_back(Strip(pid));
  } else {
    state.properties.SetProperty(pid, value);
  }
}

}  // namespace detail

// The stub's state in `view`. A deleted stub decodes to the delete alone, whatever else it still holds.
inline storage::Result<StubState> Decode(const storage::VertexAccessor &stub, storage::View view) {
  auto labels = stub.Labels(view);
  if (!labels) return std::unexpected(labels.error());
  auto props = stub.Properties(view);
  if (!props) return std::unexpected(props.error());
  StubState state;
  for (auto const l : *labels) {
    if (l == kDeleteLabel) {
      return StubState{.deleted = true};
    }
    if (storage::IsReservedId(l)) {
      state.removed_labels.push_back(Strip(l));
    } else {
      state.added_labels.push_back(l);
    }
  }
  std::ranges::sort(state.added_labels);
  std::ranges::sort(state.removed_labels);
  for (auto const &[pid, value] : *props) {
    detail::DecodeProperty(state, pid, value);
  }
  std::ranges::sort(state.removed_pids);
  return state;
}

// Brings the stub from `before` (its decoded state) to `after` with stock single-key writes. nullopt on success.
// Not atomic: an error after the first write leaves the stub half-applied inside the diff transaction, so the caller
// must abort that transaction. A deleted object stays deleted.
inline std::optional<storage::Error> Apply(storage::VertexAccessor &stub, const StubState &before,
                                           const StubState &after) {
  DMG_ASSERT(!before.deleted || after.deleted, "a deleted object cannot be revived");
  if (before.deleted) return std::nullopt;

  auto const sync_labels = [&](const std::vector<storage::LabelId> &b,
                               const std::vector<storage::LabelId> &a,
                               bool marked) -> std::optional<storage::Error> {
    for (auto const l : a) {
      if (std::ranges::binary_search(b, l)) continue;
      if (auto r = stub.AddLabel(marked ? Mark(l) : l); !r) return r.error();
    }
    for (auto const l : b) {
      if (std::ranges::binary_search(a, l)) continue;
      if (auto r = stub.RemoveLabel(marked ? Mark(l) : l); !r) return r.error();
    }
    return std::nullopt;
  };
  if (after.deleted) {
    // The delete marker hides whatever else the stub holds; nothing else is written.
    if (auto r = stub.AddLabel(kDeleteLabel); !r) return r.error();
    return std::nullopt;
  }
  if (auto e = sync_labels(before.added_labels, after.added_labels, false)) return e;
  if (auto e = sync_labels(before.removed_labels, after.removed_labels, true)) return e;
  return detail::ApplyProperties(stub, before, after);
}

// Single-key reads that need no decode.
inline storage::Result<bool> IsDeleted(const storage::VertexAccessor &stub, storage::View view) {
  return stub.HasLabel(kDeleteLabel, view);
}

// nullopt: the stub does not decide, defer to the fork vertex.
inline storage::Result<std::optional<bool>> DecidesLabel(const storage::VertexAccessor &stub, storage::LabelId label,
                                                         storage::View view) {
  auto added = stub.HasLabel(label, view);
  if (!added) return std::unexpected(added.error());
  if (*added) return std::optional<bool>{true};
  auto removed = stub.HasLabel(Mark(label), view);
  if (!removed) return std::unexpected(removed.error());
  if (*removed) return std::optional<bool>{false};
  return std::optional<bool>{};
}

// The property as the branch sees it: the stub's value, else null when the stub removed it, else the fork's.
template <typename Fork>
storage::Result<storage::PropertyValue> MergeGetProperty(const storage::VertexAccessor &stub, const Fork &fork,
                                                         storage::PropertyId key, storage::View view) {
  auto own = stub.GetProperty(key, view);
  if (!own) return own;
  if (!own->IsNull()) return own;
  auto marker = stub.GetProperty(Mark(key), view);
  if (!marker) return marker;
  if (!marker->IsNull()) return storage::PropertyValue{};
  return fork.GetProperty(key, storage::View::OLD);
}

template <typename Fork>
storage::Result<uint64_t> MergeGetPropertySize(const storage::VertexAccessor &stub, const Fork &fork,
                                               storage::PropertyId key, storage::View view) {
  auto own = stub.GetPropertySize(key, view);
  if (!own) return own;
  if (*own != 0) return own;
  auto in_fork = fork.GetPropertySize(key, storage::View::OLD);
  if (!in_fork || *in_fork == 0) return in_fork;
  auto marker = stub.GetProperty(Mark(key), view);
  if (!marker) return std::unexpected(marker.error());
  return marker->IsNull() ? *in_fork : uint64_t{0};
}

// Edge stubs: a stock Edge with the fork gid; labels do not apply, a delete is the property kEdgeDeletePid.
inline storage::Result<StubState> DecodeEdge(const storage::EdgeAccessor &stub, storage::View view) {
  auto props = stub.Properties(view);
  if (!props) return std::unexpected(props.error());
  StubState state;
  for (auto const &[pid, value] : *props) {
    if (pid == kEdgeDeletePid) return StubState{.deleted = true};
    detail::DecodeProperty(state, pid, value);
  }
  std::ranges::sort(state.removed_pids);
  return state;
}

inline storage::Result<bool> IsEdgeDeleted(const storage::EdgeAccessor &stub, storage::View view) {
  auto r = stub.GetProperty(kEdgeDeletePid, view);
  if (!r) return std::unexpected(r.error());
  return !r->IsNull();
}

// The edge counterpart of Apply; see there.
inline std::optional<storage::Error> ApplyEdge(storage::EdgeAccessor &stub, const StubState &before,
                                               const StubState &after) {
  DMG_ASSERT(!before.deleted || after.deleted, "a deleted object cannot be revived");
  DMG_ASSERT(before.added_labels.empty() && before.removed_labels.empty() && after.added_labels.empty() &&
                 after.removed_labels.empty(),
             "an edge has no labels");
  if (before.deleted) return std::nullopt;
  if (after.deleted) {
    if (auto r = stub.SetProperty(kEdgeDeletePid, storage::PropertyValue{true}); !r) return r.error();
    return std::nullopt;
  }
  return detail::ApplyProperties(stub, before, after);
}

// Links one end of a deleted fork edge into `stub`'s adjacency: an entry of type Mark(`fork_type`) towards `other_stub`
// that holds `edge_stub` (the fork edge's stub, never the fork's own EdgeRef: stock walkers read the gid through every
// entry). It is written as a stock edge creation (delta, list entry, cache invalidation) under the stub's lock, so
// visibility, conflicts, abort and collection are the stock ones; it adds nothing else of a creation (index entries,
// edge types, counts). Its delta is FORCED_SEQUENTIAL, so a concurrent edge creation on the stub conflicts with it, as
// one conflicts with an in-flight delete on main. The abort of an out-entry decrements the engine's edge count like
// that of a created edge, so each out-entry registers the matching increment. Returns false when the stub is written
// by another transaction; the transaction is then poisoned.
// @throw utils::OutOfMemoryException
inline bool LinkEdgeMarker(storage::Transaction *transaction, storage::Storage &engine, storage::Vertex *stub,
                           storage::EdgeDirection direction, storage::EdgeTypeId fork_type, storage::Vertex *other_stub,
                           storage::Edge *edge_stub) {
  auto const marked = Mark(fork_type);
  auto const edge = storage::EdgeRef{edge_stub};
  auto guard = std::unique_lock{stub->lock};
  if (!storage::PrepareForWrite(transaction, stub)) return false;
  utils::AtomicMemoryBlock([&] {
    constexpr auto kState = storage::DeltaChainState::FORCED_SEQUENTIAL;
    if (direction == storage::EdgeDirection::OUT) {
      storage::CreateAndLinkDelta(
          transaction, stub, storage::Delta::RemoveOutEdgeTag(), marked, other_stub, edge, kState);
      stub->out_edges.emplace_back(marked, other_stub, edge);
      transaction->abort_callbacks_.Add(
          [&count = engine.edge_count_] { count.fetch_add(1, std::memory_order_acq_rel); });
    } else {
      storage::CreateAndLinkDelta(
          transaction, stub, storage::Delta::RemoveInEdgeTag(), marked, other_stub, edge, kState);
      stub->in_edges.emplace_back(marked, other_stub, edge);
    }
    transaction->manyDeltasCache.Invalidate(stub, marked, direction);
  });
  return true;
}

}  // namespace memgraph::versioning::stub
