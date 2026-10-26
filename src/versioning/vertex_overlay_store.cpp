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

#include "versioning/vertex_overlay_store.hpp"

#include <cstddef>
#include <type_traits>

#include "utils/memory_tracker.hpp"

namespace memgraph::versioning {

namespace {

template <typename T>
void CopyArray(std::byte *base, size_t offset, const std::vector<T> &src) noexcept {
  static_assert(std::is_trivially_copyable_v<T>);
  if (!src.empty()) std::memcpy(base + offset, src.data(), src.size() * sizeof(T));
}

}  // namespace

OverlayVersion *OverlayVersion::Create(OverlayState &&state, OverlayStamp *stamp, OverlayVersion *older,
                                       uint32_t command_id) {
  bool const deleted = state.deleted;
  size_t const added = deleted ? 0 : state.added_labels.size();
  size_t const removed = deleted ? 0 : state.removed_labels.size();
  size_t const pids = deleted ? 0 : state.removed_pids.size();
  size_t const edges = deleted ? 0 : state.deleted_edge_gids.size();
  bool const extended = added > UINT8_MAX || removed > UINT8_MAX || pids > UINT8_MAX || edges > 0;

  Layout const layout = ComputeLayout(extended, added, removed, pids, edges);
  void *mem = memory::DbAllocateBytes(layout.total, memory::tls_db_arena_state.arena, alignof(OverlayVersion));

  std::array<uint8_t, 3> counts{};
  if (!extended) counts = {static_cast<uint8_t>(added), static_cast<uint8_t>(removed), static_cast<uint8_t>(pids)};
  auto const flags = static_cast<uint8_t>((deleted ? kDeleted : 0) | (extended ? kExtended : 0));
  auto *version = ::new (mem) OverlayVersion(
      stamp, older, command_id, deleted ? storage::PropertyStore{} : std::move(state.properties), flags, counts);

  std::byte *base = version->base();
  if (extended) {
    uint32_t const ext[4] = {static_cast<uint32_t>(added),
                             static_cast<uint32_t>(removed),
                             static_cast<uint32_t>(pids),
                             static_cast<uint32_t>(edges)};
    std::memcpy(base + kTailStart, ext, sizeof(ext));
    if (edges > 0) std::memcpy(base + layout.edges, state.deleted_edge_gids.data(), edges * sizeof(uint64_t));
  }
  if (!deleted) {
    CopyArray(base, layout.added, state.added_labels);
    CopyArray(base, layout.removed, state.removed_labels);
    CopyArray(base, layout.pids, state.removed_pids);
  }
  stamp->Ref();
  return version;
}

void OverlayVersion::Destroy(OverlayVersion *version) noexcept {
  size_t const bytes = version->AllocatedBytes();
  OverlayStamp *const stamp = version->stamp_;
  std::destroy_at(version);
  memory::DbDeallocateBytes(static_cast<void *>(version), bytes, alignof(OverlayVersion));
  stamp->Unref();
}

OverlayState OverlayVersion::ToState() const {
  OverlayState state;
  state.deleted = deleted();
  auto const added = added_labels();
  auto const removed = removed_labels();
  auto const pids = removed_pids();
  auto const edges = deleted_edge_gids();
  state.added_labels.assign(added.begin(), added.end());
  state.removed_labels.assign(removed.begin(), removed.end());
  state.removed_pids.assign(pids.begin(), pids.end());
  state.deleted_edge_gids.assign(edges.begin(), edges.end());
  state.properties = storage::PropertyStore::CreateFromBuffer(props_.StringBuffer());
  return state;
}

VertexOverlayStore::~VertexOverlayStore() {
  for (Shard &shard : shards_) {
    for (auto &[gid, head] : shard.map) {
      for (OverlayVersion *version = head; version != nullptr;) {
        OverlayVersion *const older = version->older();
        OverlayVersion::Destroy(version);
        version = older;
      }
    }
  }
  for (OverlayVersion *version : unmarked_) OverlayVersion::Destroy(version);
  for (const Marked &marked : marked_) OverlayVersion::Destroy(marked.version);
}

const OverlayVersion *VertexOverlayStore::Visible(const OverlayVersion *version, const OverlayTransaction &txn,
                                                  storage::View view) noexcept {
  // READ_UNCOMMITTED never undoes anything (ApplyDeltasForRead).
  if (txn.isolation_ == storage::IsolationLevel::READ_UNCOMMITTED) return version;

  for (; version != nullptr; version = version->older()) {
    if (version->stamp_ == txn.stamp_) {
      // Own writes, also after this transaction committed: NEW sees all of them, OLD only earlier commands'.
      if (view == storage::View::NEW || version->command_id_ < txn.command_id_) return version;
      continue;
    }
    uint64_t const ts = version->stamp_->ts();
    bool const committed_for_me = txn.isolation_ == storage::IsolationLevel::SNAPSHOT_ISOLATION
                                      ? ts < txn.start_ts_
                                      : ts < storage::kTransactionInitialId;
    if (committed_for_me) return version;
  }
  return nullptr;
}

const OverlayVersion *VertexOverlayStore::Read(uint64_t gid, const OverlayTransaction &txn, storage::View view) const {
  const OverlayVersion *head = nullptr;
  {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.lock};
    auto const it = shard.map.find(gid);
    if (it == shard.map.end()) return nullptr;
    head = it->second;
  }
  return Visible(head, txn, view);
}

void VertexOverlayStore::Commit(OverlayTransaction &txn, uint64_t commit_ts) {
  DMG_ASSERT(!txn.finished_);
  if (txn.stamp_ != nullptr) {
    const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;  // a tracked OOM must not fail a late commit
    std::lock_guard guard{garbage_mutex_};
    dirty_.reserve(dirty_.size() + txn.writes_.size());  // before the stamp flips: nothing below throws
    txn.stamp_->Commit(commit_ts);
    dirty_.insert(dirty_.end(), txn.writes_.begin(), txn.writes_.end());
  }
  txn.writes_.clear();
  txn.finished_ = true;
}

void VertexOverlayStore::Abort(OverlayTransaction &txn) {
  DMG_ASSERT(!txn.finished_);
  const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
  txn.finished_ = true;
  for (uint64_t const gid : txn.writes_) {
    Shard &shard = ShardFor(gid);
    std::unique_lock guard{shard.lock};
    auto const it = shard.map.find(gid);
    if (it == shard.map.end()) continue;
    // Another transaction cannot have put a head over an uncommitted one, so ours are the newest versions.
    OverlayVersion *version = it->second;
    while (version != nullptr && version->stamp_ == txn.stamp_) {
      OverlayVersion *const older = version->older();
      Retire(version);
      version = older;
    }
    if (version != nullptr) {
      it->second = version;
    } else {
      shard.map.erase(it);
    }
  }
  txn.writes_.clear();
}

void VertexOverlayStore::Retire(OverlayVersion *version) noexcept {
  const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
  std::lock_guard guard{garbage_mutex_};
  unmarked_.push_back(version);
}

void VertexOverlayStore::RetireChain(OverlayVersion *first) noexcept {
  const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
  std::lock_guard guard{garbage_mutex_};
  for (OverlayVersion *version = first; version != nullptr; version = version->older()) unmarked_.push_back(version);
}

void VertexOverlayStore::CollectGarbage(const std::function<Horizon()> &sample) {
  std::lock_guard gc_guard{gc_mutex_};
  uint64_t const oldest_active_start_ts = sample().oldest_active;

  std::vector<uint64_t> dirty;
  {
    std::lock_guard guard{garbage_mutex_};
    dirty.swap(dirty_);
  }
  std::ranges::sort(dirty);
  dirty.erase(std::ranges::unique(dirty).begin(), dirty.end());

  std::vector<uint64_t> again;
  try {
    for (uint64_t const gid : dirty) {
      Shard &shard = ShardFor(gid);
      std::unique_lock guard{shard.lock};
      auto const it = shard.map.find(gid);
      if (it == shard.map.end()) continue;

      // `floor` is the newest version visible to the oldest active snapshot; everything older is invisible.
      OverlayVersion *floor = nullptr;
      size_t above = 0;
      for (OverlayVersion *version = it->second; version != nullptr; version = version->older()) {
        if (version->stamp_->ts() < oldest_active_start_ts) {
          floor = version;
          break;
        }
        ++above;
      }
      // Versions above the floor may become the floor, or the first of two, once the oldest snapshot moves on.
      if (floor == nullptr ? above >= 2 : above > 0) again.push_back(gid);
      if (floor == nullptr) continue;

      OverlayVersion *const tail = floor->older();
      if (tail == nullptr) continue;
      floor->older_.store(nullptr, std::memory_order_release);
      RetireChain(tail);
    }
  } catch (...) {
    // Only `again` growing can throw; keep every gid not yet processed for the next run.
    again.insert(again.end(), dirty.begin(), dirty.end());
    std::lock_guard guard{garbage_mutex_};
    dirty_.insert(dirty_.end(), again.begin(), again.end());
    throw;
  }
  {
    std::lock_guard guard{garbage_mutex_};
    dirty_.insert(dirty_.end(), again.begin(), again.end());
  }

  // Everything unlinked so far, by this call or by writers and aborts, goes to the mark; later unlinks wait for the
  // next call. The horizon is sampled after the swap, so the mark is never older than any unlink it covers.
  std::vector<OverlayVersion *> to_mark;
  {
    std::lock_guard guard{garbage_mutex_};
    to_mark.swap(unmarked_);
  }
  Horizon const horizon = sample();

  std::vector<OverlayVersion *> free_now;
  {
    const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
    std::lock_guard guard{garbage_mutex_};
    for (OverlayVersion *version : to_mark) marked_.push_back({version, horizon.now});
    size_t expired = 0;
    while (expired < marked_.size() && marked_[expired].mark <= horizon.oldest_active) ++expired;
    free_now.reserve(expired);
    for (size_t i = 0; i < expired; ++i) free_now.push_back(marked_[i].version);
    marked_.erase(marked_.begin(), marked_.begin() + static_cast<std::ptrdiff_t>(expired));
  }
  for (OverlayVersion *version : free_now) OverlayVersion::Destroy(version);
  live_versions_.fetch_sub(free_now.size(), std::memory_order_relaxed);
}

size_t VertexOverlayStore::size() const {
  size_t total = 0;
  for (const Shard &shard : shards_) {
    std::shared_lock guard{shard.lock};
    total += shard.map.size();
  }
  return total;
}

size_t VertexOverlayStore::ChainLength(uint64_t gid) const {
  const OverlayVersion *head = nullptr;
  {
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.lock};
    auto const it = shard.map.find(gid);
    if (it == shard.map.end()) return 0;
    head = it->second;
  }
  size_t length = 0;
  for (; head != nullptr; head = head->older()) ++length;
  return length;
}

}  // namespace memgraph::versioning
