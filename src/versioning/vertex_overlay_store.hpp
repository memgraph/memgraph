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
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <shared_mutex>
#include <span>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction_constants.hpp"
#include "storage/v2/view.hpp"
#include "utils/db_aware_allocator.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/rw_spin_lock.hpp"

// Versioned per-vertex changes of a branch over its fork vertices, with a small MVCC of their own that follows the
// storage engine's visibility and conflict rules (storage/v2/mvcc.hpp) without reusing its deltas.
//
// Per fork gid the store keeps a newest-first chain of OverlayVersion. A version is the FULL overlay state after a
// write (not a diff), so a read finds the one visible version and never replays. No visible version means the fork
// vertex's own state applies.
namespace memgraph::versioning {

// Shared by all versions of one transaction: the transaction id until commit, the commit timestamp afterwards. One
// release store makes every version of the transaction visible at once. Reference counted by its versions and by the
// owning OverlayTransaction.
class OverlayStamp {
 public:
  // @throw std::bad_alloc
  static OverlayStamp *Create(uint64_t txn_id) { return memory::MakeDbAwareUnique<OverlayStamp>(txn_id).release(); }

  explicit OverlayStamp(uint64_t ts) noexcept : ts_{ts} {}

  void Ref() noexcept { refs_.fetch_add(1, std::memory_order_relaxed); }

  void Unref() noexcept {
    if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1) memory::ArenaAwareDeleter<OverlayStamp>{}(this);
  }

  uint64_t ts() const noexcept { return ts_.load(std::memory_order_acquire); }

  void Commit(uint64_t commit_ts) noexcept { ts_.store(commit_ts, std::memory_order_release); }

 private:
  std::atomic<uint64_t> ts_;
  std::atomic<uint32_t> refs_{1};
};

// Mutable, unpacked overlay state: what a write mutator edits and what a version is built from. The sets keep the
// overlay's meaning relative to the fork vertex: a label or property both "added" and "removed" cannot exist.
struct OverlayState {
  bool deleted{false};
  std::vector<storage::LabelId> added_labels;     // sorted
  std::vector<storage::LabelId> removed_labels;   // sorted
  storage::PropertyStore properties;              // changed values only
  std::vector<storage::PropertyId> removed_pids;  // sorted
  std::vector<uint64_t> deleted_edge_gids;        // sorted

  void AddLabel(storage::LabelId label) {
    Erase(removed_labels, label);
    Insert(added_labels, label);
  }

  void RemoveLabel(storage::LabelId label) {
    Erase(added_labels, label);
    Insert(removed_labels, label);
  }

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

  void AddDeletedEdge(uint64_t gid) { Insert(deleted_edge_gids, gid); }

  // A deleted vertex carries no other changes.
  void MarkDeleted() {
    deleted = true;
    added_labels.clear();
    removed_labels.clear();
    properties.ClearProperties();
    removed_pids.clear();
    deleted_edge_gids.clear();
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

// One immutable, packed version. The label, pid and edge arrays trail the header in the same allocation; after
// publication only `older` and the stamp's timestamp change.
//
// Layout: 36 B of header fields (padded to 40 B by the alignment), then at offset 36
//  - normal form: added labels, removed labels, removed pids (4 B each; each count fits the header's u8);
//  - extended form (any count above 255, or any deleted edge): four u32 counts, deleted-edge gids (8-aligned), then
//    the same three arrays.
// A single label or pid is 40 B of allocation, three fit in 48 B.
class OverlayVersion {
 public:
  static constexpr uint8_t kDeleted = 1;
  static constexpr uint8_t kExtended = 2;

  // Takes a reference on `stamp`. `older` is the next version of the chain (null at its end).
  // @throw std::bad_alloc
  static OverlayVersion *Create(OverlayState &&state, OverlayStamp *stamp, OverlayVersion *older, uint32_t command_id);

  // Releases the stamp reference and the memory; does not follow `older`.
  static void Destroy(OverlayVersion *version) noexcept;

  // Bytes of the allocation holding a version with these counts.
  static constexpr size_t AllocBytes(bool extended, size_t added, size_t removed, size_t pids, size_t edges) noexcept {
    return ComputeLayout(extended, added, removed, pids, edges).total;
  }

  const OverlayStamp *stamp() const noexcept { return stamp_; }

  OverlayVersion *older() const noexcept { return older_.load(std::memory_order_acquire); }

  uint32_t command_id() const noexcept { return command_id_; }

  bool deleted() const noexcept { return (flags_ & kDeleted) != 0; }

  const storage::PropertyStore &properties() const noexcept { return props_; }

  std::span<const storage::LabelId> added_labels() const noexcept {
    return Array<storage::LabelId>(Lay().added, Count(0));
  }

  std::span<const storage::LabelId> removed_labels() const noexcept {
    return Array<storage::LabelId>(Lay().removed, Count(1));
  }

  std::span<const storage::PropertyId> removed_pids() const noexcept {
    return Array<storage::PropertyId>(Lay().pids, Count(2));
  }

  std::span<const uint64_t> deleted_edge_gids() const noexcept {
    return Array<uint64_t>(Lay().edges, extended() ? ExtCounts()[3] : 0);
  }

  size_t AllocatedBytes() const noexcept { return Lay().total; }

  // @throw std::bad_alloc
  OverlayState ToState() const;

 private:
  friend class VertexOverlayStore;

  struct Layout {
    size_t edges{0};
    size_t added{0};
    size_t removed{0};
    size_t pids{0};
    size_t total{0};
  };

  static constexpr size_t kTailStart = 8 + 8 + 12 + 4 + 4;
  static constexpr size_t kExtHeaderBytes = 4 * sizeof(uint32_t);

  static constexpr Layout ComputeLayout(bool extended, size_t added, size_t removed, size_t pids,
                                        size_t edges) noexcept {
    Layout layout;
    size_t off = kTailStart;
    if (extended) {
      off = (off + kExtHeaderBytes + 7) & ~size_t{7};
      layout.edges = off;
      off += edges * sizeof(uint64_t);
    }
    layout.added = off;
    off += added * sizeof(storage::LabelId);
    layout.removed = off;
    off += removed * sizeof(storage::LabelId);
    layout.pids = off;
    off += pids * sizeof(storage::PropertyId);
    layout.total = std::max(off, size_t{40});
    return layout;
  }

  explicit OverlayVersion(OverlayStamp *stamp, OverlayVersion *older, uint32_t command_id,
                          storage::PropertyStore &&props, uint8_t flags, std::array<uint8_t, 3> counts) noexcept
      : stamp_{stamp},
        older_{older},
        props_{std::move(props)},
        command_id_{command_id},
        counts_{counts},
        flags_{flags} {}

  bool extended() const noexcept { return (flags_ & kExtended) != 0; }

  const std::byte *base() const noexcept { return reinterpret_cast<const std::byte *>(this); }

  std::byte *base() noexcept { return reinterpret_cast<std::byte *>(this); }

  const uint32_t *ExtCounts() const noexcept { return reinterpret_cast<const uint32_t *>(base() + kTailStart); }

  size_t Count(size_t i) const noexcept { return extended() ? ExtCounts()[i] : counts_[i]; }

  Layout Lay() const noexcept {
    return ComputeLayout(extended(), Count(0), Count(1), Count(2), extended() ? ExtCounts()[3] : 0);
  }

  template <typename T>
  std::span<const T> Array(size_t offset, size_t n) const noexcept {
    return {reinterpret_cast<const T *>(base() + offset), n};
  }

  OverlayStamp *stamp_;
  std::atomic<OverlayVersion *> older_;
  storage::PropertyStore props_;
  uint32_t command_id_;
  std::array<uint8_t, 3> counts_;  // added labels, removed labels, removed pids; zero when extended
  uint8_t flags_;
};

static_assert(sizeof(storage::PropertyStore) == 12, "OverlayVersion::kTailStart assumes a 12 B PropertyStore");
static_assert(sizeof(OverlayVersion) == 40, "OverlayVersion header must stay 40 B");
static_assert(sizeof(storage::LabelId) == 4 && sizeof(storage::PropertyId) == 4);

// Per-transaction state the store needs: the snapshot, the transaction's stamp, the command counter and the gids it
// wrote. Owned by the caller, one per overlay transaction, used from one thread at a time.
class OverlayTransaction {
 public:
  // `txn_id` must be in the transaction id space (>= storage::kTransactionInitialId), as in storage::Transaction.
  OverlayTransaction(uint64_t start_ts, uint64_t txn_id, storage::IsolationLevel isolation) noexcept
      : start_ts_{start_ts}, txn_id_{txn_id}, isolation_{isolation} {
    DMG_ASSERT(txn_id >= storage::kTransactionInitialId);
  }

  OverlayTransaction(const OverlayTransaction &) = delete;
  OverlayTransaction &operator=(const OverlayTransaction &) = delete;
  OverlayTransaction(OverlayTransaction &&) = delete;
  OverlayTransaction &operator=(OverlayTransaction &&) = delete;

  // A transaction that wrote must end in VertexOverlayStore::Commit or Abort, else its heads stay on the chains.
  ~OverlayTransaction() {
    DMG_ASSERT(writes_.empty(), "overlay transaction with writes destroyed without Commit or Abort");
    if (stamp_ != nullptr) stamp_->Unref();
  }

  uint64_t start_ts() const noexcept { return start_ts_; }

  uint64_t txn_id() const noexcept { return txn_id_; }

  uint32_t command_id() const noexcept { return command_id_; }

  storage::IsolationLevel isolation() const noexcept { return isolation_; }

  // Next command: writes made so far become visible to View::OLD.
  void AdvanceCommand() noexcept {
    MG_ASSERT(command_id_ != UINT32_MAX, "overlay command id overflow");
    ++command_id_;
  }

  bool has_writes() const noexcept { return stamp_ != nullptr; }

  // Committed or aborted.
  bool finished() const noexcept { return finished_; }

  size_t write_count() const noexcept { return writes_.size(); }

  // Gids this transaction put a head on, in first-write order; empty once it has ended.
  std::span<const uint64_t> writes() const noexcept { return writes_; }

 private:
  friend class VertexOverlayStore;

  uint64_t start_ts_;
  uint64_t txn_id_;
  OverlayStamp *stamp_{nullptr};  // created by the first write
  std::vector<uint64_t> writes_;  // gids whose chain this transaction put a head on
  uint32_t command_id_{0};
  storage::IsolationLevel isolation_;
  bool finished_{false};
};

// Concurrency: chain walks are lock-free; the gid lookup takes a shard's shared lock, writers its unique lock. A
// version removed from a chain (replaced, aborted, collapsed by GC) goes to a garbage list and is freed by
// CollectGarbage only once `oldest_active_start_ts` has passed the clock value the garbage was marked with, the same
// scheme as the storage engine's garbage_undo_buffers_: any transaction that could hold a pointer to it started before
// the mark. Every reader must therefore be a transaction that CollectGarbage's `oldest_active_start_ts` accounts for.
//
// Lock order: gc_mutex_ -> shard lock -> garbage_mutex_. The sampler runs under gc_mutex_ only, so it may take
// engine_lock_ (Commit holds engine_lock_ -> garbage_mutex_).
class VertexOverlayStore {
 public:
  VertexOverlayStore() = default;
  VertexOverlayStore(const VertexOverlayStore &) = delete;
  VertexOverlayStore &operator=(const VertexOverlayStore &) = delete;
  VertexOverlayStore(VertexOverlayStore &&) = delete;
  VertexOverlayStore &operator=(VertexOverlayStore &&) = delete;

  // Single-threaded teardown: no transaction may be using the store.
  ~VertexOverlayStore();

  // The version of `gid` that `txn` sees, or null when the fork vertex's own state applies. The pointer is valid until
  // `txn` ends. Under READ_UNCOMMITTED the newest version is returned whatever its owner.
  const OverlayVersion *Read(uint64_t gid, const OverlayTransaction &txn, storage::View view) const;

  // Lookup, conflict check, read-modify and publish under one shard lock. `mutate(OverlayState &)` receives the
  // newest state (the transaction's own, or the committed head that is visible to it) and returns whether it changed
  // anything; a no-op creates no version. Returns whether a version was written, or SERIALIZATION_ERROR when the head
  // belongs to another transaction that has not committed or committed after `txn` started (storage's PrepareForWrite
  // rule, for every isolation level). A transaction writing again in the same command replaces its head; in a later
  // command it adds a version, so View::OLD keeps seeing the earlier command's state.
  // @throw std::bad_alloc
  template <typename Mutator>
  std::expected<bool, storage::Error> Write(uint64_t gid, OverlayTransaction &txn, Mutator &&mutate) {
    DMG_ASSERT(!txn.finished_);
    Shard &shard = ShardFor(gid);
    std::unique_lock guard{shard.lock};

    auto const it = shard.map.find(gid);
    OverlayVersion *const head = it == shard.map.end() ? nullptr : it->second;
    bool const own = head != nullptr && head->stamp_ == txn.stamp_;
    if (head != nullptr && !own && !(head->stamp_->ts() < txn.start_ts_)) {
      return std::unexpected(storage::Error::SERIALIZATION_ERROR);
    }

    OverlayState state = head != nullptr ? head->ToState() : OverlayState{};
    if (!mutate(state)) return false;

    if (txn.stamp_ == nullptr) txn.stamp_ = OverlayStamp::Create(txn.txn_id_);
    if (!own) txn.writes_.reserve(txn.writes_.size() + 1);

    bool const replace = own && head->command_id_ == txn.command_id_;
    if (replace) {  // Retire below must not allocate
      std::lock_guard garbage_guard{garbage_mutex_};
      unmarked_.reserve(unmarked_.size() + 1);
    }
    OverlayVersion *const created =
        OverlayVersion::Create(std::move(state), txn.stamp_, replace ? head->older() : head, txn.command_id_);
    try {
      if (it == shard.map.end()) {
        shard.map.emplace(gid, created);
      } else {
        it->second = created;
      }
    } catch (...) {
      OverlayVersion::Destroy(created);
      throw;
    }
    live_versions_.fetch_add(1, std::memory_order_relaxed);
    if (!own) txn.writes_.push_back(gid);
    if (replace) Retire(head);
    return true;
  }

  // Makes every version of `txn` visible to snapshots taken after `commit_ts`: one release store on its stamp. The
  // caller takes `commit_ts` from the same clock as start timestamps, with no start timestamp handed out between taking
  // it and this call returning.
  void Commit(OverlayTransaction &txn, uint64_t commit_ts);

  // Unlinks the heads `txn` put on its chains, restoring the previous heads. Idempotent for a transaction without
  // writes.
  void Abort(OverlayTransaction &txn);

  // The clock state GC works from, sampled by the caller's registry of active snapshots and clock:
  // `oldest_active` is the smallest start timestamp of any transaction that may hold a version pointer (the clock's
  // next timestamp when none), `now` the clock's next timestamp, read together.
  struct Horizon {
    uint64_t oldest_active;
    uint64_t now;
  };

  // Collapses committed chains below the newest version visible at the oldest active snapshot, and frees garbage whose
  // mark has been passed. `sample` is called without any store lock but gc_mutex_ held, once before collapsing and once
  // after everything to be marked has been unlinked: garbage is marked with the second sample, so a reader that began
  // after an unlink was sampled can never be freed under. `sample` may take the engine's lock. Calls are serialized.
  void CollectGarbage(const std::function<Horizon()> &sample);

  // For a caller whose clock cannot change during the call (tests): one fixed horizon for both samples.
  void CollectGarbage(uint64_t oldest_active_start_ts, uint64_t now_ts) {
    CollectGarbage([=] { return Horizon{oldest_active_start_ts, now_ts}; });
  }

  // Gids with a chain.
  size_t size() const;

  // Versions allocated and not yet freed, garbage included.
  size_t live_versions() const noexcept { return live_versions_.load(std::memory_order_relaxed); }

  // Versions in `gid`'s chain, for diagnostics and tests.
  size_t ChainLength(uint64_t gid) const;

 private:
  static constexpr size_t kShardBits = 6;
  static constexpr size_t kShards = size_t{1} << kShardBits;

  using Map = absl::flat_hash_map<uint64_t, OverlayVersion *, absl::Hash<uint64_t>, std::equal_to<>,
                                  memory::DbAwareAllocator<std::pair<const uint64_t, OverlayVersion *>>>;

  struct alignas(64) Shard {
    mutable utils::RWSpinLock lock;
    Map map;  // gid -> newest version; guarded by `lock`
  };

  struct Marked {
    OverlayVersion *version;
    uint64_t mark;
  };

  static constexpr size_t ShardIndex(uint64_t gid) noexcept {
    return static_cast<size_t>((gid * 0x9E3779B97F4A7C15ULL) >> (64 - kShardBits));
  }

  Shard &ShardFor(uint64_t gid) noexcept { return shards_[ShardIndex(gid)]; }

  const Shard &ShardFor(uint64_t gid) const noexcept { return shards_[ShardIndex(gid)]; }

  static const OverlayVersion *Visible(const OverlayVersion *version, const OverlayTransaction &txn,
                                       storage::View view) noexcept;

  // Caller holds the version's shard lock (lock order). Not recoverable on allocation failure (terminates).
  void Retire(OverlayVersion *version) noexcept;

  void RetireChain(OverlayVersion *first) noexcept;

  std::array<Shard, kShards> shards_;
  std::atomic<size_t> live_versions_{0};

  std::mutex gc_mutex_;
  std::mutex garbage_mutex_;
  std::vector<OverlayVersion *> unmarked_;  // unlinked, not yet given a mark
  std::vector<Marked> marked_;              // in non-decreasing mark order
  std::vector<uint64_t> dirty_;             // committed gids whose chain may collapse
};

}  // namespace memgraph::versioning
