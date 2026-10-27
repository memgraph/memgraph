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
#include "versioning/reserve_for_append.hpp"

// Per-object MVCC of a branch's changes over fork vertices and edges, following storage's visibility/conflict rules.
// A key (vertex gid or EdgeKey) has a newest-first chain of full-state versions; none visible: the fork's own state.
namespace memgraph::versioning {

// Keyspace of fork edges: gids are counters that never reach bit 63, so the bit separates them from vertex gids.
inline constexpr uint64_t kEdgeKeyBit = uint64_t{1} << 63;

constexpr uint64_t EdgeKey(uint64_t edge_gid) noexcept { return edge_gid | kEdgeKeyBit; }

constexpr bool IsEdgeKey(uint64_t key) noexcept { return (key & kEdgeKeyBit) != 0; }

// Shared by all versions of one transaction: its id until commit, the commit timestamp after, so one release store
// publishes them all. Reference counted by its versions and the owning OverlayTransaction.
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

// Mutable, unpacked overlay state that a write mutator edits; a label or property is never both added and removed.
struct OverlayState {
  bool deleted{false};
  std::vector<storage::LabelId> added_labels;     // sorted
  std::vector<storage::LabelId> removed_labels;   // sorted
  storage::PropertyStore properties;              // changed values only
  std::vector<storage::PropertyId> removed_pids;  // sorted

  void AddLabel(storage::LabelId label) {
    Erase(removed_labels, label);
    Insert(added_labels, label);
  }

  void RemoveLabel(storage::LabelId label) {
    Erase(added_labels, label);
    Insert(removed_labels, label);
  }

  // A fork label the overlay hid is shown again.
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

// One immutable packed version; the label/pid arrays trail the header in one allocation. After publication only `older`
// and the stamp's timestamp change. Layout: 36 B of fields (40 B padded), then at offset 36 added labels, removed
// labels, removed pids (4 B each); a count above 255 selects the extended form, which prefixes three u32 counts.
class OverlayVersion {
 public:
  static constexpr uint8_t kDeleted = 1;
  static constexpr uint8_t kExtended = 2;

  // Takes a reference on `stamp`. `older` is the next version of the chain (null at its end).
  // @throw std::bad_alloc
  static OverlayVersion *Create(OverlayState &&state, OverlayStamp *stamp, OverlayVersion *older, uint32_t command_id);

  // Releases the stamp reference and the memory; does not follow `older`.
  static void Destroy(OverlayVersion *version) noexcept;

  OverlayVersion *older() const noexcept { return older_.load(std::memory_order_acquire); }

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

  size_t AllocatedBytes() const noexcept { return Lay().total; }

  // @throw std::bad_alloc
  OverlayState ToState() const;

 private:
  friend class VertexOverlayStore;

  struct Layout {
    size_t added{0};
    size_t removed{0};
    size_t pids{0};
    size_t total{0};
  };

  static constexpr size_t kTailStart = 8 + 8 + 12 + 4 + 4;
  static constexpr size_t kExtHeaderBytes = 3 * sizeof(uint32_t);

  static constexpr Layout ComputeLayout(bool extended, size_t added, size_t removed, size_t pids) noexcept {
    Layout layout;
    size_t off = kTailStart;
    if (extended) off += kExtHeaderBytes;
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

  Layout Lay() const noexcept { return ComputeLayout(extended(), Count(0), Count(1), Count(2)); }

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

// Per-transaction store state: snapshot, stamp, command counter, written gids. One per overlay transaction, used from
// one thread at a time.
class OverlayTransaction {
 public:
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

  uint32_t command_id() const noexcept { return command_id_; }

  // Next command: writes made so far become visible to View::OLD.
  void AdvanceCommand() noexcept {
    MG_ASSERT(command_id_ != UINT32_MAX, "overlay command id overflow");
    ++command_id_;
  }

  bool has_writes() const noexcept { return stamp_ != nullptr; }

  bool finished() const noexcept { return finished_; }

  size_t write_count() const noexcept { return writes_.size(); }

  // Gids this transaction put a head on, in first-write order; empty once it has ended.
  std::span<const uint64_t> writes() const noexcept { return writes_; }

 private:
  friend class VertexOverlayStore;

  uint64_t start_ts_;
  uint64_t txn_id_;
  OverlayStamp *stamp_{nullptr};  // created by the first write
  std::vector<uint64_t> writes_;
  uint32_t command_id_{0};
  storage::IsolationLevel isolation_;
  bool finished_{false};
};

// Chain walks are lock-free; the gid lookup takes a shard's shared lock, writers its unique lock. A version unlinked
// from a chain (replaced, aborted, collapsed) is freed by CollectGarbage only once the oldest active start timestamp
// passes the clock value it was marked with, so every reader must be a transaction that timestamp accounts for.
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

  // The version of `gid` that `txn` sees, or null when the fork object's own state applies. Valid until `txn` ends.
  // Under READ_UNCOMMITTED the newest version is returned whatever its owner.
  const OverlayVersion *Read(uint64_t gid, const OverlayTransaction &txn, storage::View view) const;

  // Whether Write of `gid` by `txn` would pass the conflict check; nothing is written.
  bool CanWrite(uint64_t gid, const OverlayTransaction &txn) const;

  // Conflict check, `mutate(OverlayState &)` on the head's state (returns whether it changed anything; a no-op writes
  // no version) and publish, under one shard lock. SERIALIZATION_ERROR when the head is another transaction's
  // uncommitted or committed after `txn` started (PrepareForWrite's rule, any isolation level). The same command
  // replaces the transaction's own head; a later command adds a version, so View::OLD keeps the earlier state.
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
    if (!own) ReserveForAppend(txn.writes_, 1);

    bool const replace = own && head->command_id_ == txn.command_id_;
    if (replace) {  // so Retire below cannot allocate
      std::lock_guard garbage_guard{garbage_mutex_};
      ReserveForAppend(unmarked_, 1);
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

  // Publishes all of `txn`'s versions with one release store on its stamp. The caller takes `commit_ts` from the
  // start-timestamp clock and hands out no start timestamp between taking it and this call returning.
  void Commit(OverlayTransaction &txn, uint64_t commit_ts);

  // Unlinks the heads `txn` put on its chains, restoring the previous heads. `txn` must not have finished.
  void Abort(OverlayTransaction &txn);

  // `oldest_active` is the smallest start timestamp of any transaction that may hold a version pointer (the clock's
  // next timestamp when none), `now` the clock's next timestamp, read together.
  struct Horizon {
    uint64_t oldest_active;
    uint64_t now;
  };

  // Collapses committed chains below the newest version visible at the oldest active snapshot, and frees garbage whose
  // mark has passed. `sample` runs under gc_mutex_ only, before collapsing and again after the unlinks; the second
  // sample is the mark, so a reader that began after an unlink cannot be freed under. Calls are serialized.
  void CollectGarbage(const std::function<Horizon()> &sample);

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

  // Caller holds the version's shard lock (lock order). Terminates on allocation failure.
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
