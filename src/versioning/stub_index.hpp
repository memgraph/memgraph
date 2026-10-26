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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <shared_mutex>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "storage/v2/vertex.hpp"
#include "utils/db_aware_allocator.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::versioning {

// Sparse, page-keyed table of a branch's fork-vertex stubs: per fork gid, the stub Vertex* and the fork (main) Vertex*
// it anchors. Owns the stubs (heap objects outside the diff engine's vertices_), freed in ~StubIndex, so the owner must
// destroy the diff engine first: its deltas and index entries point at the stubs.
//
// Structure: gids are grouped into pages of kPageSize = 16 consecutive gids (page_id = gid >> 4). kShards independently
// locked absl::flat_hash_maps map page_id -> Page*. A Page holds a 16-entry slot table (slot_of[gid & 15]: 0 = absent,
// else entry index + 1) and a variable number of {stub, fork} entries sized 1, 4 or 16 (grown 1 -> 4 -> 16 by copying
// into a larger page). Only recorded gids occupy entries, so memory is proportional to the number of entries and the
// number of touched pages, never to the gid range.
//
// Lookups carry a caller-owned Cursor that remembers the last page. Scans visit gids in order, so most lookups hit the
// cached page: no lock, no hash probe, just two acquire loads. Insert-only; both pointers are stable for the owning
// BranchCore's lifetime (stubs are never erased, and a fork vertex visible at fork_ts cannot be freed while the fork
// pin is held).
//
// A null stub result means no stub is recorded yet: a stale cursor or a replaced page can only miss entries, never
// return a wrong pointer.
//
// Memory ordering:
//  - Writers are serialized per shard by the unique lock. A new entry's pointers are stored relaxed, then
//    slot_of[off] is stored with release; readers load slot_of with acquire, so a non-zero slot always sees an
//    initialized entry at index slot-1 < capacity. Later fills of a null pointer are release stores read with acquire.
//  - A grown page is fully built (entries + slots) before it is published in the map under the unique lock; readers
//    obtain it under the shared lock (lock hand-off orders the build before the read). The replaced page is retired,
//    not freed (freed only in ~StubIndex), so readers holding it, including via a stale Cursor, read valid memory;
//    it is frozen after retirement, so they just may not see later inserts.
//  - Each shard has an epoch bumped (release) after every state-changing insert while holding the unique lock. A cursor
//    records the epoch observed under the shared lock when it was refreshed. If a lookup in the cached page does not
//    yield the wanted pointer and the shard epoch (acquire) is unchanged, no insert has completed in that shard since
//    the refresh, so the cached view (including "page absent") is current and the miss is returned without locking. A
//    changed epoch triggers one locked refresh. Hits never touch the epoch.
//
// Memory (64-bit), excluding the stub Vertex itself (one 80 B heap object per entry): entry 16 B; page header 24 B; one
// map slot ~17 B / load factor (~26 B per page).
//  - Sparse (one recorded gid per page): 40 B page + ~26 B map = ~66 B per entry.
//  - Dense (full pages): 280 B / 16 = 17.5 B per gid + ~1.6 B map + retired 1- and 4-entry copies (128 B per full page
//    = 8 B per gid) = ~27 B per gid.
// Retired copies are the cost of lock-free stale readers; they are bounded by 128 B per page.
class StubIndex {
 public:
  static constexpr unsigned kPageBits = 4;
  static constexpr uint64_t kPageSize = uint64_t{1} << kPageBits;

  struct Found {
    storage::Vertex *stub{nullptr};
    storage::Vertex *fork{nullptr};
  };

  class Page;

  // Per-reader last-page cache. Not thread-safe: one cursor per reading thread/session, and a cursor must only be used
  // with one StubIndex. Default state caches nothing.
  struct Cursor {
    uint64_t page_id{UINT64_MAX};  // gid >> kPageBits can never be UINT64_MAX
    const Page *page{nullptr};     // null: page absent at `epoch`
    uint64_t epoch{0};

    void Reset() noexcept { *this = Cursor{}; }
  };

  StubIndex() = default;

  StubIndex(const StubIndex &) = delete;
  StubIndex &operator=(const StubIndex &) = delete;
  StubIndex(StubIndex &&) = delete;
  StubIndex &operator=(StubIndex &&) = delete;
  ~StubIndex() = default;

  // Returns the stub recorded for `gid`, creating an empty, delta-less one if none exists. Concurrent creators race on
  // the insert: the loser frees its candidate and returns the winner's. `fork` fills the entry's null fork pointer.
  // @throw std::bad_alloc
  storage::Vertex *GetOrCreateStub(uint64_t gid, storage::Vertex *fork) {
    auto candidate = memory::MakeDbAwareUnique<storage::Vertex>(storage::Gid::FromUint(gid), nullptr);
    storage::Vertex *winner = InsertEntry(gid, candidate.get(), fork).stub;
    if (winner == candidate.get()) candidate.release();
    return winner;
  }

  // Cursor-less lookups: one shared-lock probe each; for callers without a cursor.
  storage::Vertex *FindStub(uint64_t gid) const noexcept {
    Cursor cursor;
    return Find(gid, cursor).stub;
  }

  storage::Vertex *FindFork(uint64_t gid) const noexcept {
    Cursor cursor;
    return Find(gid, cursor).fork;
  }

  storage::Vertex *FindStub(uint64_t gid, Cursor &cursor) const noexcept { return Lookup(gid, cursor, true).stub; }

  storage::Vertex *FindFork(uint64_t gid, Cursor &cursor) const noexcept { return Lookup(gid, cursor, false).fork; }

  Found Find(uint64_t gid, Cursor &cursor) const noexcept { return Lookup(gid, cursor, true); }

  // Calls `fn(stub)` for every recorded stub in unspecified order. Concurrent creators may or may not be visited.
  template <typename Fn>
  void ForEachStub(Fn &&fn) const {
    for (const Shard &shard : shards_) {
      std::shared_lock guard{shard.lock};
      for (const auto &[page_id, page] : shard.map) page->ForEachStub(fn);
    }
  }

  // Variable-size: header followed by `capacity` entries in the same allocation.
  class alignas(8) Page {
   public:
    Page() = default;  // use Create(); a bare Page has no entry storage

    static Page *Create(unsigned capacity) {
      void *mem = ::operator new(sizeof(Page) + capacity * sizeof(Entry));
      Page *page = std::construct_at(static_cast<Page *>(mem));
      page->capacity_ = static_cast<uint8_t>(capacity);
      Entry *entries = page->entries();
      for (unsigned i = 0; i < capacity; ++i) std::construct_at(entries + i);
      return page;
    }

    static constexpr unsigned NextCapacity(unsigned capacity) noexcept { return capacity == 1 ? 4U : 16U; }

    unsigned capacity() const noexcept { return capacity_; }

    bool HasRoom() const noexcept { return count_ < capacity_; }

    bool HasSlot(unsigned off) const noexcept { return slot_of_[off].load(std::memory_order_relaxed) != 0; }

    // Reader side.
    Found Read(unsigned off) const noexcept {
      const unsigned slot = slot_of_[off].load(std::memory_order_acquire);
      if (slot == 0) return {};
      const Entry &entry = entries()[slot - 1];
      return {entry.stub.load(std::memory_order_acquire), entry.fork.load(std::memory_order_acquire)};
    }

    // Writer side; caller holds the shard's unique lock.
    // Fills null pointers of an existing slot; true if anything changed.
    bool Fill(unsigned off, storage::Vertex *stub, storage::Vertex *fork) noexcept {
      const unsigned slot = slot_of_[off].load(std::memory_order_relaxed);
      if (slot == 0) return false;
      Entry &entry = entries()[slot - 1];
      bool changed = false;
      if (stub != nullptr && entry.stub.load(std::memory_order_relaxed) == nullptr) {
        entry.stub.store(stub, std::memory_order_release);
        changed = true;
      }
      if (fork != nullptr && entry.fork.load(std::memory_order_relaxed) == nullptr) {
        entry.fork.store(fork, std::memory_order_release);
        changed = true;
      }
      return changed;
    }

    // Requires HasRoom() and no slot for `off`. Entry first, then the release store that publishes the slot.
    void Append(unsigned off, storage::Vertex *stub, storage::Vertex *fork) noexcept {
      const unsigned idx = count_++;
      Entry &entry = entries()[idx];
      entry.stub.store(stub, std::memory_order_relaxed);
      entry.fork.store(fork, std::memory_order_relaxed);
      slot_of_[off].store(static_cast<uint8_t>(idx + 1), std::memory_order_release);
    }

    // Calls `fn(stub)` for every recorded stub. Caller excludes concurrent writers (unique/shared shard lock, or
    // single-threaded teardown).
    template <typename Fn>
    void ForEachStub(Fn &&fn) const {
      const Entry *all = entries();
      for (unsigned i = 0; i < count_; ++i) {
        if (auto *stub = all[i].stub.load(std::memory_order_relaxed)) fn(stub);
      }
    }

    // Copies all entries of `old` (smaller) into this empty page.
    void CopyFrom(const Page &old) noexcept {
      for (unsigned off = 0; off < kPageSize; ++off) {
        const unsigned slot = old.slot_of_[off].load(std::memory_order_relaxed);
        if (slot == 0) continue;
        const Entry &src = old.entries()[slot - 1];
        Append(off, src.stub.load(std::memory_order_relaxed), src.fork.load(std::memory_order_relaxed));
      }
    }

   private:
    struct Entry {
      std::atomic<storage::Vertex *> stub{nullptr};
      std::atomic<storage::Vertex *> fork{nullptr};
    };

    Entry *entries() noexcept { return reinterpret_cast<Entry *>(reinterpret_cast<std::byte *>(this) + sizeof(Page)); }

    const Entry *entries() const noexcept {
      return reinterpret_cast<const Entry *>(reinterpret_cast<const std::byte *>(this) + sizeof(Page));
    }

    std::array<std::atomic<uint8_t>, kPageSize> slot_of_{};
    uint8_t capacity_{0};
    uint8_t count_{0};  // writer-only (unique lock)
  };

 private:
  static constexpr unsigned kShardBits = 6;
  static constexpr size_t kShards = size_t{1} << kShardBits;

  struct PageDeleter {
    void operator()(Page *page) const noexcept { ::operator delete(static_cast<void *>(page)); }
  };

  struct alignas(64) Shard {
    mutable utils::RWSpinLock lock;
    std::atomic<uint64_t> epoch{0};  // bumped (release) under the unique lock; read lock-free by cursors
    absl::flat_hash_map<uint64_t, Page *> map;
    std::vector<Page *> retired;  // replaced pages, kept for stale readers; freed in ~Shard

    Shard() = default;
    Shard(const Shard &) = delete;
    Shard &operator=(const Shard &) = delete;

    // Stubs are owned through the live pages only; retired pages hold copies of the same pointers.
    ~Shard() {
      for (auto &[page_id, page] : map) {
        page->ForEachStub([](storage::Vertex *stub) { memory::ArenaAwareDeleter<storage::Vertex>{}(stub); });
        PageDeleter{}(page);
      }
      for (Page *page : retired) PageDeleter{}(page);
    }

    void BumpEpoch() noexcept { epoch.store(epoch.load(std::memory_order_relaxed) + 1, std::memory_order_release); }
  };

  // Fibonacci hash, shard from the high bits so sequential pages spread across shards.
  static constexpr size_t ShardIndex(uint64_t page_id) noexcept {
    return static_cast<size_t>((page_id * 0x9E3779B97F4A7C15ULL) >> (64 - kShardBits));
  }

  Shard &ShardFor(uint64_t page_id) noexcept { return shards_[ShardIndex(page_id)]; }

  const Shard &ShardFor(uint64_t page_id) const noexcept { return shards_[ShardIndex(page_id)]; }

  // Records non-null pointers for `gid`; a null argument leaves the existing value untouched. Returns the entry as
  // recorded afterwards. Does not take ownership of `stub`.
  // @throw std::bad_alloc
  Found InsertEntry(uint64_t gid, storage::Vertex *stub, storage::Vertex *fork) {
    if (stub == nullptr && fork == nullptr) return {};
    const uint64_t page_id = gid >> kPageBits;
    const unsigned off = static_cast<unsigned>(gid & (kPageSize - 1));
    Shard &shard = ShardFor(page_id);
    std::unique_lock guard{shard.lock};

    const auto it = shard.map.find(page_id);
    Page *page = it == shard.map.end() ? nullptr : it->second;
    if (page != nullptr) {
      if (page->Fill(off, stub, fork)) shard.BumpEpoch();
      if (page->HasSlot(off)) return page->Read(off);
    }

    // New gid in this page: append in place if there is room, else build a larger page and publish it.
    if (page != nullptr && page->HasRoom()) {
      page->Append(off, stub, fork);
      shard.BumpEpoch();
      return {stub, fork};
    }
    std::unique_ptr<Page, PageDeleter> grown{Page::Create(page == nullptr ? 1 : Page::NextCapacity(page->capacity()))};
    if (page != nullptr) grown->CopyFrom(*page);
    grown->Append(off, stub, fork);
    const Found result = grown->Read(off);
    if (page != nullptr) {
      shard.retired.reserve(shard.retired.size() + 1);  // before publishing so retiring cannot throw
      it->second = grown.release();
      shard.retired.push_back(page);
    } else {
      shard.map.emplace(page_id, grown.get());
      grown.release();
    }
    shard.BumpEpoch();
    return result;
  }

  Found Lookup(uint64_t gid, Cursor &cursor, bool want_stub) const noexcept {
    const uint64_t page_id = gid >> kPageBits;
    const unsigned off = static_cast<unsigned>(gid & (kPageSize - 1));
    const Shard &shard = ShardFor(page_id);

    if (cursor.page_id == page_id) {
      Found found = cursor.page != nullptr ? cursor.page->Read(off) : Found{};
      if ((want_stub ? found.stub : found.fork) != nullptr) return found;
      // Not satisfied by the cached view: it is current iff no insert completed in this shard since the refresh.
      if (shard.epoch.load(std::memory_order_acquire) == cursor.epoch) return found;
    }

    {
      std::shared_lock guard{shard.lock};
      cursor.epoch = shard.epoch.load(std::memory_order_relaxed);  // ordered by the lock
      const auto it = shard.map.find(page_id);
      cursor.page = it == shard.map.end() ? nullptr : it->second;
    }
    cursor.page_id = page_id;
    return cursor.page != nullptr ? cursor.page->Read(off) : Found{};
  }

  std::array<Shard, kShards> shards_;
};

}  // namespace memgraph::versioning
