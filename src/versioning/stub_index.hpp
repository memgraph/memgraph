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

#include "storage/v2/vertex.hpp"

namespace memgraph::versioning {

// Gid-indexed table of a branch's fork-vertex stubs: for each fork gid that has a stub, the stub's diff-engine Vertex*
// and the fork (main) Vertex* it shadows. Replaces a skiplist lookup per read with three dependent loads.
//
// Three-level radix over the gid, sparse below the top: only the pages of touched gid ranges exist.
//   top : ceil(vertex_gid_floor / 2^16) atomic<L2*>          8 B per 65536 gids
//   L2  : 1024 atomic<Page*>                                 8 KiB, covers 65536 gids
//   Page: 64 x {atomic<Vertex*> stub, atomic<Vertex*> fork}  1 KiB, covers 64 gids
//
// Both pointers are stable for the owning BranchCore's lifetime: stubs are never erased from the diff engine, and a
// fork vertex visible at fork_ts cannot be freed while the fork pin is held. Entries are never removed or changed, so
// Insert is idempotent and a reader sees either null or the final pointer.
//
// A null result only means "not recorded here"; callers must fall back to the skiplist, which stays authoritative.
class StubIndex {
 public:
  explicit StubIndex(uint64_t vertex_gid_floor)
      : top_size_((vertex_gid_floor + kL2Span - 1) >> kL2Shift),
        top_(std::make_unique<std::atomic<L2 *>[]>(top_size_)) {}

  StubIndex(const StubIndex &) = delete;
  StubIndex &operator=(const StubIndex &) = delete;
  StubIndex(StubIndex &&) = delete;
  StubIndex &operator=(StubIndex &&) = delete;

  ~StubIndex() {
    for (size_t i = 0; i < top_size_; ++i) {
      L2 *l2 = top_[i].load(std::memory_order_acquire);
      if (l2 == nullptr) continue;
      for (auto &page : l2->pages) delete page.load(std::memory_order_acquire);
      delete l2;
    }
  }

  // Records non-null pointers for `gid`; gids past the table's range are ignored. Concurrent-safe.
  // @throw std::bad_alloc
  void Insert(uint64_t gid, storage::Vertex *stub, storage::Vertex *fork) {
    Entry *entry = EntryFor(gid, /*create=*/true);
    if (entry == nullptr) return;
    if (fork != nullptr) entry->fork.store(fork, std::memory_order_release);
    if (stub != nullptr) entry->stub.store(stub, std::memory_order_release);
  }

  storage::Vertex *FindStub(uint64_t gid) const noexcept {
    const Entry *entry = EntryFor(gid, /*create=*/false);
    return entry == nullptr ? nullptr : entry->stub.load(std::memory_order_acquire);
  }

  storage::Vertex *FindFork(uint64_t gid) const noexcept {
    const Entry *entry = EntryFor(gid, /*create=*/false);
    return entry == nullptr ? nullptr : entry->fork.load(std::memory_order_acquire);
  }

 private:
  static constexpr unsigned kPageShift = 6;
  static constexpr unsigned kL2Bits = 10;
  static constexpr unsigned kL2Shift = kPageShift + kL2Bits;
  static constexpr uint64_t kPageSpan = uint64_t{1} << kPageShift;
  static constexpr uint64_t kL2Span = uint64_t{1} << kL2Shift;

  struct Entry {
    std::atomic<storage::Vertex *> stub{nullptr};
    std::atomic<storage::Vertex *> fork{nullptr};
  };

  struct Page {
    std::array<Entry, kPageSpan> entries;
  };

  struct L2 {
    std::array<std::atomic<Page *>, size_t{1} << kL2Bits> pages{};
  };

  // Installs a freshly allocated child unless another thread won the race, in which case the winner is returned.
  template <typename Child>
  static Child *GetOrCreate(std::atomic<Child *> &slot) {
    if (Child *existing = slot.load(std::memory_order_acquire)) return existing;
    auto fresh = std::make_unique<Child>();
    Child *expected = nullptr;
    if (slot.compare_exchange_strong(expected, fresh.get(), std::memory_order_acq_rel, std::memory_order_acquire)) {
      return fresh.release();
    }
    return expected;
  }

  Entry *EntryFor(uint64_t gid, bool create) const {
    const uint64_t top_idx = gid >> kL2Shift;
    if (top_idx >= top_size_) return nullptr;
    std::atomic<L2 *> &l2_slot = top_[top_idx];
    L2 *l2 = create ? GetOrCreate(l2_slot) : l2_slot.load(std::memory_order_acquire);
    if (l2 == nullptr) return nullptr;
    std::atomic<Page *> &page_slot = l2->pages[(gid >> kPageShift) & ((uint64_t{1} << kL2Bits) - 1)];
    Page *page = create ? GetOrCreate(page_slot) : page_slot.load(std::memory_order_acquire);
    if (page == nullptr) return nullptr;
    return &page->entries[gid & (kPageSpan - 1)];
  }

  size_t top_size_;
  std::unique_ptr<std::atomic<L2 *>[]> top_;
};

}  // namespace memgraph::versioning
