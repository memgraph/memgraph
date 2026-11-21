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
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <thread>
#include <utility>

#include <absl/container/inlined_vector.h>
#include <boost/unordered/unordered_flat_map.hpp>

#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/db_aware_allocator.hpp"
#include "utils/logging.hpp"
#include "utils/small_vector.hpp"
#include "versioning/overlay_state.hpp"

namespace memgraph::versioning {

// Base edges that may be deleted on this branch at one base vertex (EdgeOverlayIndex::BaseEdgeOverlay pointers).
// Insert-only, non-MVCC: readers check each candidate's delete mark. Guarded by the owning shard's mutex.
struct BaseEdgeDeletes {
  using EdgeCandidates = utils::small_vector<storage::Edge *, memory::DbAwareAllocator<storage::Edge *>>;
  EdgeCandidates out;
  EdgeCandidates in;
};

static_assert(sizeof(BaseEdgeDeletes) == 32, "the 32 B jemalloc class");

using CandidateCopy = absl::InlinedVector<storage::Edge *, 16>;

// A branch's base-vertex state keyed by base gid: delta-less heap Vertex overlays (outside the diff engine's vertices_)
// and BaseEdgeDeletes entries (a map under the shard lock). An entry in either means main's vertex has its branched
// bit set (callers set it first). A deletes entry for g implies MayHaveEdgeChange(g, that list's direction) (the delete
// records before it appends), so readers may skip the deletes probe on a clear bit.
// Overlay lookups take no lock but retry while a writer shifts entries in the same span; entries are never removed. A
// side a DETACH deleter did not request is the vertex's own side, covered by its delete mark or the fallback list. Owns
// the overlays, so the owner must destroy the diff engine first (its deltas point at them).
class OverlayIndex {
 public:
  // `gid_floor`: every overlay's gid is below it.
  explicit OverlayIndex(uint64_t gid_floor) : floor_(gid_floor), span_count_((gid_floor + kSpanGids - 1) >> kSpanLog2) {
    if (span_count_ == 0) return;
    spans_ = static_cast<std::atomic<uintptr_t> *>(memory::DbAllocateBytes(span_count_ * sizeof(std::atomic<uintptr_t>),
                                                                           memory::tls_db_arena_state.arena,
                                                                           alignof(std::atomic<uintptr_t>)));
    for (size_t i = 0; i < span_count_; ++i) std::construct_at(&spans_[i], uintptr_t{0});
  }

  OverlayIndex(const OverlayIndex &) = delete;
  OverlayIndex &operator=(const OverlayIndex &) = delete;

  ~OverlayIndex() {
    for (size_t i = 0; i < span_count_; ++i) {
      const uintptr_t entry = spans_[i].load(std::memory_order_relaxed);
      if (Pointer(entry) == 0) continue;
      if (IsDense(entry)) {
        auto *slots = Slots(entry);
        for (size_t k = 0; k < kSpanGids; ++k) {
          memory::ArenaAwareDeleter<BaseVertexOverlay>{}(slots[k].load(std::memory_order_relaxed));
        }
        memory::DbDeallocateBytes(slots, kSpanGids * sizeof(void *), alignof(void *));
        continue;
      }
      Chunk *chunk = AsChunk(entry);
      for (size_t k = 0, n = chunk->count.load(std::memory_order_relaxed); k < n; ++k) {
        memory::ArenaAwareDeleter<BaseVertexOverlay>{}(chunk->Ptrs()[k].load(std::memory_order_relaxed));
      }
      Free(chunk);
    }
    for (const Chunk *chunk = retired_.load(std::memory_order_relaxed); chunk != nullptr;) {
      const Chunk *next = chunk->next_retired;
      Free(chunk);
      chunk = next;
    }
    if (spans_ != nullptr) {
      memory::DbDeallocateBytes(spans_, span_count_ * sizeof(std::atomic<uintptr_t>), alignof(std::atomic<uintptr_t>));
    }
  }

  // Every Vertex* this index hands out points at the base of one of these.
  struct BaseVertexOverlay : storage::Vertex {
    BaseVertexOverlay(storage::Gid gid, storage::Vertex *base_vertex) noexcept
        : storage::Vertex(gid, nullptr), base_vertex(base_vertex) {}

    storage::Vertex *base_vertex;
  };

  static_assert(sizeof(BaseVertexOverlay) == 88, "the 96 B jemalloc class");

  struct Found {
    storage::Vertex *overlay{nullptr};
    storage::Vertex *base_vertex{nullptr};
  };

  // `candidates`: one direction's deleted-edge candidates; empty if no entry or not asked for.
  struct Expansion : Found {
    CandidateCopy candidates;
  };

  // One entry per kSpanGids gids below the floor, 0 if empty. Low bits tag the entry: kLocked while a writer holds the
  // span, kDense if it points at kSpanGids overlay slots instead of a Chunk. A sparse span is a sorted Chunk, grown by
  // doubling; a full chunk of kDenseCap entries switches the span, one way, to the slot array. Replaced chunks are
  // freed at destruction, as readers may still be inside them.
  static constexpr unsigned kSpanLog2 = 12;
  static constexpr size_t kSpanGids = size_t{1} << kSpanLog2;  // gids per table entry
  static constexpr size_t kDenseCap = 1024;  // a full chunk of this capacity switches its span to a dense array
  static_assert(kDenseCap < kSpanGids);

  Found Find(storage::Gid gid) const noexcept {
    auto *overlay = Lookup(gid);
    if (overlay == nullptr) return {};
    return {overlay, overlay->base_vertex};
  }

  // Find plus, if `with_deletes`, a copy of `direction`'s candidates under the shard's shared lock. Callers check each
  // candidate's delete mark afterwards, with no index lock held.
  // @throw std::bad_alloc past 16 candidates
  Expansion FindForExpansion(storage::Gid gid, storage::EdgeDirection direction, bool with_deletes) const {
    const Shard &shard = ShardFor(gid);
    Expansion found{Find(gid), {}};
    if (with_deletes) {
      std::shared_lock guard{shard.mutex};
      if (const auto it = shard.deletes.find(gid); it != shard.deletes.end()) {
        const auto &list = direction == storage::EdgeDirection::OUT ? it->second->out : it->second->in;
        found.candidates.assign(list.begin(), list.end());
      }
    }
    return found;
  }

  // Appends `edge_overlay` to `direction`'s candidates of `gid`, creating the entry if absent. The caller sets the
  // branched bit and the edge change filter bit first. A throw may leave an entry with the list unchanged.
  // @throw utils::OutOfMemoryException, std::bad_alloc
  void AppendDeleteCandidate(storage::Gid gid, storage::EdgeDirection direction, storage::Edge *edge_overlay) {
    Shard &shard = ShardFor(gid);
    std::unique_lock guard{shard.mutex};
    auto it = shard.deletes.find(gid);
    if (it == shard.deletes.end()) {
      it = shard.deletes.try_emplace(gid, memory::MakeDbAwareUnique<BaseEdgeDeletes>()).first;
    }
    (direction == storage::EdgeDirection::OUT ? it->second->out : it->second->in).push_back(edge_overlay);
  }

  // An overlay or a BaseEdgeDeletes entry exists for `gid`.
  bool HasEntry(storage::Gid gid) const noexcept {
    if (Lookup(gid) != nullptr) return true;
    const Shard &shard = ShardFor(gid);
    std::shared_lock guard{shard.mutex};
    return shard.deletes.contains(gid);
  }

  // Returns the overlay for `gid` (below the floor), creating one if none exists. The creator gives it `base_vertex`
  // and `base_labels` (the labels at the fork point, read by the caller: `base_vertex` is main's live vertex) before
  // publishing; later calls ignore both. Racing creators each build one; the first published stands.
  // @throw utils::OutOfMemoryException, std::bad_alloc
  storage::Vertex *GetOrCreateOverlay(storage::Gid gid, storage::Vertex *base_vertex,
                                      std::span<const storage::LabelId> base_labels) {
    MG_ASSERT(gid.AsUint() < floor_, "an overlay is only for a base vertex.");
    if (auto *found = Lookup(gid); found != nullptr) return found;
    auto overlay = memory::MakeDbAwareUnique<BaseVertexOverlay>(gid, base_vertex);
    overlay::CopyBaseLabels(overlay.get(), base_labels);
    auto *winner = Publish(gid, overlay.get());
    if (winner == overlay.get()) overlay.release();
    return winner;
  }

  // Calls `fn(overlay)` for every overlay in ascending gid order until it returns false. Concurrent creators may or may
  // not be visited. Returns true if every overlay was visited.
  template <std::predicate<storage::Vertex *> Fn>
  bool ForEachOverlay(Fn &&fn) const {
    absl::InlinedVector<BaseVertexOverlay *, 64> snapshot;
    for (size_t i = 0; i < span_count_; ++i) {
      const uintptr_t entry = spans_[i].load(std::memory_order_acquire);
      if (Pointer(entry) == 0) continue;
      snapshot.clear();
      if (IsDense(entry)) {
        for (size_t k = 0; k < kSpanGids; ++k) {
          if (auto *overlay = Slots(entry)[k].load(std::memory_order_acquire); overlay != nullptr) {
            snapshot.push_back(overlay);
          }
        }
      } else {
        const Chunk *chunk = AsChunk(entry);
        SeqRead(chunk, [&] {
          snapshot.clear();
          for (size_t k = 0, n = chunk->count.load(std::memory_order_acquire); k < n; ++k) {
            snapshot.push_back(chunk->Ptrs()[k].load(std::memory_order_relaxed));
          }
          return true;
        });
      }
      for (auto *overlay : snapshot) {
        if (!fn(static_cast<storage::Vertex *>(overlay))) return false;
      }
    }
    return true;
  }

 private:
  static constexpr size_t kShards = 64;
  static_assert(kSpanLog2 <= 16, "offsets are 16-bit");
  static constexpr uintptr_t kLocked = 1;
  static constexpr uintptr_t kDense = 2;

  struct Chunk {
    std::atomic<uint32_t> seq{0};  // odd while an in-place middle insert shifts entries
    std::atomic<uint16_t> count{0};
    uint16_t cap{0};
    const Chunk *next_retired{nullptr};  // while on `retired_`

    // `cap` overlay pointers (this + 1), then `cap` offsets; the first `count` of each are live, sorted by offset.

    std::atomic<BaseVertexOverlay *> *Ptrs() const noexcept {
      return reinterpret_cast<std::atomic<BaseVertexOverlay *> *>(const_cast<Chunk *>(this) + 1);
    }

    std::atomic<uint16_t> *Keys() const noexcept { return reinterpret_cast<std::atomic<uint16_t> *>(Ptrs() + cap); }

    static size_t BytesFor(size_t cap) noexcept { return sizeof(Chunk) + cap * (sizeof(void *) + sizeof(uint16_t)); }

    size_t Bytes() const noexcept { return BytesFor(cap); }
  };

  static_assert(sizeof(Chunk) == 16 && alignof(Chunk) == alignof(void *));

  static uintptr_t Pointer(uintptr_t entry) noexcept { return entry & ~(kLocked | kDense); }

  static bool IsDense(uintptr_t entry) noexcept { return (entry & kDense) != 0; }

  static Chunk *AsChunk(uintptr_t entry) noexcept { return reinterpret_cast<Chunk *>(Pointer(entry)); }

  static std::atomic<BaseVertexOverlay *> *Slots(uintptr_t entry) noexcept {
    return reinterpret_cast<std::atomic<BaseVertexOverlay *> *>(Pointer(entry));
  }

  static void Free(const Chunk *chunk) noexcept {
    memory::DbDeallocateBytes(const_cast<Chunk *>(chunk), chunk->Bytes(), alignof(Chunk));
  }

  // Readers may still be inside `chunk`: it is freed at destruction.
  void Retire(Chunk *chunk) noexcept {
    chunk->next_retired = retired_.load(std::memory_order_relaxed);
    while (!retired_.compare_exchange_weak(
        chunk->next_retired, chunk, std::memory_order_release, std::memory_order_relaxed)) {
    }
  }

  // Runs `read` on `chunk` until no in-place middle insert overlapped it.
  template <typename Read>
  static auto SeqRead(const Chunk *chunk, Read &&read) noexcept(noexcept(read())) {
    for (;;) {
      const uint32_t seq = chunk->seq.load(std::memory_order_acquire);
      if ((seq & 1) == 0) {
        auto result = read();
        std::atomic_thread_fence(std::memory_order_acquire);
        if (chunk->seq.load(std::memory_order_relaxed) == seq) return result;
      } else {
        std::this_thread::yield();
      }
    }
  }

  static unsigned LowerBound(const Chunk *chunk, unsigned n, unsigned offset) noexcept {
    const auto *keys = chunk->Keys();
    unsigned lo = 0;
    unsigned hi = n;
    while (lo < hi) {
      const unsigned mid = (lo + hi) / 2;
      if (keys[mid].load(std::memory_order_relaxed) < offset) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  // preserve_most keeps the callers' fast path free of register saves.
  [[gnu::noinline, clang::preserve_most]] static BaseVertexOverlay *LookupIn(const Chunk *chunk,
                                                                             unsigned offset) noexcept {
    return SeqRead(chunk, [&]() noexcept -> BaseVertexOverlay * {
      const unsigned n = chunk->count.load(std::memory_order_acquire);
      const unsigned i = LowerBound(chunk, n, offset);
      if (i < n && chunk->Keys()[i].load(std::memory_order_relaxed) == offset) {
        return chunk->Ptrs()[i].load(std::memory_order_relaxed);
      }
      return nullptr;
    });
  }

  BaseVertexOverlay *Lookup(storage::Gid gid) const noexcept {
    const uint64_t g = gid.AsUint();
    if (g >= floor_) return nullptr;
    const uintptr_t entry = spans_[g >> kSpanLog2].load(std::memory_order_acquire);
    if (Pointer(entry) == 0) return nullptr;
    const auto offset = static_cast<unsigned>(g & (kSpanGids - 1));
    if (IsDense(entry)) return Slots(entry)[offset].load(std::memory_order_acquire);
    return LookupIn(AsChunk(entry), offset);
  }

  // A chunk of capacity `cap` holding `old`'s entries (may be null) plus `overlay` inserted at `index`.
  // @throw utils::OutOfMemoryException, std::bad_alloc
  static Chunk *BuildChunk(const Chunk *old, size_t cap, unsigned index, unsigned offset, BaseVertexOverlay *overlay) {
    auto *chunk = static_cast<Chunk *>(
        memory::DbAllocateBytes(Chunk::BytesFor(cap), memory::tls_db_arena_state.arena, alignof(Chunk)));
    std::construct_at(chunk);
    chunk->cap = static_cast<uint16_t>(cap);
    auto *ptrs = chunk->Ptrs();
    auto *keys = chunk->Keys();
    for (size_t i = 0; i < cap; ++i) {
      std::construct_at(&ptrs[i], nullptr);
      std::construct_at(&keys[i], uint16_t{0});
    }
    const unsigned n = old != nullptr ? old->count.load(std::memory_order_relaxed) : 0;
    for (unsigned i = 0; i < index; ++i) {
      keys[i].store(old->Keys()[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
      ptrs[i].store(old->Ptrs()[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
    keys[index].store(static_cast<uint16_t>(offset), std::memory_order_relaxed);
    ptrs[index].store(overlay, std::memory_order_relaxed);
    for (unsigned i = index; i < n; ++i) {
      keys[i + 1].store(old->Keys()[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
      ptrs[i + 1].store(old->Ptrs()[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
    chunk->count.store(static_cast<uint16_t>(n + 1), std::memory_order_relaxed);
    return chunk;
  }

  // A dense array holding `old`'s entries plus `overlay` at `offset`.
  // @throw utils::OutOfMemoryException, std::bad_alloc
  static std::atomic<BaseVertexOverlay *> *BuildDense(const Chunk *old, unsigned offset, BaseVertexOverlay *overlay) {
    auto *slots = static_cast<std::atomic<BaseVertexOverlay *> *>(
        memory::DbAllocateBytes(kSpanGids * sizeof(void *), memory::tls_db_arena_state.arena, alignof(void *)));
    for (size_t i = 0; i < kSpanGids; ++i) std::construct_at(&slots[i], nullptr);
    for (unsigned i = 0, n = old->count.load(std::memory_order_relaxed); i < n; ++i) {
      slots[old->Keys()[i].load(std::memory_order_relaxed)].store(old->Ptrs()[i].load(std::memory_order_relaxed),
                                                                  std::memory_order_relaxed);
    }
    slots[offset].store(overlay, std::memory_order_relaxed);
    return slots;
  }

  // Makes `overlay` the one for `gid` unless one exists; returns the one that stands. The caller keeps ownership of
  // `overlay` unless the result equals it. A throw publishes nothing.
  // @throw utils::OutOfMemoryException, std::bad_alloc
  BaseVertexOverlay *Publish(storage::Gid gid, BaseVertexOverlay *overlay) {
    const uint64_t g = gid.AsUint();
    auto &entry = spans_[g >> kSpanLog2];
    const auto offset = static_cast<unsigned>(g & (kSpanGids - 1));

    // Spin on the entry's lock bit; the unlock stores the entry's new value (a bigger chunk after growth).
    uintptr_t current = entry.load(std::memory_order_relaxed);
    for (;;) {
      if ((current & kLocked) != 0) {
        std::this_thread::yield();
        current = entry.load(std::memory_order_relaxed);
      } else if (entry.compare_exchange_weak(
                     current, current | kLocked, std::memory_order_acquire, std::memory_order_relaxed)) {
        break;
      }
    }

    struct Unlock {
      std::atomic<uintptr_t> &entry;
      uintptr_t value;

      ~Unlock() { entry.store(value, std::memory_order_release); }
    } unlock{entry, current};

    if (IsDense(current)) {
      auto &slot = Slots(current)[offset];
      if (auto *existing = slot.load(std::memory_order_relaxed); existing != nullptr) return existing;
      slot.store(overlay, std::memory_order_release);
      return overlay;
    }
    Chunk *chunk = AsChunk(current);
    unsigned n = 0;
    unsigned index = 0;
    if (chunk != nullptr) {
      n = chunk->count.load(std::memory_order_relaxed);
      index = LowerBound(chunk, n, offset);
      if (index < n && chunk->Keys()[index].load(std::memory_order_relaxed) == offset) {
        return chunk->Ptrs()[index].load(std::memory_order_relaxed);
      }
    }
    if (chunk != nullptr && n == chunk->cap && n >= kDenseCap) {
      unlock.value = reinterpret_cast<uintptr_t>(BuildDense(chunk, offset, overlay)) | kDense;
      Retire(chunk);
      return overlay;
    }
    if (chunk == nullptr || n == chunk->cap) {
      Chunk *grown = BuildChunk(chunk, chunk == nullptr ? 1 : size_t{chunk->cap} * 2, index, offset, overlay);
      unlock.value = reinterpret_cast<uintptr_t>(grown);
      if (chunk != nullptr) Retire(chunk);
      return overlay;
    }
    auto *ptrs = chunk->Ptrs();
    auto *keys = chunk->Keys();
    const bool middle = index < n;
    if (middle) {
      chunk->seq.store(chunk->seq.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
      std::atomic_thread_fence(std::memory_order_release);
      for (unsigned i = n; i > index; --i) {
        keys[i].store(keys[i - 1].load(std::memory_order_relaxed), std::memory_order_relaxed);
        ptrs[i].store(ptrs[i - 1].load(std::memory_order_relaxed), std::memory_order_relaxed);
      }
    }
    keys[index].store(static_cast<uint16_t>(offset), std::memory_order_relaxed);
    ptrs[index].store(overlay, std::memory_order_relaxed);
    chunk->count.store(static_cast<uint16_t>(n + 1), std::memory_order_release);
    if (middle) chunk->seq.store(chunk->seq.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    return overlay;
  }

  struct alignas(64) Shard {
    mutable std::shared_mutex mutex;  // guards `deletes` and every BaseEdgeDeletes in it
    boost::unordered_flat_map<storage::Gid, memory::ArenaAwareUniquePtr<BaseEdgeDeletes>, std::hash<storage::Gid>>
        deletes;
  };

  Shard &ShardFor(storage::Gid gid) noexcept { return shards_[gid.AsUint() % kShards]; }

  const Shard &ShardFor(storage::Gid gid) const noexcept { return shards_[gid.AsUint() % kShards]; }

  uint64_t floor_;
  size_t span_count_;
  std::atomic<uintptr_t> *spans_{nullptr};
  std::atomic<const Chunk *> retired_{nullptr};
  std::array<Shard, kShards> shards_;
};

}  // namespace memgraph::versioning
