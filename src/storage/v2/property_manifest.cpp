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

#include "storage/v2/property_manifest.hpp"

#include <algorithm>
#include <utility>

#include "utils/logging.hpp"

namespace memgraph::storage {

namespace {
namespace r = std::ranges;

/// Canonical shape order: by property id, so the order properties were written in cannot
/// produce a second manifest for the same set.
auto Canonicalise(std::span<ManifestEntry const> entries) -> std::vector<ManifestEntry> {
  auto canonical = std::vector<ManifestEntry>{entries.begin(), entries.end()};
  r::sort(canonical, {}, &ManifestEntry::property);
  DMG_ASSERT(r::adjacent_find(canonical, {}, &ManifestEntry::property) == canonical.end(),
             "A manifest cannot hold the same property twice");
  return canonical;
}

/// The shape a thread interned last, and what it got. A load builds record after record of
/// the same shape, so this answers nearly every intern without touching anything shared.
struct InternMemo {
  uint64_t instance{0};
  std::vector<ManifestEntry> shape;
  ManifestId id;
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
thread_local InternMemo memo{};

}  // namespace

PropertyManifest::PropertyManifest(std::vector<ManifestEntry> entries) : entries_{std::move(entries)} {
  offsets_.resize(entries_.size());

  // Fixed-width values occupy the front of the record, so each one's byte offset is a
  // prefix sum over the shape and needs nothing from the record itself.
  for (size_t position = 0; position != entries_.size(); ++position) {
    auto const &stored_type = entries_[position].stored_type;
    if (!stored_type.is_fixed_width()) continue;
    offsets_[position] = fixed_region_size_;
    fixed_region_size_ += stored_type.width;
  }
  // Variable-width values follow, addressed by their index in the record's offset table.
  for (size_t position = 0; position != entries_.size(); ++position) {
    if (entries_[position].stored_type.is_fixed_width()) continue;
    offsets_[position] = variable_count_++;
  }
}

auto PropertyManifest::Find(PropertyId property) const -> std::optional<Location> {
  auto const it = r::lower_bound(entries_, property, {}, &ManifestEntry::property);
  if (it == entries_.end() || it->property != property) return std::nullopt;

  auto const position = static_cast<size_t>(it - entries_.begin());
  return Location{
      .is_fixed = it->stored_type.is_fixed_width(),
      .offset = offsets_[position],
      .stored_type = it->stored_type,
  };
}

void ManifestRegistry::Publish(ManifestId id, PropertyManifest *manifest) {
  auto const chunk_index = id.value >> kChunkBits;
  MG_ASSERT(chunk_index < kMaxChunks,
            "Ran out of room for record shapes; the data is far more heterogeneous than this "
            "store is built for");

  auto *chunk = chunks_[chunk_index].load(std::memory_order_acquire);
  if (chunk == nullptr) {
    auto *fresh = new Chunk{};
    if (!chunks_[chunk_index].compare_exchange_strong(
            chunk, fresh, std::memory_order_acq_rel, std::memory_order_acquire)) {
      // Another thread installed this chunk first; keep theirs.
      delete fresh;
    } else {
      chunk = fresh;
    }
  }
  // Release, so a thread that later finds this id through the skip list sees a whole manifest.
  chunk->at(id.value & kChunkMask).store(manifest, std::memory_order_release);
}

void ManifestRegistry::Remember(std::span<ManifestEntry const> shape, ManifestId id) const {
  // Assign rather than construct: a thread that keeps missing (every record a new shape)
  // would otherwise pay a fresh allocation for every miss.
  memo.instance = instance_;
  memo.shape.assign(shape.begin(), shape.end());
  memo.id = id;
}

auto ManifestRegistry::NextInstanceId() -> uint64_t {
  static std::atomic<uint64_t> counter{0};
  return counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

auto ManifestRegistry::Intern(std::span<ManifestEntry const> entries) -> ManifestId {
  // Only pay for a copy when the caller's entries are not already in shape order.
  auto canonical = std::vector<ManifestEntry>{};
  auto const sorted = r::is_sorted(entries, {}, &ManifestEntry::property);
  if (!sorted) canonical = Canonicalise(entries);
  auto const shape = sorted ? entries : std::span<ManifestEntry const>{canonical};
  DMG_ASSERT(r::adjacent_find(shape, {}, &ManifestEntry::property) == shape.end(),
             "A manifest cannot hold the same property twice");

  if (memo.instance == instance_ && r::equal(memo.shape, shape)) return memo.id;

  auto accessor = shapes_.access();
  if (auto const it = accessor.find(shape); it != accessor.end()) {
    Remember(shape, it->id);
    return it->id;
  }

  // Take an id, make the manifest readable at it, and only then let the shape be found. Two
  // threads racing on the same shape both publish; the one that loses the insert leaves an
  // id nothing resolves, which costs a manifest and buys a lock-free write path.
  auto const id = ManifestId{next_id_.fetch_add(1, std::memory_order_acq_rel)};
  auto owned = std::vector<ManifestEntry>{shape.begin(), shape.end()};
  Publish(id, new PropertyManifest{owned});
  auto const interned = accessor.insert(ShapeEntry{.shape = std::move(owned), .id = id}).first->id;
  Remember(shape, interned);
  return interned;
}

auto ManifestRegistry::Resolve(ManifestId id) const -> PropertyManifest const & {
  auto const chunk_index = id.value >> kChunkBits;
  DMG_ASSERT(chunk_index < kMaxChunks, "Resolving a manifest id this registry never issued");
  auto const *chunk = chunks_[chunk_index].load(std::memory_order_acquire);
  DMG_ASSERT(chunk != nullptr, "Resolving a manifest id this registry never issued");
  auto const *manifest = chunk->at(id.value & kChunkMask).load(std::memory_order_acquire);
  DMG_ASSERT(manifest != nullptr, "Resolving a manifest id this registry never issued");
  return *manifest;
}

auto ManifestRegistry::size() const -> size_t { return shapes_.size(); }

ManifestRegistry::~ManifestRegistry() {
  for (auto &slot : chunks_) {
    auto *chunk = slot.load(std::memory_order_acquire);
    if (chunk == nullptr) continue;
    for (auto &manifest : *chunk) delete manifest.load(std::memory_order_acquire);
    delete chunk;
  }
}

}  // namespace memgraph::storage
