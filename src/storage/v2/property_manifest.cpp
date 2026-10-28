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

/// Orders a manifest's entries against a shape on the way in. Both are held by property id,
/// so one pass in step decides it.
auto CompareShapes(std::span<ManifestEntry const> lhs, RecordShape const &rhs) -> std::strong_ordering {
  auto left = lhs.begin();
  auto right = rhs.begin();
  for (; left != lhs.end() && right != rhs.end(); ++left, ++right) {
    if (auto const order = left->property <=> right->first; order != std::strong_ordering::equal) return order;
    if (auto const order = left->stored_type <=> right->second; order != std::strong_ordering::equal) return order;
  }
  if (left == lhs.end() && right == rhs.end()) return std::strong_ordering::equal;
  return left == lhs.end() ? std::strong_ordering::less : std::strong_ordering::greater;
}
}  // namespace

bool ManifestRegistry::ShapeOrder::operator()(ShapeKey lhs, ShapeKey rhs) const {
  return r::lexicographical_compare(lhs.manifest->entries(), rhs.manifest->entries());
}

bool ManifestRegistry::ShapeOrder::operator()(ShapeKey lhs, RecordShape const &rhs) const {
  return CompareShapes(lhs.manifest->entries(), rhs) == std::strong_ordering::less;
}

bool ManifestRegistry::ShapeOrder::operator()(RecordShape const &lhs, ShapeKey rhs) const {
  return CompareShapes(rhs.manifest->entries(), lhs) == std::strong_ordering::greater;
}

PropertyManifest::PropertyManifest(RecordShape const &shape) {
  // A shape is keyed by property id, so iterating it yields the entries sorted and without
  // duplicates, which is what `Find` searches and what the layout below assumes.
  entries_.reserve(shape.size());
  for (auto const &[property, stored_type] : shape) {
    entries_.push_back(ManifestEntry{.property = property, .stored_type = stored_type});
  }
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

auto PropertyManifest::LocationAt(size_t position) const -> Location {
  DMG_ASSERT(position < entries_.size(), "Asking a shape where an entry it does not have sits");
  auto const &stored_type = entries_[position].stored_type;
  return Location{
      .is_fixed = stored_type.is_fixed_width(),
      .offset = offsets_[position],
      .stored_type = stored_type,
  };
}

auto PropertyManifest::Find(PropertyId property) const -> std::optional<Location> {
  auto const it = r::lower_bound(entries_, property, {}, &ManifestEntry::property);
  if (it == entries_.end() || it->property != property) return std::nullopt;

  return LocationAt(static_cast<size_t>(it - entries_.begin()));
}

void ManifestRegistry::Publish(uint32_t id, PropertyManifest *manifest) {
  auto &slot = chunks_[id >> kChunkBits];
  auto *chunk = slot.load(std::memory_order_relaxed);
  if (chunk == nullptr) {
    // Reached only while holding the write lock, so there is no race to install it.
    chunk = new Chunk{};
    slot.store(chunk, std::memory_order_release);
  }
  (*chunk)[id & kChunkMask].store(manifest, std::memory_order_release);
  // Last, and the store that admits the id: the slot above must be filled before it.
  count_.store(id + 1, std::memory_order_release);
}

auto ManifestRegistry::Intern(RecordShape const &shape) -> ManifestId {
  // The common case is a shape that already exists, so look under a read lock first and
  // only take the write lock to install a genuinely new shape. Both comparisons run against
  // the shape as given, so neither path copies it.
  auto const existing = state_.WithReadLock([&](State const &state) -> std::optional<ManifestId> {
    auto const it = state.lookup.find(shape);
    if (it == state.lookup.end()) return std::nullopt;
    return it->second;
  });
  if (existing) return *existing;

  return state_.WithLock([&](State &state) {
    // Another thread may have installed this shape between the two locks.
    auto const it = state.lookup.find(shape);
    if (it != state.lookup.end()) return it->second;

    auto const id = static_cast<uint32_t>(state.manifests.size());
    MG_ASSERT(id < kMaxShapes, "A registry holds at most {} distinct record shapes", kMaxShapes);

    auto const &owned = state.manifests.emplace_back(std::make_unique<PropertyManifest>(shape));
    state.lookup.emplace(ShapeKey{owned.get()}, ManifestId{id});
    Publish(id, owned.get());
    return ManifestId{id};
  });
}

auto ManifestRegistry::Resolve(ManifestId id) const -> PropertyManifest const & {
  // No lock. The chunk spine is fixed at construction and a published slot is never
  // cleared, so neither load can observe a container being grown underneath it.
  DMG_ASSERT(id.value < count_.load(std::memory_order_acquire), "Resolving a manifest id this registry never issued");
  auto const *chunk = chunks_[id.value >> kChunkBits].load(std::memory_order_acquire);
  auto const *manifest = (*chunk)[id.value & kChunkMask].load(std::memory_order_acquire);
  return *manifest;
}

auto ManifestRegistry::size() const -> size_t { return count_.load(std::memory_order_acquire); }

ManifestRegistry::~ManifestRegistry() {
  // The manifests belong to `state_`; the chunks that point at them are this object's.
  for (auto &slot : chunks_) {
    delete slot.load(std::memory_order_relaxed);
  }
}

}  // namespace memgraph::storage
