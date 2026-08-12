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

auto ManifestRegistry::Intern(std::span<ManifestEntry const> entries) -> ManifestId {
  auto canonical = Canonicalise(entries);

  // The common case is a shape that already exists, so look under a read lock first and
  // only take the write lock to install a genuinely new shape.
  auto const existing = state_.WithReadLock([&](State const &state) -> std::optional<ManifestId> {
    auto const it = state.lookup.find(canonical);
    if (it == state.lookup.end()) return std::nullopt;
    return it->second;
  });
  if (existing) return *existing;

  return state_.WithLock([&](State &state) {
    // Another thread may have installed this shape between the two locks.
    auto const it = state.lookup.find(canonical);
    if (it != state.lookup.end()) return it->second;

    auto const id = ManifestId{static_cast<uint32_t>(state.manifests.size())};
    state.manifests.emplace_back(std::make_unique<PropertyManifest>(canonical));
    state.lookup.emplace(std::move(canonical), id);
    return id;
  });
}

auto ManifestRegistry::Resolve(ManifestId id) const -> PropertyManifest const & {
  // Safe to hand out after the lock drops: manifests are heap-stable and never removed.
  auto const *manifest = state_.WithReadLock([&](State const &state) {
    DMG_ASSERT(id.value < state.manifests.size(), "Resolving a manifest id this registry never issued");
    return state.manifests[id.value].get();
  });
  return *manifest;
}

auto ManifestRegistry::size() const -> size_t {
  return state_.WithReadLock([](State const &state) { return state.manifests.size(); });
}

}  // namespace memgraph::storage
