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

#include <compare>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store_types.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

/// How one property is encoded in a record: its type, plus the payload width for the types
/// whose encoded width varies with the value. Carrying the width here is what lets integers
/// keep their 1/2/4/8-byte compression while their offset stays a property of the shape.
struct StoredType {
  PropertyStoreType type{};
  /// Encoded payload width in bytes; zero means the width varies per value.
  uint8_t width{};

  static constexpr StoredType Fixed(PropertyStoreType type, uint8_t width) { return {.type = type, .width = width}; }

  static constexpr StoredType Variable(PropertyStoreType type) { return {.type = type, .width = 0}; }

  constexpr bool is_fixed_width() const { return width != 0; }

  friend auto operator<=>(StoredType const &, StoredType const &) = default;
  friend bool operator==(StoredType const &, StoredType const &) = default;
};

struct ManifestEntry {
  PropertyId property;
  StoredType stored_type;

  friend auto operator<=>(ManifestEntry const &, ManifestEntry const &) = default;
  friend bool operator==(ManifestEntry const &, ManifestEntry const &) = default;
};

/// Identifies an interned manifest. This is what a record stores in place of its property
/// ids and types.
struct ManifestId {
  uint32_t value{};

  friend auto operator<=>(ManifestId const &, ManifestId const &) = default;
  friend bool operator==(ManifestId const &, ManifestId const &) = default;
};

/// The shape of a record: which properties it holds, how each is typed, and where each one
/// sits. Interned, so this is computed once and shared by every record of that shape.
///
/// Values are laid out with the fixed-width ones first, in property order, followed by the
/// variable-width ones, also in property order. A fixed value's byte offset is therefore
/// known from the shape alone; a variable value needs its bounds looked up in the record's
/// offset table, for which the shape supplies the index.
class PropertyManifest {
 public:
  struct Location {
    /// True when the value sits in the fixed region.
    bool is_fixed;
    /// Byte offset into the fixed region when `is_fixed`, otherwise the index into the
    /// record's variable-region offset table.
    uint32_t offset;
    StoredType stored_type;
  };

  explicit PropertyManifest(std::vector<ManifestEntry> entries);

  auto entries() const -> std::span<ManifestEntry const> { return entries_; }

  auto size() const -> size_t { return entries_.size(); }

  /// Total bytes the fixed-width values occupy in a record of this shape.
  auto fixed_region_size() const -> uint32_t { return fixed_region_size_; }

  /// How many values need an entry in the record's offset table.
  auto variable_count() const -> uint32_t { return variable_count_; }

  auto Find(PropertyId property) const -> std::optional<Location>;

 private:
  std::vector<ManifestEntry> entries_;
  /// Parallel to `entries_`: byte offset for fixed values, offset-table index for variable ones.
  std::vector<uint32_t> offsets_;
  uint32_t fixed_region_size_{};
  uint32_t variable_count_{};
};

/// Interns record shapes. Manifests are never removed and never move, so a resolved
/// reference stays valid for the lifetime of the registry.
class ManifestRegistry {
 public:
  ManifestRegistry() = default;

  ManifestRegistry(ManifestRegistry const &) = delete;
  ManifestRegistry &operator=(ManifestRegistry const &) = delete;
  ManifestRegistry(ManifestRegistry &&) = delete;
  ManifestRegistry &operator=(ManifestRegistry &&) = delete;

  ~ManifestRegistry() = default;

  /// Interns `entries`, which need not be sorted; the shape is canonicalised by property id
  /// so the order properties were written in never creates a second manifest.
  auto Intern(std::span<ManifestEntry const> entries) -> ManifestId;

  auto Resolve(ManifestId id) const -> PropertyManifest const &;

  /// Number of distinct shapes seen. Watch this: it is the manifest-explosion metric.
  auto size() const -> size_t;

 private:
  struct State {
    /// Held by unique_ptr so growing the vector never moves a manifest.
    std::vector<std::unique_ptr<PropertyManifest>> manifests;
    /// Canonical shape to its id, for recognising a shape that has been seen before.
    std::map<std::vector<ManifestEntry>, ManifestId> lookup;
  };

  mutable utils::Synchronized<State, utils::RWSpinLock> state_;
};

}  // namespace memgraph::storage
