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

/// How a record is described on the way in: one stored type per property. Keyed by property
/// id, as a set of properties is elsewhere, which is what makes a shape sorted and free of
/// duplicates without anyone checking. A shape that named a property twice, or named them out
/// of order, cannot be written down.
using RecordShape = std::map<PropertyId, StoredType>;

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

  explicit PropertyManifest(RecordShape const &shape);

  auto entries() const -> std::span<ManifestEntry const> { return entries_; }

  auto size() const -> size_t { return entries_.size(); }

  /// Total bytes the fixed-width values occupy in a record of this shape.
  auto fixed_region_size() const -> uint32_t { return fixed_region_size_; }

  /// How many values need an entry in the record's offset table.
  auto variable_count() const -> uint32_t { return variable_count_; }

  auto Find(PropertyId property) const -> std::optional<Location>;

  /// Where the entry at `position` in `entries()` sits. Reading a whole record walks the two
  /// together, so it never searches for a property it is already holding.
  auto LocationAt(size_t position) const -> Location;

 private:
  std::vector<ManifestEntry> entries_;
  /// Parallel to `entries_`, holding each one's `Location::offset`.
  std::vector<uint32_t> offsets_;
  uint32_t fixed_region_size_{};
  uint32_t variable_count_{};
};

/// Interns record shapes. Manifests are never removed and never move, so a resolved
/// reference stays valid for the lifetime of the registry.
///
/// Resolving takes no lock. Ids are dense from zero, so they index an array of chunks whose
/// spine is fixed at construction and whose slots are filled once: a reader follows two
/// loads to a manifest that another thread may be adding neighbours to, and never reads a
/// container that is being grown underneath it. Recognising a shape still takes the lock,
/// because that walks an ordered map, and it sits on the write path rather than on every
/// property read.
class ManifestRegistry {
 public:
  ManifestRegistry() = default;

  ManifestRegistry(ManifestRegistry const &) = delete;
  ManifestRegistry &operator=(ManifestRegistry const &) = delete;
  ManifestRegistry(ManifestRegistry &&) = delete;
  ManifestRegistry &operator=(ManifestRegistry &&) = delete;

  ~ManifestRegistry();

  /// Interns `shape`. Keying by property id is what makes the order properties were written
  /// in unable to produce a second manifest for the same set.
  auto Intern(RecordShape const &shape) -> ManifestId;

  auto Resolve(ManifestId id) const -> PropertyManifest const &;

  /// Number of distinct shapes seen. Watch this: it is the manifest-explosion metric.
  auto size() const -> size_t;

 private:
  static constexpr uint32_t kChunkBits = 6;
  static constexpr uint32_t kChunkSize = 1U << kChunkBits;
  static constexpr uint32_t kChunkMask = kChunkSize - 1;
  static constexpr uint32_t kMaxChunks = 1024;
  /// How many distinct shapes one registry can hold. Reaching it is refused rather than
  /// wrapped, and means the data's types or widths vary far more per record than expected.
  static constexpr uint32_t kMaxShapes = kChunkSize * kMaxChunks;

  using Chunk = std::array<std::atomic<PropertyManifest *>, kChunkSize>;

  /// A shape, named by the manifest that holds it rather than copied beside it.
  struct ShapeKey {
    PropertyManifest const *manifest;
  };

  /// Orders shapes by property and stored type, and does so for a `RecordShape` as well, so
  /// recognising a shape needs neither a manifest built for it first nor a copy of it.
  struct ShapeOrder {
    using is_transparent = void;

    bool operator()(ShapeKey lhs, ShapeKey rhs) const;
    bool operator()(ShapeKey lhs, RecordShape const &rhs) const;
    bool operator()(RecordShape const &lhs, ShapeKey rhs) const;
  };

  struct State {
    /// Held by unique_ptr so growing the vector never moves a manifest.
    std::vector<std::unique_ptr<PropertyManifest>> manifests;
    /// Shape to its id, for recognising a shape that has been seen before. Keyed on the
    /// manifest, so the shape itself is stored once.
    std::map<ShapeKey, ManifestId, ShapeOrder> lookup;
  };

  /// Makes `manifest` readable at `id`. The slot is written before the count that admits
  /// it, so a reader that can see the id sees a manifest that is already built.
  void Publish(uint32_t id, PropertyManifest *manifest);

  mutable utils::Synchronized<State, utils::RWSpinLock> state_;
  std::array<std::atomic<Chunk *>, kMaxChunks> chunks_{};
  std::atomic<uint32_t> count_{0};
};

}  // namespace memgraph::storage
