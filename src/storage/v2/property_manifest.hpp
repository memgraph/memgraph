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
#include <compare>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store_types.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

/// How one property is encoded in a record: its type, plus the payload width for the types
/// whose encoded width varies with the value. Carrying the width here is what lets integers
/// keep their 1/2/4/8-byte compression while their offset stays a property of the shape.
struct StoredType {
  PropertyStoreType type{};
  /// Encoded payload width in bytes; zero means the width varies per value.
  uint8_t width{};
  /// For the types whose value carries a discriminator of its own, that discriminator: which
  /// temporal type, which enum type, which coordinate system. Keeping it here rather than in
  /// the record is what makes those types fixed width.
  uint32_t discriminator{};

  static constexpr StoredType Fixed(PropertyStoreType type, uint8_t width, uint32_t discriminator = 0) {
    return {.type = type, .width = width, .discriminator = discriminator};
  }

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

/// Orders two entries by their shape class: everything about them except how wide an integer
/// happens to be. Integers are the only type whose stored width varies with the value rather
/// than with the type, so leaving that width out of the ordering is what makes the width
/// variants of one shape order and compare as one.
///
/// This sits on the interning path, where a lookup runs it once per entry per skip list node
/// it passes, so it answers in one pass rather than through a pair of `<` comparisons.
constexpr auto ClassCompare(ManifestEntry const &lhs, ManifestEntry const &rhs) -> std::strong_ordering {
  if (auto const order = lhs.property <=> rhs.property; order != 0) return order;
  if (auto const order = lhs.stored_type.type <=> rhs.stored_type.type; order != 0) return order;
  if (auto const order = lhs.stored_type.discriminator <=> rhs.stored_type.discriminator; order != 0) return order;
  if (lhs.stored_type.type == PropertyStoreType::INT) return std::strong_ordering::equal;
  return lhs.stored_type.width <=> rhs.stored_type.width;
}

constexpr auto SameShapeClass(std::span<ManifestEntry const> lhs, std::span<ManifestEntry const> rhs) -> bool {
  if (lhs.size() != rhs.size()) return false;
  for (size_t position = 0; position != lhs.size(); ++position) {
    if (ClassCompare(lhs[position], rhs[position]) != 0) return false;
  }
  return true;
}

constexpr auto ShapeClassLess(std::span<ManifestEntry const> lhs, std::span<ManifestEntry const> rhs) -> bool {
  auto const common = std::min(lhs.size(), rhs.size());
  for (size_t position = 0; position != common; ++position) {
    if (auto const order = ClassCompare(lhs[position], rhs[position]); order != 0) return order < 0;
  }
  return lhs.size() < rhs.size();
}

/// True when `wide` can stand in for `narrow`: the same fields, in the same order, at the
/// same types, with every integer at least as wide. Every value `narrow` can hold encodes
/// into `wide` unchanged, so a caller that asked for `narrow` loses nothing by being handed
/// `wide` instead.
constexpr auto Dominates(std::span<ManifestEntry const> wide, std::span<ManifestEntry const> narrow) -> bool {
  if (wide.size() != narrow.size()) return false;
  for (size_t position = 0; position != wide.size(); ++position) {
    if (ClassCompare(wide[position], narrow[position]) != 0) return false;
    // Outside of integers the class already fixes the width, so this only tests integers.
    if (wide[position].stored_type.width < narrow[position].stored_type.width) return false;
  }
  return true;
}

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
    /// Which field of the shape this is, and so which of the record's presence bits says
    /// whether the record carries a value for it.
    uint32_t position;
  };

  explicit PropertyManifest(std::vector<ManifestEntry> entries);

  auto entries() const -> std::span<ManifestEntry const> { return entries_; }

  auto size() const -> size_t { return entries_.size(); }

  /// Total bytes the fixed-width values occupy in a record of this shape.
  auto fixed_region_size() const -> uint32_t { return fixed_region_size_; }

  /// How many values need an entry in the record's offset table.
  auto variable_count() const -> uint32_t { return variable_count_; }

  auto Find(PropertyId property) const -> std::optional<Location>;

  /// Where the `position`-th entry of this shape lives. Lets a caller that already walks the
  /// shape in order skip the search entirely.
  auto LocationAt(size_t position) const -> Location;

 private:
  std::vector<ManifestEntry> entries_;
  /// The ids of `entries_`, packed on their own so a search reads only the ids and touches
  /// half the cache lines that walking the entries would.
  std::vector<PropertyId> ids_;
  /// Parallel to `entries_`: byte offset for fixed values, offset-table index for variable ones.
  std::vector<uint32_t> offsets_;
  uint32_t fixed_region_size_{};
  uint32_t variable_count_{};
};

class ManifestRegistry;

/// Remembers where a property sat in the shape a record was last read at, so a reader walking
/// record after record resolves each property once rather than per read.
///
/// It amortises over whatever holds it. Held by an expression evaluator it therefore pays off in
/// an operator that pulls its whole input under one evaluator, as the aggregation does, and not
/// in one that builds an evaluator per row.
///
/// Every answer is kept with the shape and the registry it came from, and checked against the
/// record being read, because a location only means anything for the shape it was resolved in.
/// A property the shape does not hold is remembered as such: on a scan over records that do not
/// carry it, that is the answer being asked for over and over.
///
/// Holds a few properties at once, directly mapped, because a row typically reads more than one
/// and a single slot would be evicted by the next property before the next row could use it.
class PropertyLocationMemo {
 public:
  struct Answer {
    /// The shape itself. Manifests are never freed and never move while their registry lives, so
    /// holding one saves resolving the id again on every hit.
    PropertyManifest const *manifest;
    /// Where the property sits, or nothing when the shape does not hold it.
    std::optional<PropertyManifest::Location> location;
  };

  /// What was remembered for `property` in the shape `manifest` of the registry `instance`, if
  /// anything. The registry is named by its instance number rather than its address, so a memo
  /// cannot answer with something a registry that has since been destroyed told it.
  auto Lookup(uint64_t instance, ManifestId manifest, PropertyId property) const -> std::optional<Answer> {
    auto const &slot = slots_[SlotFor(property)];
    if (!slot.filled || slot.instance != instance || slot.manifest != manifest || slot.property != property) {
      return std::nullopt;
    }
    return Answer{slot.shape, slot.location};
  }

  void Remember(uint64_t instance, ManifestId manifest, PropertyManifest const *shape, PropertyId property,
                std::optional<PropertyManifest::Location> location) {
    slots_[SlotFor(property)] = Slot{.instance = instance,
                                     .manifest = manifest,
                                     .shape = shape,
                                     .property = property,
                                     .location = location,
                                     .filled = true};
  }

 private:
  /// Enough for the several properties a row reads, and small enough to stay in one cache line
  /// pair. A power of two so the mapping is a mask.
  static constexpr size_t kSlots = 8;

  static constexpr auto SlotFor(PropertyId property) -> size_t { return property.AsUint() & (kSlots - 1); }

  struct Slot {
    uint64_t instance{0};
    ManifestId manifest{};
    PropertyManifest const *shape{nullptr};
    PropertyId property{PropertyId::FromUint(0)};
    std::optional<PropertyManifest::Location> location{};
    bool filled{false};
  };

  std::array<Slot, kSlots> slots_{};
};

/// Interns record shapes. Manifests are never removed and never move, so a resolved
/// reference stays valid for the lifetime of the registry.
///
/// Neither direction takes a lock. Recognising a shape goes through a skip list, because
/// interning happens on every write that changes a record's shape and a load does that from
/// every thread at once. Resolving an id indexes into an append-only chunked array, because
/// that sits on the per-property read path where even a skip list's epoch bookkeeping would
/// cost more than the read.
class ManifestRegistry {
 public:
  ManifestRegistry() = default;

  ManifestRegistry(ManifestRegistry const &) = delete;
  ManifestRegistry &operator=(ManifestRegistry const &) = delete;
  ManifestRegistry(ManifestRegistry &&) = delete;
  ManifestRegistry &operator=(ManifestRegistry &&) = delete;

  ~ManifestRegistry();

  /// Interns `entries`, which need not be sorted; the shape is canonicalised by property id
  /// so the order properties were written in never creates a second manifest. Passing them
  /// already sorted avoids a copy.
  ///
  /// Shapes that differ only by how wide their integers are do not each get a manifest. A
  /// shape whose integers all fit in one already known is answered with that one, and a
  /// shape that is wider moves its class up to the widest of the two. The returned manifest
  /// therefore stores every field at least as wide as asked for, which is what a caller has
  /// to expect: encode and decode through the manifest, never through the widths handed in.
  auto Intern(std::span<ManifestEntry const> entries) -> ManifestId;

  auto Resolve(ManifestId id) const -> PropertyManifest const &;

  /// Number of distinct shapes seen. Watch this: it is the manifest-explosion metric.
  auto size() const -> size_t;

  /// Distinguishes this registry from any other, including one that reuses its address after it
  /// is destroyed. Anything holding on to an answer this registry gave should key on this rather
  /// than on the registry's address.
  auto instance() const -> uint64_t { return instance_; }

 private:
  static constexpr uint32_t kChunkBits = 6;
  static constexpr uint32_t kChunkSize = 1U << kChunkBits;
  static constexpr uint32_t kChunkMask = kChunkSize - 1;
  static constexpr uint32_t kMaxChunks = 1024;

  using Chunk = std::array<std::atomic<PropertyManifest *>, kChunkSize>;

  /// A shape and the id it was given. Ordered and compared by the shape alone, including
  /// against a bare span so a lookup needs no copy of the shape it is looking for.
  struct ShapeEntry {
    std::vector<ManifestEntry> shape;
    ManifestId id;

    bool operator<(ShapeEntry const &other) const { return shape < other.shape; }

    bool operator==(ShapeEntry const &other) const { return shape == other.shape; }

    bool operator<(std::span<ManifestEntry const> other) const {
      return std::lexicographical_compare(shape.begin(), shape.end(), other.begin(), other.end());
    }

    bool operator==(std::span<ManifestEntry const> other) const {
      return std::equal(shape.begin(), shape.end(), other.begin(), other.end());
    }
  };

  /// The widest shape interned so far for one shape class, meaning one set of fields at one
  /// set of types, however wide its integers happen to be. Interning any member of the class
  /// hands back this shape, so a load whose integers straddle a width boundary builds one
  /// manifest rather than the cross product of its fields' widths.
  ///
  /// Only ever widened, never retired: a record already encoded to a narrower shape of the
  /// class keeps that shape and goes on decoding through it.
  struct PromotionEntry {
    /// Any shape of the class; only its width-erased form is ever compared.
    std::vector<ManifestEntry> shape_class;
    /// Id of the widest shape seen for the class. Read and updated through `std::atomic_ref`
    /// rather than held as an atomic, because the skip list has to be able to move the entry
    /// into place. The widths that go with it are read from the manifest the id names, so one
    /// load gives a consistent pair.
    uint32_t widest;

    bool operator<(PromotionEntry const &other) const { return ShapeClassLess(shape_class, other.shape_class); }

    bool operator==(PromotionEntry const &other) const { return SameShapeClass(shape_class, other.shape_class); }

    bool operator<(std::span<ManifestEntry const> other) const { return ShapeClassLess(shape_class, other); }

    bool operator==(std::span<ManifestEntry const> other) const { return SameShapeClass(shape_class, other); }
  };

  /// Makes `manifest` readable at `id`. Published before the id can be found, so a thread
  /// that sees the id sees a complete manifest.
  void Publish(ManifestId id, PropertyManifest *manifest);

  /// Interns `shape` exactly as given, creating a manifest for it if this is the first time
  /// it has been seen. Knows nothing about width classes.
  auto InternCanonical(std::span<ManifestEntry const> shape) -> ManifestId;

  /// The manifest `shape` is served by: the widest shape of its class, widening the class
  /// first if `shape` is wider than anything seen for it.
  auto Promote(std::span<ManifestEntry const> shape) -> ManifestId;

  /// Distinguishes this registry from any other, including one that reuses its address
  /// after it is destroyed. The per-thread memo is keyed on it so it can never answer with
  /// an id that belonged to a registry that is gone.
  uint64_t instance_{NextInstanceId()};

  static auto NextInstanceId() -> uint64_t;

  /// Points this thread's memo at `shape`, so the next record of the same shape skips the
  /// registry entirely.
  void Remember(std::span<ManifestEntry const> shape, ManifestId id) const;

  utils::SkipList<ShapeEntry> shapes_;
  utils::SkipList<PromotionEntry> promotions_;
  std::array<std::atomic<Chunk *>, kMaxChunks> chunks_{};
  std::atomic<uint32_t> next_id_{0};
};

}  // namespace memgraph::storage
