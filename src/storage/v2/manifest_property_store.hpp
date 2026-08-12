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
#include <cstdint>
#include <map>
#include <span>
#include <utility>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_manifest.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::storage {

/// A property store that keeps property ids and types in a shared manifest instead of in
/// every record, so locating a value costs a lookup in the shape rather than a scan over
/// the record.
///
/// A record is laid out as
///
///     [presence bits][offset table][fixed payload][variable payload]
///
/// The presence bits say which of the shape's fields this record actually carries, so
/// removing a property clears a bit and leaves the shape, the layout and the allocation
/// alone. The offset table holds one end-offset per variable-width field, at an entry width
/// sized to the record, and is present only when the shape has variable-width fields.
///
/// Small records live inside the object; larger ones move to the heap.
///
/// The registry is passed in rather than held, so a record costs nothing to point at the
/// shapes it shares with every other record.
///
/// Prototype scope: bool, integer, double, string, temporal data, enum and point. The
/// remaining property types (zoned temporal, list, map, vector) are not encodable yet and
/// will assert.
class ManifestPropertyStore {
 public:
  using PropertyPair = std::pair<PropertyId, PropertyValue>;

  ManifestPropertyStore() = default;

  ManifestPropertyStore(ManifestPropertyStore const &) = delete;
  ManifestPropertyStore &operator=(ManifestPropertyStore const &) = delete;
  ManifestPropertyStore(ManifestPropertyStore &&other) noexcept;
  ManifestPropertyStore &operator=(ManifestPropertyStore &&other) noexcept;

  ~ManifestPropertyStore();

  auto GetProperty(ManifestRegistry const &registry, PropertyId property) const -> PropertyValue;

  auto HasProperty(ManifestRegistry const &registry, PropertyId property) const -> bool;

  /// Reads a value whose place in the shape the caller has already worked out. A scan over
  /// records of one shape resolves that once and then reads every record with it, which is
  /// the whole point of the shape being shared. The caller is responsible for the location
  /// having come from this record's shape; check `manifest()` before trusting it.
  auto GetProperty(PropertyManifest const &manifest, PropertyManifest::Location location) const -> PropertyValue;

  /// The properties this record carries, in property order.
  auto Properties(ManifestRegistry const &registry) const -> utils::small_vector<PropertyPair>;

  /// Returns true when the property was not already present.
  auto SetProperty(ManifestRegistry &registry, PropertyId property, PropertyValue const &value) -> bool;

  /// Sets every property at once on a record that has none, interning a single shape rather
  /// than one per property added. Returns false, changing nothing, if the record is not
  /// empty. `properties` must be sorted by property id.
  auto InitProperties(ManifestRegistry &registry, std::span<PropertyPair const> properties) -> bool;

  /// Convenience overload for callers that already hold a map.
  auto InitProperties(ManifestRegistry &registry, std::map<PropertyId, PropertyValue> const &properties) -> bool;

  /// Returns true when there was anything to remove.
  auto ClearProperties() -> bool;

  auto manifest() const -> ManifestId { return manifest_; }

  /// Encoded bytes this record holds, excluding the shared manifest.
  auto buffer_size() const -> uint32_t { return size_; }

 private:
  static constexpr uint32_t kInlineCapacity = 8;

  /// Re-interns the shape from the fields that carry a value, and re-encodes the record.
  void Rebuild(ManifestRegistry &registry, std::span<PropertyPair const> properties);

  auto data() -> uint8_t * { return size_ <= kInlineCapacity ? inline_.data() : heap_; }

  auto data() const -> uint8_t const * { return size_ <= kInlineCapacity ? inline_.data() : heap_; }

  /// Takes ownership of `size` bytes of storage, freeing whatever the record held before.
  auto Reset(uint32_t size) -> uint8_t *;

  ManifestId manifest_{};
  uint32_t size_{};

  union {
    uint8_t *heap_;
    std::array<uint8_t, kInlineCapacity> inline_;
  };
};

static_assert(sizeof(ManifestPropertyStore) == 16, "A record should cost no more than its shape id, size and storage");

}  // namespace memgraph::storage
