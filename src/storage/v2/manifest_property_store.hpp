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
#include <bit>
#include <cstdint>
#include <cstring>
#include <map>
#include <span>
#include <utility>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_manifest.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/exceptions.hpp"
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
/// throw `UnsupportedType`.
class ManifestPropertyStore {
 public:
  using PropertyPair = std::pair<PropertyId, PropertyValue>;

  /// Thrown when a value's property type has no encoding yet. Thrown before the record is
  /// touched, so the store is left as it was.
  class UnsupportedType : public utils::BasicException {
   public:
    using BasicException::BasicException;
    SPECIALIZE_GET_EXCEPTION_NAME(UnsupportedType)
  };

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

  /// Whether the record holds `value` for `property`, comparing against the encoded bytes
  /// rather than decoding them. A property the record does not carry equals Null.
  ///
  /// Values are compared, not encodings: an integer stored at one width equals the same
  /// integer stored at another, and an integer equals a double of the same value, as
  /// `PropertyValue::operator==` has them.
  auto IsPropertyEqual(ManifestRegistry const &registry, PropertyId property, PropertyValue const &value) const -> bool;

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

  /// Lays a record out for fields it is about to be given, leaving all of them absent. A
  /// caller that knows which properties it will set, as a planner does for the properties
  /// named in a query, can then set each one straight into its slot instead of reshaping the
  /// record once per property. A value that does not match the type or width reserved for it
  /// still works, at the cost of the reshaping this avoids.
  void ReserveFields(ManifestRegistry &registry, std::span<ManifestEntry const> fields);

  /// Returns true when there was anything to remove.
  auto ClearProperties() -> bool;

  auto manifest() const -> ManifestId { return manifest_; }

  /// Bytes of storage this record occupies, excluding the shared manifest. A record that fits
  /// inline owns the whole inline payload whatever it encodes into, so that is what it reports.
  auto buffer_size() const -> uint32_t {
    if (empty()) return 0;
    return is_inline() ? kInlineCapacity : encoded_size();
  }

 private:
  /// Payload bytes a record gets without allocating. One byte of `buffer_` is spent marking
  /// that the record is inline, so the payload is that one byte short of the buffer.
  static constexpr uint32_t kInlineCapacity = sizeof(uint32_t) + sizeof(uint8_t *) - 1;

  /// Written to the first byte of an inline record, whose remaining bytes are its payload.
  /// Not a multiple of eight, and heap sizes are, so the first four bytes read as a size tell
  /// the two apart whatever payload follows the marker.
  static constexpr uint8_t kInlineMarker = 1;
  static_assert(kInlineMarker % 8 != 0, "The inline marker has to be distinguishable from a heap record's size");
  static_assert(std::endian::native == std::endian::little, "The inline marker has to be the low byte of the size");

  /// Re-interns the shape from the fields that carry a value, and re-encodes the record.
  void Rebuild(ManifestRegistry &registry, std::span<PropertyPair const> properties);

  /// The first four bytes of `buffer_` read as a size: zero when the record is empty, the
  /// allocated byte count when it is on the heap, and a value carrying `kInlineMarker` in its
  /// low byte when it is inline.
  auto encoded_size() const -> uint32_t {
    uint32_t size = 0;
    std::memcpy(&size, buffer_.data(), sizeof(size));
    return size;
  }

  auto empty() const -> bool { return encoded_size() == 0; }

  auto is_inline() const -> bool { return (encoded_size() % 8) != 0; }

  auto heap_data() const -> uint8_t * {
    uint8_t *heap = nullptr;
    std::memcpy(static_cast<void *>(&heap), buffer_.data() + sizeof(uint32_t), sizeof(heap));
    return heap;
  }

  /// Points the record at `size` bytes of heap storage. `size` must be a multiple of eight so
  /// it stays distinguishable from an inline record.
  void SetHeap(uint32_t size, uint8_t *heap) {
    std::memcpy(buffer_.data(), &size, sizeof(size));
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    std::memcpy(buffer_.data() + sizeof(size), static_cast<void const *>(&heap), sizeof(heap));
  }

  auto data() -> uint8_t * { return is_inline() ? buffer_.data() + 1 : heap_data(); }

  auto data() const -> uint8_t const * { return is_inline() ? buffer_.data() + 1 : heap_data(); }

  /// Takes ownership of `size` bytes of storage, freeing whatever the record held before.
  auto Reset(uint32_t size) -> uint8_t *;

  /// Frees the record's storage, if it had any of its own. Leaves `buffer_` as it was.
  void Release();

  ManifestId manifest_{};
  /// Either the record itself, marker byte first, or the size and address of the heap storage
  /// holding it. Overlapping the two is what keeps a record that fits inline to eleven bytes
  /// of payload without growing the object past a shape id and one word of storage.
  std::array<uint8_t, 1 + kInlineCapacity> buffer_{};
};

static_assert(sizeof(ManifestPropertyStore) == 16, "A record should cost no more than its shape id, size and storage");

}  // namespace memgraph::storage
