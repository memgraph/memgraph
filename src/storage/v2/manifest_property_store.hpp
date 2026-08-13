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
#include <optional>
#include <set>
#include <span>
#include <utility>
#include <vector>

#include "storage/v2/constraints/type_constraints_validator.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_manifest.hpp"
#include "storage/v2/property_store_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/exceptions.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::storage {

struct PropertyPath;
template <typename T>
struct IndexedPropertyDecoder;

/// A property store that keeps property ids and types in a shared manifest instead of in
/// every record, so locating a value costs a lookup in the shape rather than a scan over
/// the record.
///
/// A record is laid out as
///
///     [manifest id][presence bits][offset table][fixed payload][variable payload]
///
/// The manifest id is part of the record's bytes rather than a member of the object, so the
/// object is the twelve bytes of its storage and nothing else: embedded in a vertex it then
/// packs into the same sixteen-byte slot as the lock beside it, and a vertex with no
/// properties at all pays nothing for the shape it does not have.
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
/// A list or a map is one variable-width value like a string is: a self-describing blob of
/// bytes in the variable region, tagged element by element. Nothing inside it is addressable
/// on its own, so a nested value costs a walk of the blob rather than a shape of its own.
///
/// A zoned temporal is stored as its UTC instant plus its timezone, and the two kinds of
/// timezone are two stored types: a fixed offset is small enough to sit in the shape's
/// discriminator, leaving the payload fixed width, while a named zone's name is a string of
/// unbounded length and lives in the variable region. A record holding a named zone therefore
/// has a different shape from one holding an offset.
///
/// Prototype scope: bool, integer, double, string, list, map, temporal data, zoned temporal
/// data, enum and point. The remaining property types (vector) are not encodable yet and throw
/// `UnsupportedType`, wherever in a value they appear.
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

  /// As above, but resolving a value the record only holds a handle to, as a value offloaded
  /// into the vector index is. No stored type is offloaded yet, so today this decodes nothing
  /// the plain overload would not; it is here so a caller reads through the decoder from the
  /// start and keeps working once one is.
  template <typename T>
  auto GetProperty(ManifestRegistry const &registry, PropertyId property, IndexedPropertyDecoder<T> const &decoder,
                   PropertyLocationMemo *memo = nullptr) const -> PropertyValue;

  auto HasProperty(ManifestRegistry const &registry, PropertyId property) const -> bool;

  /// Reads a value whose place in the shape the caller has already worked out. A scan over
  /// records of one shape resolves that once and then reads every record with it, which is
  /// the whole point of the shape being shared. The caller is responsible for the location
  /// having come from this record's shape; check `manifest()` before trusting it.
  auto GetProperty(PropertyManifest const &manifest, PropertyManifest::Location location) const -> PropertyValue;

  /// As the plain read, but remembering where the property sat in the shape it was last read
  /// from, so a caller reading one property from record after record resolves it once. The memo
  /// is checked against this record's own shape, so a record shaped differently, or no longer
  /// shaped as it was, resolves afresh rather than reading at the wrong offset.
  auto GetProperty(ManifestRegistry const &registry, PropertyId property, PropertyLocationMemo &memo) const
      -> PropertyValue;

  /// Whether the record holds `value` for `property`, comparing against the encoded bytes
  /// rather than decoding them. A property the record does not carry equals Null.
  ///
  /// Values are compared, not encodings: an integer stored at one width equals the same
  /// integer stored at another, and an integer equals a double of the same value, as
  /// `PropertyValue::operator==` has them.
  auto IsPropertyEqual(ManifestRegistry const &registry, PropertyId property, PropertyValue const &value) const -> bool;

  /// The properties this record carries, in property order.
  auto Properties(ManifestRegistry const &registry) const -> utils::small_vector<PropertyPair>;

  /// As above, with each value resolved through `decoder`. See the decoding `GetProperty`.
  template <typename T>
  auto Properties(ManifestRegistry const &registry, IndexedPropertyDecoder<T> const &decoder) const
      -> utils::small_vector<PropertyPair>;

  /// How each property this record carries is typed. Answered from the shape, which holds
  /// every discriminator a type of its own is made of, so no payload is read.
  auto ExtendedPropertyTypes(ManifestRegistry const &registry) const -> std::map<PropertyId, ExtendedPropertyType>;

  /// How `property` is typed, or Null when the record does not carry it. Read from the shape.
  auto GetExtendedPropertyType(ManifestRegistry const &registry, PropertyId property) const -> ExtendedPropertyType;

  /// The ids of the properties this record carries, in property order. Read from the shape.
  auto ExtractPropertyIds(ManifestRegistry const &registry) const -> std::vector<PropertyId>;

  /// The properties this record carries at one of `types`, in property order. Read from the
  /// shape: a type is a property of the shape, so which fields qualify costs no payload read.
  auto PropertiesOfTypes(ManifestRegistry const &registry, std::span<PropertyStoreType const> types) const
      -> std::vector<PropertyId>;

  /// The value of `property` when the record carries it at one of `types`, otherwise nothing.
  /// The type is decided from the shape, so a property of the wrong type is rejected without
  /// its payload being read.
  auto GetPropertyOfTypes(ManifestRegistry const &registry, PropertyId property,
                          std::span<PropertyStoreType const> types) const -> std::optional<PropertyValue>;

  /// Bytes `property`'s value occupies in this record, or zero when the record does not carry
  /// it. Fixed-width values cost what their shape reserves them; a variable-width value costs
  /// its extent in the variable region.
  auto PropertySize(ManifestRegistry const &registry, PropertyId property) const -> uint32_t;

  /// Whether the record carries every one of `properties`. Read from the shape.
  auto HasAllProperties(ManifestRegistry const &registry, std::set<PropertyId> const &properties) const -> bool;

  /// The values of `properties`, in property order, or nothing when the record is missing any
  /// of them.
  auto ExtractPropertyValues(ManifestRegistry const &registry, std::set<PropertyId> const &properties) const
      -> std::optional<std::vector<PropertyValue>>;

  /// The values `ordered_properties` name, Null wherever the record has no value for one. A
  /// path of more than one element reads into the nested maps of the top-level property it
  /// starts at. `ordered_properties` must be sorted, so paths that share a top-level property
  /// are together and decode it once between them.
  auto ExtractPropertyValuesMissingAsNull(ManifestRegistry const &registry,
                                          std::span<PropertyPath const> ordered_properties) const
      -> std::vector<PropertyValue>;

  /// As above, but writes into the caller's `out`, one slot per path, so a fixed-size buffer
  /// can be filled without allocating. `out.size()` must equal `ordered_properties.size()`.
  void ExtractPropertyValuesMissingAsNull(ManifestRegistry const &registry,
                                          std::span<PropertyPath const> ordered_properties,
                                          std::span<PropertyValue> out) const;

  /// As above for top-level properties only, so a caller reading a fixed set of plain
  /// properties needs neither a path per property nor any ordering between them. Each entry of
  /// `properties` fills the slot of `out` at the same index. `out.size()` must equal
  /// `properties.size()`.
  void ExtractPropertyValuesMissingAsNull(ManifestRegistry const &registry, std::span<PropertyId const> properties,
                                          std::span<PropertyValue> out) const;

  /// Whether each of `ordered_properties` holds the value it is paired with, the value for
  /// `ordered_properties[i]` being `values[position_lookup[i]]`. A path the record has no
  /// value for holds Null. Top-level paths are compared against the encoded bytes rather than
  /// decoded, as `IsPropertyEqual` does.
  auto ArePropertiesEqual(ManifestRegistry const &registry, std::span<PropertyPath const> ordered_properties,
                          std::span<PropertyValue const> values, std::span<std::size_t const> position_lookup) const
      -> std::vector<bool>;

  /// The first of `constraint`'s type constraints this record's properties break, if any.
  /// Read from the shape, which carries both the stored type and the temporal type a
  /// constraint is checked against.
  auto PropertiesMatchTypes(ManifestRegistry const &registry, TypeConstraintsValidator const &constraint) const
      -> std::optional<PropertyStoreConstraintViolation>;

  /// Returns true when the property was not already present.
  auto SetProperty(ManifestRegistry &registry, PropertyId property, PropertyValue const &value) -> bool;

  /// Sets every property at once on a record that has none, interning a single shape rather
  /// than one per property added. Returns false, changing nothing, if the record is not
  /// empty. `properties` must be sorted by property id.
  auto InitProperties(ManifestRegistry &registry, std::span<PropertyPair const> properties) -> bool;

  /// Convenience overload for callers that already hold a map.
  auto InitProperties(ManifestRegistry &registry, std::map<PropertyId, PropertyValue> const &properties) -> bool;

  /// Merges `properties` into the record, and reports every property whose value the merge
  /// decided: one entry per property `properties` names, holding the value the record held for
  /// it (Null when it held none) and the value it now holds. A property the record held and
  /// `properties` does not name is left as it was and not reported, but is added to
  /// `properties`, which is left holding everything the record now carries.
  auto UpdateProperties(ManifestRegistry &registry, std::map<PropertyId, PropertyValue> &properties)
      -> std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>>;

  /// Lays a record out for fields it is about to be given, leaving all of them absent. A
  /// caller that knows which properties it will set, as a planner does for the properties
  /// named in a query, can then set each one straight into its slot instead of reshaping the
  /// record once per property. A value that does not match the type or width reserved for it
  /// still works, at the cost of the reshaping this avoids.
  void ReserveFields(ManifestRegistry &registry, std::span<ManifestEntry const> fields);

  /// Returns true when there was anything to remove.
  auto ClearProperties() -> bool;

  /// The shape this record is encoded to, read from the record's own bytes. An empty record
  /// has no shape.
  auto manifest() const -> ManifestId {
    if (empty()) return ManifestId{};
    // Four bytes are read for the three the id occupies, which is always a read the record's
    // storage can answer: inline records live in `buffer_`, which has eleven bytes past the
    // marker, and a heap record only exists once it outgrows those eleven, so its allocation
    // is at least sixteen bytes.
    uint32_t id = 0;
    std::memcpy(&id, storage(), sizeof(id));
    return ManifestId{id & kManifestIdMask};
  }

  /// Bytes of storage this record occupies, excluding the shared manifest. A record that fits
  /// inline owns the whole inline capacity whatever it encodes into, so that is what it
  /// reports. The manifest id counts, being part of the record's bytes.
  auto buffer_size() const -> uint32_t {
    if (empty()) return 0;
    return is_inline() ? kInlineCapacity : encoded_size();
  }

 private:
  /// Bytes a record spends on its manifest id, which its bytes start with. Three of them cap
  /// the number of distinct shapes at sixteen million, far above the cap the registry already
  /// imposes on itself (`kMaxChunks` manifests), and leave one more byte of payload inline
  /// than a fourth byte would.
  static constexpr uint32_t kManifestIdWidth = 3;
  static constexpr uint32_t kManifestIdMask = (1U << (8U * kManifestIdWidth)) - 1U;

  /// Record bytes a store gets without allocating, the manifest id among them. One byte of
  /// `buffer_` is spent marking that the record is inline, so the capacity is that one byte
  /// short of the buffer.
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

  /// The record's bytes, manifest id first.
  auto storage() -> uint8_t * { return is_inline() ? buffer_.data() + 1 : heap_data(); }

  auto storage() const -> uint8_t const * { return is_inline() ? buffer_.data() + 1 : heap_data(); }

  /// The record's payload: the presence bits and everything after them, the manifest id the
  /// storage starts with having been stepped over.
  auto data() -> uint8_t * { return storage() + kManifestIdWidth; }

  auto data() const -> uint8_t const * { return storage() + kManifestIdWidth; }

  /// Takes ownership of storage for `manifest` and a payload of `size` bytes, freeing whatever
  /// the record held before, and returns the payload.
  auto Reset(ManifestId manifest, uint32_t size) -> uint8_t *;

  /// Frees the record's storage, if it had any of its own. Leaves `buffer_` as it was.
  void Release();

  /// Either the record itself, marker byte first, or the size and address of the heap storage
  /// holding it. Overlapping the two is what keeps a record that fits inline to eleven bytes
  /// without growing the object past one word of storage.
  std::array<uint8_t, 1 + kInlineCapacity> buffer_{};
};

static_assert(sizeof(ManifestPropertyStore) == 12, "A record should cost no more than its size and storage");

}  // namespace memgraph::storage
