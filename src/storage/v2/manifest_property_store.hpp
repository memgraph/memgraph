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

#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/property_manifest.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::storage {

/// Holds a record's property values, with the property ids and types kept outside it in an
/// interned shape. The record names its shape, so what it carries and where each value sits
/// is known before any of its bytes are read.
///
/// Every call takes the registry that issued the shape. A record cannot find its own, and a
/// shape id means nothing to a registry that did not issue it.
class ManifestPropertyStore {
 public:
  ManifestPropertyStore() = default;
  ~ManifestPropertyStore() = default;

  ManifestPropertyStore(ManifestPropertyStore const &) = delete;
  ManifestPropertyStore &operator=(ManifestPropertyStore const &) = delete;
  ManifestPropertyStore(ManifestPropertyStore &&) noexcept = default;
  ManifestPropertyStore &operator=(ManifestPropertyStore &&) noexcept = default;

  /// Null when the record does not carry `property`.
  auto GetProperty(ManifestRegistry const &registry, PropertyId property) const -> PropertyValue;

  /// Every property the record carries, in property order.
  auto Properties(ManifestRegistry const &registry) const -> std::map<PropertyId, PropertyValue>;

  /// Whether the record's `property` is equivalent to `value`, answered without building the
  /// stored value up again.
  ///
  /// Equivalence rather than equality, which is the relation an index is keyed by and the one
  /// a constraint needs if it is not to admit a duplicate. It is two-valued, and it holds
  /// alike what equality does not hold equal to itself: a NaN is equivalent to a NaN, and a
  /// property the record does not carry is equivalent to Null and to nothing else. A number is
  /// equivalent to a number of the other numeric type when the two are the same number.
  ///
  /// Named for the question `PropertyStore` answers rather than for the relation, so that the
  /// one can stand in for the other.
  auto IsPropertyEqual(ManifestRegistry const &registry, PropertyId property, PropertyValue const &value) const -> bool;

  /// The same question for several properties at once, which is what a composite index asks.
  /// `result` is given one answer per path, in the paths' order; `position_lookup` says which
  /// of `values` each path is asked about.
  ///
  /// A path of more than one property names a key inside a value, as an index on a nested
  /// property does. A path arriving at nothing, because the record lacks the property or the
  /// key is not there, is equivalent to Null.
  void ArePropertiesEqual(ManifestRegistry const &registry, std::span<PropertyPath const> ordered_properties,
                          std::span<PropertyValue const> values, std::span<std::size_t const> position_lookup,
                          std::vector<bool> &result) const;

  /// The values at `ordered_properties`, one per path in the paths' order, which is the key an
  /// index builds for a record. A path arriving at nothing, because the record lacks the
  /// property or the key is not there, reads as Null, so a record is keyed on what it does not
  /// carry as well as on what it does.
  ///
  /// Named for the question `PropertyStore` answers, so the one can stand in for the other.
  /// Unlike that store this one needs no order among the paths: a shape gives each property's
  /// place without reading what sits before it.
  auto ExtractPropertyValuesMissingAsNull(ManifestRegistry const &registry,
                                          std::span<PropertyPath const> ordered_properties) const
      -> std::vector<PropertyValue>;

  /// As above, writing into a caller's buffer so a key of known arity is built without
  /// allocating. `out.size()` must equal `ordered_properties.size()`.
  void ExtractPropertyValuesMissingAsNull(ManifestRegistry const &registry,
                                          std::span<PropertyPath const> ordered_properties,
                                          std::span<PropertyValue> out) const;

  /// True when this gave the record a property it did not carry. A Null value removes the
  /// property, as it does for the store this replaces.
  auto SetProperty(ManifestRegistry &registry, PropertyId property, PropertyValue const &value) -> bool;

  /// The shape this record names, absent while it carries nothing.
  auto manifest() const -> std::optional<ManifestId> {
    if (!buffer_) return std::nullopt;
    auto value = uint32_t{};
    std::memcpy(&value, buffer_.get(), sizeof(value));
    return ManifestId{value};
  }

 private:
  /// Gives the record the shape `properties` needs and writes them into it. Every value is
  /// rewritten because a changed shape moves the ones that were already there.
  void Rebuild(ManifestRegistry &registry, std::map<PropertyId, PropertyValue> const &properties);

  /// Reads one value out of the record, from where `layout` says it sits.
  auto ValueAt(PropertyManifest const &layout, PropertyManifest::Location location) const -> PropertyValue;

  /// The value the keys after a path's first property arrive at, Null when they arrive at
  /// nothing. `location` must be where that first property sits. The shape says nothing about
  /// what is inside a value, so this reads the value and walks it.
  auto ValueAtKeys(PropertyManifest const &layout, PropertyManifest::Location location,
                   std::span<PropertyId const> keys) const -> PropertyValue;

  /// The bytes of a value the shape does not know the width of. `location` must name one.
  auto PayloadAt(PropertyManifest const &layout, PropertyManifest::Location location) const -> std::span<uint8_t const>;

  /// Whether the value at `location` is equivalent to `value`.
  auto CompareAt(PropertyManifest const &layout, PropertyManifest::Location location, PropertyValue const &value) const
      -> bool;

  /// A record names its shape in its own first bytes, so a vertex holds nothing but the
  /// pointer to it. Naming it beside that pointer instead would cost the vertex four more
  /// bytes, which it has not got, and the id is read from bytes the values are read from
  /// anyway.
  static constexpr uint32_t kShapeIdBytes = sizeof(uint32_t);

  /// Where the values begin, which is where every offset a shape gives is measured from.
  auto Record() const -> uint8_t const * { return buffer_.get() + kShapeIdBytes; }

  auto Record() -> uint8_t * { return buffer_.get() + kShapeIdBytes; }

  /// Null while the record carries nothing. Its first bytes are the shape it names, and the
  /// values follow. Nothing says how long it is: a shape and the record's own table of where
  /// its variable values end give that, and nothing asks.
  std::unique_ptr<uint8_t[]> buffer_;
};

/// A vertex holds one of these, and has twelve bytes to spare for it: the four-byte lock
/// beside it fills what a pointer's alignment would otherwise waste, and the two together must
/// leave the vertex's size where it is. Eight bytes aligned to eight satisfies that, where
/// twelve aligned to eight would not, since a size is rounded up to its alignment.
static_assert(sizeof(ManifestPropertyStore) <= 12, "A vertex has twelve bytes for its properties");

/// A record together with the registry that describes it, which is the smallest thing that can
/// be asked anything: a record names its shape and cannot resolve it, and a shape id means
/// nothing to a registry that did not issue it.
///
/// It answers the reading questions `PropertyStore` answers, with the registry already in hand,
/// so whatever reads a record can read either store without being told which it has.
class ManifestRecord {
 public:
  ManifestRecord(ManifestPropertyStore const &store, ManifestRegistry const &registry)
      : store_{&store}, registry_{&registry} {}

  auto GetProperty(PropertyId property) const -> PropertyValue { return store_->GetProperty(*registry_, property); }

  auto ExtractPropertyValuesMissingAsNull(std::span<PropertyPath const> ordered_properties) const
      -> std::vector<PropertyValue> {
    return store_->ExtractPropertyValuesMissingAsNull(*registry_, ordered_properties);
  }

  void ExtractPropertyValuesMissingAsNull(std::span<PropertyPath const> ordered_properties,
                                          std::span<PropertyValue> out) const {
    store_->ExtractPropertyValuesMissingAsNull(*registry_, ordered_properties, out);
  }

  void ArePropertiesEqual(std::span<PropertyPath const> ordered_properties, std::span<PropertyValue const> values,
                          std::span<std::size_t const> position_lookup, std::vector<bool> &result) const {
    store_->ArePropertiesEqual(*registry_, ordered_properties, values, position_lookup, result);
  }

 private:
  ManifestPropertyStore const *store_;
  ManifestRegistry const *registry_;
};

}  // namespace memgraph::storage
