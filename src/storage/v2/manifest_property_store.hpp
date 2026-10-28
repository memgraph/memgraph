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

  /// True when this gave the record a property it did not carry. A Null value removes the
  /// property, as it does for the store this replaces.
  auto SetProperty(ManifestRegistry &registry, PropertyId property, PropertyValue const &value) -> bool;

  /// The shape this record names, absent while it carries nothing.
  auto manifest() const -> std::optional<ManifestId> { return manifest_; }

 private:
  /// Gives the record the shape `properties` needs and writes them into it. Every value is
  /// rewritten because a changed shape moves the ones that were already there.
  void Rebuild(ManifestRegistry &registry, std::map<PropertyId, PropertyValue> const &properties);

  /// Reads one value out of the record, from where `layout` says it sits.
  auto ValueAt(PropertyManifest const &layout, PropertyManifest::Location location) const -> PropertyValue;

  /// The bytes of a value the shape does not know the width of. `location` must name one.
  auto PayloadAt(PropertyManifest const &layout, PropertyManifest::Location location) const -> std::span<uint8_t const>;

  /// Whether the value at `location` is equivalent to `value`.
  auto CompareAt(PropertyManifest const &layout, PropertyManifest::Location location, PropertyValue const &value) const
      -> bool;

  std::unique_ptr<uint8_t[]> buffer_;
  uint32_t size_{};
  std::optional<ManifestId> manifest_;
};

}  // namespace memgraph::storage
