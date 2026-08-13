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

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_manifest.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::storage {

/// A property store that keeps property ids and types in a shared manifest instead of in
/// every record, so locating a value costs a lookup in the shape rather than a scan over
/// the record.
///
/// A record is laid out as
///
///     [offset table][fixed payload][variable payload]
///
/// where the offset table holds one end-offset per variable-width value, and is present
/// only when the shape has variable-width values. Its entry width is recorded in a leading
/// byte, sized to the record so a small record spends one byte per variable value.
///
/// The registry is passed in rather than held, so a record costs nothing to point at the
/// shapes it shares with every other record.
///
/// Prototype scope: bool, integer, double, string, temporal data, enum and point. The
/// remaining property types (zoned temporal, list, map, vector) are not encodable yet and
/// will assert.
class ManifestPropertyStore {
 public:
  ManifestPropertyStore() = default;

  ManifestPropertyStore(ManifestPropertyStore const &) = delete;
  ManifestPropertyStore &operator=(ManifestPropertyStore const &) = delete;
  ManifestPropertyStore(ManifestPropertyStore &&) noexcept = default;
  ManifestPropertyStore &operator=(ManifestPropertyStore &&) noexcept = default;

  ~ManifestPropertyStore() = default;

  auto GetProperty(ManifestRegistry const &registry, PropertyId property) const -> PropertyValue;

  auto HasProperty(ManifestRegistry const &registry, PropertyId property) const -> bool;

  auto Properties(ManifestRegistry const &registry) const -> std::map<PropertyId, PropertyValue>;

  /// Returns true when the property was not already present.
  auto SetProperty(ManifestRegistry &registry, PropertyId property, PropertyValue const &value) -> bool;

  /// Sets every property at once on a record that has none, interning a single shape rather
  /// than one per property added. Returns false, changing nothing, if the record is not empty.
  auto InitProperties(ManifestRegistry &registry, std::map<PropertyId, PropertyValue> const &properties) -> bool;

  /// Returns true when there was anything to remove.
  auto ClearProperties() -> bool;

  auto manifest() const -> ManifestId { return manifest_; }

  /// Encoded bytes this record holds, excluding the shared manifest.
  auto buffer_size() const -> uint32_t { return size_; }

 private:
  /// Re-interns the shape and re-encodes the record from scratch.
  void Rebuild(ManifestRegistry &registry, std::map<PropertyId, PropertyValue> const &properties);

  ManifestId manifest_{};
  std::unique_ptr<uint8_t[]> buffer_{};
  uint32_t size_{};
};

}  // namespace memgraph::storage
