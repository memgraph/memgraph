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
#include <memory>

#include "storage/v2/id_types.hpp"
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

  /// True when this gave the record a property it did not carry.
  auto SetProperty(ManifestRegistry &registry, PropertyId property, PropertyValue const &value) -> bool;

  /// The shape this record names, absent while it carries nothing.
  auto manifest() const -> std::optional<ManifestId> { return manifest_; }

 private:
  std::unique_ptr<uint8_t[]> buffer_;
  uint32_t size_{};
  std::optional<ManifestId> manifest_;
};

}  // namespace memgraph::storage
