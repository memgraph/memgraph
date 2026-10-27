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

#include "storage/v2/manifest_property_store.hpp"

#include <cstring>
#include <limits>

#include "utils/logging.hpp"

namespace memgraph::storage {

namespace {

/// The narrowest of the encoding's four integer widths that still holds `value`. Widening is
/// what keeps the offsets a property of the shape, so the width belongs to the shape rather
/// than to the record.
auto WidthFor(int64_t value) -> uint8_t {
  if (value >= std::numeric_limits<int8_t>::min() && value <= std::numeric_limits<int8_t>::max()) return 1;
  if (value >= std::numeric_limits<int16_t>::min() && value <= std::numeric_limits<int16_t>::max()) return 2;
  if (value >= std::numeric_limits<int32_t>::min() && value <= std::numeric_limits<int32_t>::max()) return 4;
  return 8;
}

void WriteInt(uint8_t *at, int64_t value, uint8_t width) {
  // Little endian, so the low `width` bytes are the value and the rest is its sign.
  auto const unsigned_value = static_cast<uint64_t>(value);
  for (uint8_t byte = 0; byte != width; ++byte) {
    at[byte] = static_cast<uint8_t>(unsigned_value >> (byte * 8));
  }
}

auto ReadInt(uint8_t const *at, uint8_t width) -> int64_t {
  auto unsigned_value = uint64_t{};
  for (uint8_t byte = 0; byte != width; ++byte) {
    unsigned_value |= static_cast<uint64_t>(at[byte]) << (byte * 8);
  }
  // Sign-extend from the stored width, so a negative value read at one width is the value
  // written at another.
  auto const sign_bit = uint64_t{1} << (width * 8 - 1);
  if ((unsigned_value & sign_bit) != 0) {
    unsigned_value |= ~((sign_bit << 1) - 1);
  }
  return static_cast<int64_t>(unsigned_value);
}

/// How a value is stored, which is what a shape is made of.
auto StoredTypeFor(PropertyValue const &value) -> StoredType {
  MG_ASSERT(value.IsInt(), "Only integers are encoded so far");
  return StoredType::Fixed(PropertyStoreType::INT, WidthFor(value.ValueInt()));
}

}  // namespace

auto ManifestPropertyStore::GetProperty(ManifestRegistry const &registry, PropertyId property) const -> PropertyValue {
  if (!manifest_) return {};

  auto const location = registry.Resolve(*manifest_).Find(property);
  if (!location) return {};

  MG_ASSERT(location->stored_type.type == PropertyStoreType::INT, "Only integers are encoded so far");
  return PropertyValue{ReadInt(buffer_.get() + location->offset, location->stored_type.width)};
}

auto ManifestPropertyStore::Properties(ManifestRegistry const &registry) const -> std::map<PropertyId, PropertyValue> {
  auto properties = std::map<PropertyId, PropertyValue>{};
  if (!manifest_) return properties;

  auto const &layout = registry.Resolve(*manifest_);
  for (auto const &entry : layout.entries()) {
    auto const location = layout.Find(entry.property);
    MG_ASSERT(location->stored_type.type == PropertyStoreType::INT, "Only integers are encoded so far");
    properties.emplace(entry.property,
                       PropertyValue{ReadInt(buffer_.get() + location->offset, location->stored_type.width)});
  }
  return properties;
}

void ManifestPropertyStore::Rebuild(ManifestRegistry &registry, std::map<PropertyId, PropertyValue> const &properties) {
  if (properties.empty()) {
    buffer_.reset();
    size_ = 0;
    manifest_.reset();
    return;
  }

  auto shape = RecordShape{};
  for (auto const &[property, value] : properties) {
    shape.emplace(property, StoredTypeFor(value));
  }

  auto const id = registry.Intern(shape);
  auto const &layout = registry.Resolve(id);

  size_ = layout.fixed_region_size();
  buffer_ = std::make_unique<uint8_t[]>(size_);
  for (auto const &[property, value] : properties) {
    auto const location = layout.Find(property);
    WriteInt(buffer_.get() + location->offset, value.ValueInt(), location->stored_type.width);
  }
  manifest_ = id;
}

auto ManifestPropertyStore::SetProperty(ManifestRegistry &registry, PropertyId property, PropertyValue const &value)
    -> bool {
  MG_ASSERT(value.IsInt() || value.IsNull(), "Only integers are encoded so far");

  // A changed shape moves the values already in the record, so the record is read out, changed
  // and written back rather than edited in place.
  auto properties = Properties(registry);
  // Insertion rather than assignment, which is what the record not already carrying the
  // property means. A Null removes it, and removing one it never carried is still an insertion.
  auto const inserted = !properties.contains(property);

  if (value.IsNull()) {
    properties.erase(property);
  } else {
    properties.insert_or_assign(property, value);
  }
  Rebuild(registry, properties);

  return inserted;
}

}  // namespace memgraph::storage
