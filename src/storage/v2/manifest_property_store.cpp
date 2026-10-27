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
#include <string>

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
  if (value.IsInt()) return StoredType::Fixed(PropertyStoreType::INT, WidthFor(value.ValueInt()));
  if (value.IsString()) return StoredType::Variable(PropertyStoreType::STRING);
  LOG_FATAL("Only integers and strings are encoded so far");
}

/// A record lays out the values whose width its shape knows, then a table of where each of the
/// rest ends, then those values. The table is what a shape's index into it refers to.
struct Regions {
  uint32_t table_begin;
  uint32_t payload_begin;
};

auto RegionsOf(PropertyManifest const &layout) -> Regions {
  auto const table_begin = layout.fixed_region_size();
  return Regions{.table_begin = table_begin,
                 .payload_begin = table_begin + static_cast<uint32_t>(layout.variable_count() * sizeof(uint32_t))};
}

/// Where a variable value ends. Its start is where the one before it ended, or the beginning
/// of the payload region for the first.
auto EndOffsetAt(uint8_t const *buffer, Regions regions, uint32_t index) -> uint32_t {
  auto end = uint32_t{};
  std::memcpy(&end, buffer + regions.table_begin + (index * sizeof(uint32_t)), sizeof(uint32_t));
  return end;
}

void WriteEndOffset(uint8_t *buffer, Regions regions, uint32_t index, uint32_t end) {
  std::memcpy(buffer + regions.table_begin + (index * sizeof(uint32_t)), &end, sizeof(uint32_t));
}

}  // namespace

auto ManifestPropertyStore::ValueAt(PropertyManifest const &layout, PropertyManifest::Location location) const
    -> PropertyValue {
  if (location.is_fixed) {
    MG_ASSERT(location.stored_type.type == PropertyStoreType::INT, "Only integers are stored at a known width");
    return PropertyValue{ReadInt(buffer_.get() + location.offset, location.stored_type.width)};
  }

  MG_ASSERT(location.stored_type.type == PropertyStoreType::STRING, "Only strings vary in width so far");
  auto const regions = RegionsOf(layout);
  auto const end = EndOffsetAt(buffer_.get(), regions, location.offset);
  auto const begin =
      location.offset == 0 ? regions.payload_begin : EndOffsetAt(buffer_.get(), regions, location.offset - 1);
  return PropertyValue{std::string{reinterpret_cast<char const *>(buffer_.get() + begin), end - begin}};
}

auto ManifestPropertyStore::GetProperty(ManifestRegistry const &registry, PropertyId property) const -> PropertyValue {
  if (!manifest_) return {};

  auto const &layout = registry.Resolve(*manifest_);
  auto const location = layout.Find(property);
  if (!location) return {};

  return ValueAt(layout, *location);
}

auto ManifestPropertyStore::Properties(ManifestRegistry const &registry) const -> std::map<PropertyId, PropertyValue> {
  auto properties = std::map<PropertyId, PropertyValue>{};
  if (!manifest_) return properties;

  auto const &layout = registry.Resolve(*manifest_);
  for (auto const &entry : layout.entries()) {
    properties.emplace(entry.property, ValueAt(layout, *layout.Find(entry.property)));
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
  auto const regions = RegionsOf(layout);

  auto payload_bytes = uint32_t{};
  for (auto const &[property, value] : properties) {
    if (value.IsString()) payload_bytes += static_cast<uint32_t>(value.ValueString().size());
  }

  size_ = regions.payload_begin + payload_bytes;
  buffer_ = std::make_unique<uint8_t[]>(size_);

  // A shape numbers its variable values in property order, so walking the properties in that
  // order fills the table in order and the payloads follow one another.
  auto cursor = regions.payload_begin;
  [[maybe_unused]] auto next_variable = uint32_t{};
  for (auto const &[property, value] : properties) {
    auto const location = layout.Find(property);
    if (location->is_fixed) {
      WriteInt(buffer_.get() + location->offset, value.ValueInt(), location->stored_type.width);
      continue;
    }

    DMG_ASSERT(location->offset == next_variable, "A shape numbers its variable values in property order");
    ++next_variable;
    auto const &text = value.ValueString();
    std::memcpy(buffer_.get() + cursor, text.data(), text.size());
    cursor += static_cast<uint32_t>(text.size());
    WriteEndOffset(buffer_.get(), regions, location->offset, cursor);
  }
  manifest_ = id;
}

auto ManifestPropertyStore::SetProperty(ManifestRegistry &registry, PropertyId property, PropertyValue const &value)
    -> bool {
  MG_ASSERT(value.IsInt() || value.IsString() || value.IsNull(), "Only integers and strings are encoded so far");

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
