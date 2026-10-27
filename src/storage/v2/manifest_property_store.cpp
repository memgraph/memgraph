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

#include <array>
#include <bit>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>

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

/// A value that is nothing but a small object: its bytes are its value, so the shape knowing
/// its width is all that reading it takes.
template <typename T>
void WriteObject(uint8_t *at, T const &value) {
  static_assert(std::is_trivially_copyable_v<T>, "Only an object whose bytes are its value");
  std::memcpy(at, &value, sizeof(T));
}

template <typename T>
auto ReadObject(uint8_t const *at) -> T {
  static_assert(std::is_trivially_copyable_v<T>, "Only an object whose bytes are its value");
  // Through a byte array rather than a default-constructed T, because not all of these have a
  // default constructor.
  auto raw = std::array<std::byte, sizeof(T)>{};
  std::memcpy(raw.data(), at, sizeof(T));
  return std::bit_cast<T>(raw);
}

/// How a value is stored, which is what a shape is made of. The one place that decides what
/// can be encoded, so a kind that is not handled is refused here rather than part way in.
auto StoredTypeFor(PropertyValue const &value) -> StoredType {
  if (value.IsBool()) return StoredType::Fixed(PropertyStoreType::BOOL, 1);
  if (value.IsInt()) return StoredType::Fixed(PropertyStoreType::INT, WidthFor(value.ValueInt()));
  if (value.IsDouble()) return StoredType::Fixed(PropertyStoreType::DOUBLE, sizeof(double));
  if (value.IsString()) return StoredType::Variable(PropertyStoreType::STRING);
  if (value.IsEnum()) return StoredType::Fixed(PropertyStoreType::ENUM, sizeof(Enum));
  if (value.IsTemporalData()) return StoredType::Fixed(PropertyStoreType::TEMPORAL_DATA, sizeof(TemporalData));
  // One tag for both kinds of point, told apart by the width the shape carries.
  if (value.IsPoint2d()) return StoredType::Fixed(PropertyStoreType::POINT, sizeof(Point2d));
  if (value.IsPoint3d()) return StoredType::Fixed(PropertyStoreType::POINT, sizeof(Point3d));
  LOG_FATAL("A value of type {} is not encoded yet", static_cast<unsigned>(value.type()));
}

/// Writes a value whose width its shape knows.
void WriteFixed(uint8_t *at, StoredType stored_type, PropertyValue const &value) {
  switch (stored_type.type) {
    case PropertyStoreType::BOOL:
      *at = value.ValueBool() ? 1 : 0;
      return;
    case PropertyStoreType::INT:
      WriteInt(at, value.ValueInt(), stored_type.width);
      return;
    case PropertyStoreType::DOUBLE: {
      // The bytes rather than the number, so a negative zero and a NaN read back as the ones
      // written rather than as some other value that compares the same.
      auto const number = value.ValueDouble();
      std::memcpy(at, &number, sizeof(number));
      return;
    }
    case PropertyStoreType::ENUM:
      WriteObject(at, value.ValueEnum());
      return;
    case PropertyStoreType::TEMPORAL_DATA:
      WriteObject(at, value.ValueTemporalData());
      return;
    case PropertyStoreType::POINT:
      if (stored_type.width == sizeof(Point2d)) {
        WriteObject(at, value.ValuePoint2d());
      } else {
        WriteObject(at, value.ValuePoint3d());
      }
      return;
    default:
      LOG_FATAL("This kind of value is not stored at a known width");
  }
}

/// Reads a value whose width its shape knows.
auto ReadFixed(uint8_t const *at, StoredType stored_type) -> PropertyValue {
  switch (stored_type.type) {
    case PropertyStoreType::BOOL:
      return PropertyValue{*at != 0};
    case PropertyStoreType::INT:
      return PropertyValue{ReadInt(at, stored_type.width)};
    case PropertyStoreType::DOUBLE: {
      auto number = double{};
      std::memcpy(&number, at, sizeof(number));
      return PropertyValue{number};
    }
    case PropertyStoreType::ENUM:
      return PropertyValue{ReadObject<Enum>(at)};
    case PropertyStoreType::TEMPORAL_DATA:
      return PropertyValue{ReadObject<TemporalData>(at)};
    case PropertyStoreType::POINT:
      if (stored_type.width == sizeof(Point2d)) return PropertyValue{ReadObject<Point2d>(at)};
      return PropertyValue{ReadObject<Point3d>(at)};
    default:
      LOG_FATAL("This kind of value is not stored at a known width");
  }
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
    return ReadFixed(buffer_.get() + location.offset, location.stored_type);
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
      WriteFixed(buffer_.get() + location->offset, location->stored_type, value);
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
