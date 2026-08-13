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
#include <span>
#include <string>
#include <vector>

#include "utils/logging.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::storage {

namespace {

constexpr uint8_t kBoolWidth = 1;
constexpr uint8_t kDoubleWidth = 8;

/// Narrowest signed width that still round-trips `value`.
auto IntWidth(int64_t value) -> uint8_t {
  if (value >= std::numeric_limits<int8_t>::min() && value <= std::numeric_limits<int8_t>::max()) return 1;
  if (value >= std::numeric_limits<int16_t>::min() && value <= std::numeric_limits<int16_t>::max()) return 2;
  if (value >= std::numeric_limits<int32_t>::min() && value <= std::numeric_limits<int32_t>::max()) return 4;
  return 8;
}

auto StoredTypeOf(PropertyValue const &value) -> StoredType {
  switch (value.type()) {
    case PropertyValueType::Bool:
      return StoredType::Fixed(PropertyStoreType::BOOL, kBoolWidth);
    case PropertyValueType::Int:
      return StoredType::Fixed(PropertyStoreType::INT, IntWidth(value.ValueInt()));
    case PropertyValueType::Double:
      return StoredType::Fixed(PropertyStoreType::DOUBLE, kDoubleWidth);
    case PropertyValueType::String:
      return StoredType::Variable(PropertyStoreType::STRING);
    default:
      LOG_FATAL("ManifestPropertyStore cannot yet encode this property type");
  }
}

/// Encoded length a variable-width value contributes to the variable region.
auto VariableSize(PropertyValue const &value) -> uint32_t {
  DMG_ASSERT(value.IsString(), "Only strings are variable width so far");
  return static_cast<uint32_t>(value.ValueString().size());
}

/// Offset-table entry width for a variable region of `size` bytes.
auto OffsetWidth(uint32_t size) -> uint8_t {
  if (size <= std::numeric_limits<uint8_t>::max()) return 1;
  if (size <= std::numeric_limits<uint16_t>::max()) return 2;
  return 4;
}

void EncodeFixed(PropertyValue const &value, StoredType stored_type, std::span<uint8_t> out) {
  switch (stored_type.type) {
    case PropertyStoreType::BOOL:
      out[0] = static_cast<uint8_t>(value.ValueBool());
      return;
    case PropertyStoreType::INT: {
      // Little-endian, truncated to the width the shape says this value was stored at.
      auto const raw = static_cast<uint64_t>(value.ValueInt());
      std::memcpy(out.data(), &raw, stored_type.width);
      return;
    }
    case PropertyStoreType::DOUBLE: {
      auto const raw = value.ValueDouble();
      std::memcpy(out.data(), &raw, kDoubleWidth);
      return;
    }
    default:
      LOG_FATAL("Not a fixed-width property type");
  }
}

auto DecodeFixed(StoredType stored_type, std::span<uint8_t const> in) -> PropertyValue {
  switch (stored_type.type) {
    case PropertyStoreType::BOOL:
      return PropertyValue{in[0] != 0};
    case PropertyStoreType::INT: {
      uint64_t raw = 0;
      std::memcpy(&raw, in.data(), stored_type.width);
      switch (stored_type.width) {
        case 1:
          return PropertyValue{static_cast<int64_t>(static_cast<int8_t>(raw))};
        case 2:
          return PropertyValue{static_cast<int64_t>(static_cast<int16_t>(raw))};
        case 4:
          return PropertyValue{static_cast<int64_t>(static_cast<int32_t>(raw))};
        default:
          return PropertyValue{static_cast<int64_t>(raw)};
      }
    }
    case PropertyStoreType::DOUBLE: {
      double raw = 0.0;
      std::memcpy(&raw, in.data(), kDoubleWidth);
      return PropertyValue{raw};
    }
    default:
      LOG_FATAL("Not a fixed-width property type");
  }
}

/// Where the regions of a record sit, given its shape.
struct Regions {
  uint8_t offset_width;
  uint32_t offset_table;  // byte offset of the offset table (only meaningful with variable values)
  uint32_t fixed;         // byte offset of the fixed region
  uint32_t variable;      // byte offset of the variable region
};

auto RegionsOf(PropertyManifest const &manifest, uint8_t const *buffer) -> Regions {
  if (manifest.variable_count() == 0) {
    return Regions{.offset_width = 0, .offset_table = 0, .fixed = 0, .variable = 0};
  }
  auto const offset_width = buffer[0];
  auto const fixed = 1U + manifest.variable_count() * offset_width;
  return Regions{
      .offset_width = offset_width,
      .offset_table = 1,
      .fixed = fixed,
      .variable = fixed + manifest.fixed_region_size(),
  };
}

auto ReadOffset(uint8_t const *table, uint8_t width, uint32_t index) -> uint32_t {
  uint32_t offset = 0;
  std::memcpy(&offset, table + static_cast<size_t>(index) * width, width);
  return offset;
}

void WriteOffset(uint8_t *table, uint8_t width, uint32_t index, uint32_t offset) {
  std::memcpy(table + static_cast<size_t>(index) * width, &offset, width);
}

}  // namespace

auto ManifestPropertyStore::GetProperty(ManifestRegistry const &registry, PropertyId property) const -> PropertyValue {
  if (!buffer_) return PropertyValue{};

  auto const &manifest = registry.Resolve(manifest_);
  auto const location = manifest.Find(property);
  if (!location) return PropertyValue{};

  auto const regions = RegionsOf(manifest, buffer_.get());
  if (location->is_fixed) {
    return DecodeFixed(location->stored_type,
                       std::span{buffer_.get() + regions.fixed + location->offset, location->stored_type.width});
  }

  // Offsets are ends, so a value starts where the previous one finished.
  auto const *table = buffer_.get() + regions.offset_table;
  auto const end = ReadOffset(table, regions.offset_width, location->offset);
  auto const begin = location->offset == 0 ? 0U : ReadOffset(table, regions.offset_width, location->offset - 1);
  return PropertyValue{std::string{reinterpret_cast<char const *>(buffer_.get() + regions.variable + begin),
                                   static_cast<size_t>(end - begin)}};
}

auto ManifestPropertyStore::HasProperty(ManifestRegistry const &registry, PropertyId property) const -> bool {
  // Answered by the shape alone; the record is never touched.
  if (!buffer_) return false;
  return registry.Resolve(manifest_).Find(property).has_value();
}

auto ManifestPropertyStore::Properties(ManifestRegistry const &registry) const -> std::map<PropertyId, PropertyValue> {
  if (!buffer_) return {};

  auto const &manifest = registry.Resolve(manifest_);
  auto properties = std::map<PropertyId, PropertyValue>{};
  for (auto const &entry : manifest.entries()) {
    properties.emplace(entry.property, GetProperty(registry, entry.property));
  }
  return properties;
}

auto ManifestPropertyStore::SetProperty(ManifestRegistry &registry, PropertyId property, PropertyValue const &value)
    -> bool {
  auto const existing = buffer_ ? registry.Resolve(manifest_).Find(property) : std::nullopt;

  // The common update keeps the shape: same property, same type, same width class. Then the
  // value can go straight into the slot the shape already points at.
  if (existing && !value.IsNull() && existing->is_fixed && existing->stored_type == StoredTypeOf(value)) {
    auto const &manifest = registry.Resolve(manifest_);
    auto const regions = RegionsOf(manifest, buffer_.get());
    EncodeFixed(value,
                existing->stored_type,
                std::span{buffer_.get() + regions.fixed + existing->offset, existing->stored_type.width});
    return false;
  }

  auto properties = Properties(registry);
  if (value.IsNull()) {
    properties.erase(property);
  } else {
    properties.insert_or_assign(property, value);
  }
  Rebuild(registry, properties);
  return !existing.has_value();
}

auto ManifestPropertyStore::InitProperties(ManifestRegistry &registry,
                                           std::map<PropertyId, PropertyValue> const &properties) -> bool {
  if (buffer_) return false;

  // Nulls are skipped by Rebuild, so the caller's map goes straight through rather than
  // being copied to filter them out.
  Rebuild(registry, properties);
  return true;
}

auto ManifestPropertyStore::ClearProperties() -> bool {
  if (!buffer_) return false;
  buffer_.reset();
  size_ = 0;
  manifest_ = ManifestId{};
  return true;
}

void ManifestPropertyStore::Rebuild(ManifestRegistry &registry, std::map<PropertyId, PropertyValue> const &properties) {
  auto entries = utils::small_vector<ManifestEntry>{};
  entries.reserve(properties.size());
  auto variable_bytes = uint32_t{0};
  for (auto const &[id, value] : properties) {
    if (value.IsNull()) continue;
    auto const stored_type = StoredTypeOf(value);
    entries.push_back(ManifestEntry{.property = id, .stored_type = stored_type});
    if (!stored_type.is_fixed_width()) variable_bytes += VariableSize(value);
  }

  if (entries.empty()) {
    ClearProperties();
    return;
  }

  auto const manifest_id = registry.Intern(entries);
  auto const &manifest = registry.Resolve(manifest_id);
  auto const offset_width = OffsetWidth(variable_bytes);
  auto const header = manifest.variable_count() == 0 ? 0U : 1U + manifest.variable_count() * offset_width;
  auto const total = header + manifest.fixed_region_size() + variable_bytes;

  auto buffer = std::make_unique<uint8_t[]>(total);
  if (manifest.variable_count() != 0) buffer[0] = offset_width;

  auto const regions = RegionsOf(manifest, buffer.get());
  auto variable_end = uint32_t{0};
  // The properties and the shape's entries are both in property order, and the entries were
  // built from these very properties, so the shape can be walked by position.
  auto position = size_t{0};
  for (auto const &[id, value] : properties) {
    if (value.IsNull()) continue;
    auto const location = manifest.LocationAt(position++);
    if (location.is_fixed) {
      EncodeFixed(value,
                  location.stored_type,
                  std::span{buffer.get() + regions.fixed + location.offset, location.stored_type.width});
      continue;
    }
    auto const size = VariableSize(value);
    std::memcpy(buffer.get() + regions.variable + variable_end, value.ValueString().data(), size);
    variable_end += size;
    WriteOffset(buffer.get() + regions.offset_table, offset_width, location.offset, variable_end);
  }

  manifest_ = manifest_id;
  buffer_ = std::move(buffer);
  size_ = total;
}

}  // namespace memgraph::storage
