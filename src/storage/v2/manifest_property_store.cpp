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

#include <algorithm>
#include <cstring>
#include <limits>
#include <span>
#include <sstream>
#include <string>

#include "utils/logging.hpp"

namespace memgraph::storage {

namespace {

constexpr uint8_t kBoolWidth = 1;
constexpr uint8_t kDoubleWidth = 8;
constexpr uint8_t kTemporalWidth = 8;  // microseconds; which temporal type is part of the shape
constexpr uint8_t kEnumWidth = 8;      // value id; which enum type is part of the shape
constexpr uint8_t kPoint2dWidth = 16;  // x, y; the coordinate system is part of the shape
constexpr uint8_t kPoint3dWidth = 24;  // x, y, z

/// Narrowest signed width that still round-trips `value`.
auto IntWidth(int64_t value) -> uint8_t {
  if (value >= std::numeric_limits<int8_t>::min() && value <= std::numeric_limits<int8_t>::max()) return 1;
  if (value >= std::numeric_limits<int16_t>::min() && value <= std::numeric_limits<int16_t>::max()) return 2;
  if (value >= std::numeric_limits<int32_t>::min() && value <= std::numeric_limits<int32_t>::max()) return 4;
  return 8;
}

/// A value that carries a discriminator of its own puts it in the shape, which is what keeps
/// its payload fixed width and every record of that shape identically laid out.
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
    case PropertyValueType::TemporalData:
      return StoredType::Fixed(
          PropertyStoreType::TEMPORAL_DATA, kTemporalWidth, static_cast<uint32_t>(value.ValueTemporalData().type));
    case PropertyValueType::Enum:
      return StoredType::Fixed(
          PropertyStoreType::ENUM, kEnumWidth, static_cast<uint32_t>(value.ValueEnum().type_id().value_of()));
    case PropertyValueType::Point2d:
      return StoredType::Fixed(
          PropertyStoreType::POINT, kPoint2dWidth, static_cast<uint32_t>(value.ValuePoint2d().crs()));
    case PropertyValueType::Point3d:
      return StoredType::Fixed(
          PropertyStoreType::POINT, kPoint3dWidth, static_cast<uint32_t>(value.ValuePoint3d().crs()));
    default: {
      auto message = std::ostringstream{};
      message << "ManifestPropertyStore cannot yet encode a " << value.type() << " property";
      throw ManifestPropertyStore::UnsupportedType{std::move(message).str()};
    }
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
    case PropertyStoreType::TEMPORAL_DATA: {
      auto const raw = value.ValueTemporalData().microseconds;
      std::memcpy(out.data(), &raw, kTemporalWidth);
      return;
    }
    case PropertyStoreType::ENUM: {
      auto const raw = value.ValueEnum().value_id().value_of();
      std::memcpy(out.data(), &raw, kEnumWidth);
      return;
    }
    case PropertyStoreType::POINT: {
      if (stored_type.width == kPoint2dWidth) {
        auto const &point = value.ValuePoint2d();
        auto const coordinates = std::array{point.x(), point.y()};
        std::memcpy(out.data(), coordinates.data(), kPoint2dWidth);
        return;
      }
      auto const &point = value.ValuePoint3d();
      auto const coordinates = std::array{point.x(), point.y(), point.z()};
      std::memcpy(out.data(), coordinates.data(), kPoint3dWidth);
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
    case PropertyStoreType::TEMPORAL_DATA: {
      int64_t microseconds = 0;
      std::memcpy(&microseconds, in.data(), kTemporalWidth);
      return PropertyValue{TemporalData{static_cast<TemporalType>(stored_type.discriminator), microseconds}};
    }
    case PropertyStoreType::ENUM: {
      uint64_t value_id = 0;
      std::memcpy(&value_id, in.data(), kEnumWidth);
      return PropertyValue{Enum{EnumTypeId{stored_type.discriminator}, EnumValueId{value_id}}};
    }
    case PropertyStoreType::POINT: {
      auto const crs = static_cast<CoordinateReferenceSystem>(stored_type.discriminator);
      if (stored_type.width == kPoint2dWidth) {
        std::array<double, 2> coordinates{};
        std::memcpy(coordinates.data(), in.data(), kPoint2dWidth);
        return PropertyValue{Point2d{crs, coordinates[0], coordinates[1]}};
      }
      std::array<double, 3> coordinates{};
      std::memcpy(coordinates.data(), in.data(), kPoint3dWidth);
      return PropertyValue{Point3d{crs, coordinates[0], coordinates[1], coordinates[2]}};
    }
    default:
      LOG_FATAL("Not a fixed-width property type");
  }
}

/// Bytes of presence bits a shape of `fields` fields needs.
constexpr auto PresenceBytes(size_t fields) -> uint32_t { return static_cast<uint32_t>((fields + 7) / 8); }

auto IsPresent(uint8_t const *record, uint32_t position) -> bool {
  return (record[position / 8] & (1U << (position % 8))) != 0;
}

void SetPresent(uint8_t *record, uint32_t position, bool present) {
  auto const mask = static_cast<uint8_t>(1U << (position % 8));
  if (present) {
    record[position / 8] |= mask;
  } else {
    record[position / 8] &= static_cast<uint8_t>(~mask);
  }
}

/// Where the regions of a record sit, given its shape.
struct Regions {
  uint8_t offset_width;
  uint32_t offset_table;
  uint32_t fixed;
  uint32_t variable;
};

auto RegionsOf(PropertyManifest const &manifest, uint8_t const *record) -> Regions {
  auto const presence = PresenceBytes(manifest.size());
  if (manifest.variable_count() == 0) {
    return Regions{.offset_width = 0, .offset_table = presence, .fixed = presence, .variable = presence};
  }
  auto const offset_width = record[presence];
  auto const fixed = presence + 1U + manifest.variable_count() * offset_width;
  return Regions{
      .offset_width = offset_width,
      .offset_table = presence + 1U,
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

ManifestPropertyStore::ManifestPropertyStore(ManifestPropertyStore &&other) noexcept
    : manifest_{other.manifest_}, size_{other.size_} {
  if (size_ <= kInlineCapacity) {
    inline_ = other.inline_;
  } else {
    heap_ = other.heap_;
  }
  other.manifest_ = ManifestId{};
  other.size_ = 0;
}

auto ManifestPropertyStore::operator=(ManifestPropertyStore &&other) noexcept -> ManifestPropertyStore & {
  if (this == &other) return *this;
  if (size_ > kInlineCapacity) delete[] heap_;
  manifest_ = other.manifest_;
  size_ = other.size_;
  if (size_ <= kInlineCapacity) {
    inline_ = other.inline_;
  } else {
    heap_ = other.heap_;
  }
  other.manifest_ = ManifestId{};
  other.size_ = 0;
  return *this;
}

ManifestPropertyStore::~ManifestPropertyStore() {
  if (size_ > kInlineCapacity) delete[] heap_;
}

auto ManifestPropertyStore::Reset(uint32_t size) -> uint8_t * {
  if (size_ > kInlineCapacity) delete[] heap_;
  size_ = size;
  if (size > kInlineCapacity) {
    heap_ = new uint8_t[size]{};
  } else {
    inline_ = {};
  }
  return data();
}

auto ManifestPropertyStore::GetProperty(PropertyManifest const &manifest, PropertyManifest::Location location) const
    -> PropertyValue {
  if (size_ == 0) return PropertyValue{};

  auto const *record = data();
  if (!IsPresent(record, location.position)) return PropertyValue{};

  auto const regions = RegionsOf(manifest, record);
  if (location.is_fixed) {
    return DecodeFixed(location.stored_type,
                       std::span{record + regions.fixed + location.offset, location.stored_type.width});
  }

  // Offsets are ends, so a value starts where the previous one finished.
  auto const *table = record + regions.offset_table;
  auto const end = ReadOffset(table, regions.offset_width, location.offset);
  auto const begin = location.offset == 0 ? 0U : ReadOffset(table, regions.offset_width, location.offset - 1);
  return PropertyValue{
      std::string{reinterpret_cast<char const *>(record + regions.variable + begin), static_cast<size_t>(end - begin)}};
}

auto ManifestPropertyStore::GetProperty(ManifestRegistry const &registry, PropertyId property) const -> PropertyValue {
  if (size_ == 0) return PropertyValue{};

  auto const &manifest = registry.Resolve(manifest_);
  auto const found = manifest.Find(property);
  if (!found) return PropertyValue{};
  return GetProperty(manifest, *found);
}

auto ManifestPropertyStore::HasProperty(ManifestRegistry const &registry, PropertyId property) const -> bool {
  if (size_ == 0) return false;
  auto const found = registry.Resolve(manifest_).Find(property);
  return found && IsPresent(data(), found->position);
}

auto ManifestPropertyStore::Properties(ManifestRegistry const &registry) const -> utils::small_vector<PropertyPair> {
  auto properties = utils::small_vector<PropertyPair>{};
  if (size_ == 0) return properties;

  auto const &manifest = registry.Resolve(manifest_);
  auto const *record = data();
  properties.reserve(manifest.size());
  for (uint32_t position = 0; position != manifest.size(); ++position) {
    if (!IsPresent(record, position)) continue;
    auto const property = manifest.entries()[position].property;
    properties.emplace_back(property, GetProperty(registry, property));
  }
  return properties;
}

auto ManifestPropertyStore::SetProperty(ManifestRegistry &registry, PropertyId property, PropertyValue const &value)
    -> bool {
  auto const found = size_ == 0 ? std::nullopt : registry.Resolve(manifest_).Find(property);
  auto const present = found && IsPresent(data(), found->position);

  // Removing only clears a bit: the shape, the layout and the allocation all stay as they
  // are, and the field is there to be filled again if a value comes back.
  if (value.IsNull()) {
    if (present) SetPresent(data(), found->position, false);
    return !present;
  }

  // The common update keeps the shape: the field is already there, at the same type and
  // width, so the value goes straight into the slot the shape points at.
  if (found && found->is_fixed && found->stored_type == StoredTypeOf(value)) {
    auto const &manifest = registry.Resolve(manifest_);
    auto *record = data();
    auto const regions = RegionsOf(manifest, record);
    EncodeFixed(value, found->stored_type, std::span{record + regions.fixed + found->offset, found->stored_type.width});
    SetPresent(record, found->position, true);
    return !present;
  }

  auto const properties = Properties(registry);
  auto merged = utils::small_vector<PropertyPair>{};
  merged.reserve(properties.size() + 1);
  auto placed = false;
  for (auto const &[id, existing] : properties) {
    if (!placed && property < id) {
      merged.emplace_back(property, value);
      placed = true;
    }
    if (id == property) continue;  // replaced by the new value
    merged.emplace_back(id, existing);
  }
  if (!placed) merged.emplace_back(property, value);

  Rebuild(registry, merged);
  return !present;
}

auto ManifestPropertyStore::InitProperties(ManifestRegistry &registry, std::span<PropertyPair const> properties)
    -> bool {
  if (size_ != 0) return false;
  Rebuild(registry, properties);
  return true;
}

auto ManifestPropertyStore::InitProperties(ManifestRegistry &registry,
                                           std::map<PropertyId, PropertyValue> const &properties) -> bool {
  if (size_ != 0) return false;
  auto ordered = utils::small_vector<PropertyPair>{};
  ordered.reserve(properties.size());
  for (auto const &[id, value] : properties) ordered.emplace_back(id, value);
  Rebuild(registry, ordered);
  return true;
}

void ManifestPropertyStore::ReserveFields(ManifestRegistry &registry, std::span<ManifestEntry const> fields) {
  if (fields.empty()) return;

  auto const manifest_id = registry.Intern(fields);
  auto const &manifest = registry.Resolve(manifest_id);
  auto const presence = PresenceBytes(manifest.size());
  // Nothing is present yet, so the variable region is empty and its offsets are all zero.
  auto const offset_width = OffsetWidth(0);
  auto const header = presence + (manifest.variable_count() == 0 ? 0U : 1U + manifest.variable_count() * offset_width);
  auto const total = header + manifest.fixed_region_size();

  manifest_ = manifest_id;
  auto *record = Reset(total);
  if (manifest.variable_count() != 0) record[presence] = offset_width;
}

auto ManifestPropertyStore::ClearProperties() -> bool {
  if (size_ == 0) return false;
  if (size_ > kInlineCapacity) delete[] heap_;
  size_ = 0;
  manifest_ = ManifestId{};
  return true;
}

void ManifestPropertyStore::Rebuild(ManifestRegistry &registry, std::span<PropertyPair const> properties) {
  auto entries = utils::small_vector<ManifestEntry>{};
  entries.reserve(properties.size());
  auto variable_bytes = uint32_t{0};
  for (auto const &[id, value] : properties) {
    if (value.IsNull()) continue;
    auto const stored_type = StoredTypeOf(value);
    entries.push_back(ManifestEntry{.property = id, .stored_type = stored_type});
    if (!stored_type.is_fixed_width()) variable_bytes += VariableSize(value);
  }

  // Fields the record is laid out for but does not carry stay in the shape. A caller that
  // laid the record out for what it was about to set would otherwise lose that the first
  // time a value arrived that could not go straight into its slot.
  if (size_ != 0) {
    auto const &current = registry.Resolve(manifest_);
    auto const *record = data();
    auto merged = utils::small_vector<ManifestEntry>{};
    merged.reserve(entries.size() + current.size());
    auto next = size_t{0};
    for (uint32_t position = 0; position != current.size(); ++position) {
      if (IsPresent(record, position)) continue;
      auto const &field = current.entries()[position];
      while (next != entries.size() && entries[next].property < field.property) merged.push_back(entries[next++]);
      if (next != entries.size() && entries[next].property == field.property) continue;  // a value is arriving
      merged.push_back(field);
    }
    while (next != entries.size()) merged.push_back(entries[next++]);
    entries = std::move(merged);
  }

  if (entries.empty()) {
    ClearProperties();
    return;
  }

  auto const manifest_id = registry.Intern(entries);
  auto const &manifest = registry.Resolve(manifest_id);
  auto const presence = PresenceBytes(manifest.size());
  auto const offset_width = OffsetWidth(variable_bytes);
  auto const header = presence + (manifest.variable_count() == 0 ? 0U : 1U + manifest.variable_count() * offset_width);
  auto const total = header + manifest.fixed_region_size() + variable_bytes;

  // Safe to write straight into the record's new storage: the values come from the caller or
  // from a decoded copy, so none of them point into the storage being replaced.
  manifest_ = manifest_id;
  auto *record = Reset(total);
  if (manifest.variable_count() != 0) record[presence] = offset_width;

  auto const regions = RegionsOf(manifest, record);
  auto variable_end = uint32_t{0};
  // Walk every field of the shape, not only those a value is arriving for. A field the record
  // is laid out for but does not carry still owns a slot in the offset table, and that slot
  // has to hold the running end, or the value after it would appear to start where it does.
  auto next = size_t{0};
  for (uint32_t position = 0; position != manifest.size(); ++position) {
    auto const field = manifest.entries()[position].property;
    while (next != properties.size() && (properties[next].second.IsNull() || properties[next].first < field)) ++next;

    auto const location = manifest.LocationAt(position);
    if (next == properties.size() || properties[next].first != field) {
      if (!location.is_fixed) WriteOffset(record + regions.offset_table, offset_width, location.offset, variable_end);
      continue;
    }

    auto const &value = properties[next++].second;
    SetPresent(record, position, true);
    if (location.is_fixed) {
      EncodeFixed(
          value, location.stored_type, std::span{record + regions.fixed + location.offset, location.stored_type.width});
      continue;
    }
    auto const size = VariableSize(value);
    std::memcpy(record + regions.variable + variable_end, value.ValueString().data(), size);
    variable_end += size;
    WriteOffset(record + regions.offset_table, offset_width, location.offset, variable_end);
  }
}

}  // namespace memgraph::storage
