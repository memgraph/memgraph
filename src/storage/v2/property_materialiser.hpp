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

#include <fp16.h>  // Taken via usearch (seems like _Float16 is broken on some platforms)
#include <chrono>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string_view>

#include "storage/v2/property_manifest.hpp"
#include "storage/v2/property_store_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/logging.hpp"
#include "utils/temporal.hpp"

/// Reading a record into whatever the caller wants to hold it in.
///
/// A read used to build a `PropertyValue` whatever its destination, so a query that wanted a
/// `TypedValue` built the value twice: once here and once on the way out. The pieces below are
/// in a header so a caller can supply its own materialiser and have its value constructed in
/// place instead. Storage still knows nothing about what that value is; the template parameter
/// is the whole of the coupling.
namespace memgraph::storage {

constexpr uint8_t kBoolWidth = 1;
constexpr uint8_t kHalfWidth = 2;      // --storage_floating_point_resolution_bits=16
constexpr uint8_t kFloatWidth = 4;     // --storage_floating_point_resolution_bits=32
constexpr uint8_t kDoubleWidth = 8;    // --storage_floating_point_resolution_bits=64
constexpr uint8_t kTemporalWidth = 8;  // microseconds; which temporal type is part of the shape
constexpr uint8_t kEnumWidth = 8;      // value id; which enum type is part of the shape
constexpr uint8_t kPoint2dWidth = 16;  // x, y; the coordinate system is part of the shape
constexpr uint8_t kPoint3dWidth = 24;  // x, y, z

constexpr uint8_t kOffsetZonedTemporalWidth = 8;

constexpr uint32_t kZonedTemporalMicrosecondsWidth = 8;

/// The shape discriminator an offset-defined zoned temporal carries: the zoned temporal type
/// above the offset itself. An offset is at most +/-18 hours (`utils::MAX_OFFSET_MINUTES`), so
/// it fits in the low half of the discriminator, the type rides above it and the payload stays
/// the microseconds alone.
constexpr auto PackZonedOffset(ZonedTemporalType type, int64_t offset_minutes) -> uint32_t {
  static_assert(utils::MAX_OFFSET_MINUTES <= std::numeric_limits<int16_t>::max());
  return (static_cast<uint32_t>(type) << 16U) | static_cast<uint16_t>(static_cast<int16_t>(offset_minutes));
}

constexpr auto UnpackZonedType(uint32_t discriminator) -> ZonedTemporalType {
  return static_cast<ZonedTemporalType>(discriminator >> 16U);
}

constexpr auto UnpackZonedOffset(uint32_t discriminator) -> std::chrono::minutes {
  return std::chrono::minutes{static_cast<int16_t>(discriminator & 0xFFFFU)};
}

inline auto DecodeInt(uint8_t width, std::span<uint8_t const> in) -> int64_t {
  uint64_t raw = 0;
  std::memcpy(&raw, in.data(), width);
  switch (width) {
    case 1:
      return static_cast<int8_t>(raw);
    case 2:
      return static_cast<int16_t>(raw);
    case 4:
      return static_cast<int32_t>(raw);
    default:
      return static_cast<int64_t>(raw);
  }
}

/// The value a floating point payload holds, at the resolution it was stored at rather than
/// the one currently in force.
inline auto DecodeDouble(uint8_t width, std::span<uint8_t const> in) -> double {
  switch (width) {
    case kHalfWidth: {
      uint16_t half = 0;
      std::memcpy(&half, in.data(), kHalfWidth);
      return fp16_ieee_to_fp32_value(half);
    }
    case kFloatWidth: {
      float single = 0.0F;
      std::memcpy(&single, in.data(), kFloatWidth);
      return single;
    }
    default: {
      double raw = 0.0;
      std::memcpy(&raw, in.data(), kDoubleWidth);
      return raw;
    }
  }
}

/// The instant and the zone name a named-zone temporal payload holds.
inline auto ReadZonedTemporal(uint8_t const *begin, uint32_t size) -> std::pair<int64_t, std::string_view> {
  int64_t microseconds = 0;
  std::memcpy(&microseconds, begin, kZonedTemporalMicrosecondsWidth);
  return {microseconds,
          std::string_view{reinterpret_cast<char const *>(begin + kZonedTemporalMicrosecondsWidth),
                           size - kZonedTemporalMicrosecondsWidth}};
}

/// Bytes of presence bits a shape of `fields` fields needs.
constexpr auto PresenceBytes(size_t fields) -> uint32_t { return static_cast<uint32_t>((fields + 7) / 8); }

inline auto IsPresent(uint8_t const *record, uint32_t position) -> bool {
  return (record[position / 8] & (1U << (position % 8))) != 0;
}

/// Where the regions of a record sit, given its shape.
struct Regions {
  uint8_t offset_width;
  uint32_t offset_table;
  uint32_t fixed;
  uint32_t variable;
};

inline auto RegionsOf(PropertyManifest const &manifest, uint8_t const *record) -> Regions {
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

inline auto ReadOffset(uint8_t const *table, uint8_t width, uint32_t index) -> uint32_t {
  uint32_t offset = 0;
  std::memcpy(&offset, table + static_cast<size_t>(index) * width, width);
  return offset;
}

/// Decodes a payload into a storage value. Defined beside the encoder, because a list or a map
/// is a blob whose reader is not worth making header-visible; this is the fallback a
/// materialiser lands on for the types it is not handed directly.
auto DecodePayload(StoredType stored_type, uint8_t const *begin, uint32_t size) -> PropertyValue;

/// Whether a payload holds `value`, without decoding it. Defined beside the encoder for the
/// same reason as `DecodePayload`.
auto PayloadEquals(StoredType stored_type, uint8_t const *begin, uint32_t size, PropertyValue const &value) -> bool;

/// A reader positioned on one record.
class RecordReader {
 public:
  RecordReader(PropertyManifest const &manifest, uint8_t const *record)
      : manifest_{&manifest}, record_{record}, regions_{RegionsOf(manifest, record)} {}

  auto manifest() const -> PropertyManifest const & { return *manifest_; }

  auto Carries(uint32_t position) const -> bool { return IsPresent(record_, position); }

  /// Where `property` sits, or nothing when the record carries no value for it. A field the
  /// shape has but the record's presence bits deny is no value at all.
  auto Find(PropertyId property) const -> std::optional<PropertyManifest::Location> {
    auto const found = manifest_->Find(property);
    if (!found || !Carries(found->position)) return std::nullopt;
    return found;
  }

  /// The bytes a value occupies. A variable-width value is bounded by the offset table, whose
  /// entries are ends, so a value starts where the one before it finished.
  auto Payload(PropertyManifest::Location const &location) const -> std::span<uint8_t const> {
    if (location.is_fixed) {
      return std::span{record_ + regions_.fixed + location.offset, location.stored_type.width};
    }
    auto const *table = record_ + regions_.offset_table;
    auto const end = ReadOffset(table, regions_.offset_width, location.offset);
    auto const begin = location.offset == 0 ? 0U : ReadOffset(table, regions_.offset_width, location.offset - 1);
    return std::span{record_ + regions_.variable + begin, end - begin};
  }

  auto Read(PropertyManifest::Location const &location) const -> PropertyValue {
    auto const payload = Payload(location);
    return DecodePayload(location.stored_type, payload.data(), static_cast<uint32_t>(payload.size()));
  }

  auto Equals(PropertyManifest::Location const &location, PropertyValue const &value) const -> bool {
    auto const payload = Payload(location);
    return PayloadEquals(location.stored_type, payload.data(), static_cast<uint32_t>(payload.size()), value);
  }

  /// Hands the value at `location` to `out` as whatever it is, so the caller builds it once.
  ///
  /// Everything whose value is fully described by the shape plus a flat payload goes straight
  /// across. A list, a map or a vector-index handle is a blob, and still decodes into a
  /// `PropertyValue` first; that is the fallback, not the path this exists for.
  template <typename Materialiser>
  void ReadInto(PropertyManifest::Location const &location, size_t index, Materialiser &out) const {
    auto const payload = Payload(location);
    auto const stored_type = location.stored_type;
    switch (stored_type.type) {
      case PropertyStoreType::BOOL:
        return out.Emit(index, payload[0] != 0);
      case PropertyStoreType::INT:
        return out.Emit(index, DecodeInt(stored_type.width, payload));
      case PropertyStoreType::DOUBLE:
        return out.Emit(index, DecodeDouble(stored_type.width, payload));
      case PropertyStoreType::STRING:
        return out.Emit(index, std::string_view{reinterpret_cast<char const *>(payload.data()), payload.size()});
      case PropertyStoreType::TEMPORAL_DATA: {
        int64_t microseconds = 0;
        std::memcpy(&microseconds, payload.data(), kTemporalWidth);
        return out.Emit(index, TemporalData{static_cast<TemporalType>(stored_type.discriminator), microseconds});
      }
      case PropertyStoreType::OFFSET_ZONED_TEMPORAL_DATA: {
        int64_t microseconds = 0;
        std::memcpy(&microseconds, payload.data(), kOffsetZonedTemporalWidth);
        return out.Emit(index,
                        ZonedTemporalData{UnpackZonedType(stored_type.discriminator),
                                          utils::AsSysTime(microseconds),
                                          utils::Timezone{UnpackZonedOffset(stored_type.discriminator)}});
      }
      case PropertyStoreType::ZONED_TEMPORAL_DATA: {
        auto const [microseconds, name] = ReadZonedTemporal(payload.data(), static_cast<uint32_t>(payload.size()));
        return out.Emit(index,
                        ZonedTemporalData{static_cast<ZonedTemporalType>(stored_type.discriminator),
                                          utils::AsSysTime(microseconds),
                                          utils::Timezone{name}});
      }
      case PropertyStoreType::ENUM: {
        uint64_t value_id = 0;
        std::memcpy(&value_id, payload.data(), kEnumWidth);
        return out.Emit(index, Enum{EnumTypeId{stored_type.discriminator}, EnumValueId{value_id}});
      }
      case PropertyStoreType::POINT: {
        auto const crs = static_cast<CoordinateReferenceSystem>(stored_type.discriminator);
        if (stored_type.width == kPoint2dWidth) {
          std::array<double, 2> coordinates{};
          std::memcpy(coordinates.data(), payload.data(), kPoint2dWidth);
          return out.Emit(index, Point2d{crs, coordinates[0], coordinates[1]});
        }
        std::array<double, 3> coordinates{};
        std::memcpy(coordinates.data(), payload.data(), kPoint3dWidth);
        return out.Emit(index, Point3d{crs, coordinates[0], coordinates[1], coordinates[2]});
      }
      default:
        return out.Emit(index, DecodePayload(stored_type, payload.data(), static_cast<uint32_t>(payload.size())));
    }
  }

 private:
  PropertyManifest const *manifest_;
  uint8_t const *record_;
  Regions regions_;
};

}  // namespace memgraph::storage
