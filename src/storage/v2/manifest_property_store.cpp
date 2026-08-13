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

#include <fp16.h>  // Taken via usearch (seems like _Float16 is broken on some platforms)
#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <ranges>
#include <set>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include "storage/v2/indexed_property_decoder.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/property_store.hpp"  // FLAGS_storage_floating_point_resolution_bits
#include "utils/logging.hpp"

namespace memgraph::storage {

namespace {

constexpr uint8_t kBoolWidth = 1;
constexpr uint8_t kHalfWidth = 2;      // --storage_floating_point_resolution_bits=16
constexpr uint8_t kFloatWidth = 4;     // --storage_floating_point_resolution_bits=32
constexpr uint8_t kDoubleWidth = 8;    // --storage_floating_point_resolution_bits=64
constexpr uint8_t kTemporalWidth = 8;  // microseconds; which temporal type is part of the shape
constexpr uint8_t kEnumWidth = 8;      // value id; which enum type is part of the shape
constexpr uint8_t kPoint2dWidth = 16;  // x, y; the coordinate system is part of the shape
constexpr uint8_t kPoint3dWidth = 24;  // x, y, z
/// Microseconds; the zoned temporal type and the offset are both part of the shape.
constexpr uint8_t kOffsetZonedTemporalWidth = 8;
/// Microseconds, which the timezone name follows for the rest of the value's bytes.
constexpr uint32_t kZonedTemporalMicrosecondsWidth = 8;

/// The shape discriminator an offset-defined zoned temporal carries: the zoned temporal type
/// above the offset itself. An offset is at most ±18 hours (`utils::MAX_OFFSET_MINUTES`), so it
/// fits in the low half of the discriminator, the type rides above it and the payload stays the
/// microseconds alone. Two records whose offsets differ therefore have different shapes, which
/// is what the discriminator is for and what keeps every record of one shape identically laid
/// out; the number of distinct offsets a zone can have is small and fixed.
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

/// A zoned temporal is stored as the UTC instant plus the timezone it is to be read in, never
/// as a local time plus an offset, so nothing here has to pick between the two instants a
/// daylight saving transition gives a local time.
auto MakeZoned(ZonedTemporalType type, int64_t microseconds, utils::Timezone timezone) -> PropertyValue {
  return PropertyValue{ZonedTemporalData{type, utils::AsSysTime(microseconds), timezone}};
}

/// Payload width a double is stored at under the resolution currently in force. Consulted
/// only when a value is encoded: a record is decoded at the width its own shape records, so a
/// resolution change between writing a record and reading it back cannot reinterpret bytes
/// that were written at another width.
auto DoubleWidth() -> uint8_t {
  switch (FLAGS_storage_floating_point_resolution_bits) {
    case 16:
      return kHalfWidth;
    case 32:
      return kFloatWidth;
    default:
      return kDoubleWidth;
  }
}

/// Narrowest signed width that still round-trips `value`.
auto IntWidth(int64_t value) -> uint8_t {
  if (value >= std::numeric_limits<int8_t>::min() && value <= std::numeric_limits<int8_t>::max()) return 1;
  if (value >= std::numeric_limits<int16_t>::min() && value <= std::numeric_limits<int16_t>::max()) return 2;
  if (value >= std::numeric_limits<int32_t>::min() && value <= std::numeric_limits<int32_t>::max()) return 4;
  return 8;
}

/// Opens every value inside a list or a map. A list or a map is one opaque variable-width
/// blob, so nothing about what it holds is in the shape and everything about it is in these
/// tags: the blob describes itself.
///
/// Integers and doubles carry their width in the tag rather than in a shape, so a list keeps
/// the same narrowing a scalar field gets, and a double the same reduced precision.
enum class BlobTag : uint8_t {
  Null,
  False,
  True,
  Int8,
  Int16,
  Int32,
  Int64,
  Half,
  Float,
  Double,
  String,
  List,
  IntList,
  DoubleList,
  NumericList,
  Map,
  TemporalData,
  /// A zoned temporal whose timezone is a fixed offset, which is written after the instant.
  OffsetZonedTemporalData,
  /// A zoned temporal whose timezone is a named one, whose name is written after the instant.
  NamedZonedTemporalData,
  Enum,
  Point2d,
  Point3d,
};

/// Writes a blob, or measures one: given no destination it counts the bytes instead of
/// writing them, which is how a record learns what room a list or a map needs before it is
/// laid out.
class BlobWriter {
 public:
  BlobWriter() = default;

  explicit BlobWriter(uint8_t *out) : out_{out} {}

  auto size() const -> uint32_t { return size_; }

  void Bytes(void const *data, uint32_t count) {
    if (out_ != nullptr) std::memcpy(out_ + size_, data, count);
    size_ += count;
  }

  void Tag(BlobTag tag) {
    auto const raw = static_cast<uint8_t>(tag);
    Bytes(&raw, sizeof(raw));
  }

  template <typename T>
  void Raw(T value) {
    Bytes(&value, sizeof(value));
  }

  void Count(size_t count) { Raw(static_cast<uint32_t>(count)); }

 private:
  uint8_t *out_{};
  uint32_t size_{};
};

/// Reads a blob back. The bytes were written by `BlobWriter`, so every read is of a length
/// the tag stream has already fixed and there is nothing to bound check.
class BlobReader {
 public:
  explicit BlobReader(uint8_t const *in) : in_{in} {}

  auto Tag() -> BlobTag { return static_cast<BlobTag>(*in_++); }

  template <typename T>
  auto Raw() -> T {
    T value{};
    std::memcpy(&value, in_, sizeof(value));
    in_ += sizeof(value);
    return value;
  }

  auto Count() -> uint32_t { return Raw<uint32_t>(); }

  /// The next `count` bytes, which the reader then skips over.
  auto Bytes(uint32_t count) -> uint8_t const * {
    auto const *at = in_;
    in_ += count;
    return at;
  }

 private:
  uint8_t const *in_;
};

[[noreturn]] void ThrowUnsupported(PropertyValue const &value) {
  auto message = std::ostringstream{};
  message << "ManifestPropertyStore cannot yet encode a " << value.type() << " property";
  throw ManifestPropertyStore::UnsupportedType{std::move(message).str()};
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
      return StoredType::Fixed(PropertyStoreType::DOUBLE, DoubleWidth());
    case PropertyValueType::String:
      return StoredType::Variable(PropertyStoreType::STRING);
    case PropertyValueType::List:
    case PropertyValueType::IntList:
    case PropertyValueType::DoubleList:
    case PropertyValueType::NumericList:
      return StoredType::Variable(PropertyStoreType::LIST);
    case PropertyValueType::Map:
      return StoredType::Variable(PropertyStoreType::MAP);
    case PropertyValueType::TemporalData:
      return StoredType::Fixed(
          PropertyStoreType::TEMPORAL_DATA, kTemporalWidth, static_cast<uint32_t>(value.ValueTemporalData().type));
    case PropertyValueType::ZonedTemporalData: {
      // The two kinds of timezone are two stored types: an offset is small enough to live in
      // the shape and leave the payload fixed width, while a name is a string of unbounded
      // length and so has to go in the variable region. A record holding a named zone and one
      // holding an offset therefore have different shapes.
      auto const zoned = value.ValueZonedTemporalData();
      if (zoned.timezone.InTzDatabase()) {
        return StoredType{.type = PropertyStoreType::ZONED_TEMPORAL_DATA,
                          .width = 0,
                          .discriminator = static_cast<uint32_t>(zoned.type)};
      }
      return StoredType::Fixed(PropertyStoreType::OFFSET_ZONED_TEMPORAL_DATA,
                               kOffsetZonedTemporalWidth,
                               PackZonedOffset(zoned.type, zoned.timezone.DefiningOffset()));
    }
    case PropertyValueType::Enum:
      return StoredType::Fixed(
          PropertyStoreType::ENUM, kEnumWidth, static_cast<uint32_t>(value.ValueEnum().type_id().value_of()));
    case PropertyValueType::Point2d:
      return StoredType::Fixed(
          PropertyStoreType::POINT, kPoint2dWidth, static_cast<uint32_t>(value.ValuePoint2d().crs()));
    case PropertyValueType::Point3d:
      return StoredType::Fixed(
          PropertyStoreType::POINT, kPoint3dWidth, static_cast<uint32_t>(value.ValuePoint3d().crs()));
    default:
      ThrowUnsupported(value);
  }
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
      switch (stored_type.width) {
        case kHalfWidth: {
          auto const half = fp16_ieee_from_fp32_value(static_cast<float>(raw));
          std::memcpy(out.data(), &half, kHalfWidth);
          return;
        }
        case kFloatWidth: {
          auto const single = static_cast<float>(raw);
          std::memcpy(out.data(), &single, kFloatWidth);
          return;
        }
        default:
          std::memcpy(out.data(), &raw, kDoubleWidth);
          return;
      }
    }
    case PropertyStoreType::TEMPORAL_DATA: {
      auto const raw = value.ValueTemporalData().microseconds;
      std::memcpy(out.data(), &raw, kTemporalWidth);
      return;
    }
    case PropertyStoreType::OFFSET_ZONED_TEMPORAL_DATA: {
      // The offset and the type are both in the shape, so only the instant is left to write.
      auto const raw = value.ValueZonedTemporalData().IntMicroseconds();
      std::memcpy(out.data(), &raw, kOffsetZonedTemporalWidth);
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

/// The value an integer payload holds, sign-extended from whatever width it was stored at.
/// Two records of the same value may disagree on that width, so nothing may be concluded from
/// the bytes without widening them first.
auto DecodeInt(uint8_t width, std::span<uint8_t const> in) -> int64_t {
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
auto DecodeDouble(uint8_t width, std::span<uint8_t const> in) -> double {
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

auto DecodeFixed(StoredType stored_type, std::span<uint8_t const> in) -> PropertyValue {
  switch (stored_type.type) {
    case PropertyStoreType::BOOL:
      return PropertyValue{in[0] != 0};
    case PropertyStoreType::INT:
      return PropertyValue{DecodeInt(stored_type.width, in)};
    case PropertyStoreType::DOUBLE:
      return PropertyValue{DecodeDouble(stored_type.width, in)};
    case PropertyStoreType::TEMPORAL_DATA: {
      int64_t microseconds = 0;
      std::memcpy(&microseconds, in.data(), kTemporalWidth);
      return PropertyValue{TemporalData{static_cast<TemporalType>(stored_type.discriminator), microseconds}};
    }
    case PropertyStoreType::OFFSET_ZONED_TEMPORAL_DATA: {
      int64_t microseconds = 0;
      std::memcpy(&microseconds, in.data(), kOffsetZonedTemporalWidth);
      return MakeZoned(UnpackZonedType(stored_type.discriminator),
                       microseconds,
                       utils::Timezone{UnpackZonedOffset(stored_type.discriminator)});
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

/// Whether a fixed-width payload holds `value`, without decoding it into a `PropertyValue`.
///
/// The comparison is on values, never on encodings: the same value may be stored at different
/// widths in different records, so the record's payload is widened to what it means and
/// compared with that. Taking the shape's stored type as the answer instead would report two
/// records holding the same integer as unequal, which on the unique constraint path is a
/// duplicate let through.
auto FixedEquals(StoredType stored_type, std::span<uint8_t const> in, PropertyValue const &value) -> bool {
  switch (stored_type.type) {
    case PropertyStoreType::BOOL:
      return value.IsBool() && value.ValueBool() == (in[0] != 0);
    case PropertyStoreType::INT: {
      // Integers and doubles compare across types, as `PropertyValue::operator==` has them do.
      if (value.IsInt()) return value.ValueInt() == DecodeInt(stored_type.width, in);
      if (value.IsDouble()) return value.ValueDouble() == static_cast<double>(DecodeInt(stored_type.width, in));
      return false;
    }
    case PropertyStoreType::DOUBLE: {
      if (value.IsDouble()) return value.ValueDouble() == DecodeDouble(stored_type.width, in);
      if (value.IsInt()) return static_cast<double>(value.ValueInt()) == DecodeDouble(stored_type.width, in);
      return false;
    }
    case PropertyStoreType::TEMPORAL_DATA: {
      if (!value.IsTemporalData()) return false;
      auto const &temporal = value.ValueTemporalData();
      if (temporal.type != static_cast<TemporalType>(stored_type.discriminator)) return false;
      int64_t microseconds = 0;
      std::memcpy(&microseconds, in.data(), kTemporalWidth);
      return temporal.microseconds == microseconds;
    }
    case PropertyStoreType::OFFSET_ZONED_TEMPORAL_DATA: {
      if (!value.IsZonedTemporalData()) return false;
      auto const zoned = value.ValueZonedTemporalData();
      if (zoned.type != UnpackZonedType(stored_type.discriminator)) return false;
      // A named zone is never a fixed offset, whatever offset it happens to be at right now:
      // the two are different timezones and `utils::Timezone` has them unequal.
      if (zoned.timezone.InTzDatabase()) return false;
      if (zoned.timezone.DefiningOffset() != UnpackZonedOffset(stored_type.discriminator).count()) return false;
      int64_t microseconds = 0;
      std::memcpy(&microseconds, in.data(), kOffsetZonedTemporalWidth);
      return zoned.IntMicroseconds() == microseconds;
    }
    case PropertyStoreType::ENUM: {
      if (!value.IsEnum()) return false;
      auto const &enum_value = value.ValueEnum();
      if (enum_value.type_id().value_of() != stored_type.discriminator) return false;
      uint64_t value_id = 0;
      std::memcpy(&value_id, in.data(), kEnumWidth);
      return enum_value.value_id().value_of() == value_id;
    }
    case PropertyStoreType::POINT: {
      auto const crs = static_cast<CoordinateReferenceSystem>(stored_type.discriminator);
      if (stored_type.width == kPoint2dWidth) {
        if (!value.IsPoint2d()) return false;
        std::array<double, 2> coordinates{};
        std::memcpy(coordinates.data(), in.data(), kPoint2dWidth);
        return value.ValuePoint2d() == Point2d{crs, coordinates[0], coordinates[1]};
      }
      if (!value.IsPoint3d()) return false;
      std::array<double, 3> coordinates{};
      std::memcpy(coordinates.data(), in.data(), kPoint3dWidth);
      return value.ValuePoint3d() == Point3d{crs, coordinates[0], coordinates[1], coordinates[2]};
    }
    default:
      LOG_FATAL("Not a fixed-width property type");
  }
}

void EncodeBlobValue(PropertyValue const &value, BlobWriter &out);

void EncodeBlobInt(int64_t value, BlobWriter &out) {
  switch (IntWidth(value)) {
    case 1:
      out.Tag(BlobTag::Int8);
      out.Raw(static_cast<int8_t>(value));
      return;
    case 2:
      out.Tag(BlobTag::Int16);
      out.Raw(static_cast<int16_t>(value));
      return;
    case 4:
      out.Tag(BlobTag::Int32);
      out.Raw(static_cast<int32_t>(value));
      return;
    default:
      out.Tag(BlobTag::Int64);
      out.Raw(value);
      return;
  }
}

void EncodeBlobDouble(double value, BlobWriter &out) {
  switch (DoubleWidth()) {
    case kHalfWidth:
      out.Tag(BlobTag::Half);
      out.Raw(fp16_ieee_from_fp32_value(static_cast<float>(value)));
      return;
    case kFloatWidth:
      out.Tag(BlobTag::Float);
      out.Raw(static_cast<float>(value));
      return;
    default:
      out.Tag(BlobTag::Double);
      out.Raw(value);
      return;
  }
}

void EncodeBlobValue(PropertyValue const &value, BlobWriter &out) {
  switch (value.type()) {
    case PropertyValueType::Null:
      out.Tag(BlobTag::Null);
      return;
    case PropertyValueType::Bool:
      out.Tag(value.ValueBool() ? BlobTag::True : BlobTag::False);
      return;
    case PropertyValueType::Int:
      EncodeBlobInt(value.ValueInt(), out);
      return;
    case PropertyValueType::Double:
      EncodeBlobDouble(value.ValueDouble(), out);
      return;
    case PropertyValueType::String: {
      auto const &string = value.ValueString();
      out.Tag(BlobTag::String);
      out.Count(string.size());
      out.Bytes(string.data(), static_cast<uint32_t>(string.size()));
      return;
    }
    case PropertyValueType::List: {
      auto const &list = value.ValueList();
      out.Tag(BlobTag::List);
      out.Count(list.size());
      for (auto const &element : list) EncodeBlobValue(element, out);
      return;
    }
    case PropertyValueType::IntList: {
      auto const &list = value.ValueIntList();
      out.Tag(BlobTag::IntList);
      out.Count(list.size());
      for (auto const element : list) EncodeBlobInt(element, out);
      return;
    }
    case PropertyValueType::DoubleList: {
      auto const &list = value.ValueDoubleList();
      out.Tag(BlobTag::DoubleList);
      out.Count(list.size());
      for (auto const element : list) EncodeBlobDouble(element, out);
      return;
    }
    case PropertyValueType::NumericList: {
      auto const &list = value.ValueNumericList();
      out.Tag(BlobTag::NumericList);
      out.Count(list.size());
      for (auto const &element : list) {
        // Which alternative an element holds is kept, so a numeric list comes back with the
        // integers it went in with rather than as doubles.
        std::visit(
            [&out](auto const number) {
              if constexpr (std::is_integral_v<decltype(number)>) {
                EncodeBlobInt(number, out);
              } else {
                EncodeBlobDouble(number, out);
              }
            },
            element);
      }
      return;
    }
    case PropertyValueType::Map: {
      auto const &map = value.ValueMap();
      out.Tag(BlobTag::Map);
      out.Count(map.size());
      // In key order, which the map is already in, so decoding rebuilds it by appending and
      // a comparison can walk both sides in step.
      for (auto const &[key, element] : map) {
        out.Raw(key.AsUint());
        EncodeBlobValue(element, out);
      }
      return;
    }
    case PropertyValueType::TemporalData: {
      auto const temporal = value.ValueTemporalData();
      out.Tag(BlobTag::TemporalData);
      out.Raw(static_cast<uint8_t>(temporal.type));
      out.Raw(temporal.microseconds);
      return;
    }
    case PropertyValueType::ZonedTemporalData: {
      auto const zoned = value.ValueZonedTemporalData();
      if (zoned.timezone.InTzDatabase()) {
        auto const name = zoned.timezone.TimezoneName();
        out.Tag(BlobTag::NamedZonedTemporalData);
        out.Raw(static_cast<uint8_t>(zoned.type));
        out.Raw(zoned.IntMicroseconds());
        out.Count(name.size());
        out.Bytes(name.data(), static_cast<uint32_t>(name.size()));
        return;
      }
      out.Tag(BlobTag::OffsetZonedTemporalData);
      out.Raw(static_cast<uint8_t>(zoned.type));
      out.Raw(zoned.IntMicroseconds());
      out.Raw(static_cast<int16_t>(zoned.timezone.DefiningOffset()));
      return;
    }
    case PropertyValueType::Enum: {
      auto const enum_value = value.ValueEnum();
      out.Tag(BlobTag::Enum);
      out.Raw(enum_value.type_id().value_of());
      out.Raw(enum_value.value_id().value_of());
      return;
    }
    case PropertyValueType::Point2d: {
      auto const point = value.ValuePoint2d();
      out.Tag(BlobTag::Point2d);
      out.Raw(static_cast<uint8_t>(point.crs()));
      out.Raw(point.x());
      out.Raw(point.y());
      return;
    }
    case PropertyValueType::Point3d: {
      auto const point = value.ValuePoint3d();
      out.Tag(BlobTag::Point3d);
      out.Raw(static_cast<uint8_t>(point.crs()));
      out.Raw(point.x());
      out.Raw(point.y());
      out.Raw(point.z());
      return;
    }
    default:
      ThrowUnsupported(value);
  }
}

/// The value an integer payload holds, at the width its tag gave it.
auto DecodeBlobInt(BlobTag tag, BlobReader &in) -> int64_t {
  switch (tag) {
    case BlobTag::Int8:
      return in.Raw<int8_t>();
    case BlobTag::Int16:
      return in.Raw<int16_t>();
    case BlobTag::Int32:
      return in.Raw<int32_t>();
    default:
      return in.Raw<int64_t>();
  }
}

/// The value a floating point payload holds, at the resolution it was written at rather than
/// the one currently in force.
auto DecodeBlobDouble(BlobTag tag, BlobReader &in) -> double {
  switch (tag) {
    case BlobTag::Half:
      return fp16_ieee_to_fp32_value(in.Raw<uint16_t>());
    case BlobTag::Float:
      return in.Raw<float>();
    default:
      return in.Raw<double>();
  }
}

auto DecodeBlobValue(BlobReader &in) -> PropertyValue {
  auto const tag = in.Tag();
  switch (tag) {
    case BlobTag::Null:
      return PropertyValue{};
    case BlobTag::False:
      return PropertyValue{false};
    case BlobTag::True:
      return PropertyValue{true};
    case BlobTag::Int8:
    case BlobTag::Int16:
    case BlobTag::Int32:
    case BlobTag::Int64:
      return PropertyValue{DecodeBlobInt(tag, in)};
    case BlobTag::Half:
    case BlobTag::Float:
    case BlobTag::Double:
      return PropertyValue{DecodeBlobDouble(tag, in)};
    case BlobTag::String: {
      auto const size = in.Count();
      return PropertyValue{std::string_view{reinterpret_cast<char const *>(in.Bytes(size)), size}};
    }
    case BlobTag::List: {
      auto const count = in.Count();
      auto list = PropertyValue::list_t{};
      list.reserve(count);
      for (uint32_t i = 0; i != count; ++i) list.push_back(DecodeBlobValue(in));
      return PropertyValue{std::move(list)};
    }
    case BlobTag::IntList: {
      auto const count = in.Count();
      auto list = PropertyValue::int_list_t{};
      list.reserve(count);
      for (uint32_t i = 0; i != count; ++i) list.push_back(static_cast<int>(DecodeBlobInt(in.Tag(), in)));
      return PropertyValue{std::move(list)};
    }
    case BlobTag::DoubleList: {
      auto const count = in.Count();
      auto list = PropertyValue::double_list_t{};
      list.reserve(count);
      for (uint32_t i = 0; i != count; ++i) list.push_back(DecodeBlobDouble(in.Tag(), in));
      return PropertyValue{std::move(list)};
    }
    case BlobTag::NumericList: {
      auto const count = in.Count();
      auto list = PropertyValue::numeric_list_t{};
      list.reserve(count);
      for (uint32_t i = 0; i != count; ++i) {
        auto const element_tag = in.Tag();
        if (element_tag == BlobTag::Half || element_tag == BlobTag::Float || element_tag == BlobTag::Double) {
          list.emplace_back(DecodeBlobDouble(element_tag, in));
        } else {
          list.emplace_back(static_cast<int>(DecodeBlobInt(element_tag, in)));
        }
      }
      return PropertyValue{std::move(list)};
    }
    case BlobTag::Map: {
      auto const count = in.Count();
      auto map = PropertyValue::map_t{};
      map.reserve(count);
      for (uint32_t i = 0; i != count; ++i) {
        auto const key = PropertyId::FromUint(in.Raw<uint32_t>());
        map.emplace(key, DecodeBlobValue(in));
      }
      return PropertyValue{std::move(map)};
    }
    case BlobTag::TemporalData: {
      auto const type = static_cast<TemporalType>(in.Raw<uint8_t>());
      return PropertyValue{TemporalData{type, in.Raw<int64_t>()}};
    }
    case BlobTag::OffsetZonedTemporalData: {
      auto const type = static_cast<ZonedTemporalType>(in.Raw<uint8_t>());
      auto const microseconds = in.Raw<int64_t>();
      return MakeZoned(type, microseconds, utils::Timezone{std::chrono::minutes{in.Raw<int16_t>()}});
    }
    case BlobTag::NamedZonedTemporalData: {
      auto const type = static_cast<ZonedTemporalType>(in.Raw<uint8_t>());
      auto const microseconds = in.Raw<int64_t>();
      auto const size = in.Count();
      auto const name = std::string_view{reinterpret_cast<char const *>(in.Bytes(size)), size};
      return MakeZoned(type, microseconds, utils::Timezone{name});
    }
    case BlobTag::Enum: {
      auto const type_id = EnumTypeId{in.Raw<uint64_t>()};
      return PropertyValue{Enum{type_id, EnumValueId{in.Raw<uint64_t>()}}};
    }
    case BlobTag::Point2d: {
      auto const crs = static_cast<CoordinateReferenceSystem>(in.Raw<uint8_t>());
      auto const x = in.Raw<double>();
      return PropertyValue{Point2d{crs, x, in.Raw<double>()}};
    }
    case BlobTag::Point3d: {
      auto const crs = static_cast<CoordinateReferenceSystem>(in.Raw<uint8_t>());
      auto const x = in.Raw<double>();
      auto const y = in.Raw<double>();
      return PropertyValue{Point3d{crs, x, y, in.Raw<double>()}};
    }
  }
  LOG_FATAL("Not a blob tag");
}

auto BlobEquals(BlobReader &in, PropertyValue const &value) -> bool;

/// Whether the `count` elements a list blob holds are the elements of `value`, which may be a
/// list of any of the four flavours. Numeric elements compare across flavours and across the
/// integer/double divide, as `PropertyValue::operator==` has them.
auto BlobListEquals(BlobReader &in, uint32_t count, PropertyValue const &value) -> bool {
  auto const each_equal = [&in, count](auto const &list, auto as_value) {
    if (list.size() != count) return false;
    return std::ranges::all_of(list,
                               [&in, &as_value](auto const &element) { return BlobEquals(in, as_value(element)); });
  };

  switch (value.type()) {
    case PropertyValueType::List: {
      auto const &list = value.ValueList();
      if (list.size() != count) return false;
      return std::ranges::all_of(list, [&in](PropertyValue const &element) { return BlobEquals(in, element); });
    }
    case PropertyValueType::IntList:
      return each_equal(value.ValueIntList(), [](int element) { return PropertyValue{static_cast<int64_t>(element)}; });
    case PropertyValueType::DoubleList:
      return each_equal(value.ValueDoubleList(), [](double element) { return PropertyValue{element}; });
    case PropertyValueType::NumericList:
      return each_equal(value.ValueNumericList(), [](std::variant<int, double> const &element) {
        return std::visit(
            [](auto const number) {
              if constexpr (std::is_integral_v<decltype(number)>) {
                return PropertyValue{static_cast<int64_t>(number)};
              } else {
                return PropertyValue{number};
              }
            },
            element);
      });
    default:
      return false;
  }
}

/// Whether the next value in a blob is `value`, without decoding the blob.
///
/// As with the fixed-width payloads, the comparison is on values and never on encodings: the
/// same list may be written narrow in one record and wide in another, and a list of integers
/// equals a list of the same numbers written as doubles.
auto BlobEquals(BlobReader &in, PropertyValue const &value) -> bool {
  auto const tag = in.Tag();
  switch (tag) {
    case BlobTag::Null:
      return value.IsNull();
    case BlobTag::False:
      return value.IsBool() && !value.ValueBool();
    case BlobTag::True:
      return value.IsBool() && value.ValueBool();
    case BlobTag::Int8:
    case BlobTag::Int16:
    case BlobTag::Int32:
    case BlobTag::Int64: {
      auto const stored = DecodeBlobInt(tag, in);
      if (value.IsInt()) return value.ValueInt() == stored;
      if (value.IsDouble()) return value.ValueDouble() == static_cast<double>(stored);
      return false;
    }
    case BlobTag::Half:
    case BlobTag::Float:
    case BlobTag::Double: {
      auto const stored = DecodeBlobDouble(tag, in);
      if (value.IsDouble()) return value.ValueDouble() == stored;
      if (value.IsInt()) return static_cast<double>(value.ValueInt()) == stored;
      return false;
    }
    case BlobTag::String: {
      auto const size = in.Count();
      auto const *bytes = in.Bytes(size);
      if (!value.IsString()) return false;
      auto const &string = value.ValueString();
      return string.size() == size && std::memcmp(bytes, string.data(), size) == 0;
    }
    case BlobTag::List:
    case BlobTag::IntList:
    case BlobTag::DoubleList:
    case BlobTag::NumericList: {
      auto const count = in.Count();
      return BlobListEquals(in, count, value);
    }
    case BlobTag::Map: {
      auto const count = in.Count();
      if (!value.IsMap()) return false;
      auto const &map = value.ValueMap();
      if (map.size() != count) return false;
      // Both sides are in key order, so one walk in step decides it.
      for (auto const &[key, element] : map) {
        if (in.Raw<uint32_t>() != key.AsUint()) return false;
        if (!BlobEquals(in, element)) return false;
      }
      return true;
    }
    case BlobTag::TemporalData: {
      auto const type = static_cast<TemporalType>(in.Raw<uint8_t>());
      auto const microseconds = in.Raw<int64_t>();
      return value.IsTemporalData() && value.ValueTemporalData() == TemporalData{type, microseconds};
    }
    case BlobTag::OffsetZonedTemporalData: {
      auto const type = static_cast<ZonedTemporalType>(in.Raw<uint8_t>());
      auto const microseconds = in.Raw<int64_t>();
      auto const offset = in.Raw<int16_t>();
      if (!value.IsZonedTemporalData()) return false;
      auto const zoned = value.ValueZonedTemporalData();
      if (zoned.timezone.InTzDatabase()) return false;
      return zoned.type == type && zoned.IntMicroseconds() == microseconds && zoned.timezone.DefiningOffset() == offset;
    }
    case BlobTag::NamedZonedTemporalData: {
      auto const type = static_cast<ZonedTemporalType>(in.Raw<uint8_t>());
      auto const microseconds = in.Raw<int64_t>();
      auto const size = in.Count();
      auto const *name = in.Bytes(size);
      if (!value.IsZonedTemporalData()) return false;
      auto const zoned = value.ValueZonedTemporalData();
      if (!zoned.timezone.InTzDatabase()) return false;
      if (zoned.type != type || zoned.IntMicroseconds() != microseconds) return false;
      // The name is compared where it lies rather than looked up, so nothing is allocated and
      // no timezone is constructed to decide this.
      auto const stored_name = zoned.timezone.TimezoneName();
      return stored_name.size() == size && std::memcmp(name, stored_name.data(), size) == 0;
    }
    case BlobTag::Enum: {
      auto const type_id = EnumTypeId{in.Raw<uint64_t>()};
      auto const value_id = EnumValueId{in.Raw<uint64_t>()};
      return value.IsEnum() && value.ValueEnum() == Enum{type_id, value_id};
    }
    case BlobTag::Point2d: {
      auto const crs = static_cast<CoordinateReferenceSystem>(in.Raw<uint8_t>());
      auto const x = in.Raw<double>();
      auto const y = in.Raw<double>();
      return value.IsPoint2d() && value.ValuePoint2d() == Point2d{crs, x, y};
    }
    case BlobTag::Point3d: {
      auto const crs = static_cast<CoordinateReferenceSystem>(in.Raw<uint8_t>());
      auto const x = in.Raw<double>();
      auto const y = in.Raw<double>();
      auto const z = in.Raw<double>();
      return value.IsPoint3d() && value.ValuePoint3d() == Point3d{crs, x, y, z};
    }
  }
  LOG_FATAL("Not a blob tag");
}

/// Encoded length a variable-width value contributes to the variable region. A string is its
/// own bytes; a named-zone temporal is its instant and its zone's name; a list or a map is a
/// blob, whose length is what writing it would come to.
auto VariableSize(PropertyValue const &value) -> uint32_t {
  if (value.IsString()) return static_cast<uint32_t>(value.ValueString().size());
  if (value.IsZonedTemporalData()) {
    return kZonedTemporalMicrosecondsWidth +
           static_cast<uint32_t>(value.ValueZonedTemporalData().timezone.TimezoneName().size());
  }
  auto measured = BlobWriter{};
  EncodeBlobValue(value, measured);
  return measured.size();
}

/// Writes a variable-width value into the variable region, returning its length.
auto WriteVariable(PropertyValue const &value, uint8_t *out) -> uint32_t {
  if (value.IsString()) {
    auto const &string = value.ValueString();
    std::memcpy(out, string.data(), string.size());
    return static_cast<uint32_t>(string.size());
  }
  if (value.IsZonedTemporalData()) {
    // The zoned temporal type is in the shape, so the instant is followed straight by the
    // zone's name, which runs to the end of the value and so needs no length of its own.
    auto const zoned = value.ValueZonedTemporalData();
    auto const microseconds = zoned.IntMicroseconds();
    std::memcpy(out, &microseconds, kZonedTemporalMicrosecondsWidth);
    auto const name = zoned.timezone.TimezoneName();
    std::memcpy(out + kZonedTemporalMicrosecondsWidth, name.data(), name.size());
    return kZonedTemporalMicrosecondsWidth + static_cast<uint32_t>(name.size());
  }
  auto written = BlobWriter{out};
  EncodeBlobValue(value, written);
  return written.size();
}

/// The instant and the zone name a named-zone temporal payload holds.
auto ReadZonedTemporal(uint8_t const *begin, uint32_t size) -> std::pair<int64_t, std::string_view> {
  int64_t microseconds = 0;
  std::memcpy(&microseconds, begin, kZonedTemporalMicrosecondsWidth);
  return {microseconds,
          std::string_view{reinterpret_cast<char const *>(begin + kZonedTemporalMicrosecondsWidth),
                           size - kZonedTemporalMicrosecondsWidth}};
}

auto DecodeVariable(StoredType stored_type, uint8_t const *begin, uint32_t size) -> PropertyValue {
  if (stored_type.type == PropertyStoreType::STRING) {
    return PropertyValue{std::string_view{reinterpret_cast<char const *>(begin), size}};
  }
  if (stored_type.type == PropertyStoreType::ZONED_TEMPORAL_DATA) {
    auto const [microseconds, name] = ReadZonedTemporal(begin, size);
    return MakeZoned(static_cast<ZonedTemporalType>(stored_type.discriminator), microseconds, utils::Timezone{name});
  }
  auto reader = BlobReader{begin};
  return DecodeBlobValue(reader);
}

/// Whether a variable-width payload holds `value`, without decoding it.
auto VariableEquals(StoredType stored_type, uint8_t const *begin, uint32_t size, PropertyValue const &value) -> bool {
  if (stored_type.type == PropertyStoreType::STRING) {
    if (!value.IsString()) return false;
    auto const &string = value.ValueString();
    return string.size() == size && std::memcmp(begin, string.data(), size) == 0;
  }
  if (stored_type.type == PropertyStoreType::ZONED_TEMPORAL_DATA) {
    if (!value.IsZonedTemporalData()) return false;
    auto const zoned = value.ValueZonedTemporalData();
    if (zoned.type != static_cast<ZonedTemporalType>(stored_type.discriminator)) return false;
    // A fixed offset is never the named zone stored here, whatever offset that zone is at.
    if (!zoned.timezone.InTzDatabase()) return false;
    auto const [microseconds, name] = ReadZonedTemporal(begin, size);
    if (zoned.IntMicroseconds() != microseconds) return false;
    // Compared where it lies, so nothing is allocated and no timezone is looked up.
    auto const stored_name = zoned.timezone.TimezoneName();
    return stored_name.size() == name.size() && std::memcmp(name.data(), stored_name.data(), name.size()) == 0;
  }
  auto reader = BlobReader{begin};
  return BlobEquals(reader, value);
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

/// Heap records are sized in multiples of eight so that their size can never be mistaken for
/// the marker an inline record carries.
constexpr auto ToMultipleOf8(uint32_t size) -> uint32_t { return (size + 7U) & ~7U; }

/// How a stored type reads as a property type. Everything a type is made of that is not the
/// value itself lives in the shape, so nothing here reads a payload: which temporal type, which
/// enum type, and, through the width the shape reserved, whether a point is 2d or 3d.
auto ExtendedTypeOf(StoredType stored_type) -> ExtendedPropertyType {
  switch (stored_type.type) {
    case PropertyStoreType::BOOL:
      return ExtendedPropertyType{PropertyValueType::Bool};
    case PropertyStoreType::INT:
      return ExtendedPropertyType{PropertyValueType::Int};
    case PropertyStoreType::DOUBLE:
      return ExtendedPropertyType{PropertyValueType::Double};
    case PropertyStoreType::STRING:
      return ExtendedPropertyType{PropertyValueType::String};
    case PropertyStoreType::LIST:
      return ExtendedPropertyType{PropertyValueType::List};
    case PropertyStoreType::MAP:
      return ExtendedPropertyType{PropertyValueType::Map};
    case PropertyStoreType::TEMPORAL_DATA:
      return ExtendedPropertyType{static_cast<TemporalType>(stored_type.discriminator)};
    case PropertyStoreType::ZONED_TEMPORAL_DATA:
    case PropertyStoreType::OFFSET_ZONED_TEMPORAL_DATA:
      return ExtendedPropertyType{PropertyValueType::ZonedTemporalData};
    case PropertyStoreType::ENUM:
      return ExtendedPropertyType{EnumTypeId{stored_type.discriminator}};
    case PropertyStoreType::POINT:
      // A point's dimension is not a stored type of its own; the width the shape reserved is
      // what tells the two apart.
      return ExtendedPropertyType{stored_type.width == kPoint2dWidth ? PropertyValueType::Point2d
                                                                     : PropertyValueType::Point3d};
    default:
      LOG_FATAL("Not a stored property type");
  }
}

/// One record read through its shape. Resolving the shape and locating the record's regions is
/// done once here, so an operation that reads several values pays for it once.
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
    if (location.is_fixed) return DecodeFixed(location.stored_type, payload);
    return DecodeVariable(location.stored_type, payload.data(), static_cast<uint32_t>(payload.size()));
  }

  auto Equals(PropertyManifest::Location const &location, PropertyValue const &value) const -> bool {
    auto const payload = Payload(location);
    if (location.is_fixed) return FixedEquals(location.stored_type, payload, value);
    return VariableEquals(location.stored_type, payload.data(), static_cast<uint32_t>(payload.size()), value);
  }

 private:
  PropertyManifest const *manifest_;
  uint8_t const *record_;
  Regions regions_;
};

/// Reads the values a run of property paths names. Paths come in property order, so those
/// sharing a top-level property arrive together and decode it once between them, which is what
/// keeps a composite index over several branches of one map to a single decode.
class PathReader {
 public:
  explicit PathReader(std::optional<RecordReader> const &record) : record_{record} {}

  /// The value at `path`, or Null when any step of it is missing.
  auto Read(PropertyPath const &path) -> PropertyValue {
    if (!record_) return PropertyValue{};
    if (path.size() == 1) {
      auto const found = record_->Find(path.front());
      return found ? record_->Read(*found) : PropertyValue{};
    }
    auto const *nested = ReadNested(path);
    return nested == nullptr ? PropertyValue{} : *nested;
  }

  /// The value the nested `path` names, or null when a step of it is missing. Points into the
  /// decoded top-level value, so it lives until a path of another top-level property is read.
  auto ReadNested(PropertyPath const &path) -> PropertyValue const * {
    if (!record_) return nullptr;
    if (top_level_property_ != path.front()) {
      auto const found = record_->Find(path.front());
      top_level_ = found ? record_->Read(*found) : PropertyValue{};
      top_level_property_ = path.front();
    }
    return ReadNestedPropertyValue(top_level_, path.as_span().subspan(1));
  }

 private:
  std::optional<RecordReader> const &record_;
  std::optional<PropertyId> top_level_property_;
  PropertyValue top_level_;
};

}  // namespace

ManifestPropertyStore::ManifestPropertyStore(ManifestPropertyStore &&other) noexcept : buffer_{other.buffer_} {
  other.buffer_ = {};
}

auto ManifestPropertyStore::operator=(ManifestPropertyStore &&other) noexcept -> ManifestPropertyStore & {
  if (this == &other) return *this;
  Release();
  buffer_ = other.buffer_;
  other.buffer_ = {};
  return *this;
}

ManifestPropertyStore::~ManifestPropertyStore() { Release(); }

void ManifestPropertyStore::Release() {
  if (!empty() && !is_inline()) delete[] heap_data();
}

auto ManifestPropertyStore::Reset(ManifestId manifest, uint32_t size) -> uint8_t * {
  DMG_ASSERT(manifest.value <= kManifestIdMask, "A manifest id has to fit in the bytes a record gives it");
  Release();
  buffer_ = {};
  auto const total = kManifestIdWidth + size;
  auto *storage = [&] {
    if (total <= kInlineCapacity) {
      buffer_[0] = kInlineMarker;
      return buffer_.data() + 1;
    }
    auto const allocated = ToMultipleOf8(total);
    auto *heap = new uint8_t[allocated]{};
    SetHeap(allocated, heap);
    return heap;
  }();
  std::memcpy(storage, &manifest.value, kManifestIdWidth);
  return storage + kManifestIdWidth;
}

auto ManifestPropertyStore::GetProperty(PropertyManifest const &manifest, PropertyManifest::Location location) const
    -> PropertyValue {
  if (empty()) return PropertyValue{};

  auto const reader = RecordReader{manifest, data()};
  if (!reader.Carries(location.position)) return PropertyValue{};
  return reader.Read(location);
}

auto ManifestPropertyStore::GetProperty(ManifestRegistry const &registry, PropertyId property) const -> PropertyValue {
  if (empty()) return PropertyValue{};

  auto const &manifest = registry.Resolve(this->manifest());
  auto const found = manifest.Find(property);
  if (!found) return PropertyValue{};
  return GetProperty(manifest, *found);
}

auto ManifestPropertyStore::HasProperty(ManifestRegistry const &registry, PropertyId property) const -> bool {
  if (empty()) return false;
  auto const found = registry.Resolve(this->manifest()).Find(property);
  return found && IsPresent(data(), found->position);
}

auto ManifestPropertyStore::IsPropertyEqual(ManifestRegistry const &registry, PropertyId property,
                                            PropertyValue const &value) const -> bool {
  if (empty()) return value.IsNull();

  auto const reader = RecordReader{registry.Resolve(this->manifest()), data()};
  auto const found = reader.Find(property);
  if (!found) return value.IsNull();
  return reader.Equals(*found, value);
}

auto ManifestPropertyStore::Properties(ManifestRegistry const &registry) const -> utils::small_vector<PropertyPair> {
  auto properties = utils::small_vector<PropertyPair>{};
  if (empty()) return properties;

  auto const &manifest = registry.Resolve(this->manifest());
  auto const *record = data();
  properties.reserve(manifest.size());
  for (uint32_t position = 0; position != manifest.size(); ++position) {
    if (!IsPresent(record, position)) continue;
    auto const property = manifest.entries()[position].property;
    properties.emplace_back(property, GetProperty(registry, property));
  }
  return properties;
}

template <typename T>
auto ManifestPropertyStore::GetProperty(ManifestRegistry const &registry, PropertyId property,
                                        IndexedPropertyDecoder<T> const &decoder) const -> PropertyValue {
  auto value = GetProperty(registry, property);
  decoder.DecodeProperty(value);
  return value;
}

template auto ManifestPropertyStore::GetProperty(ManifestRegistry const &, PropertyId,
                                                 IndexedPropertyDecoder<Vertex> const &) const -> PropertyValue;
template auto ManifestPropertyStore::GetProperty(ManifestRegistry const &, PropertyId,
                                                 IndexedPropertyDecoder<Edge> const &) const -> PropertyValue;

template <typename T>
auto ManifestPropertyStore::Properties(ManifestRegistry const &registry, IndexedPropertyDecoder<T> const &decoder) const
    -> utils::small_vector<PropertyPair> {
  auto properties = Properties(registry);
  for (auto &[property, value] : properties) decoder.DecodeProperty(value);
  return properties;
}

template auto ManifestPropertyStore::Properties(ManifestRegistry const &, IndexedPropertyDecoder<Vertex> const &) const
    -> utils::small_vector<PropertyPair>;
template auto ManifestPropertyStore::Properties(ManifestRegistry const &, IndexedPropertyDecoder<Edge> const &) const
    -> utils::small_vector<PropertyPair>;

auto ManifestPropertyStore::ExtendedPropertyTypes(ManifestRegistry const &registry) const
    -> std::map<PropertyId, ExtendedPropertyType> {
  auto types = std::map<PropertyId, ExtendedPropertyType>{};
  if (empty()) return types;

  auto const reader = RecordReader{registry.Resolve(this->manifest()), data()};
  auto const &manifest = reader.manifest();
  for (uint32_t position = 0; position != manifest.size(); ++position) {
    if (!reader.Carries(position)) continue;
    auto const &field = manifest.entries()[position];
    types.emplace(field.property, ExtendedTypeOf(field.stored_type));
  }
  return types;
}

auto ManifestPropertyStore::GetExtendedPropertyType(ManifestRegistry const &registry, PropertyId property) const
    -> ExtendedPropertyType {
  if (empty()) return ExtendedPropertyType{};

  auto const reader = RecordReader{registry.Resolve(this->manifest()), data()};
  auto const found = reader.Find(property);
  if (!found) return ExtendedPropertyType{};
  return ExtendedTypeOf(found->stored_type);
}

auto ManifestPropertyStore::ExtractPropertyIds(ManifestRegistry const &registry) const -> std::vector<PropertyId> {
  auto properties = std::vector<PropertyId>{};
  if (empty()) return properties;

  auto const reader = RecordReader{registry.Resolve(this->manifest()), data()};
  auto const &manifest = reader.manifest();
  properties.reserve(manifest.size());
  for (uint32_t position = 0; position != manifest.size(); ++position) {
    if (reader.Carries(position)) properties.push_back(manifest.entries()[position].property);
  }
  return properties;
}

auto ManifestPropertyStore::PropertiesOfTypes(ManifestRegistry const &registry,
                                              std::span<PropertyStoreType const> types) const
    -> std::vector<PropertyId> {
  auto properties = std::vector<PropertyId>{};
  if (empty()) return properties;

  auto const reader = RecordReader{registry.Resolve(this->manifest()), data()};
  auto const &manifest = reader.manifest();
  for (uint32_t position = 0; position != manifest.size(); ++position) {
    if (!reader.Carries(position)) continue;
    auto const &field = manifest.entries()[position];
    if (std::ranges::contains(types, field.stored_type.type)) properties.push_back(field.property);
  }
  return properties;
}

auto ManifestPropertyStore::GetPropertyOfTypes(ManifestRegistry const &registry, PropertyId property,
                                               std::span<PropertyStoreType const> types) const
    -> std::optional<PropertyValue> {
  if (empty()) return std::nullopt;

  auto const reader = RecordReader{registry.Resolve(this->manifest()), data()};
  auto const found = reader.Find(property);
  if (!found || !std::ranges::contains(types, found->stored_type.type)) return std::nullopt;
  return reader.Read(*found);
}

auto ManifestPropertyStore::PropertySize(ManifestRegistry const &registry, PropertyId property) const -> uint32_t {
  if (empty()) return 0;

  auto const reader = RecordReader{registry.Resolve(this->manifest()), data()};
  auto const found = reader.Find(property);
  if (!found) return 0;
  return static_cast<uint32_t>(reader.Payload(*found).size());
}

auto ManifestPropertyStore::HasAllProperties(ManifestRegistry const &registry,
                                             std::set<PropertyId> const &properties) const -> bool {
  if (properties.empty()) return true;
  if (empty()) return false;

  auto const reader = RecordReader{registry.Resolve(this->manifest()), data()};
  return std::ranges::all_of(properties, [&reader](PropertyId property) { return reader.Find(property).has_value(); });
}

auto ManifestPropertyStore::ExtractPropertyValues(ManifestRegistry const &registry,
                                                  std::set<PropertyId> const &properties) const
    -> std::optional<std::vector<PropertyValue>> {
  if (properties.empty()) return std::vector<PropertyValue>{};
  if (empty()) return std::nullopt;

  auto const reader = RecordReader{registry.Resolve(this->manifest()), data()};
  auto values = std::vector<PropertyValue>{};
  values.reserve(properties.size());
  for (auto const property : properties) {
    auto const found = reader.Find(property);
    if (!found) return std::nullopt;
    values.push_back(reader.Read(*found));
  }
  return values;
}

auto ManifestPropertyStore::ExtractPropertyValuesMissingAsNull(ManifestRegistry const &registry,
                                                               std::span<PropertyPath const> ordered_properties) const
    -> std::vector<PropertyValue> {
  auto values = std::vector<PropertyValue>{};
  values.reserve(ordered_properties.size());
  auto const record = empty() ? std::nullopt : std::optional{RecordReader{registry.Resolve(this->manifest()), data()}};
  auto reader = PathReader{record};
  for (auto const &path : ordered_properties) values.push_back(reader.Read(path));
  return values;
}

void ManifestPropertyStore::ExtractPropertyValuesMissingAsNull(ManifestRegistry const &registry,
                                                               std::span<PropertyPath const> ordered_properties,
                                                               std::span<PropertyValue> out) const {
  DMG_ASSERT(out.size() == ordered_properties.size(), "Output buffer size must match the number of properties");
  auto const record = empty() ? std::nullopt : std::optional{RecordReader{registry.Resolve(this->manifest()), data()}};
  auto reader = PathReader{record};
  auto slot = out.begin();
  for (auto const &path : ordered_properties) *slot++ = reader.Read(path);
}

auto ManifestPropertyStore::ArePropertiesEqual(ManifestRegistry const &registry,
                                               std::span<PropertyPath const> ordered_properties,
                                               std::span<PropertyValue const> values,
                                               std::span<std::size_t const> position_lookup) const
    -> std::vector<bool> {
  auto results = std::vector<bool>(ordered_properties.size(), false);
  auto const record = empty() ? std::nullopt : std::optional{RecordReader{registry.Resolve(this->manifest()), data()}};
  auto reader = PathReader{record};
  for (size_t position = 0; position != ordered_properties.size(); ++position) {
    auto const &path = ordered_properties[position];
    auto const &value = values[position_lookup[position]];
    if (path.size() == 1) {
      // A top-level value is compared where it lies, which is what keeps a unique constraint
      // check from decoding a record it is about to reject.
      auto const found = record ? record->Find(path.front()) : std::nullopt;
      results[position] = found ? record->Equals(*found, value) : value.IsNull();
      continue;
    }
    auto const *nested = reader.ReadNested(path);
    results[position] = nested == nullptr ? value.IsNull() : *nested == value;
  }
  return results;
}

auto ManifestPropertyStore::PropertiesMatchTypes(ManifestRegistry const &registry,
                                                 TypeConstraintsValidator const &constraint) const
    -> std::optional<PropertyStoreConstraintViolation> {
  if (constraint.empty() || empty()) return std::nullopt;

  auto const reader = RecordReader{registry.Resolve(this->manifest()), data()};
  auto const &manifest = reader.manifest();
  for (uint32_t position = 0; position != manifest.size(); ++position) {
    if (!reader.Carries(position)) continue;
    auto const &field = manifest.entries()[position];
    // A temporal type is checked exactly and every other type by its class, and both are in
    // the shape, so a record is validated without a byte of it being read.
    auto const temporal_type = field.stored_type.type == PropertyStoreType::TEMPORAL_DATA
                                   ? std::optional{static_cast<TemporalType>(field.stored_type.discriminator)}
                                   : std::nullopt;
    auto const member = PropertyStoreMemberInfo{field.property, field.stored_type.type, temporal_type};
    if (auto violation = constraint.validate(member); violation) return violation;
  }
  return std::nullopt;
}

auto ManifestPropertyStore::SetProperty(ManifestRegistry &registry, PropertyId property, PropertyValue const &value)
    -> bool {
  auto const found = empty() ? std::nullopt : registry.Resolve(this->manifest()).Find(property);
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
    auto const &manifest = registry.Resolve(this->manifest());
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
  if (!empty()) return false;
  Rebuild(registry, properties);
  return true;
}

auto ManifestPropertyStore::InitProperties(ManifestRegistry &registry,
                                           std::map<PropertyId, PropertyValue> const &properties) -> bool {
  if (!empty()) return false;
  auto ordered = utils::small_vector<PropertyPair>{};
  ordered.reserve(properties.size());
  for (auto const &[id, value] : properties) ordered.emplace_back(id, value);
  Rebuild(registry, ordered);
  return true;
}

auto ManifestPropertyStore::UpdateProperties(ManifestRegistry &registry,
                                             std::map<PropertyId, PropertyValue> &properties)
    -> std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>> {
  auto const held = Properties(registry);

  auto changes = std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>>{};
  changes.reserve(properties.size() + held.size());
  // Two passes rather than one merge, so a property arriving where the record had none is
  // reported ahead of every property the record already carried, as `PropertyStore` has it.
  for (auto const &[property, arriving] : properties) {
    if (!std::ranges::binary_search(held, property, {}, &PropertyPair::first)) {
      changes.emplace_back(property, PropertyValue{}, arriving);
    }
  }
  for (auto const &[property, existing] : held) {
    auto const [it, inserted] = properties.emplace(property, existing);
    if (!inserted) changes.emplace_back(property, existing, it->second);
  }

  // Encoded into a record of its own and moved in, so a value that cannot be encoded leaves
  // the store holding what it held.
  auto updated = ManifestPropertyStore{};
  updated.InitProperties(registry, properties);
  *this = std::move(updated);
  return changes;
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

  auto *record = Reset(manifest_id, total);
  if (manifest.variable_count() != 0) record[presence] = offset_width;
}

auto ManifestPropertyStore::ClearProperties() -> bool {
  if (empty()) return false;
  Release();
  buffer_ = {};
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
  if (!empty()) {
    auto const &current = registry.Resolve(this->manifest());
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
  auto *record = Reset(manifest_id, total);
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
    variable_end += WriteVariable(value, record + regions.variable + variable_end);
    WriteOffset(record + regions.offset_table, offset_width, location.offset, variable_end);
  }
}

}  // namespace memgraph::storage
