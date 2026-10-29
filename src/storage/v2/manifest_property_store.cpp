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
// Taken via usearch, since the platform's own half-precision type is not dependable.
#include <fp16.h>
#include <gflags/gflags.h>

#include <chrono>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include "storage/v2/timezone_registry.hpp"
#include "utils/logging.hpp"

/// How many bits of a double are kept. A record is written at whatever this says and then
/// carries that width in its shape, so nothing a record says depends on this afterwards.
DECLARE_uint64(storage_floating_point_resolution_bits);

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

/// The width a double is written at. Read once, when a record is written, because the width it
/// gives then goes into the shape: a record read against a width other than the one it was
/// written at would say something it was never given.
auto DoubleWidthToWriteAt() -> uint8_t {
  auto const bits = FLAGS_storage_floating_point_resolution_bits;
  if (bits == 16) return sizeof(uint16_t);
  if (bits == 32) return sizeof(float);
  DMG_ASSERT(bits == 64, "Invalid floating point resolution: {}", bits);
  return sizeof(double);
}

void WriteDouble(uint8_t *at, double value, uint8_t width) {
  switch (width) {
    case sizeof(uint16_t): {
      auto const half = fp16_ieee_from_fp32_value(static_cast<float>(value));
      std::memcpy(at, &half, sizeof(half));
      return;
    }
    case sizeof(float): {
      auto const single = static_cast<float>(value);
      std::memcpy(at, &single, sizeof(single));
      return;
    }
    default:
      DMG_ASSERT(width == sizeof(double), "A double is stored at one of three widths");
      // The bytes rather than the number, so a negative zero and a NaN read back as the ones
      // written rather than as some other value that compares the same.
      std::memcpy(at, &value, sizeof(value));
      return;
  }
}

auto ReadDouble(uint8_t const *at, uint8_t width) -> double {
  switch (width) {
    case sizeof(uint16_t): {
      auto half = uint16_t{};
      std::memcpy(&half, at, sizeof(half));
      return static_cast<double>(fp16_ieee_to_fp32_value(half));
    }
    case sizeof(float): {
      auto single = float{};
      std::memcpy(&single, at, sizeof(single));
      return static_cast<double>(single);
    }
    default:
      DMG_ASSERT(width == sizeof(double), "A double is stored at one of three widths");
      auto number = double{};
      std::memcpy(&number, at, sizeof(number));
      return number;
  }
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

/// A zoned temporal value laid out flat: the kind, the moment, and the zone named either by
/// the offset it is or by the id standing for it. Twelve bytes, so a shape knows where one
/// sits and the record needs nothing else to read it.
struct StoredZoned {
  uint8_t type;
  int64_t microseconds;
  /// True when `zone` is an interned id rather than an offset in minutes.
  uint8_t named;
  int16_t zone;
};

auto StoredZonedFrom(ZonedTemporalData const &zoned) -> StoredZoned {
  auto const offset = zoned.timezone.GetOffset();
  if (zoned.timezone.InTzDatabase()) {
    return StoredZoned{.type = static_cast<uint8_t>(zoned.type),
                       .microseconds = zoned.IntMicroseconds(),
                       .named = 1,
                       .zone = static_cast<int16_t>(TimezoneRegistry::Instance().Intern(zoned.timezone))};
  }
  // Offsets run from -18:00 to +18:00, so the minutes fit and are written as themselves.
  return StoredZoned{.type = static_cast<uint8_t>(zoned.type),
                     .microseconds = zoned.IntMicroseconds(),
                     .named = 0,
                     .zone = static_cast<int16_t>(std::get<std::chrono::minutes>(offset).count())};
}

auto ZonedFrom(StoredZoned const &stored) -> ZonedTemporalData {
  auto const timezone = stored.named == 1 ? TimezoneRegistry::Instance().Resolve(static_cast<uint16_t>(stored.zone))
                                          : utils::Timezone{std::chrono::minutes{stored.zone}};
  return ZonedTemporalData{
      static_cast<ZonedTemporalType>(stored.type),
      std::chrono::sys_time<std::chrono::microseconds>{std::chrono::microseconds{stored.microseconds}},
      timezone};
}

/// A shape takes the type tag off a record's properties. It says nothing about the values
/// nested inside one, so those are written self-describing: a tag, then a payload whose
/// shape the tag decides. The tag is the `PropertyValue` kind rather than the stored kind,
/// because that is what reading one has to reconstruct.
void EncodeNested(std::vector<uint8_t> &out, PropertyValue const &value);

void AppendBytes(std::vector<uint8_t> &out, void const *from, std::size_t count) {
  auto const *bytes = static_cast<uint8_t const *>(from);
  out.insert(out.end(), bytes, bytes + count);
}

template <typename T>
void AppendObject(std::vector<uint8_t> &out, T const &value) {
  static_assert(std::is_trivially_copyable_v<T>, "Only an object whose bytes are its value");
  AppendBytes(out, &value, sizeof(T));
}

void AppendCount(std::vector<uint8_t> &out, std::size_t count) { AppendObject(out, static_cast<uint32_t>(count)); }

void EncodeNested(std::vector<uint8_t> &out, PropertyValue const &value) {
  AppendObject(out, static_cast<uint8_t>(value.type()));
  switch (value.type()) {
    case PropertyValueType::Null:
      return;
    case PropertyValueType::Bool:
      AppendObject(out, static_cast<uint8_t>(value.ValueBool() ? 1 : 0));
      return;
    case PropertyValueType::Int:
      AppendObject(out, value.ValueInt());
      return;
    case PropertyValueType::Double:
      AppendObject(out, value.ValueDouble());
      return;
    case PropertyValueType::String: {
      auto const &text = value.ValueString();
      AppendCount(out, text.size());
      AppendBytes(out, text.data(), text.size());
      return;
    }
    case PropertyValueType::List: {
      auto const &list = value.ValueList();
      AppendCount(out, list.size());
      for (auto const &element : list) EncodeNested(out, PropertyValue{element});
      return;
    }
    case PropertyValueType::Map: {
      auto const &map = value.ValueMap();
      AppendCount(out, map.size());
      for (auto const &[property, element] : map) {
        AppendObject(out, property.AsUint());
        EncodeNested(out, PropertyValue{element});
      }
      return;
    }
    // A packed list holds its elements more compactly than a general one, but it is still a
    // list of values, so it is written as one and the tag says how to pack it again.
    case PropertyValueType::IntList: {
      auto const &list = value.ValueIntList();
      AppendCount(out, list.size());
      for (auto const element : list) EncodeNested(out, PropertyValue{static_cast<int64_t>(element)});
      return;
    }
    case PropertyValueType::DoubleList: {
      auto const &list = value.ValueDoubleList();
      AppendCount(out, list.size());
      for (auto const element : list) EncodeNested(out, PropertyValue{element});
      return;
    }
    case PropertyValueType::NumericList: {
      auto const &list = value.ValueNumericList();
      AppendCount(out, list.size());
      for (auto const &element : list) {
        std::visit(
            [&out](auto const number) {
              if constexpr (std::is_same_v<decltype(number), int const>) {
                EncodeNested(out, PropertyValue{static_cast<int64_t>(number)});
              } else {
                EncodeNested(out, PropertyValue{number});
              }
            },
            element);
      }
      return;
    }
    case PropertyValueType::TemporalData:
      AppendObject(out, value.ValueTemporalData());
      return;
    case PropertyValueType::ZonedTemporalData:
      // The same flat twelve bytes a record holds one in, so there is one encoding of a zoned
      // value rather than one for a property and another for an element of a list.
      AppendObject(out, StoredZonedFrom(value.ValueZonedTemporalData()));
      return;
    case PropertyValueType::Enum:
      AppendObject(out, value.ValueEnum());
      return;
    case PropertyValueType::Point2d:
      AppendObject(out, value.ValuePoint2d());
      return;
    case PropertyValueType::Point3d:
      AppendObject(out, value.ValuePoint3d());
      return;
    default:
      LOG_FATAL("A nested value of type {} is not encoded yet", static_cast<unsigned>(value.type()));
  }
}

/// Walks the bytes `EncodeNested` wrote, in step with how it wrote them.
class NestedReader {
 public:
  explicit NestedReader(uint8_t const *at) : at_{at} {}

  template <typename T>
  auto Object() -> T {
    static_assert(std::is_trivially_copyable_v<T>, "Only an object whose bytes are its value");
    auto raw = std::array<std::byte, sizeof(T)>{};
    std::memcpy(raw.data(), at_, sizeof(T));
    at_ += sizeof(T);
    return std::bit_cast<T>(raw);
  }

  auto Count() -> uint32_t { return Object<uint32_t>(); }

  auto Bytes(std::size_t count) -> char const * {
    auto const *from = reinterpret_cast<char const *>(at_);
    at_ += count;
    return from;
  }

  auto Value() -> PropertyValue;

 private:
  /// A counted run of values, which is what every kind of list is written as.
  auto Elements() -> PropertyValue::list_t;

  uint8_t const *at_;
};

auto NestedReader::Elements() -> PropertyValue::list_t {
  auto const count = Count();
  auto list = PropertyValue::list_t{};
  list.reserve(count);
  for (uint32_t at = 0; at != count; ++at) list.emplace_back(Value());
  return list;
}

auto NestedReader::Value() -> PropertyValue {
  switch (static_cast<PropertyValueType>(Object<uint8_t>())) {
    case PropertyValueType::Null:
      return PropertyValue{};
    case PropertyValueType::Bool:
      return PropertyValue{Object<uint8_t>() != 0};
    case PropertyValueType::Int:
      return PropertyValue{Object<int64_t>()};
    case PropertyValueType::Double:
      return PropertyValue{Object<double>()};
    case PropertyValueType::String: {
      auto const count = Count();
      return PropertyValue{std::string{Bytes(count), count}};
    }
    case PropertyValueType::List:
      return PropertyValue{Elements()};
    case PropertyValueType::Map: {
      auto const count = Count();
      auto map = PropertyValue::map_t{};
      for (uint32_t at = 0; at != count; ++at) {
        auto const property = PropertyId::FromUint(Object<uint32_t>());
        map.emplace(property, Value());
      }
      return PropertyValue{std::move(map)};
    }
    case PropertyValueType::IntList:
      return PropertyValue{IntListTag{}, Elements()};
    case PropertyValueType::DoubleList:
      return PropertyValue{DoubleListTag{}, Elements()};
    case PropertyValueType::NumericList:
      return PropertyValue{NumericListTag{}, Elements()};
    case PropertyValueType::TemporalData:
      return PropertyValue{Object<TemporalData>()};
    case PropertyValueType::ZonedTemporalData:
      return PropertyValue{ZonedFrom(Object<StoredZoned>())};
    case PropertyValueType::Enum:
      return PropertyValue{Object<Enum>()};
    case PropertyValueType::Point2d:
      return PropertyValue{Object<Point2d>()};
    case PropertyValueType::Point3d:
      return PropertyValue{Object<Point3d>()};
    default:
      LOG_FATAL("A nested value of this type is not encoded yet");
  }
}

/// The bytes a variable-width value occupies. A string is its own bytes, since the span the
/// record's table gives already says how many there are. Anything else describes itself.
auto EncodePayload(PropertyValue const &value) -> std::vector<uint8_t> {
  if (value.IsString()) {
    auto const &text = value.ValueString();
    auto payload = std::vector<uint8_t>{};
    AppendBytes(payload, text.data(), text.size());
    return payload;
  }
  auto payload = std::vector<uint8_t>{};
  EncodeNested(payload, value);
  return payload;
}

/// How a value is stored, which is what a shape is made of. The one place that decides what
/// can be encoded, so a kind that is not handled is refused here rather than part way in.
auto StoredTypeFor(PropertyValue const &value) -> StoredType {
  if (value.IsBool()) return StoredType::Fixed(PropertyStoreType::BOOL, 1);
  if (value.IsInt()) return StoredType::Fixed(PropertyStoreType::INT, WidthFor(value.ValueInt()));
  if (value.IsDouble()) return StoredType::Fixed(PropertyStoreType::DOUBLE, DoubleWidthToWriteAt());
  if (value.IsString()) return StoredType::Variable(PropertyStoreType::STRING);
  if (value.IsList() || value.IsIntList() || value.IsDoubleList() || value.IsNumericList()) {
    return StoredType::Variable(PropertyStoreType::LIST);
  }
  if (value.IsMap()) return StoredType::Variable(PropertyStoreType::MAP);
  if (value.IsEnum()) return StoredType::Fixed(PropertyStoreType::ENUM, sizeof(Enum));
  if (value.IsTemporalData()) return StoredType::Fixed(PropertyStoreType::TEMPORAL_DATA, sizeof(TemporalData));
  if (value.IsZonedTemporalData()) {
    return StoredType::Fixed(PropertyStoreType::ZONED_TEMPORAL_DATA, sizeof(StoredZoned));
  }
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
    case PropertyStoreType::DOUBLE:
      WriteDouble(at, value.ValueDouble(), stored_type.width);
      return;
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
    case PropertyStoreType::ZONED_TEMPORAL_DATA:
      WriteObject(at, StoredZonedFrom(value.ValueZonedTemporalData()));
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
    case PropertyStoreType::DOUBLE:
      return PropertyValue{ReadDouble(at, stored_type.width)};
    case PropertyStoreType::ENUM:
      return PropertyValue{ReadObject<Enum>(at)};
    case PropertyStoreType::TEMPORAL_DATA:
      return PropertyValue{ReadObject<TemporalData>(at)};
    case PropertyStoreType::POINT:
      if (stored_type.width == sizeof(Point2d)) return PropertyValue{ReadObject<Point2d>(at)};
      return PropertyValue{ReadObject<Point3d>(at)};
    case PropertyStoreType::ZONED_TEMPORAL_DATA:
      return PropertyValue{ZonedFrom(ReadObject<StoredZoned>(at))};
    default:
      LOG_FATAL("This kind of value is not stored at a known width");
  }
}

/// Whether two doubles are equivalent, which for a NaN is not what the arithmetic says: a NaN
/// is equivalent to a NaN, and equal to nothing.
bool Equivalent(double lhs, double rhs) { return CompareDoublesNaNLast(lhs, rhs) == std::weak_ordering::equivalent; }

/// Whether the value stored at `at` is equivalent to `value`, without building the stored one
/// up again.
///
/// The shape already carries the stored kind, so a pair of different kinds is refused before
/// the record is touched at all. A number is the exception: an integer and a double of the
/// same number are equivalent, so either kind is accepted against either and the pair is
/// placed the way equivalence places it.
bool CompareFixed(uint8_t const *at, StoredType stored_type, PropertyValue const &value) {
  switch (stored_type.type) {
    case PropertyStoreType::BOOL:
      return value.IsBool() && value.ValueBool() == (*at != 0);
    case PropertyStoreType::INT: {
      if (!value.IsInt() && !value.IsDouble()) return false;
      // Read at the width it was stored at rather than widened to a double, which is what
      // stops two integers both answering true against one double.
      auto const stored = ReadInt(at, stored_type.width);
      if (value.IsInt()) return value.ValueInt() == stored;
      return std::is_eq(PlaceIntegerAgainstDouble(stored, value.ValueDouble()));
    }
    case PropertyStoreType::DOUBLE: {
      if (!value.IsInt() && !value.IsDouble()) return false;
      auto const stored = ReadDouble(at, stored_type.width);
      if (value.IsInt()) return std::is_eq(PlaceIntegerAgainstDouble(value.ValueInt(), stored));
      return Equivalent(value.ValueDouble(), stored);
    }
    case PropertyStoreType::ENUM:
      return value.IsEnum() && value.ValueEnum() == ReadObject<Enum>(at);
    case PropertyStoreType::TEMPORAL_DATA:
      return value.IsTemporalData() && value.ValueTemporalData() == ReadObject<TemporalData>(at);
    case PropertyStoreType::POINT: {
      // A coordinate is a double, so each is placed the way a double beside a point is.
      if (stored_type.width == sizeof(Point2d)) {
        if (!value.IsPoint2d()) return false;
        auto const stored = ReadObject<Point2d>(at);
        auto const &point = value.ValuePoint2d();
        return point.crs() == stored.crs() && Equivalent(point.x(), stored.x()) && Equivalent(point.y(), stored.y());
      }
      if (!value.IsPoint3d()) return false;
      auto const stored = ReadObject<Point3d>(at);
      auto const &point = value.ValuePoint3d();
      return point.crs() == stored.crs() && Equivalent(point.x(), stored.x()) && Equivalent(point.y(), stored.y()) &&
             Equivalent(point.z(), stored.z());
    }
    case PropertyStoreType::ZONED_TEMPORAL_DATA: {
      if (!value.IsZonedTemporalData()) return false;
      auto const stored = ReadObject<StoredZoned>(at);
      auto const &zoned = value.ValueZonedTemporalData();
      // The moment is in the record, so a pair that differs in it is answered without naming
      // the zone, which is what asking the registry for one costs.
      if (static_cast<uint8_t>(zoned.type) != stored.type || zoned.IntMicroseconds() != stored.microseconds) {
        return false;
      }
      return zoned.timezone == ZonedFrom(stored).timezone;
    }
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

auto ManifestPropertyStore::PayloadAt(PropertyManifest const &layout, PropertyManifest::Location location) const
    -> std::span<uint8_t const> {
  auto const regions = RegionsOf(layout);
  auto const end = EndOffsetAt(Record(), regions, location.offset);
  auto const begin = location.offset == 0 ? regions.payload_begin : EndOffsetAt(Record(), regions, location.offset - 1);
  return {Record() + begin, end - begin};
}

auto ManifestPropertyStore::ValueAt(PropertyManifest const &layout, PropertyManifest::Location location) const
    -> PropertyValue {
  if (location.is_fixed) {
    return ReadFixed(Record() + location.offset, location.stored_type);
  }

  auto const payload = PayloadAt(layout, location);
  if (location.stored_type.type == PropertyStoreType::STRING) {
    return PropertyValue{std::string{reinterpret_cast<char const *>(payload.data()), payload.size()}};
  }
  return NestedReader{payload.data()}.Value();
}

auto ManifestPropertyStore::GetProperty(ManifestRegistry const &registry, PropertyId property) const -> PropertyValue {
  auto const shape = manifest();
  if (!shape) return {};

  auto const &layout = registry.Resolve(*shape);
  auto const location = layout.Find(property);
  if (!location) return {};

  return ValueAt(layout, *location);
}

auto ManifestPropertyStore::CompareAt(PropertyManifest const &layout, PropertyManifest::Location location,
                                      PropertyValue const &value) const -> bool {
  // A stored value is one the record carries, and a record never carries a Null.
  if (value.IsNull()) return false;

  if (location.is_fixed) {
    return CompareFixed(Record() + location.offset, location.stored_type, value);
  }

  auto const payload = PayloadAt(layout, location);
  if (location.stored_type.type == PropertyStoreType::STRING) {
    if (!value.IsString()) return false;
    auto const &text = value.ValueString();
    // The record holds the characters, so they are compared where they lie rather than copied
    // into a string first.
    return text.size() == payload.size() && std::memcmp(text.data(), payload.data(), payload.size()) == 0;
  }
  // A list or a map is still built up to be compared, because the nested encoding describes
  // itself and walking it against a value means decoding it as it goes.
  return ValueAt(layout, location) == value;
}

auto ManifestPropertyStore::IsPropertyEqual(ManifestRegistry const &registry, PropertyId property,
                                            PropertyValue const &value) const -> bool {
  auto const shape = manifest();
  if (!shape) return value.IsNull();

  auto const &layout = registry.Resolve(*shape);
  auto const location = layout.Find(property);
  if (!location) return value.IsNull();
  return CompareAt(layout, *location, value);
}

void ManifestPropertyStore::ArePropertiesEqual(ManifestRegistry const &registry,
                                               std::span<PropertyPath const> ordered_properties,
                                               std::span<PropertyValue const> values,
                                               std::span<std::size_t const> position_lookup,
                                               std::vector<bool> &result) const {
  // Assigned rather than constructed, so a caller reusing the buffer keeps its capacity.
  result.assign(ordered_properties.size(), false);

  // A shape gives a property's place without reading what sits before it, so each path is
  // looked up on its own and the paths need not be in any particular order.
  auto const shape = manifest();
  auto const *layout = shape ? &registry.Resolve(*shape) : nullptr;
  for (std::size_t pos = 0; pos != ordered_properties.size(); ++pos) {
    auto const &path = ordered_properties[pos];
    auto const &value = values[position_lookup[pos]];

    auto const location = layout ? layout->Find(path.front()) : std::nullopt;
    if (!location) {
      result[pos] = value.IsNull();
      continue;
    }
    if (path.size() == 1) {
      result[pos] = CompareAt(*layout, *location, value);
      continue;
    }

    // A path arriving at nothing is equivalent to Null and to nothing else.
    result[pos] = ValueAtKeys(*layout, *location, path.as_span().subspan(1)) == value;
  }
}

auto ManifestPropertyStore::ValueAtKeys(PropertyManifest const &layout, PropertyManifest::Location location,
                                        std::span<PropertyId const> keys) const -> PropertyValue {
  auto nested = ValueAt(layout, location);
  for (auto const key : keys) {
    // Only a map holds keys, so a path through anything else arrives at nothing, as does one
    // naming a key the map does not hold.
    if (!nested.IsMap()) return {};
    auto const &map = nested.ValueMap();
    auto const at = map.find(key);
    if (at == map.end()) return {};
    nested = at->second;
  }
  return nested;
}

auto ManifestPropertyStore::ExtractPropertyValuesMissingAsNull(ManifestRegistry const &registry,
                                                               std::span<PropertyPath const> ordered_properties) const
    -> std::vector<PropertyValue> {
  auto values = std::vector<PropertyValue>(ordered_properties.size());
  ExtractPropertyValuesMissingAsNull(registry, ordered_properties, values);
  return values;
}

void ManifestPropertyStore::ExtractPropertyValuesMissingAsNull(ManifestRegistry const &registry,
                                                               std::span<PropertyPath const> ordered_properties,
                                                               std::span<PropertyValue> out) const {
  MG_ASSERT(out.size() == ordered_properties.size(), "One value is read per path asked about");

  auto const shape = manifest();
  auto const *layout = shape ? &registry.Resolve(*shape) : nullptr;
  for (std::size_t pos = 0; pos != ordered_properties.size(); ++pos) {
    auto const &path = ordered_properties[pos];
    auto const location = layout ? layout->Find(path.front()) : std::nullopt;
    out[pos] = location ? ValueAtKeys(*layout, *location, path.as_span().subspan(1)) : PropertyValue{};
  }
}

auto ManifestPropertyStore::Properties(ManifestRegistry const &registry) const -> std::map<PropertyId, PropertyValue> {
  auto properties = std::map<PropertyId, PropertyValue>{};
  auto const shape = manifest();
  if (!shape) return properties;

  auto const &layout = registry.Resolve(*shape);
  auto const entries = layout.entries();
  for (size_t position = 0; position != entries.size(); ++position) {
    // Entries come out in property order, so each one goes straight on the end of the map.
    properties.emplace_hint(properties.end(), entries[position].property, ValueAt(layout, layout.LocationAt(position)));
  }
  return properties;
}

void ManifestPropertyStore::Rebuild(ManifestRegistry &registry, std::map<PropertyId, PropertyValue> const &properties) {
  if (properties.empty()) {
    // Carrying nothing is carrying no shape either, which is what having no record says.
    buffer_.reset();
    return;
  }

  auto shape = RecordShape{};
  for (auto const &[property, value] : properties) {
    shape.emplace(property, StoredTypeFor(value));
  }

  auto const id = registry.Intern(shape);
  auto const &layout = registry.Resolve(id);
  auto const regions = RegionsOf(layout);

  // Encoded once, because how many bytes a variable value takes is only known by encoding it
  // and the record has to be sized before anything is written into it.
  // The shape was interned from these same properties, so its entries and they are in the
  // same order and walking them together needs no search.
  auto payloads = std::map<PropertyId, std::vector<uint8_t>>{};
  auto payload_bytes = uint32_t{};
  auto position = size_t{};
  for (auto const &[property, value] : properties) {
    if (layout.LocationAt(position++).is_fixed) continue;
    auto encoded = EncodePayload(value);
    payload_bytes += static_cast<uint32_t>(encoded.size());
    payloads.emplace_hint(payloads.end(), property, std::move(encoded));
  }

  buffer_ = std::make_unique<uint8_t[]>(kShapeIdBytes + regions.payload_begin + payload_bytes);
  std::memcpy(buffer_.get(), &id.value, sizeof(id.value));

  // A shape numbers its variable values in property order, so walking the properties in that
  // order fills the table in order and the payloads follow one another.
  auto cursor = regions.payload_begin;
  [[maybe_unused]] auto next_variable = uint32_t{};
  position = 0;
  for (auto const &[property, value] : properties) {
    DMG_ASSERT(layout.entries()[position].property == property, "A shape is in the order its properties were in");
    auto const location = layout.LocationAt(position++);
    if (location.is_fixed) {
      WriteFixed(Record() + location.offset, location.stored_type, value);
      continue;
    }

    DMG_ASSERT(location.offset == next_variable, "A shape numbers its variable values in property order");
    ++next_variable;
    auto const &payload = payloads.at(property);
    std::memcpy(Record() + cursor, payload.data(), payload.size());
    cursor += static_cast<uint32_t>(payload.size());
    WriteEndOffset(Record(), regions, location.offset, cursor);
  }
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
