// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/property_store.hpp"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/cast.hpp"
#include "utils/compressor.hpp"
#include "utils/logging.hpp"
#include "utils/temporal.hpp"

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_property_store_compression_enabled, false,
            "Controls whether the properties should be compressed in the storage.");

namespace memgraph::storage {

namespace {

namespace r = ranges;
namespace rv = r::views;

// `PropertyValue` is a very large object. It is implemented as a `union` of all
// possible types that could be stored as a property value. That causes the
// object to be 50+ bytes in size. Many use-cases only use primitive property
// types (such as booleans, integers and doubles). When storing an integer in
// the `PropertyValue` there is a lot of memory being wasted (40+ bytes). For
// boolean values the memory wastage is even worse (almost all of the 50+
// bytes). Also, the `PropertyValue` must have a `type` member that (even though
// it is small) causes a padding hole to be inserted in the `PropertyValue` that
// wastes even more memory. Memory is wasted even more when the `PropertyValue`
// stores a list or a map of `PropertyValue`s because each of the internal
// values also wastes memory.
//
// Even though there is a lot of memory being wasted in `PropertyValue`, all of
// the data structures used inside it enable very fast object traversal because
// there is no additional decoding that should be done. Time complexity of all
// functions used to access compound values is very good (usually O(log(n))).
//
// Because the values stored in a vertex or edge must be reconstructed
// (specifically, they must be copied) every time a property is accessed or
// modified (because of the MVCC implementation) it makes sense to optimize the
// data structure that is used as permanent storage of the property values in
// vertices and edges.
//
// The `PropertyStore` is used to provide a very efficient means of permanently
// storing a map of `PropertyId` to `PropertyValue` mappings. It reduces memory
// usage with the cost of a higher time complexity of the operations that
// access the store. Compared to a `std::map<PropertyValue>`, the
// `PropertyStore` uses approximately 10 times less memory. But, the time
// complexity of its get and set operations is O(n) instead of O(log(n)).
//
// The values themselves are stored encoded in a flat buffer. On an insertion
// the underlying storage buffer is resized if necessary and on removal the
// underlying storage buffer is shrinked if the new data can fit into a buffer
// that is 1/3 smaller than the current buffer. If it can't fit into a smaller
// buffer, the current buffer is used. All mappings are encoded independently of
// each other.
//
// Each mapping starts with an encoded metadata field that is used for several
// purposes:
//   * to determine the encoded type
//   * to determine the encoded property ID size
//   * to determine the encoded payload size
//
// The metadata field is always a single byte and its bits are used as follows:
//  0b0000 0000
//    ++++      -> type (4 bits)
//         ++   -> size of property ID (2 bits)
//           ++ -> size of payload OR size of payload size indicator (2 bits)
//
// When encoding integers (`int64_t` and `uint32_t`) they are compressed so that
// they are stored into 1, 2, 4 or 8 bytes depending on their value.
//
// The size of the metadata field is very important because it is encoded with
// each and every ID to value mapping. That is why every possible bit is used
// to store some useful information. Increasing the size of the metadata field
// will increase memory usage for every stored ID to value mapping.

enum class Size : uint8_t {
  INT8 = 0x00,
  INT16 = 0x01,
  INT32 = 0x02,
  INT64 = 0x03,
};

constexpr uint32_t SizeToByteSize(Size size) {
  switch (size) {
    case Size::INT8:
      return 1;
    case Size::INT16:
      return 2;
    case Size::INT32:
      return 4;
    case Size::INT64:
      return 8;
  }
}

using Type = PropertyStoreType;

const uint8_t kMaskType = 0xf0;
const uint8_t kMaskIdSize = 0x0c;
const uint8_t kMaskPayloadSize = 0x03;
const uint8_t kShiftIdSize = 2;

// Values are encoded as follows:
//   * NULL
//     - type; payload size is not used
//   * BOOL
//     - type; payload size is used as value (INT64 = true, INT8 = false)
//     - encoded property ID
//   * INT
//     - type; payload size is used to indicate whether the value is encoded as
//       `int8_t`, `int16_t`, `int32_t` or `int64_t`
//     - encoded property ID
//     - encoded property value
//   * DOUBLE
//     - type; payload size isn't used
//     - encoded property ID
//     - encoded value
//   * STRING
//     - type; payload size is used to indicate whether the string size is
//       encoded as `uint8_t`, `uint16_t`, `uint32_t` or `uint32_t`
//     - encoded property ID
//     - encoded string size
//     - string data
//   * LIST
//     - type; payload size is used to indicate whether the list size is encoded
//       as `uint8_t`, `uint16_t`, `uint32_t` or `uint32_t`
//     - encoded property ID
//     - encoded list size
//     - list items
//       + type; id size is not used; payload size is used to indicate the size
//         of the item
//       + encoded item size
//       + encoded item data
//   * MAP
//     - type; payload size is used to indicate whether the map size is encoded
//       as `uint8_t`, `uint16_t`, `uint32_t` or `uint32_t`
//     - encoded property ID
//     - encoded map size
//     - map items
//       + type; id size is used to indicate whether the key size is encoded as
//         `uint8_t`, `uint16_t`, `uint32_t` or `uint32_t`; payload size is used
//         as described above for the inner payload type
//       + encoded key property ID
//       + encoded value size
//       + encoded value data
//   * TEMPORAL_DATA
//     - type; payload size isn't used
//     - encoded property ID
//     - value saved as Metadata
//       + type; id size is used to indicate whether the temporal data type is encoded
//         as `uint8_t`, `uint16_t`, `uint32_t` or `uint32_t`; payload size used to
//         indicate whether the microseconds are encoded as `uint8_t`, `uint16_t, `uint32_t
//         or `uint32_t`
//       + encoded temporal data type value
//       + encoded microseconds value
//   * ZONED_TEMPORAL_DATA
//     - type; payload size isn't used
//     - encoded property ID
//     - value saved as Metadata (the same way as in TEMPORAL_DATA)
//       + timezone offset
//         + string size (always uint_8; see TZ_NAME_LENGTH_SIZE)
//         + string data
//   * OFFSET_ZONED_TEMPORAL_DATA
//     - type; payload size isn't used
//     - encoded property ID
//     - value saved as Metadata (the same way as in TEMPORAL_DATA)
//       + timezone offset
//         + encoded value (always uint_16; see tz_offset_int)
//   * ENUM
//     - type; payload size is used to indicate whether the enum type and enum value are
//       encoded as `uint8_t`, `uint16_t`, `uint32_t` or `uint64_t` uses the largest of both
//     - encoded property ID
//     - encoded property value as two ints, enum type then enum value, both same size
//   * POINT
//     - type; payload size is used to encode the crs type (this only works becuase there are 4 sizes + 4 crs types)
//     - encoded property ID
//     - encoded value as 2 (for 2D) or 3 (for 3D) doubles forced to be encoded as int64

const auto TZ_NAME_LENGTH_SIZE = Size::INT8;
// As the underlying type for zoned temporal data is std::chrono::zoned_time, valid timezone names are limited
// to those in the IANA time zone database.
// The timezone names in the IANA database follow https://data.iana.org/time-zones/theory.html#naming rules:
// * Maximal form: AREA/LOCATION/QUALIFIER
// * Length of subcomponents (AREA, LOCATION, and QUALIFIER): <= 14
// * All legacy names are shorter than this
// Therefore, the longest valid timezone name has the length of 44 (14 + 1 + 14 + 1 + 14), a 8-bit integer.

using tz_offset_int = int16_t;
// When a zoned temporal value is specified with a UTC offset (as opposed to a timezone name), the following applies:
// * Offsets are defined in minutes
// * Valid offsets are in the UTC + [-18h, +18h] range
// Therefore, every possible value is in the [-1080, +1080] range and it's thus stored with a 16-bit integer.

struct Metadata {
  Type type{Type::EMPTY};
  Size id_size{Size::INT8};
  Size payload_size{Size::INT8};
};

// Helper class used to write data to the binary stream.
class Writer {
 public:
  class MetadataHandle {
   public:
    MetadataHandle() = default;

    explicit MetadataHandle(uint8_t *value) : value_(value) {}

    void Set(Metadata metadata) {
      if (!value_) return;
      auto value = static_cast<uint8_t>(metadata.type);
      value |= static_cast<uint8_t>(static_cast<uint8_t>(metadata.id_size) << kShiftIdSize);
      value |= static_cast<uint8_t>(metadata.payload_size);
      *value_ = value;
    }

   private:
    uint8_t *value_{nullptr};
  };

  Writer() = default;

  Writer(uint8_t *data, uint32_t size) : data_(data), size_(size) {}

  std::optional<MetadataHandle> WriteMetadata() {
    if (data_ && pos_ + 1 > size_) return std::nullopt;
    MetadataHandle handle;
    if (data_) handle = MetadataHandle(&data_[pos_]);
    ++pos_;
    return handle;
  }

  std::optional<Size> WriteInt(int64_t value) {
    if (InternalWriteInt<int8_t>(value)) {
      return Size::INT8;
    } else if (InternalWriteInt<int16_t>(value)) {
      return Size::INT16;
    } else if (InternalWriteInt<int32_t>(value)) {
      return Size::INT32;
    } else if (InternalWriteInt<int64_t>(value)) {
      return Size::INT64;
    } else {
      return std::nullopt;
    }
  }

  std::optional<Size> WriteUint(uint64_t value) {
    if (InternalWriteInt<uint8_t>(value)) {
      return Size::INT8;
    } else if (InternalWriteInt<uint16_t>(value)) {
      return Size::INT16;
    } else if (InternalWriteInt<uint32_t>(value)) {
      return Size::INT32;
    } else if (InternalWriteInt<uint64_t>(value)) {
      return Size::INT64;
    } else {
      return std::nullopt;
    }
  }

  std::optional<Size> WriteDouble(double value) { return WriteUint(utils::MemcpyCast<uint64_t>(value)); }
  bool WriteDoubleForceInt64(double value) { return InternalWriteInt<uint64_t>(utils::MemcpyCast<uint64_t>(value)); }

  bool WriteTimezoneOffset(int64_t offset) { return InternalWriteInt<tz_offset_int>(offset); }

  bool WriteBytes(const uint8_t *data, uint64_t size) {
    if (data_ && pos_ + size > size_) return false;
    if (data_) memcpy(data_ + pos_, data, size);
    pos_ += size;
    return true;
  }

  bool WriteBytes(const char *data, uint32_t size) {
    static_assert(std::is_same_v<uint8_t, unsigned char>);
    return WriteBytes(reinterpret_cast<const uint8_t *>(data), size);
  }

  uint32_t Written() const { return pos_; }

  template <typename T, typename V>
  static constexpr bool FitsInt(V value) {
    static_assert(std::numeric_limits<T>::is_integer);
    static_assert(std::numeric_limits<V>::is_integer);
    static_assert(std::numeric_limits<T>::is_signed == std::numeric_limits<V>::is_signed);
    return (std::numeric_limits<T>::min() <= value) && (value <= std::numeric_limits<T>::max());
  }

  static constexpr std::optional<Size> UIntSize(uint64_t value) {
    if (FitsInt<uint8_t>(value)) {
      return Size::INT8;
    }
    if (FitsInt<uint16_t>(value)) {
      return Size::INT16;
    }
    if (FitsInt<uint32_t>(value)) {
      return Size::INT32;
    }
    if (FitsInt<uint64_t>(value)) {
      return Size::INT64;
    }
    return std::nullopt;
  }

  template <typename T, typename V>
  bool InternalWriteInt(V value) {
    static_assert(std::numeric_limits<T>::is_integer);
    static_assert(std::numeric_limits<V>::is_integer);
    static_assert(std::numeric_limits<T>::is_signed == std::numeric_limits<V>::is_signed);
    if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max()) return false;
    if (data_ && pos_ + sizeof(T) > size_) return false;
    T tmp = value;
    if (data_) memcpy(data_ + pos_, &tmp, sizeof(T));
    pos_ += sizeof(T);
    return true;
  }

 private:
  uint8_t *data_{nullptr};
  uint32_t size_{0};
  uint32_t pos_{0};
};

// Helper class used to read data from the binary stream.
class Reader {
 public:
  Reader(const uint8_t *data, uint32_t size) : data_(data), size_(size) {}
  Reader(Reader const &other, uint32_t offset, uint32_t size) : data_(other.data_ + offset), size_(size) {
    DMG_ASSERT(other.size_ - offset >= size);
  }

  std::optional<Metadata> ReadMetadata() {
    if (pos_ + 1 > size_) return std::nullopt;
    uint8_t value = data_[pos_++];
    Metadata metadata;
    metadata.type = static_cast<Type>(value & kMaskType);
    metadata.id_size = static_cast<Size>(static_cast<uint8_t>(value & kMaskIdSize) >> kShiftIdSize);
    metadata.payload_size = static_cast<Size>(value & kMaskPayloadSize);
    return metadata;
  }

  std::optional<int64_t> ReadInt(Size size) {
    int64_t ret = 0;
    switch (size) {
      case Size::INT8: {
        auto value = InternalReadInt<int8_t>();
        if (!value) return std::nullopt;
        ret = *value;
        break;
      }
      case Size::INT16: {
        auto value = InternalReadInt<int16_t>();
        if (!value) return std::nullopt;
        ret = *value;
        break;
      }
      case Size::INT32: {
        auto value = InternalReadInt<int32_t>();
        if (!value) return std::nullopt;
        ret = *value;
        break;
      }
      case Size::INT64: {
        auto value = InternalReadInt<int64_t>();
        if (!value) return std::nullopt;
        ret = *value;
        break;
      }
    }
    return ret;
  }

  std::optional<uint64_t> ReadUint(Size size) {
    uint64_t ret = 0;
    switch (size) {
      case Size::INT8: {
        auto value = InternalReadInt<uint8_t>();
        if (!value) return std::nullopt;
        ret = *value;
        break;
      }
      case Size::INT16: {
        auto value = InternalReadInt<uint16_t>();
        if (!value) return std::nullopt;
        ret = *value;
        break;
      }
      case Size::INT32: {
        auto value = InternalReadInt<uint32_t>();
        if (!value) return std::nullopt;
        ret = *value;
        break;
      }
      case Size::INT64: {
        auto value = InternalReadInt<uint64_t>();
        if (!value) return std::nullopt;
        ret = *value;
        break;
      }
    }
    return ret;
  }

  std::optional<double> ReadDouble(Size size) {
    auto value = ReadUint(size);
    if (!value) return std::nullopt;
    return utils::MemcpyCast<double>(*value);
  }

  std::optional<double> ReadDoubleForce64() {
    auto value = InternalReadInt<uint64_t>();
    if (!value) return std::nullopt;
    return utils::MemcpyCast<double>(*value);
  }

  std::optional<utils::Timezone> ReadTimezone(auto type) {
    if (type == Type::ZONED_TEMPORAL_DATA) {
      auto tz_str_length = ReadUint(TZ_NAME_LENGTH_SIZE);
      if (!tz_str_length) return std::nullopt;
      std::string tz_str_v(*tz_str_length, '\0');
      if (!ReadBytes(tz_str_v.data(), *tz_str_length)) return std::nullopt;
      return utils::Timezone(tz_str_v);
    }

    if (type == Type::OFFSET_ZONED_TEMPORAL_DATA) {
      auto offset_value = InternalReadInt<tz_offset_int>();
      if (!offset_value) return std::nullopt;
      return utils::Timezone(std::chrono::minutes{static_cast<int64_t>(*offset_value)});
    }

    return std::nullopt;
  }

  bool ReadBytes(uint8_t *data, uint32_t size) {
    if (pos_ + size > size_) return false;
    memcpy(data, data_ + pos_, size);
    pos_ += size;
    return true;
  }

  auto ReadBytesToSpan(uint32_t size) -> std::optional<std::span<uint8_t const>> {
    if (pos_ + size > size_) return std::nullopt;
    auto data_view = std::span{data_ + pos_, size};
    pos_ += size;
    return data_view;
  }

  bool ReadBytes(char *data, uint32_t size) { return ReadBytes(reinterpret_cast<uint8_t *>(data), size); }

  auto ReadBytesToStringView(uint32_t size) -> std::optional<std::string_view> {
    auto span = ReadBytesToSpan(size);
    if (!span) return std::nullopt;
    return std::string_view{reinterpret_cast<char const *>(span->data()), span->size()};
  }

  bool VerifyBytes(const uint8_t *data, uint32_t size) {
    if (pos_ + size > size_) return false;
    if (memcmp(data, data_ + pos_, size) != 0) return false;
    pos_ += size;
    return true;
  }

  bool VerifyBytes(const char *data, uint32_t size) {
    return VerifyBytes(reinterpret_cast<const uint8_t *>(data), size);
  }

  bool SkipBytes(uint32_t size) {
    if (pos_ + size > size_) return false;
    pos_ += size;
    return true;
  }

  uint32_t GetPosition() const { return pos_; }

  void SetPosition(uint32_t pos) { pos_ = pos; }

 private:
  template <typename T>
  std::optional<T> InternalReadInt() {
    if (pos_ + sizeof(T) > size_) return std::nullopt;
    T value;
    memcpy(&value, data_ + pos_, sizeof(T));
    pos_ += sizeof(T);
    return value;
  }

  const uint8_t *data_;
  uint32_t size_ = 0;
  uint32_t pos_ = 0;
};

auto CrsToSize(CoordinateReferenceSystem value) -> Size {
  switch (value) {
    using enum Size;
    using enum CoordinateReferenceSystem;
    case WGS84_2d:
      return INT8;
    case WGS84_3d:
      return INT16;
    case Cartesian_2d:
      return INT32;
    case Cartesian_3d:
      return INT64;
  }
}

auto SizeToCrs(Size value) -> CoordinateReferenceSystem {
  switch (value) {
    using enum Size;
    using enum CoordinateReferenceSystem;
    case INT8:
      return WGS84_2d;
    case INT16:
      return WGS84_3d;
    case INT32:
      return Cartesian_2d;
    case INT64:
      return Cartesian_3d;
  }
}

// Function used to encode a PropertyValue into a byte stream.
std::optional<std::pair<Type, Size>> EncodePropertyValue(Writer *writer, const PropertyValue &value) {
  switch (value.type()) {
    case PropertyValue::Type::Null: {
      return {{Type::NONE, Size::INT8}};
    }
    case PropertyValue::Type::Bool: {
      if (value.ValueBool()) {
        return {{Type::BOOL, Size::INT64}};
      }
      return {{Type::BOOL, Size::INT8}};
    }
    case PropertyValue::Type::Int: {
      auto size = writer->WriteInt(value.ValueInt());
      if (!size) return std::nullopt;
      return {{Type::INT, *size}};
    }
    case PropertyValue::Type::Double: {
      auto size = writer->WriteDouble(value.ValueDouble());
      if (!size) return std::nullopt;
      return {{Type::DOUBLE, *size}};
    }
    case PropertyValue::Type::String: {
      const auto &str = value.ValueString();
      auto size = writer->WriteUint(str.size());
      if (!size) return std::nullopt;
      if (!writer->WriteBytes(str.data(), str.size())) return std::nullopt;
      return {{Type::STRING, *size}};
    }
    case PropertyValue::Type::List: {
      const auto &list = value.ValueList();
      auto size = writer->WriteUint(list.size());
      if (!size) return std::nullopt;
      for (const auto &item : list) {
        auto metadata = writer->WriteMetadata();
        if (!metadata) return std::nullopt;
        auto ret = EncodePropertyValue(writer, item);
        if (!ret) return std::nullopt;
        metadata->Set({ret->first, Size::INT8, ret->second});
      }
      return {{Type::LIST, *size}};
    }
    case PropertyValue::Type::NumericList: {
      const auto &list = value.ValueNumericList();
      auto size = writer->WriteUint(list.size());
      if (!size) return std::nullopt;
      for (const auto &item : list) {
        auto metadata = writer->WriteMetadata();
        if (!metadata) return std::nullopt;
        if (std::holds_alternative<int>(item)) {
          auto ret = writer->WriteInt(static_cast<int64_t>(std::get<int>(item)));
          if (!ret) return std::nullopt;
          metadata->Set({Type::INT, Size::INT8, *ret});
        } else {
          auto ret = writer->WriteDouble(std::get<double>(item));
          if (!ret) return std::nullopt;
          metadata->Set({Type::DOUBLE, Size::INT8, *ret});
        }
      }
      return {{Type::LIST, *size}};
    }
    case PropertyValue::Type::IntList: {
      const auto &list = value.ValueIntList();
      auto size = writer->WriteUint(list.size());
      if (!size) return std::nullopt;
      for (const auto &item : list) {
        auto metadata = writer->WriteMetadata();
        if (!metadata) return std::nullopt;
        auto ret = writer->WriteInt(static_cast<int64_t>(item));
        if (!ret) return std::nullopt;
        metadata->Set({Type::INT, Size::INT8, *ret});
      }
      return {{Type::LIST, *size}};
    }
    case PropertyValue::Type::DoubleList: {
      const auto &list = value.ValueDoubleList();
      auto size = writer->WriteUint(list.size());
      if (!size) return std::nullopt;
      for (const auto &item : list) {
        auto metadata = writer->WriteMetadata();
        if (!metadata) return std::nullopt;
        auto ret = writer->WriteDouble(item);
        if (!ret) return std::nullopt;
        metadata->Set({Type::DOUBLE, Size::INT8, *ret});
      }
      return {{Type::LIST, *size}};
    }
    case PropertyValue::Type::Map: {
      const auto &map = value.ValueMap();
      auto size = writer->WriteUint(map.size());
      if (!size) return std::nullopt;
      for (const auto &item : map) {
        auto metadata = writer->WriteMetadata();
        if (!metadata) return std::nullopt;
        auto property_id_size = writer->WriteUint(item.first.AsUint());
        if (!property_id_size) return std::nullopt;
        auto ret = EncodePropertyValue(writer, item.second);
        if (!ret) return std::nullopt;
        metadata->Set({ret->first, *property_id_size, ret->second});
      }
      return {{Type::MAP, *size}};
    }
    case PropertyValue::Type::TemporalData: {
      auto metadata = writer->WriteMetadata();
      if (!metadata) return std::nullopt;

      const auto temporal_data = value.ValueTemporalData();
      auto type_size = writer->WriteUint(utils::UnderlyingCast(temporal_data.type));
      if (!type_size) return std::nullopt;

      auto microseconds_size = writer->WriteInt(temporal_data.microseconds);
      if (!microseconds_size) return std::nullopt;
      metadata->Set({Type::TEMPORAL_DATA, *type_size, *microseconds_size});

      // We don't need payload size so we set it to a random value
      return {{Type::TEMPORAL_DATA, Size::INT8}};
    }
    case PropertyValue::Type::ZonedTemporalData: {
      auto metadata = writer->WriteMetadata();
      if (!metadata) return std::nullopt;

      const auto zoned_temporal_data = value.ValueZonedTemporalData();
      auto type_size = writer->WriteUint(utils::UnderlyingCast(zoned_temporal_data.type));
      if (!type_size) return std::nullopt;

      auto microseconds_size = writer->WriteInt(zoned_temporal_data.IntMicroseconds());
      if (!microseconds_size) return std::nullopt;

      if (zoned_temporal_data.timezone.InTzDatabase()) {
        metadata->Set({Type::ZONED_TEMPORAL_DATA, *type_size, *microseconds_size});

        const auto &tz_str = zoned_temporal_data.timezone.TimezoneName();
        if (!writer->WriteUint(tz_str.size())) return std::nullopt;
        if (!writer->WriteBytes(tz_str.data(), tz_str.size())) return std::nullopt;

        // We don't need payload size so we set it to a random value
        return {{Type::ZONED_TEMPORAL_DATA, Size::INT8}};
      }
      // Valid timezone offsets may be -18 to +18 hours, with minute precision. This means that the range of possible
      // offset values is [-1080, +1080], which is represented with 16-bit integers.

      if (!writer->WriteTimezoneOffset(zoned_temporal_data.timezone.DefiningOffset())) return std::nullopt;
      metadata->Set({Type::OFFSET_ZONED_TEMPORAL_DATA, *type_size, *microseconds_size});
      // We don't need payload size so we set it to a random value
      return {{Type::OFFSET_ZONED_TEMPORAL_DATA, Size::INT8}};
    }
    case PropertyValue::Type::Enum: {
      auto const &[e_type, e_value] = value.ValueEnum();

      auto merged = e_type.value_of() | e_value.value_of();
      auto size = Writer::UIntSize(merged);
      if (!size) return std::nullopt;
      switch (*size) {
        case Size::INT8:
          if (!writer->InternalWriteInt<uint8_t>(e_type.value_of())) return std::nullopt;
          if (!writer->InternalWriteInt<uint8_t>(e_value.value_of())) return std::nullopt;
          break;
        case Size::INT16:
          if (!writer->InternalWriteInt<uint16_t>(e_type.value_of())) return std::nullopt;
          if (!writer->InternalWriteInt<uint16_t>(e_value.value_of())) return std::nullopt;
          break;
        case Size::INT32:
          if (!writer->InternalWriteInt<uint32_t>(e_type.value_of())) return std::nullopt;
          if (!writer->InternalWriteInt<uint32_t>(e_value.value_of())) return std::nullopt;
          break;
        case Size::INT64:
          if (!writer->InternalWriteInt<uint64_t>(e_type.value_of())) return std::nullopt;
          if (!writer->InternalWriteInt<uint64_t>(e_value.value_of())) return std::nullopt;
          break;
      }

      return {{Type::ENUM, *size}};
    }
    case PropertyValue::Type::Point2d: {
      auto const &point = value.ValuePoint2d();
      if (!writer->WriteDoubleForceInt64(point.x())) return std::nullopt;
      if (!writer->WriteDoubleForceInt64(point.y())) return std::nullopt;
      return {{Type::POINT, CrsToSize(point.crs())}};
    }
    case PropertyValue::Type::Point3d: {
      auto const &point = value.ValuePoint3d();
      if (!writer->WriteDoubleForceInt64(point.x())) return std::nullopt;
      if (!writer->WriteDoubleForceInt64(point.y())) return std::nullopt;
      if (!writer->WriteDoubleForceInt64(point.z())) return std::nullopt;
      return {{Type::POINT, CrsToSize(point.crs())}};
    }
  }
}

namespace {
std::optional<TemporalData> DecodeTemporalData(Reader &reader) {
  auto metadata = reader.ReadMetadata();
  if (!metadata || metadata->type != Type::TEMPORAL_DATA) return std::nullopt;

  auto type_value = reader.ReadUint(metadata->id_size);
  if (!type_value) return std::nullopt;

  auto microseconds_value = reader.ReadInt(metadata->payload_size);
  if (!microseconds_value) return std::nullopt;

  return TemporalData{static_cast<TemporalType>(*type_value), *microseconds_value};
}

std::optional<uint32_t> DecodeTemporalDataSize(Reader &reader) {
  uint32_t temporal_data_size = 0;

  auto metadata = reader.ReadMetadata();
  if (!metadata || metadata->type != Type::TEMPORAL_DATA) return std::nullopt;

  temporal_data_size += 1;

  auto type_value = reader.ReadUint(metadata->id_size);
  if (!type_value) return std::nullopt;

  temporal_data_size += SizeToByteSize(metadata->id_size);

  auto microseconds_value = reader.ReadInt(metadata->payload_size);
  if (!microseconds_value) return std::nullopt;

  temporal_data_size += SizeToByteSize(metadata->payload_size);

  return temporal_data_size;
}

std::optional<ZonedTemporalData> DecodeZonedTemporalData(Reader &reader) {
  auto metadata = reader.ReadMetadata();

  if (!metadata ||
      (metadata->type != Type::ZONED_TEMPORAL_DATA && metadata->type != Type::OFFSET_ZONED_TEMPORAL_DATA)) {
    return std::nullopt;
  }

  auto type_value = reader.ReadUint(metadata->id_size);
  if (!type_value) return std::nullopt;

  auto microseconds_value = reader.ReadInt(metadata->payload_size);
  if (!microseconds_value) return std::nullopt;

  auto timezone = reader.ReadTimezone(metadata->type);
  if (!timezone) return std::nullopt;

  return ZonedTemporalData{static_cast<ZonedTemporalType>(*type_value), utils::AsSysTime(*microseconds_value),
                           *timezone};
}

std::optional<uint64_t> DecodeZonedTemporalDataSize(Reader &reader) {
  uint64_t zoned_temporal_data_size = 0;

  auto metadata = reader.ReadMetadata();
  if (!metadata ||
      (metadata->type != Type::ZONED_TEMPORAL_DATA && metadata->type != Type::OFFSET_ZONED_TEMPORAL_DATA)) {
    return std::nullopt;
  }

  zoned_temporal_data_size += 1;

  auto type_value = reader.ReadUint(metadata->id_size);
  if (!type_value) return std::nullopt;

  zoned_temporal_data_size += SizeToByteSize(metadata->id_size);

  auto microseconds_value = reader.ReadInt(metadata->payload_size);
  if (!microseconds_value) return std::nullopt;

  zoned_temporal_data_size += SizeToByteSize(metadata->payload_size);

  if (metadata->type == Type::ZONED_TEMPORAL_DATA) {
    auto tz_str_length = reader.ReadUint(TZ_NAME_LENGTH_SIZE);
    if (!tz_str_length) return std::nullopt;
    zoned_temporal_data_size += (1 + *tz_str_length);
    reader.SkipBytes(*tz_str_length);
  } else if (metadata->type == Type::OFFSET_ZONED_TEMPORAL_DATA) {
    zoned_temporal_data_size += 2;  // tz_offset_int is 16-bit
    reader.SkipBytes(2);
  }

  return zoned_temporal_data_size;
}

}  // namespace

// Function used to decode a PropertyValue from a byte stream.
//
// @sa ComparePropertyValue
[[nodiscard]] bool DecodePropertyValue(Reader *reader, Type type, Size payload_size, PropertyValue &value) {
  switch (type) {
    case Type::EMPTY: {
      return false;
    }
    case Type::NONE: {
      value = PropertyValue();
      return true;
    }
    case Type::BOOL: {
      if (payload_size == Size::INT64) {
        value = PropertyValue(true);
      } else {
        value = PropertyValue(false);
      }
      return true;
    }
    case Type::INT: {
      auto int_v = reader->ReadInt(payload_size);
      if (!int_v) return false;
      value = PropertyValue(*int_v);
      return true;
    }
    case Type::DOUBLE: {
      auto double_v = reader->ReadDouble(payload_size);
      if (!double_v) return false;
      value = PropertyValue(*double_v);
      return true;
    }
    case Type::STRING: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;
      std::string str_v(*size, '\0');
      if (!reader->ReadBytes(str_v.data(), *size)) return false;
      value = PropertyValue(std::move(str_v));
      return true;
    }
    case Type::LIST: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;
      std::vector<PropertyValue> list;
      list.reserve(*size);
      bool all_numeric = true;
      bool all_int = true;
      bool all_double = true;
      for (uint32_t i = 0; i < *size; ++i) {
        auto metadata = reader->ReadMetadata();
        if (!metadata) return false;
        PropertyValue item;
        if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, item)) return false;
        list.emplace_back(std::move(item));
        all_int = all_int && item.IsInt();
        all_double = all_double && item.IsDouble();
        all_numeric = all_int || all_double;
      }
      value = std::invoke([&]() {
        if (all_numeric) {
          return PropertyValue(NumericListTag{}, std::move(list));
        }
        if (all_int) {
          return PropertyValue(IntListTag{}, std::move(list));
        }
        if (all_double) {
          return PropertyValue(DoubleListTag{}, std::move(list));
        }
        return PropertyValue(std::move(list));
      });
      return true;
    }
    case Type::MAP: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;
      auto map = PropertyValue::map_t{};
      do_reserve(map, *size);
      for (uint32_t i = 0; i < *size; ++i) {
        auto metadata = reader->ReadMetadata();
        if (!metadata) return false;
        auto property_id = reader->ReadUint(metadata->id_size);
        if (!property_id) return false;
        PropertyValue item;
        if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, item)) return false;
        map.emplace(PropertyId::FromUint(*property_id), std::move(item));
      }
      value = PropertyValue(std::move(map));
      return true;
    }
    case Type::TEMPORAL_DATA: {
      const auto maybe_temporal_data = DecodeTemporalData(*reader);
      if (!maybe_temporal_data) return false;
      value = PropertyValue(*maybe_temporal_data);
      return true;
    }
    case Type::ZONED_TEMPORAL_DATA:
    case Type::OFFSET_ZONED_TEMPORAL_DATA: {
      const auto maybe_zoned_temporal_data = DecodeZonedTemporalData(*reader);
      if (!maybe_zoned_temporal_data) return false;
      value = PropertyValue(*maybe_zoned_temporal_data);
      return true;
    }
    case Type::ENUM: {
      auto e_type = reader->ReadUint(payload_size);
      if (!e_type) return false;
      auto e_value = reader->ReadUint(payload_size);
      if (!e_value) return false;
      value = PropertyValue(Enum{EnumTypeId{*e_type}, EnumValueId{*e_value}});
      return true;
    }
    case Type::POINT: {
      auto crs = SizeToCrs(payload_size);
      auto x_opt = reader->ReadDouble(Size::INT64);  // because we forced it as int64 on write
      if (!x_opt) return false;
      auto y_opt = reader->ReadDouble(Size::INT64);  // because we forced it as int64 on write
      if (!y_opt) return false;
      if (valid2d(crs)) {
        value = PropertyValue(Point2d{crs, *x_opt, *y_opt});
      } else {
        auto z_opt = reader->ReadDouble(Size::INT64);  // because we forced it as int64 on write
        if (!z_opt) return false;
        value = PropertyValue(Point3d{crs, *x_opt, *y_opt, *z_opt});
      }
      return true;
    }
  }
  // in case of corrupt storage, handle unknown types
  return false;
}

[[nodiscard]] std::optional<PropertyValue> DecodePropertyValue(Reader *reader, Type type, Size payload_size) {
  switch (type) {
    case Type::EMPTY: {
      return std::nullopt;
    }
    case Type::NONE: {
      return std::optional<PropertyValue>{std::in_place};
    }
    case Type::BOOL: {
      return std::optional<PropertyValue>{std::in_place, payload_size == Size::INT64};
    }
    case Type::INT: {
      auto int_v = reader->ReadInt(payload_size);
      if (!int_v) return std::nullopt;
      return std::optional<PropertyValue>{std::in_place, *int_v};
    }
    case Type::DOUBLE: {
      auto double_v = reader->ReadDouble(payload_size);
      if (!double_v) return std::nullopt;
      return std::optional<PropertyValue>{std::in_place, *double_v};
    }
    case Type::STRING: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return std::nullopt;

      auto sv = reader->ReadBytesToStringView(*size);
      if (!sv) return std::nullopt;
      return std::optional<PropertyValue>{std::in_place, *sv};
    }
    case Type::LIST: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return std::nullopt;
      std::vector<PropertyValue> list;
      list.reserve(*size);
      bool all_numeric = true;

      for (uint32_t i = 0; i < *size; ++i) {
        auto metadata = reader->ReadMetadata();
        if (!metadata) return std::nullopt;
        auto item = DecodePropertyValue(reader, metadata->type, metadata->payload_size);
        if (!item) return std::nullopt;
        if (all_numeric && !item->IsInt() && !item->IsDouble()) {
          all_numeric = false;
        }

        list.emplace_back(*std::move(item));
      }
      return std::optional<PropertyValue>{std::in_place, std::move(list)};
    }
    case Type::MAP: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return std::nullopt;
      auto map = PropertyValue::map_t{};
      do_reserve(map, *size);
      for (uint32_t i = 0; i < *size; ++i) {
        auto metadata = reader->ReadMetadata();
        if (!metadata) return std::nullopt;
        auto property_id = reader->ReadUint(metadata->id_size);
        if (!property_id) return std::nullopt;
        auto item = DecodePropertyValue(reader, metadata->type, metadata->payload_size);
        if (!item) return std::nullopt;
        map.emplace(PropertyId::FromUint(*property_id), *std::move(item));
      }
      return std::optional<PropertyValue>{std::in_place, std::move(map)};
    }
    case Type::TEMPORAL_DATA: {
      const auto maybe_temporal_data = DecodeTemporalData(*reader);
      if (!maybe_temporal_data) return std::nullopt;
      return std::optional<PropertyValue>{std::in_place, *maybe_temporal_data};
    }
    case Type::ZONED_TEMPORAL_DATA:
    case Type::OFFSET_ZONED_TEMPORAL_DATA: {
      const auto maybe_zoned_temporal_data = DecodeZonedTemporalData(*reader);
      if (!maybe_zoned_temporal_data) return std::nullopt;
      return std::optional<PropertyValue>{std::in_place, *maybe_zoned_temporal_data};
    }
    case Type::ENUM: {
      auto e_type = reader->ReadUint(payload_size);
      if (!e_type) return std::nullopt;
      auto e_value = reader->ReadUint(payload_size);
      if (!e_value) return std::nullopt;
      return std::optional<PropertyValue>{std::in_place, Enum{EnumTypeId{*e_type}, EnumValueId{*e_value}}};
    }
    case Type::POINT: {
      auto crs = SizeToCrs(payload_size);
      auto x_opt = reader->ReadDouble(Size::INT64);  // because we forced it as int64 on write
      if (!x_opt) return std::nullopt;
      auto y_opt = reader->ReadDouble(Size::INT64);  // because we forced it as int64 on write
      if (!y_opt) return std::nullopt;
      if (valid2d(crs)) {
        return std::optional<PropertyValue>{std::in_place, Point2d{crs, *x_opt, *y_opt}};
      } else {
        auto z_opt = reader->ReadDouble(Size::INT64);  // because we forced it as int64 on write
        if (!z_opt) return std::nullopt;
        return std::optional<PropertyValue>{std::in_place, Point3d{crs, *x_opt, *y_opt, *z_opt}};
      }
    }
  }
}

[[nodiscard]] bool DecodePropertyValueSize(Reader *reader, Type type, Size payload_size, uint32_t &property_size) {
  switch (type) {
    case Type::EMPTY: {
      return false;
    }
    case Type::NONE:
    case Type::BOOL: {
      return true;
    }
    case Type::INT:
    case Type::DOUBLE: {
      const auto size = SizeToByteSize(payload_size);
      property_size += size;
      reader->SkipBytes(size);
      return true;
    }
    case Type::STRING: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;
      property_size += SizeToByteSize(payload_size);

      if (!reader->SkipBytes(*size)) return false;
      property_size += *size;

      return true;
    }
    case Type::LIST: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;

      uint32_t list_property_size = SizeToByteSize(payload_size);

      for (uint32_t i = 0; i < *size; ++i) {
        auto metadata = reader->ReadMetadata();
        if (!metadata) return false;

        list_property_size += 1;
        if (!DecodePropertyValueSize(reader, metadata->type, metadata->payload_size, list_property_size)) return false;
      }

      property_size += list_property_size;
      return true;
    }
    case Type::MAP: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;

      uint32_t map_property_size = SizeToByteSize(payload_size);

      for (uint32_t i = 0; i < *size; ++i) {
        auto metadata = reader->ReadMetadata();
        if (!metadata) return false;

        map_property_size += 1;  // metadata size
        auto metadata_id_size = SizeToByteSize(metadata->id_size);
        map_property_size += metadata_id_size;

        if (!reader->SkipBytes(metadata_id_size)) return false;
        if (!DecodePropertyValueSize(reader, metadata->type, metadata->payload_size, map_property_size)) return false;
      }

      property_size += map_property_size;
      return true;
    }
    case Type::TEMPORAL_DATA: {
      const auto maybe_temporal_data_size = DecodeTemporalDataSize(*reader);
      if (!maybe_temporal_data_size) return false;

      property_size += *maybe_temporal_data_size;
      return true;
    }
    case Type::ZONED_TEMPORAL_DATA:
    case Type::OFFSET_ZONED_TEMPORAL_DATA: {
      const auto maybe_zoned_temporal_data_size = DecodeZonedTemporalDataSize(*reader);
      if (!maybe_zoned_temporal_data_size) return false;

      property_size += *maybe_zoned_temporal_data_size;
      return true;
    }
    case Type::ENUM: {
      // double payload
      // - first for enum type
      // - second for enum value
      auto const bytes = SizeToByteSize(payload_size) * 2;
      if (!reader->SkipBytes(bytes)) return false;
      property_size += bytes;
      return true;
    }
    case Type::POINT: {
      auto payload_members = valid2d(SizeToCrs(payload_size)) ? 2 : 3;
      auto bytes_size = payload_members * SizeToByteSize(Size::INT64);
      if (!reader->SkipBytes(bytes_size)) return false;
      property_size += bytes_size;
      return true;
    }
  }
}

// Function used to skip a PropertyValue from a byte stream.
//
// @sa ComparePropertyValue
[[nodiscard]] bool SkipPropertyValue(Reader *reader, Type type, Size payload_size) {
  switch (type) {
    case Type::EMPTY: {
      return false;
    }
    case Type::NONE:
    case Type::BOOL: {
      return true;
    }
    case Type::INT: {
      return reader->ReadInt(payload_size).has_value();
    }
    case Type::DOUBLE: {
      return reader->ReadDouble(payload_size).has_value();
    }
    case Type::STRING: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;
      if (!reader->SkipBytes(*size)) return false;
      return true;
    }
    case Type::LIST: {
      auto const size = reader->ReadUint(payload_size);
      if (!size) return false;
      auto size_val = *size;
      for (uint32_t i = 0; i != size_val; ++i) {
        auto metadata = reader->ReadMetadata();
        if (!metadata) return false;
        if (!SkipPropertyValue(reader, metadata->type, metadata->payload_size)) return false;
      }
      return true;
    }
    case Type::MAP: {
      auto const size = reader->ReadUint(payload_size);
      if (!size) return false;
      auto size_val = *size;
      for (uint32_t i = 0; i != size_val; ++i) {
        auto metadata = reader->ReadMetadata();
        if (!metadata) return false;
        if (!reader->SkipBytes(SizeToByteSize(metadata->id_size))) return false;
        if (!SkipPropertyValue(reader, metadata->type, metadata->payload_size)) return false;
      }
      return true;
    }
    case Type::TEMPORAL_DATA: {
      return DecodeTemporalData(*reader).has_value();
    }
    case Type::ZONED_TEMPORAL_DATA:
    case Type::OFFSET_ZONED_TEMPORAL_DATA: {
      return DecodeZonedTemporalData(*reader).has_value();
    }
    case Type::ENUM: {
      auto bytes_to_skip = 2 * SizeToByteSize(payload_size);
      return reader->SkipBytes(bytes_to_skip);
    }
    case Type::POINT: {
      auto payload_members = valid2d(SizeToCrs(payload_size)) ? 2 : 3;
      auto bytes_to_skip = payload_members * SizeToByteSize(Size::INT64);
      return reader->SkipBytes(bytes_to_skip);
    }
  }
}

// Function used to compare a PropertyValue to the one stored in the byte
// stream.
//
// NOTE: The logic in this function *MUST* be equal to the logic in
// `PropertyValue::operator==`. If you change this function make sure to change
// the operator so that they have identical functionality.
//
// @sa DecodePropertyValue
[[nodiscard]] bool ComparePropertyValue(Reader *reader, Type type, Size payload_size, const PropertyValue &value) {
  switch (type) {
    case Type::EMPTY: {
      return false;
    }
    case Type::NONE: {
      return value.IsNull();
    }
    case Type::BOOL: {
      if (!value.IsBool()) return false;
      bool bool_v = payload_size == Size::INT64;
      return value.ValueBool() == bool_v;
    }
    case Type::INT: {
      // Integer and double values are treated as the same in
      // `PropertyValue::operator==`. That is why we accept both integer and
      // double values here and use the `operator==` between them to verify that
      // they are the same.
      if (!value.IsInt() && !value.IsDouble()) return false;
      auto int_v = reader->ReadInt(payload_size);
      if (!int_v) return false;
      if (value.IsInt()) {
        return value.ValueInt() == int_v;
      } else {
        return value.ValueDouble() == int_v;
      }
    }
    case Type::DOUBLE: {
      // Integer and double values are treated as the same in
      // `PropertyValue::operator==`. That is why we accept both integer and
      // double values here and use the `operator==` between them to verify that
      // they are the same.
      if (!value.IsInt() && !value.IsDouble()) return false;
      auto double_v = reader->ReadDouble(payload_size);
      if (!double_v) return false;
      if (value.IsDouble()) {
        return value.ValueDouble() == double_v;
      } else {
        return value.ValueInt() == double_v;
      }
    }
    case Type::STRING: {
      if (!value.IsString()) return false;
      const auto &str = value.ValueString();
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;
      if (*size != str.size()) return false;
      return reader->VerifyBytes(str.data(), *size);
    }
    case Type::LIST: {
      // Handle all list types: regular List, IntList, DoubleList, NumericList
      if (!value.IsList() && !value.IsIntList() && !value.IsDoubleList() && !value.IsNumericList()) {
        return false;
      }

      auto size = reader->ReadUint(payload_size);
      if (!size) return false;

      // Get the appropriate list size based on the actual type
      size_t list_size = 0;
      if (value.IsList()) {
        list_size = value.ValueList().size();
      } else if (value.IsIntList()) {
        list_size = value.ValueIntList().size();
      } else if (value.IsDoubleList()) {
        list_size = value.ValueDoubleList().size();
      } else if (value.IsNumericList()) {
        list_size = value.ValueNumericList().size();
      }

      if (*size != list_size) return false;

      // For optimized numeric lists, we need to reconstruct the original PropertyValue list
      // to compare with the stored format
      if (value.IsIntList()) {
        const auto &int_list = value.ValueIntList();
        for (uint32_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return false;
          PropertyValue reconstructed_item(static_cast<int64_t>(int_list[i]));
          if (!ComparePropertyValue(reader, metadata->type, metadata->payload_size, reconstructed_item)) return false;
        }
      } else if (value.IsDoubleList()) {
        const auto &double_list = value.ValueDoubleList();
        for (uint32_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return false;
          PropertyValue reconstructed_item(double_list[i]);
          if (!ComparePropertyValue(reader, metadata->type, metadata->payload_size, reconstructed_item)) return false;
        }
      } else if (value.IsNumericList()) {
        const auto &numeric_list = value.ValueNumericList();
        for (uint32_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return false;
          PropertyValue reconstructed_item;
          if (std::holds_alternative<int>(numeric_list[i])) {
            reconstructed_item = PropertyValue(static_cast<int64_t>(std::get<int>(numeric_list[i])));
          } else {
            reconstructed_item = PropertyValue(std::get<double>(numeric_list[i]));
          }
          if (!ComparePropertyValue(reader, metadata->type, metadata->payload_size, reconstructed_item)) return false;
        }
      } else {
        // Regular list
        const auto &list = value.ValueList();
        for (uint32_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return false;
          if (!ComparePropertyValue(reader, metadata->type, metadata->payload_size, list[i])) return false;
        }
      }
      return true;
    }
    case Type::MAP: {
      if (!value.IsMap()) return false;
      const auto &map = value.ValueMap();
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;
      if (*size != map.size()) return false;
      for (const auto &item : map) {
        auto metadata = reader->ReadMetadata();
        if (!metadata) return false;
        auto property_id = reader->ReadUint(metadata->id_size);
        if (!property_id) return false;
        if (PropertyId::FromUint(*property_id) != item.first) return false;
        if (!ComparePropertyValue(reader, metadata->type, metadata->payload_size, item.second)) return false;
      }
      return true;
    }
    case Type::TEMPORAL_DATA: {
      if (!value.IsTemporalData()) return false;

      const auto maybe_temporal_data = DecodeTemporalData(*reader);
      if (!maybe_temporal_data) {
        return false;
      }

      return *maybe_temporal_data == value.ValueTemporalData();
    }
    case Type::ZONED_TEMPORAL_DATA:
    case Type::OFFSET_ZONED_TEMPORAL_DATA: {
      if (!value.IsZonedTemporalData()) return false;

      const auto maybe_zoned_temporal_data = DecodeZonedTemporalData(*reader);
      if (!maybe_zoned_temporal_data) {
        return false;
      }

      return *maybe_zoned_temporal_data == value.ValueZonedTemporalData();
    }
    case Type::ENUM: {
      if (!value.IsEnum()) return false;
      auto e_type = reader->ReadUint(payload_size);
      if (!e_type) return false;
      auto e_value = reader->ReadUint(payload_size);
      if (!e_value) return false;
      return value.ValueEnum() == Enum{EnumTypeId{*e_type}, EnumValueId{*e_value}};
    }
    case Type::POINT: {
      auto crs = SizeToCrs(payload_size);
      auto x_opt = reader->ReadDouble(Size::INT64);  // because we forced it as int64 on write
      if (!x_opt) return false;
      auto y_opt = reader->ReadDouble(Size::INT64);  // because we forced it as int64 on write
      if (!y_opt) return false;
      if (valid2d(crs) && value.IsPoint2d()) {
        return value.ValuePoint2d() == Point2d{crs, *x_opt, *y_opt};
      }
      if (valid3d(crs) && value.IsPoint3d()) {
        auto z_opt = reader->ReadDouble(Size::INT64);  // because we forced it as int64 on write
        if (!z_opt) return false;
        return value.ValuePoint3d() == Point3d{crs, *x_opt, *y_opt, *z_opt};
      }
      return false;
    }
  }
}

// Function used to encode a property (PropertyId, PropertyValue) into a byte
// stream.
bool EncodeProperty(Writer *writer, PropertyId property, const PropertyValue &value) {
  auto metadata = writer->WriteMetadata();
  if (!metadata) return false;

  auto id_size = writer->WriteUint(property.AsUint());
  if (!id_size) return false;

  auto type_property_size = EncodePropertyValue(writer, value);
  if (!type_property_size) return false;

  metadata->Set({type_property_size->first, *id_size, type_property_size->second});
  return true;
}

// Enum used to return status from the `DecodeExpectedProperty` function.
enum class ExpectedPropertyStatus {
  MISSING_DATA,
  SMALLER,
  EQUAL,
  GREATER,
};

// Function used to decode a property (PropertyId, PropertyValue) from a byte
// stream. The `expected_property` provides another hint whether the property
// should be decoded or skipped.
//
// @return MISSING_DATA when there is not enough data in the buffer to decode
//                      the property
// @return SMALLER when the property that was currently read has a smaller
//                 property ID than the expected property; the value isn't
//                 loaded in this case
// @return EQUAL when the property that was currently read has an ID equal to
//               the expected property ID; the value is loaded in this case
// @return GREATER when the property that was currenly read has a greater
//                 property ID than the expected property; the value isn't
//                 loaded in this case
//
// @sa DecodeAnyProperty
// @sa CompareExpectedProperty
// @sa TryDecodeExpectedProperty
[[nodiscard]] ExpectedPropertyStatus DecodeExpectedProperty(Reader *reader, PropertyId expected_property,
                                                            PropertyValue &value) {
  auto metadata = reader->ReadMetadata();
  if (!metadata) return ExpectedPropertyStatus::MISSING_DATA;

  auto property_id = reader->ReadUint(metadata->id_size);
  if (!property_id) return ExpectedPropertyStatus::MISSING_DATA;

  if (*property_id == expected_property.AsUint()) {
    if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, value))
      return ExpectedPropertyStatus::MISSING_DATA;
    return ExpectedPropertyStatus::EQUAL;
  }
  // Don't load the value if this isn't the expected property.
  if (!SkipPropertyValue(reader, metadata->type, metadata->payload_size)) return ExpectedPropertyStatus::MISSING_DATA;
  return (*property_id < expected_property.AsUint()) ? ExpectedPropertyStatus::SMALLER
                                                     : ExpectedPropertyStatus::GREATER;
}

// Similar to DecodeExpectedProperty, except that if the `expected_property`
// would be ordered before the next read property, the reader will not advance
// over the next property.
//
// @sa DecodeExpectedProperty
// @sa DecodeAnyProperty
// @sa CompareExpectedProperty
[[nodiscard]] auto TryDecodeExpectedProperty(Reader *reader, PropertyId expected_property)
    -> std::pair<ExpectedPropertyStatus, std::optional<PropertyValue>> {
  uint32_t const prior_position = reader->GetPosition();

  auto metadata = reader->ReadMetadata();
  if (!metadata) return {ExpectedPropertyStatus::MISSING_DATA, std::nullopt};

  auto property_id = reader->ReadUint(metadata->id_size);
  if (!property_id) return {ExpectedPropertyStatus::MISSING_DATA, std::nullopt};

  if (expected_property.AsUint() < property_id) {
    reader->SetPosition(prior_position);
    return {ExpectedPropertyStatus::GREATER, std::nullopt};
  }

  if (*property_id == expected_property.AsUint()) {
    auto decoded = DecodePropertyValue(reader, metadata->type, metadata->payload_size);
    if (!decoded) return {ExpectedPropertyStatus::MISSING_DATA, std::nullopt};
    return {ExpectedPropertyStatus::EQUAL, std::move(decoded)};
  }
  // Don't load the value if this isn't the expected property.
  if (!SkipPropertyValue(reader, metadata->type, metadata->payload_size))
    return {ExpectedPropertyStatus::MISSING_DATA, std::nullopt};
  return {ExpectedPropertyStatus::SMALLER, std::nullopt};
}

[[nodiscard]] ExpectedPropertyStatus DecodeExpectedPropertySize(Reader *reader, PropertyId expected_property,
                                                                uint32_t &size) {
  auto metadata = reader->ReadMetadata();
  if (!metadata) return ExpectedPropertyStatus::MISSING_DATA;

  auto property_id = reader->ReadUint(metadata->id_size);
  if (!property_id) return ExpectedPropertyStatus::MISSING_DATA;

  if (*property_id == expected_property.AsUint()) {
    // Add one byte for reading metadata + add the number of bytes for the property key
    size += (1 + SizeToByteSize(metadata->id_size));
    if (!DecodePropertyValueSize(reader, metadata->type, metadata->payload_size, size))
      return ExpectedPropertyStatus::MISSING_DATA;
    return ExpectedPropertyStatus::EQUAL;
  }
  // Don't load the value if this isn't the expected property.
  if (!SkipPropertyValue(reader, metadata->type, metadata->payload_size)) return ExpectedPropertyStatus::MISSING_DATA;
  return (*property_id < expected_property.AsUint()) ? ExpectedPropertyStatus::SMALLER
                                                     : ExpectedPropertyStatus::GREATER;
}

[[nodiscard]] ExpectedPropertyStatus DecodeExpectedPropertyType(Reader *reader, PropertyId expected_property,
                                                                ExtendedPropertyType &type) {
  auto metadata = reader->ReadMetadata();
  if (!metadata) return ExpectedPropertyStatus::MISSING_DATA;

  auto property_id = reader->ReadUint(metadata->id_size);
  if (!property_id) return ExpectedPropertyStatus::MISSING_DATA;

  switch (metadata->type) {
    using enum Type;
    case EMPTY:
    case NONE:
      type = ExtendedPropertyType{PropertyValue::Type::Null};
      break;
    case BOOL:
      type = ExtendedPropertyType{PropertyValue::Type::Bool};
      break;
    case INT:
      type = ExtendedPropertyType{PropertyValue::Type::Int};
      break;
    case DOUBLE:
      type = ExtendedPropertyType{PropertyValue::Type::Double};
      break;
    case STRING:
      type = ExtendedPropertyType{PropertyValue::Type::String};
      break;
    case LIST:
      type = ExtendedPropertyType{PropertyValue::Type::List};
      break;
    case MAP:
      type = ExtendedPropertyType{PropertyValue::Type::Map};
      break;
    case TEMPORAL_DATA: {
      // Found the property
      if (*property_id == expected_property.AsUint()) {
        PropertyValue value;
        if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, value))
          return ExpectedPropertyStatus::MISSING_DATA;
        type = ExtendedPropertyType{value.ValueTemporalData().type};
        return ExpectedPropertyStatus::EQUAL;
      }
      break;
    }
    case ZONED_TEMPORAL_DATA:
    case OFFSET_ZONED_TEMPORAL_DATA:
      type = ExtendedPropertyType{PropertyValue::Type::ZonedTemporalData};  // NOT SURE
      break;
    case ENUM: {
      // Found the property
      if (*property_id == expected_property.AsUint()) {
        PropertyValue value;
        if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, value))
          return ExpectedPropertyStatus::MISSING_DATA;
        type = ExtendedPropertyType{value.ValueEnum().type_id()};
        return ExpectedPropertyStatus::EQUAL;
      }
    } break;
    case POINT: {
      // Found the property
      if (*property_id == expected_property.AsUint()) {
        PropertyValue value;
        if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, value))
          return ExpectedPropertyStatus::MISSING_DATA;
        type = ExtendedPropertyType{
            value.type()};  // PropertyStoreType has only Point; while PropertyValueType has point 2d and 3d
        return ExpectedPropertyStatus::EQUAL;
      }
    } break;
  }

  if (*property_id == expected_property.AsUint()) {
    return ExpectedPropertyStatus::EQUAL;
  }

  // Don't load the value if this isn't the expected property.
  if (!SkipPropertyValue(reader, metadata->type, metadata->payload_size)) return ExpectedPropertyStatus::MISSING_DATA;

  return (*property_id < expected_property.AsUint()) ? ExpectedPropertyStatus::SMALLER
                                                     : ExpectedPropertyStatus::GREATER;
}

// Function used to check a property exists (PropertyId) from a byte stream.
// It will skip the encoded PropertyValue.
//
// @return MISSING_DATA when there is not enough data in the buffer to decode
//                      the property
// @return SMALLER when the property that was currently read has a smaller
//                 property ID than the expected property; the value isn't
//                 loaded in this case
// @return EQUAL when the property that was currently read has an ID equal to
//               the expected property ID; the value is loaded in this case
// @return GREATER when the property that was currenly read has a greater
//                 property ID than the expected property; the value isn't
//                 loaded in this case
//
// @sa DecodeAnyProperty
// @sa CompareExpectedProperty
[[nodiscard]] ExpectedPropertyStatus HasExpectedProperty(Reader *reader, PropertyId expected_property) {
  auto metadata = reader->ReadMetadata();
  if (!metadata) return ExpectedPropertyStatus::MISSING_DATA;

  auto property_id = reader->ReadUint(metadata->id_size);
  if (!property_id) return ExpectedPropertyStatus::MISSING_DATA;

  if (!SkipPropertyValue(reader, metadata->type, metadata->payload_size)) return ExpectedPropertyStatus::MISSING_DATA;

  if (*property_id < expected_property.AsUint()) {
    return ExpectedPropertyStatus::SMALLER;
  } else if (*property_id == expected_property.AsUint()) {
    return ExpectedPropertyStatus::EQUAL;
  } else {
    return ExpectedPropertyStatus::GREATER;
  }
}

[[nodiscard]] auto NextPropertyAndType(Reader *reader) -> std::optional<PropertyStoreMemberInfo> {
  auto metadata = reader->ReadMetadata();
  if (!metadata) return std::nullopt;

  auto property_id = reader->ReadUint(metadata->id_size);
  if (!property_id) return std::nullopt;

  // Special case: TEMPORAL_DATA has a subtype we need to extract
  if (metadata->type == Type::TEMPORAL_DATA) {
    auto temporal_data = DecodeTemporalData(*reader);
    if (!temporal_data) return std::nullopt;
    return PropertyStoreMemberInfo{PropertyId::FromUint(*property_id), metadata->type, temporal_data->type};
  }

  if (!SkipPropertyValue(reader, metadata->type, metadata->payload_size)) return std::nullopt;

  return PropertyStoreMemberInfo{PropertyId::FromUint(*property_id), metadata->type, std::nullopt};
}

// Function used to decode a property (PropertyId, PropertyValue) from a byte
// stream.
//
// @sa DecodeExpectedProperty
// @sa CompareExpectedProperty
[[nodiscard]] std::optional<PropertyId> DecodeAnyProperty(Reader *reader, PropertyValue &value) {
  auto metadata = reader->ReadMetadata();
  if (!metadata) return std::nullopt;

  auto property_id = reader->ReadUint(metadata->id_size);
  if (!property_id) return std::nullopt;

  if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, value)) return std::nullopt;

  return PropertyId::FromUint(*property_id);
}

[[nodiscard]] std::optional<PropertyId> DecodeAnyExtendedPropertyType(Reader *reader, ExtendedPropertyType &type) {
  auto metadata = reader->ReadMetadata();
  if (!metadata) return std::nullopt;

  auto property_id = reader->ReadUint(metadata->id_size);
  if (!property_id) return std::nullopt;

  switch (metadata->type) {
    using enum Type;
    case EMPTY:
    case NONE:
      type = ExtendedPropertyType{PropertyValue::Type::Null};
      break;
    case BOOL:
      type = ExtendedPropertyType{PropertyValue::Type::Bool};
      break;
    case INT:
      type = ExtendedPropertyType{PropertyValue::Type::Int};
      break;
    case DOUBLE:
      type = ExtendedPropertyType{PropertyValue::Type::Double};
      break;
    case STRING:
      type = ExtendedPropertyType{PropertyValue::Type::String};
      break;
    case LIST:
      type = ExtendedPropertyType{PropertyValue::Type::List};
      break;
    case MAP:
      type = ExtendedPropertyType{PropertyValue::Type::Map};
      break;
    case TEMPORAL_DATA: {
      PropertyValue value;
      if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, value)) return std::nullopt;
      type = ExtendedPropertyType{value.ValueTemporalData().type};
      return PropertyId::FromUint(*property_id);
    }
    case ZONED_TEMPORAL_DATA:
    case OFFSET_ZONED_TEMPORAL_DATA:
      type = ExtendedPropertyType{PropertyValue::Type::ZonedTemporalData};  // NOT SURE
      break;
    case ENUM: {
      PropertyValue value;
      if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, value)) return std::nullopt;
      type = ExtendedPropertyType{value.ValueEnum().type_id()};
      return PropertyId::FromUint(*property_id);
    }
    case POINT: {
      PropertyValue value;
      if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, value)) return std::nullopt;
      type = ExtendedPropertyType{
          value.type()};  // PropertyStoreType has only Point; while PropertyValueType has point 2d and 3d
      return PropertyId::FromUint(*property_id);
    }
  }

  if (!SkipPropertyValue(reader, metadata->type, metadata->payload_size)) return std::nullopt;

  return PropertyId::FromUint(*property_id);
}

// Function used to compare a property (PropertyId, PropertyValue) to current
// property in the byte stream.
//
// @sa DecodeExpectedProperty
// @sa DecodeAnyProperty
[[nodiscard]] bool CompareExpectedProperty(Reader *reader, PropertyId expected_property, const PropertyValue &value) {
  auto metadata = reader->ReadMetadata();
  if (!metadata) return false;

  auto property_id = reader->ReadUint(metadata->id_size);
  if (!property_id) return false;
  if (*property_id != expected_property.AsUint()) return false;

  return ComparePropertyValue(reader, metadata->type, metadata->payload_size, value);
}

// Function used to find and (selectively) get the property value of the
// property whose ID is `property`. It relies on the fact that the properties
// are sorted (by ID) in the buffer. If the function doesn't find the property,
// the `value` won't be updated.
//
// @sa FindSpecificPropertyAndBufferInfo
// @sa MatchSpecificProperty
[[nodiscard]] ExpectedPropertyStatus FindSpecificProperty(Reader *reader, PropertyId property, PropertyValue &value) {
  while (true) {
    auto ret = DecodeExpectedProperty(reader, property, value);
    // Because the properties are sorted in the buffer, we only need to
    // continue searching for the property while this function returns a
    // `SMALLER` value indicating that the ID of the found property is smaller
    // than the seeked ID. All other return values (`MISSING_DATA`, `EQUAL` and
    // `GREATER`) terminate the search.
    if (ret != ExpectedPropertyStatus::SMALLER) {
      return ret;
    }
  }
}

// Similar to FindSpecificProperty, except that the reader will not consume
// the next property if it is ordered after the requested property.
//
// @sa FindSpecificProperty
// @sa FindSpecificPropertyAndBufferInfo
[[nodiscard]] auto MatchSpecificProperty(Reader *reader, PropertyId property)
    -> std::pair<ExpectedPropertyStatus, std::optional<PropertyValue>> {
  while (true) {
    auto ret = TryDecodeExpectedProperty(reader, property);
    // Because the properties are sorted in the buffer, we only need to
    // continue searching for the property while this function returns a
    // `SMALLER` value indicating that the ID of the found property is smaller
    // than the seeked ID. All other return values (`MISSING_DATA`, `EQUAL` and
    // `GREATER`) terminate the search.
    if (ret.first != ExpectedPropertyStatus::SMALLER) {
      return ret;
    }
  }
}

[[nodiscard]] ExpectedPropertyStatus FindSpecificPropertySize(Reader *reader, PropertyId property, uint32_t &size) {
  ExpectedPropertyStatus ret = ExpectedPropertyStatus::SMALLER;
  while ((ret = DecodeExpectedPropertySize(reader, property, size)) == ExpectedPropertyStatus::SMALLER) {
  }
  return ret;
}

[[nodiscard]] ExpectedPropertyStatus FindSpecificExtendedPropertyType(Reader *reader, PropertyId property,
                                                                      ExtendedPropertyType &value) {
  while (true) {
    auto ret = DecodeExpectedPropertyType(reader, property, value);
    if (ret != ExpectedPropertyStatus::SMALLER) {
      return ret;
    }
  }
}

// Function used to find if property is set. It relies on the fact that the properties
// are sorted (by ID) in the buffer.
//
// @sa FindSpecificPropertyAndBufferInfo
[[nodiscard]] ExpectedPropertyStatus ExistsSpecificProperty(Reader *reader, PropertyId property) {
  while (true) {
    auto ret = HasExpectedProperty(reader, property);
    // Because the properties are sorted in the buffer, we only need to
    // continue searching for the property while this function returns a
    // `SMALLER` value indicating that the ID of the found property is smaller
    // than the seeked ID. All other return values (`MISSING_DATA`, `EQUAL` and
    // `GREATER`) terminate the search.
    if (ret != ExpectedPropertyStatus::SMALLER) {
      return ret;
    }
  }
}

// Struct used to return info about the property position and buffer size.
struct SpecificPropertyAndBufferInfo {
  uint32_t property_begin;
  uint32_t property_end;
  uint32_t property_size;
  uint32_t all_begin;
  uint32_t all_end;
  uint32_t all_size;
};

// Struct used to return info about the property position
struct SpecificPropertyAndBufferInfoMinimal {
  ExpectedPropertyStatus status;
  uint64_t property_begin;
  uint64_t property_end;

  auto property_size() const { return property_end - property_begin; }
};

// Function used to find the position where the property should be in the data
// buffer. It keeps the properties in the buffer sorted by `PropertyId` and
// returns the positions in the buffer where the seeked property starts and
// ends. It also returns the positions where all of the properties start and
// end. Also, sizes are calculated.
// If the function doesn't find the property, the `property_size` will be `0`
// and `property_begin` will be equal to `property_end`. Positions and size of
// all properties is always calculated (even if the specific property isn't
// found).
//
// @sa FindSpecificProperty
SpecificPropertyAndBufferInfo FindSpecificPropertyAndBufferInfo(Reader *reader, PropertyId property) {
  uint32_t property_begin = reader->GetPosition();
  uint32_t property_end = reader->GetPosition();
  const uint32_t all_begin = reader->GetPosition();
  uint32_t all_end = reader->GetPosition();
  while (true) {
    auto ret = HasExpectedProperty(reader, property);
    if (ret == ExpectedPropertyStatus::MISSING_DATA) {
      break;
    }
    if (ret == ExpectedPropertyStatus::SMALLER) {
      property_begin = reader->GetPosition();
      property_end = reader->GetPosition();
    } else if (ret == ExpectedPropertyStatus::EQUAL) {
      property_end = reader->GetPosition();
    }
    all_end = reader->GetPosition();
  }
  return {property_begin, property_end, property_end - property_begin, all_begin, all_end, all_end - all_begin};
}

// Like FindSpecificPropertyAndBufferInfo, but will early exit. No need to find the "all" information
SpecificPropertyAndBufferInfoMinimal FindSpecificPropertyAndBufferInfoMinimal(Reader *reader, PropertyId property) {
  uint64_t property_begin = reader->GetPosition();
  while (true) {
    auto status = HasExpectedProperty(reader, property);
    switch (status) {
      case ExpectedPropertyStatus::MISSING_DATA: {
        return {status, 0, 0};
      }
      case ExpectedPropertyStatus::GREATER: {
        // Restore the reader position so that the next property isn't skipped.
        // This allows `FindSpecificPropertyAndBufferInfoMinimal` to be
        // additional properties to be read from the same reader.
        reader->SetPosition(property_begin);
        return {status, 0, 0};
      }
      case ExpectedPropertyStatus::EQUAL: {
        return {status, property_begin, reader->GetPosition()};
      }
      case ExpectedPropertyStatus::SMALLER: {
        property_begin = reader->GetPosition();
        break;
      }
    }
  }
}

// All data buffers will be allocated to a multiple of 8 size.
uint32_t ToMultipleOf8(uint32_t size) {
  const uint32_t mod = size % 8;
  if (mod == 0) return size;
  return size - mod + 8;
}

// The `PropertyStore` also uses a small buffer optimization in it. If the data
// fits into the size of the internally stored pointer and size, then the
// pointer and size are used as a in-place buffer. In order to be able to do
// this we store a `union` of the two sets of data. Because the storage is a
// `union`, only one set of information (pointer+size or buffer) can be used at
// any time. The buffer perfectly overlaps with the memory locations of the
// pointer+size. This is illustrated in the following diagram:
//
// Memory (hex):
// 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
// |---------------------|                         -> size
//                         |---------------------| -> data
// 0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 -> buffer_ (positions)
//
// When we are using the pointer+size we know that the size must
// be a multiple of 8 (because we always allocate a buffer whose size is a
// multiple of 8). That means that the lower 3 bits of the `size` field must be
// zero when the data is used as a pointer+size.
//
// Because this architecture is little-endian, we know that `buffer_[0]` will be
// aligned with the lowest byte of the `size` field. When we use the inline
// `buffer_` we write `kUseLocalBuffer` (which is exactly 1) to `buffer_[0]`
// which will make the `size` read (independent of the other values in the
// buffer) always not be a multiple of 8. We use that fact to distinguish which
// of the two sets of data is currently active. Because the first byte of the
// buffer is used to distinguish which of the two sets of data is used, we can
// only use the leftover 15 bytes for raw data storage.
static_assert(std::endian::native == std::endian::little, "Our code assumes little endian");

const uint8_t kUseLocalBuffer = 0x01;
const uint8_t kUseCompressedBuffer = 0x02;
static_assert(kUseLocalBuffer % 8 != 0, "Special storage modes need to be not a multiple of 8");
static_assert(kUseCompressedBuffer % 8 != 0, "Special storage modes need to be not a multiple of 8");

enum class StorageMode : uint8_t {
  EMPTY,
  BUFFER,
  LOCAL,
  COMPRESSED,
};

struct DecodedBufferConst {
  std::span<uint8_t const> view;
  StorageMode storage_mode;
};
struct DecodedBuffer {
  std::span<uint8_t> view;
  StorageMode storage_mode;

  // implicit conversion operator
  // NOLINTNEXTLINE( hicpp-explicit-conversions )
  explicit(false) operator DecodedBufferConst() {
    return {
        .view = view,
        .storage_mode = storage_mode,
    };
  }
};

void FreeMemory(DecodedBuffer const &buffer_info) {
  switch (buffer_info.storage_mode) {
    case StorageMode::BUFFER:
    case StorageMode::COMPRESSED:
      delete[] buffer_info.view.data();
      break;
    case StorageMode::LOCAL:
    case StorageMode::EMPTY:
      break;
  }
}

void SetSizeData(std::array<uint8_t, 12> &buffer, uint32_t size, const uint8_t *data) {
  memcpy(buffer.data(), &size, sizeof(size));
  // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
  memcpy(buffer.data() + sizeof(size), static_cast<void const *>(&data), sizeof(uint8_t *));
}
DecodedBuffer SetupLocalBuffer(std::array<uint8_t, 12> &buffer) {
  buffer[0] = kUseLocalBuffer;
  return DecodedBuffer{
      .view = std::span{&buffer[1], sizeof(buffer) - 1},
      .storage_mode = StorageMode::LOCAL,
  };
}
DecodedBuffer SetupExternalBuffer(uint32_t size) {
  auto alloc_size = ToMultipleOf8(size);
  auto *alloc_data = new uint8_t[alloc_size];

  return DecodedBuffer{
      .view = std::span{alloc_data, alloc_size},
      .storage_mode = StorageMode::BUFFER,
  };
}
DecodedBuffer SetupBuffer(std::array<uint8_t, 12> &buffer, uint32_t size) {
  auto can_fit_in_local = size <= sizeof(buffer) - 1;
  return can_fit_in_local ? SetupLocalBuffer(buffer) : SetupExternalBuffer(size);
}

std::optional<utils::DecompressedBuffer> DecompressBuffer(DecodedBufferConst const &buffer_info) {
  if (buffer_info.storage_mode != StorageMode::COMPRESSED) return std::nullopt;

  // Memory (hex):
  // 00 00 00 00 00 00 00
  // |------------|         -> original size
  //                ||      -> size modifier to get back to non multiple of 8
  //                   |--- -> compressed data
  // 0  1  2  3  4  5  6  (positions)

  uint32_t original_size = 0;
  auto const *data = buffer_info.view.data();
  memcpy(&original_size, data, sizeof(uint32_t));

  // we have to restore the original size of the compressed buffer + the size of the original buffer
  auto modifier = data[sizeof(uint32_t)];
  auto buffer_size = buffer_info.view.size_bytes();
  auto compressed_size = (modifier != 0) ? (buffer_size - 8 + modifier) : buffer_size;

  auto data_offset = sizeof(uint32_t) + 1;
  auto compressed_buffer = std::span(data + data_offset, compressed_size - data_offset);
  auto const *compressor = utils::Compressor::GetInstance();
  auto decompressed_buffer = compressor->Decompress(compressed_buffer, original_size);

  if (!decompressed_buffer) [[unlikely]] {
    throw PropertyValueException("Failed to decompress buffer");
  }

  return decompressed_buffer;
}

void CompressBuffer(std::array<uint8_t, 12> &buffer, DecodedBuffer const &buffer_info) {
  if (buffer_info.storage_mode != StorageMode::BUFFER) {
    return;
  }
  auto uncompressed_size = buffer_info.view.size_bytes();

  auto const *compressor = utils::Compressor::GetInstance();
  auto compressed_buffer = compressor->Compress(buffer_info.view);
  if (!compressed_buffer) {
    throw PropertyValueException("Failed to compress buffer");
  }

  auto compressed_view = compressed_buffer->view();
  auto const metadata_size = sizeof(uint32_t) + 1;
  auto size_needed = compressed_view.size_bytes() + metadata_size;
  auto compressed_size_to_multiple_of_8 = ToMultipleOf8(size_needed);
  if (compressed_size_to_multiple_of_8 >= uncompressed_size) {
    // Compressed buffer + metadata are larger than the original buffer, so we don't perform the compression.
    return;
  }

  auto compressed_data = std::make_unique_for_overwrite<uint8_t[]>(compressed_size_to_multiple_of_8);

  // We have compressed data + new buffer to put it into, no need for old uncompressed buffer
  FreeMemory(buffer_info);

  // first 4 bytes are the size of the original buffer
  auto orig_size = compressed_buffer->original_size();
  memcpy(compressed_data.get(), &orig_size, sizeof(uint32_t));

  // next byte is the mod before multiple of 8
  const uint8_t mod = size_needed % 8;
  compressed_data[sizeof(uint32_t)] = mod;

  // the rest of the buffer is the compressed data
  memcpy(compressed_data.get() + metadata_size, compressed_view.data(), compressed_view.size_bytes());

  SetSizeData(buffer, compressed_size_to_multiple_of_8 + kUseCompressedBuffer, compressed_data.release());
}

// Helper functions used to retrieve/store `size` and `data` from/into the
// `buffer_`.
auto GetDecodedBuffer(std::array<uint8_t, 12> &buffer) -> DecodedBuffer {
  uint32_t size = 0;
  uint8_t *data = nullptr;
  memcpy(static_cast<void *>(&size), buffer.data(), sizeof(uint32_t));
  // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
  memcpy(static_cast<void *>(&data), buffer.data() + sizeof(uint32_t), sizeof(uint8_t *));

  if (size == 0) {
    return {std::span<uint8_t>{}, StorageMode::EMPTY};
  }

  if (size % 8 == 0) {
    return {std::span{data, size}, StorageMode::BUFFER};
  }

  auto special_mode_value = static_cast<uint8_t>(size & (sizeof(uint8_t) * CHAR_BIT - 1));
  switch (special_mode_value) {
    case kUseLocalBuffer: {
      auto *local_start = &buffer[1];
      auto local_size = static_cast<uint32_t>(sizeof(buffer) - 1);
      return {std::span{local_start, local_size}, StorageMode::LOCAL};
    }
    case kUseCompressedBuffer: {
      auto real_size = static_cast<uint32_t>(size & ~(sizeof(uint8_t) * CHAR_BIT - 1));
      return {std::span{data, real_size}, StorageMode::COMPRESSED};
    }
    default: {
      MG_ASSERT(false, "Corrupt property storage");
    }
  }
}

auto GetDecodedBuffer(std::array<uint8_t, 12> const &buffer) -> DecodedBufferConst {
  uint32_t size = 0;
  uint8_t *data = nullptr;
  memcpy(static_cast<void *>(&size), buffer.data(), sizeof(uint32_t));
  // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
  memcpy(static_cast<void *>(&data), static_cast<const uint8_t *>(buffer.data() + sizeof(uint32_t)), sizeof(uint8_t *));

  if (size == 0) {
    return {std::span<uint8_t>{}, StorageMode::EMPTY};
  }

  if (size % 8 == 0) {
    return {std::span{data, size}, StorageMode::BUFFER};
  }

  auto special_mode_value = static_cast<uint8_t>(size & (sizeof(uint8_t) * CHAR_BIT - 1));
  switch (special_mode_value) {
    case kUseLocalBuffer: {
      auto const *local_start = &buffer[1];
      auto local_size = static_cast<uint32_t>(sizeof(buffer) - 1);
      return {std::span{local_start, local_size}, StorageMode::LOCAL};
    }
    case kUseCompressedBuffer: {
      auto real_size = static_cast<uint32_t>(size & ~(sizeof(uint8_t) * CHAR_BIT - 1));
      return {std::span{data, real_size}, StorageMode::COMPRESSED};
    }
    default: {
      MG_ASSERT(false, "Corrupt property storage");
    }
  }
}

}  // namespace

PropertyStore::PropertyStore() = default;

PropertyStore::PropertyStore(PropertyStore &&other) noexcept : buffer_(other.buffer_) {
  // std::array assignment
  other.buffer_ = {};  // Zero-initialize
}

PropertyStore &PropertyStore::operator=(PropertyStore &&other) noexcept {
  if (this == std::addressof(other)) return *this;

  auto buffer_info = GetDecodedBuffer(buffer_);
  FreeMemory(buffer_info);

  // copy over the buffer
  buffer_ = other.buffer_;  // std::array assignment
  // make other empty
  other.buffer_ = {};  // Zero-initialize

  return *this;
}

PropertyStore::~PropertyStore() {
  auto buffer_info = GetDecodedBuffer(buffer_);
  FreeMemory(buffer_info);
}

template <typename Func>
auto PropertyStore::WithReader(Func &&func) const {
  auto buffer_info = GetDecodedBuffer(buffer_);
  if (buffer_info.storage_mode == StorageMode::COMPRESSED) {
    auto decompressed_buffer = DecompressBuffer(buffer_info);
    auto view = decompressed_buffer->view();
    Reader reader(view.data(), view.size_bytes());
    return std::forward<Func>(func)(reader);
  }
  Reader reader(buffer_info.view.data(), buffer_info.view.size_bytes());
  return std::forward<Func>(func)(reader);
}

/// When reading from the reader, once you have hit MISSING_DATA its no longer safe to keep reading
/// example: reader could be in local buffer with junk data after the EMPTY marker, hence not safe to read that junk
template <typename GetFunc, typename ApplyFunc, typename MissingValue>
struct SafeReader {
  template <typename GetFuncCtr, typename ApplyFuncCtr>
  SafeReader(Reader &reader, GetFuncCtr &&get_result, ApplyFuncCtr &&apply_result, MissingValue missing_value)
      : reader_(reader),
        get_result_(std::forward<GetFunc>(get_result)),
        apply_result_(std::forward<ApplyFunc>(apply_result)),
        missing_value_{std::move(missing_value)} {}
  template <typename... Args, typename... Args2>
  auto operator()(std::tuple<Args...> args, std::tuple<Args2...> args2) {
    auto got_result = std::invoke([&]() -> typename std::invoke_result_t<GetFunc, Reader &, Args...>::second_type {
      if (still_safe_) {
        auto ret = std::apply(get_result_, std::tuple_cat(std::tuple{std::ref(reader_)}, std::move(args)));
        if (ret.first != ExpectedPropertyStatus::MISSING_DATA) {
          return std::move(ret.second);
        }
        still_safe_ = false;
      }
      return missing_value_;
    });
    std::apply(apply_result_, std::tuple_cat(std::tuple{std::move(got_result)}, std::move(args2)));
  }

 private:
  Reader &reader_;
  GetFunc get_result_;
  ApplyFunc apply_result_;
  bool still_safe_ = true;
  MissingValue missing_value_;
};

template <typename GetFunc, typename ApplyFunc, typename MissingValue>
SafeReader(Reader &, GetFunc &&, ApplyFunc &&, MissingValue) -> SafeReader<GetFunc, ApplyFunc, MissingValue>;

PropertyValue PropertyStore::GetProperty(PropertyId property) const {
  auto get_property = [&](Reader &reader) -> PropertyValue {
    PropertyValue value;
    if (FindSpecificProperty(&reader, property, value) != ExpectedPropertyStatus::EQUAL) return {};
    return value;
  };
  return WithReader(get_property);
}

ExtendedPropertyType PropertyStore::GetExtendedPropertyType(PropertyId property) const {
  auto get_property_type = [&](Reader &reader) -> ExtendedPropertyType {
    ExtendedPropertyType type{};
    if (FindSpecificExtendedPropertyType(&reader, property, type) != ExpectedPropertyStatus::EQUAL) return {};
    return type;
  };
  return WithReader(get_property_type);
}

uint32_t PropertyStore::PropertySize(PropertyId property) const {
  auto get_property_size = [&](Reader &reader) -> uint32_t {
    uint32_t property_size = 0;
    if (FindSpecificPropertySize(&reader, property, property_size) != ExpectedPropertyStatus::EQUAL) return 0;
    return property_size;
  };
  return WithReader(get_property_size);
}

bool PropertyStore::HasProperty(PropertyId property) const {
  auto property_exists = [&](Reader &reader) -> uint32_t {
    return ExistsSpecificProperty(&reader, property) == ExpectedPropertyStatus::EQUAL;
  };
  return WithReader(property_exists);
}

bool PropertyStore::HasAllProperties(const std::set<PropertyId> &properties) const {
  return std::all_of(properties.begin(), properties.end(), [this](const auto &prop) { return HasProperty(prop); });
}

bool PropertyStore::HasAllPropertyValues(const std::vector<PropertyValue> &property_values) const {
  auto property_map = Properties();
  std::vector<PropertyValue> all_property_values;
  transform(property_map.begin(), property_map.end(), back_inserter(all_property_values),
            [](const auto &kv_entry) { return kv_entry.second; });

  return std::all_of(
      property_values.begin(), property_values.end(), [&all_property_values](const PropertyValue &value) {
        return std::find(all_property_values.begin(), all_property_values.end(), value) != all_property_values.end();
      });
}

std::optional<std::vector<PropertyValue>> PropertyStore::ExtractPropertyValues(
    const std::set<PropertyId> &properties) const {
  auto get_property = [&](Reader &reader) -> std::optional<std::vector<PropertyValue>> {
    PropertyValue value;
    auto values = std::vector<PropertyValue>{};
    values.reserve(properties.size());
    for (auto property : properties) {
      if (FindSpecificProperty(&reader, property, value) != ExpectedPropertyStatus::EQUAL) return std::nullopt;
      values.emplace_back(std::move(value));
    }
    return values;
  };
  return WithReader(get_property);
}

/**
 * In order to read multiple nested properties with minimal backtracking, we
 * need a history of where previously seen map properties are stored in the
 * `PropertyStore` buffer.
 */
class ReaderPropPositionHistory {
 public:
  // The maximum history depth will always be one less than the maximum
  // amount of nesting in properties we expect to read. For example, to
  // read a.b.c.d, we need to store three levels of nesting (`a`, `b`, and `c`).
  ReaderPropPositionHistory(std::size_t max_size) { history_.reserve(max_size); }

  // Move the reader to a position where the expected leaf property will be
  // located after the reader.
  ExpectedPropertyStatus ScanToPropertyPathParent(Reader &reader, PropertyPath const &path) {
    std::span<PropertyId const> parent_map{path.begin(), path.end() - 1};

    auto [history_fork_it, parent_map_fork_it] = r::mismatch(history_, parent_map, {}, &History::property_id);

    if (history_fork_it != history_.end()) {
      reader.SetPosition(history_fork_it->offset_to_property_end);
      history_.erase(history_fork_it, history_.end());
    }

    for (auto inner_property_id : std::ranges::subrange(parent_map_fork_it, parent_map.end())) {
      auto info = FindSpecificPropertyAndBufferInfoMinimal(&reader, inner_property_id);
      if (info.status != ExpectedPropertyStatus::EQUAL) return info.status;
      history_.emplace_back(inner_property_id, info.property_end);

      reader.SetPosition(info.property_begin);
      auto metadata = reader.ReadMetadata();
      reader.SkipBytes(SizeToByteSize(metadata->id_size) + SizeToByteSize(metadata->payload_size));
    }

    return ExpectedPropertyStatus::EQUAL;
  }

 private:
  struct History {
    PropertyId property_id;
    uint32_t offset_to_property_end;
  };
  std::vector<History> history_;
};

std::vector<PropertyValue> PropertyStore::ExtractPropertyValuesMissingAsNull(
    std::span<PropertyPath const> ordered_properties) const {
  auto get_properties = [&](Reader &reader) -> std::vector<PropertyValue> {
    auto values = std::vector<PropertyValue>{};
    values.reserve(ordered_properties.size());

    auto max_history_depth = r::max_element(ordered_properties, {}, std::mem_fn(&PropertyPath::size))->size() - 1;
    ReaderPropPositionHistory history{max_history_depth};

    auto const get_value =
        [&](Reader &reader,
            PropertyPath const &path) -> std::pair<ExpectedPropertyStatus, std::optional<PropertyValue>> {
      auto result = history.ScanToPropertyPathParent(reader, path);
      if (result != ExpectedPropertyStatus::EQUAL) {
        return {result, std::nullopt};
      }

      auto leaf_property_id = path.back();
      return MatchSpecificProperty(&reader, leaf_property_id);
    };

    auto const insert_value = [&](std::optional<PropertyValue> value) {
      if (value) {
        values.emplace_back(*std::move(value));
      } else {
        values.emplace_back();
      }
    };

    auto safe_reader = SafeReader{reader, get_value, insert_value, std::nullopt};
    for (auto &&path : ordered_properties) {
      safe_reader(std::forward_as_tuple(path), std::tuple{});
    }
    return values;
  };
  return WithReader(get_properties);
}

bool PropertyStore::IsPropertyEqual(PropertyId property, const PropertyValue &value) const {
  auto property_equal = [&](Reader &reader) -> bool {
    auto const orig_reader = reader;
    auto info = FindSpecificPropertyAndBufferInfoMinimal(&reader, property);
    auto property_size = info.property_size();
    if (property_size == 0) return value.IsNull();
    auto prop_reader = Reader(orig_reader, info.property_begin, property_size);
    if (!CompareExpectedProperty(&prop_reader, property, value)) return false;
    return prop_reader.GetPosition() == property_size;
  };
  return WithReader(property_equal);
}

auto PropertyStore::ArePropertiesEqual(std::span<PropertyPath const> ordered_properties,
                                       std::span<PropertyValue const> values,
                                       std::span<std::size_t const> position_lookup) const -> std::vector<bool> {
  auto max_history_depth = r::max_element(ordered_properties, {}, std::mem_fn(&PropertyPath::size))->size() - 1;
  ReaderPropPositionHistory history{max_history_depth};

  auto properties_are_equal = [&](Reader &reader) -> std::vector<bool> {
    auto result = std::vector<bool>(ordered_properties.size(), false);

    auto const get_result = [&](Reader &reader, PropertyPath const &path, PropertyValue const &cmp_val) {
      auto const orig_reader = reader;

      auto result = history.ScanToPropertyPathParent(reader, path);
      if (result != ExpectedPropertyStatus::EQUAL) {
        return std::pair{result, std::optional<bool>{cmp_val.IsNull()}};
      }

      auto leaf_property_id = path.back();

      auto info = FindSpecificPropertyAndBufferInfoMinimal(&reader, leaf_property_id);
      auto property_size = info.property_size();
      if (property_size != 0) {
        auto prop_reader = Reader(orig_reader, info.property_begin, property_size);
        auto cmp_res = CompareExpectedProperty(&prop_reader, leaf_property_id, cmp_val);
        return std::pair{info.status, std::optional{cmp_res}};
      } else {
        return std::pair{info.status, std::optional<bool>{cmp_val.IsNull()}};
      }
    };
    auto const apply_result = [&](std::optional<bool> extracted_result, PropertyValue const &cmp_val, size_t pos) {
      result[pos] = extracted_result ? *extracted_result : cmp_val.IsNull();
    };

    auto safe_reader = SafeReader{reader, get_result, apply_result, std::nullopt};
    for (auto [pos, property] : ranges::views::enumerate(ordered_properties)) {
      auto const &value = values[position_lookup[pos]];
      safe_reader(std::tuple{property, std::cref(value)}, std::tuple{std::cref(value), pos});
    }
    return result;
  };
  return WithReader(properties_are_equal);
}

std::map<PropertyId, PropertyValue> PropertyStore::Properties() const {
  auto get_properties = [&](Reader &reader) {
    std::map<PropertyId, PropertyValue> props;
    PropertyValue value;
    while (true) {
      auto prop = DecodeAnyProperty(&reader, value);
      if (!prop) break;
      props.emplace(*prop, std::move(value));
    }
    return props;
  };
  return WithReader(get_properties);
}

std::map<PropertyId, ExtendedPropertyType> PropertyStore::ExtendedPropertyTypes() const {
  auto get_properties = [&](Reader &reader) {
    std::map<PropertyId, ExtendedPropertyType> props;
    while (true) {
      ExtendedPropertyType type{PropertyValue::Type::Null};
      auto prop = DecodeAnyExtendedPropertyType(&reader, type);
      if (!prop) break;
      props.emplace(*prop, type);
    }
    return props;
  };
  return WithReader(get_properties);
}

std::vector<PropertyId> PropertyStore::ExtractPropertyIds() const {
  auto get_properties = [&](Reader &reader) {
    std::vector<PropertyId> props;
    while (true) {
      // TODO: no need to capture ExtendedPropertyType, make dedicated DecodeAny
      ExtendedPropertyType type{PropertyValue::Type::Null};
      auto prop = DecodeAnyExtendedPropertyType(&reader, type);
      if (!prop) break;
      props.emplace_back(*prop);
    }
    return props;
  };
  return WithReader(get_properties);
}

bool PropertyStore::SetProperty(PropertyId property, const PropertyValue &value) {
  uint32_t property_size = 0;
  if (!value.IsNull()) {
    Writer writer;
    EncodeProperty(&writer, property, value);
    property_size = writer.Written();
  }

  auto buffer_info = GetDecodedBuffer(buffer_);

  bool existed = false;
  if (buffer_info.storage_mode == StorageMode::EMPTY) {
    if (!value.IsNull()) {
      // We don't have a data buffer. Setup on for writting
      auto new_buffer_info = SetupBuffer(buffer_, property_size);
      auto new_view = new_buffer_info.view;

      // Encode the property into the data buffer.
      Writer writer(new_view.data(), new_view.size());
      MG_ASSERT(EncodeProperty(&writer, property, value), "Invalid database state!");
      auto metadata = writer.WriteMetadata();
      if (metadata) {
        // If there is any space left in the buffer we add a tombstone to
        // indicate that there are no more properties to be decoded.
        metadata->Set({Type::EMPTY});
      }

      // Make buffer perminant
      if (new_buffer_info.storage_mode == StorageMode::BUFFER) {
        SetSizeData(buffer_, new_view.size_bytes(), new_view.data());
      }

      buffer_info = new_buffer_info;
    } else {
      // We don't have to do anything. We don't have a buffer and we are trying
      // to set a property to `Null` (we are trying to remove the property).
      return !existed;
    }
  } else {
    std::optional<utils::DecompressedBuffer> decompressed_buffer;

    auto current_view = std::invoke([&] {
      if (buffer_info.storage_mode == StorageMode::COMPRESSED) {
        decompressed_buffer = DecompressBuffer(buffer_info);
        return decompressed_buffer->view();
      }
      return buffer_info.view;
    });

    auto reader = Reader(current_view.data(), current_view.size_bytes());
    auto info = FindSpecificPropertyAndBufferInfo(&reader, property);
    existed = info.property_size != 0;
    auto new_size = info.all_size - info.property_size + property_size;
    auto new_size_to_multiple_of_8 = ToMultipleOf8(new_size);

    if (new_size == 0) {
      // We don't have any data to encode anymore.
      FreeMemory(buffer_info);
      SetSizeData(buffer_, 0, nullptr);
      return !existed;
    }

    if (new_size_to_multiple_of_8 > current_view.size_bytes() ||
        new_size_to_multiple_of_8 <= current_view.size_bytes() * 2 / 3) {
      // We need to enlarge/shrink the buffer.
      auto new_buffer_info = SetupBuffer(buffer_, new_size);
      auto new_view = new_buffer_info.view;

      // Copy everything before the property to the new buffer.
      memmove(new_view.data(), current_view.data(), info.property_begin);
      // Copy everything after the property to the new buffer.
      memmove(new_view.data() + info.property_begin + property_size, current_view.data() + info.property_end,
              info.all_end - info.property_end);

      // Make buffer perminant
      if (new_buffer_info.storage_mode == StorageMode::BUFFER) {
        SetSizeData(buffer_, new_view.size_bytes(), new_view.data());
      }

      // Free the old buffers
      decompressed_buffer.reset();  // no longer needed, if it existed we have now copied from it
      FreeMemory(buffer_info);      // original buffer no longer needed
      buffer_info = new_buffer_info;
      current_view = new_buffer_info.view;

    } else if (property_size != info.property_size) {
      // We can keep the data in the same buffer, but the new property is
      // larger/smaller than the old property. We need to move the following
      // properties to the right/left.
      memmove(current_view.data() + info.property_begin + property_size, current_view.data() + info.property_end,
              info.all_end - info.property_end);
    }

    // If we still started with compressed buffer
    // take ownership of the decompressed buffer before writing
    if (buffer_info.storage_mode == StorageMode::COMPRESSED) {
      // remove compressed buffer
      FreeMemory(buffer_info);
      // take ownership of decompressed buffer
      decompressed_buffer->release();
      SetSizeData(buffer_, current_view.size_bytes(), current_view.data());
      decompressed_buffer.reset();
      buffer_info = DecodedBuffer{
          .view = current_view,
          .storage_mode = StorageMode::BUFFER,  // decompressed buffer is now a regular buffer
      };
    }

    if (!value.IsNull()) {
      // We need to encode the new value.
      Writer writer(current_view.data() + info.property_begin, property_size);
      MG_ASSERT(EncodeProperty(&writer, property, value), "Invalid database state!");
    }

    // We need to recreate the tombstone (if possible).
    Writer writer(current_view.data() + new_size, current_view.size_bytes() - new_size);
    auto metadata = writer.WriteMetadata();
    if (metadata) {
      metadata->Set({Type::EMPTY});
    }
  }

  if (FLAGS_storage_property_store_compression_enabled) {
    CompressBuffer(buffer_, buffer_info);
  }

  return !existed;
}

template <typename TContainer>
bool PropertyStore::DoInitProperties(const TContainer &properties) {
  auto orig_buffer_info = GetDecodedBuffer(buffer_);
  if (orig_buffer_info.storage_mode != StorageMode::EMPTY) {
    return false;
  }

  uint32_t property_size = 0;
  {
    Writer writer;
    for (const auto &[property, value] : properties) {
      if (value.IsNull()) {
        continue;
      }
      EncodeProperty(&writer, property, value);
      property_size = writer.Written();
    }
  }

  auto buffer_info = SetupBuffer(buffer_, property_size);
  auto view = buffer_info.view;

  // Encode the property into the data buffer.
  Writer writer(view.data(), view.size_bytes());

  for (const auto &[property, value] : properties) {
    if (value.IsNull()) {
      continue;
    }
    MG_ASSERT(EncodeProperty(&writer, property, value), "Invalid database state!");
    writer.Written();
  }

  auto metadata = writer.WriteMetadata();
  if (metadata) {
    // If there is any space left in the buffer we add a tombstone to
    // indicate that there are no more properties to be decoded.
    metadata->Set({Type::EMPTY});
  }

  // Make buffer perminant
  if (buffer_info.storage_mode == StorageMode::BUFFER) {
    SetSizeData(buffer_, view.size_bytes(), view.data());
  }

  if (FLAGS_storage_property_store_compression_enabled) {
    CompressBuffer(buffer_, buffer_info);
  }

  return true;
}

std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>> PropertyStore::UpdateProperties(
    std::map<PropertyId, PropertyValue> &properties) {
  auto old_properties = Properties();
  ClearProperties();

  std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>> id_old_new_change;
  id_old_new_change.reserve(properties.size() + old_properties.size());
  for (const auto &[prop_id, new_value] : properties) {
    if (!old_properties.contains(prop_id)) {
      id_old_new_change.emplace_back(prop_id, PropertyValue(), new_value);
    }
  }

  for (const auto &[old_key, old_value] : old_properties) {
    auto [it, inserted] = properties.emplace(old_key, old_value);
    if (!inserted) {
      auto &new_value = it->second;
      id_old_new_change.emplace_back(it->first, old_value, new_value);
    }
  }

  MG_ASSERT(InitProperties(properties));
  return id_old_new_change;
}

template bool PropertyStore::DoInitProperties<std::map<PropertyId, PropertyValue>>(
    const std::map<PropertyId, PropertyValue> &);
template bool PropertyStore::DoInitProperties<std::vector<std::pair<PropertyId, PropertyValue>>>(
    const std::vector<std::pair<PropertyId, PropertyValue>> &);

bool PropertyStore::InitProperties(const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  return DoInitProperties(properties);
}

bool PropertyStore::InitProperties(std::vector<std::pair<storage::PropertyId, storage::PropertyValue>> properties) {
  std::sort(properties.begin(), properties.end());

  return DoInitProperties(properties);
}

bool PropertyStore::ClearProperties() {
  auto buffer_info = GetDecodedBuffer(buffer_);

  if (buffer_info.storage_mode == StorageMode::EMPTY) return false;
  FreeMemory(buffer_info);
  SetSizeData(buffer_, 0, nullptr);

  return true;
}

std::string PropertyStore::StringBuffer() const {
  auto buffer_info = GetDecodedBuffer(buffer_);
  return {buffer_info.view.begin(), buffer_info.view.end()};
}

void PropertyStore::SetBuffer(const std::string_view buffer) {
  if (buffer.empty()) {
    return;
  }

  auto size = buffer.size();
  auto buffer_info = SetupBuffer(buffer_, size);
  auto view = buffer_info.view;

  for (uint i = 0; i < size; ++i) {
    view[i] = static_cast<uint8_t>(buffer[i]);
  }

  // Make buffer perminant
  if (buffer_info.storage_mode == StorageMode::BUFFER) {
    SetSizeData(buffer_, view.size_bytes(), view.data());
  }
}

std::vector<PropertyId> PropertyStore::PropertiesOfTypes(std::span<Type const> types) const {
  auto get_properties = [&](Reader &reader) {
    std::vector<PropertyId> props;
    while (true) {
      auto metadata = reader.ReadMetadata();
      if (!metadata || metadata->type == Type::EMPTY) break;

      auto property_id = reader.ReadUint(metadata->id_size);
      if (!property_id) break;

      if (utils::Contains(types, metadata->type)) {
        props.emplace_back(PropertyId::FromUint(*property_id));
      }

      if (!SkipPropertyValue(&reader, metadata->type, metadata->payload_size)) break;
    }
    return props;
  };
  return WithReader(get_properties);
}

std::optional<PropertyValue> PropertyStore::GetPropertyOfTypes(PropertyId property, std::span<Type const> types) const {
  auto get_properties = [&](Reader &reader) -> std::optional<PropertyValue> {
    PropertyValue value;
    while (true) {
      auto metadata = reader.ReadMetadata();
      if (!metadata || metadata->type == Type::EMPTY) {
        return std::nullopt;
      }

      auto property_id = reader.ReadUint(metadata->id_size);
      if (!property_id) {
        return std::nullopt;
      }

      // found property
      if (*property_id == property.AsUint()) {
        // check its the type we are looking for
        if (!utils::Contains(types, metadata->type)) {
          return std::nullopt;
        }
        if (!DecodePropertyValue(&reader, metadata->type, metadata->payload_size, value)) {
          return std::nullopt;
        }

        return value;
      }
      // Don't load the value if this isn't the expected property.
      if (!SkipPropertyValue(&reader, metadata->type, metadata->payload_size)) {
        return std::nullopt;
      }
      if (*property_id > property.AsUint()) return std::nullopt;
    }
    return std::nullopt;
  };

  return WithReader(get_properties);
}

auto PropertyStore::PropertiesMatchTypes(TypeConstraintsValidator const &constraint) const
    -> std::optional<PropertyStoreConstraintViolation> {
  if (constraint.empty()) return std::nullopt;

  auto property_matches_types = [&](Reader &reader) -> std::optional<PropertyStoreConstraintViolation> {
    while (true) {
      auto res = NextPropertyAndType(&reader);
      if (!res) return std::nullopt;  // No more properties to read

      if (auto violation = constraint.validate(*res); violation) {
        return violation;
      }
    }
  };
  return WithReader(property_matches_types);
}

}  // namespace memgraph::storage
