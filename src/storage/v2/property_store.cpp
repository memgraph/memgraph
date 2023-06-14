// Copyright 2023 Memgraph Ltd.
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

#include <cstring>
#include <limits>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "storage/v2/temporal.hpp"
#include "utils/cast.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

namespace {

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
// When encoding integers (`int64_t` and `uint64_t`) they are compressed so that
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

// All of these values must have the lowest 4 bits set to zero because they are
// used to store two `Size` values as described in the comment above.
enum class Type : uint8_t {
  EMPTY = 0x00,  // Special value used to indicate end of buffer.
  NONE = 0x10,   // NONE used instead of NULL because NULL is defined to
                 // something...
  BOOL = 0x20,
  INT = 0x30,
  DOUBLE = 0x40,
  STRING = 0x50,
  LIST = 0x60,
  MAP = 0x70,
  TEMPORAL_DATA = 0x80
};

const uint8_t kMaskType = 0xf0;
const uint8_t kMaskIdSize = 0x0c;
const uint8_t kMaskPayloadSize = 0x03;
const uint8_t kShiftIdSize = 2;

// Values are encoded as follows:
//   * NULL
//     - type; payload size is not used
//   * BOOL
//     - type; payload size is used as value
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
//       encoded as `uint8_t`, `uint16_t`, `uint32_t` or `uint64_t`
//     - encoded property ID
//     - encoded string size
//     - string data
//   * LIST
//     - type; payload size is used to indicate whether the list size is encoded
//       as `uint8_t`, `uint16_t`, `uint32_t` or `uint64_t`
//     - encoded property ID
//     - encoded list size
//     - list items
//       + type; id size is not used; payload size is used to indicate the size
//         of the item
//       + encoded item size
//       + encoded item data
//   * MAP
//     - type; payload size is used to indicate whether the map size is encoded
//       as `uint8_t`, `uint16_t`, `uint32_t` or `uint64_t`
//     - encoded property ID
//     - encoded map size
//     - map items
//       + type; id size is used to indicate whether the key size is encoded as
//         `uint8_t`, `uint16_t`, `uint32_t` or `uint64_t`; payload size is used
//         as described above for the inner payload type
//       + encoded key size
//       + encoded key data
//       + encoded value size
//       + encoded value data
//   * TEMPORAL_DATE
//     - type; payload size isn't used
//     - encoded property ID
//     - value saved as Metadata
//       + type; id size is used to indicate whether the temporal data type is encoded
//         as `uint8_t`, `uint16_t`, `uint32_t` or `uint64_t`; payload size used to
//         indicate whether the microseconds are encoded as `uint8_t`, `uint16_t, `uint32_t
//         or `uint64_t`
//       + encoded temporal data type value
//       + encoded microseconds value

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
    MetadataHandle() {}

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

  Writer() {}

  Writer(uint8_t *data, uint64_t size) : data_(data), size_(size) {}

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

  bool WriteBytes(const uint8_t *data, uint64_t size) {
    if (data_ && pos_ + size > size_) return false;
    if (data_) memcpy(data_ + pos_, data, size);
    pos_ += size;
    return true;
  }

  bool WriteBytes(const char *data, uint64_t size) {
    static_assert(std::is_same_v<uint8_t, unsigned char>);
    return WriteBytes(reinterpret_cast<const uint8_t *>(data), size);
  }

  uint64_t Written() const { return pos_; }

 private:
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

  uint8_t *data_{nullptr};
  uint64_t size_{0};
  uint64_t pos_{0};
};

// Helper class used to read data from the binary stream.
class Reader {
 public:
  Reader(const uint8_t *data, uint64_t size) : data_(data), size_(size), pos_(0) {}

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

  std::optional<int64_t> ReadUint(Size size) {
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

  bool ReadBytes(uint8_t *data, uint64_t size) {
    if (pos_ + size > size_) return false;
    memcpy(data, data_ + pos_, size);
    pos_ += size;
    return true;
  }

  bool ReadBytes(char *data, uint64_t size) { return ReadBytes(reinterpret_cast<uint8_t *>(data), size); }

  bool VerifyBytes(const uint8_t *data, uint64_t size) {
    if (pos_ + size > size_) return false;
    if (memcmp(data, data_ + pos_, size) != 0) return false;
    pos_ += size;
    return true;
  }

  bool VerifyBytes(const char *data, uint64_t size) {
    return VerifyBytes(reinterpret_cast<const uint8_t *>(data), size);
  }

  bool SkipBytes(uint64_t size) {
    if (pos_ + size > size_) return false;
    pos_ += size;
    return true;
  }

  uint64_t GetPosition() const { return pos_; }

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
  uint64_t size_;
  uint64_t pos_;
};

// Function used to encode a PropertyValue into a byte stream.
std::optional<std::pair<Type, Size>> EncodePropertyValue(Writer *writer, const PropertyValue &value) {
  switch (value.type()) {
    case PropertyValue::Type::Null:
      return {{Type::NONE, Size::INT8}};
    case PropertyValue::Type::Bool: {
      if (value.ValueBool()) {
        return {{Type::BOOL, Size::INT64}};
      } else {
        return {{Type::BOOL, Size::INT8}};
      }
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
    case PropertyValue::Type::Map: {
      const auto &map = value.ValueMap();
      auto size = writer->WriteUint(map.size());
      if (!size) return std::nullopt;
      for (const auto &item : map) {
        auto metadata = writer->WriteMetadata();
        if (!metadata) return std::nullopt;
        auto key_size = writer->WriteUint(item.first.size());
        if (!key_size) return std::nullopt;
        if (!writer->WriteBytes(item.first.data(), item.first.size())) return std::nullopt;
        auto ret = EncodePropertyValue(writer, item.second);
        if (!ret) return std::nullopt;
        metadata->Set({ret->first, *key_size, ret->second});
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

}  // namespace

// Function used to decode a PropertyValue from a byte stream. It can either
// decode or skip the encoded PropertyValue, depending on the supplied value
// pointer.
//
// @sa ComparePropertyValue
[[nodiscard]] bool DecodePropertyValue(Reader *reader, Type type, Size payload_size, PropertyValue *value) {
  switch (type) {
    case Type::EMPTY: {
      return false;
    }
    case Type::NONE: {
      if (value) {
        *value = PropertyValue();
      }
      return true;
    }
    case Type::BOOL: {
      if (value) {
        if (payload_size == Size::INT64) {
          *value = PropertyValue(true);
        } else {
          *value = PropertyValue(false);
        }
      }
      return true;
    }
    case Type::INT: {
      auto int_v = reader->ReadInt(payload_size);
      if (!int_v) return false;
      if (value) {
        *value = PropertyValue(*int_v);
      }
      return true;
    }
    case Type::DOUBLE: {
      auto double_v = reader->ReadDouble(payload_size);
      if (!double_v) return false;
      if (value) {
        *value = PropertyValue(*double_v);
      }
      return true;
    }
    case Type::STRING: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;
      if (value) {
        std::string str_v(*size, '\0');
        if (!reader->ReadBytes(str_v.data(), *size)) return false;
        *value = PropertyValue(std::move(str_v));
      } else {
        if (!reader->SkipBytes(*size)) return false;
      }
      return true;
    }
    case Type::LIST: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;
      if (value) {
        std::vector<PropertyValue> list;
        list.reserve(*size);
        for (uint64_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return false;
          PropertyValue item;
          if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, &item)) return false;
          list.emplace_back(std::move(item));
        }
        *value = PropertyValue(std::move(list));
      } else {
        for (uint64_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return false;
          if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, nullptr)) return false;
        }
      }
      return true;
    }
    case Type::MAP: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;
      if (value) {
        std::map<std::string, PropertyValue> map;
        for (uint64_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return false;
          auto key_size = reader->ReadUint(metadata->id_size);
          if (!key_size) return false;
          std::string key(*key_size, '\0');
          if (!reader->ReadBytes(key.data(), *key_size)) return false;
          PropertyValue item;
          if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, &item)) return false;
          map.emplace(std::move(key), std::move(item));
        }
        *value = PropertyValue(std::move(map));
      } else {
        for (uint64_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return false;
          auto key_size = reader->ReadUint(metadata->id_size);
          if (!key_size) return false;
          if (!reader->SkipBytes(*key_size)) return false;
          if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, nullptr)) return false;
        }
      }
      return true;
    }

    case Type::TEMPORAL_DATA: {
      const auto maybe_temporal_data = DecodeTemporalData(*reader);

      if (!maybe_temporal_data) return false;

      if (value) {
        *value = PropertyValue(*maybe_temporal_data);
      }

      return true;
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
      if (!value.IsList()) return false;
      const auto &list = value.ValueList();
      auto size = reader->ReadUint(payload_size);
      if (!size) return false;
      if (*size != list.size()) return false;
      for (uint64_t i = 0; i < *size; ++i) {
        auto metadata = reader->ReadMetadata();
        if (!metadata) return false;
        if (!ComparePropertyValue(reader, metadata->type, metadata->payload_size, list[i])) return false;
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
        auto key_size = reader->ReadUint(metadata->id_size);
        if (!key_size) return false;
        if (*key_size != item.first.size()) return false;
        if (!reader->VerifyBytes(item.first.data(), *key_size)) return false;
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
enum class DecodeExpectedPropertyStatus {
  MISSING_DATA,
  SMALLER,
  EQUAL,
  GREATER,
};

// Function used to decode a property (PropertyId, PropertyValue) from a byte
// stream. It can either decode or skip the encoded PropertyValue, depending on
// the provided PropertyValue. The `expected_property` provides another hint
// whether the property should be decoded or skipped.
//
// @return MISSING_DATA when there is not enough data in the buffer to decode
//                      the property
// @return SMALLER when the property that was currently read has a smaller
//                 property ID than the expected property; the value isn't
//                 loaded in this case
// @return EQUAL when the property that was currently read has an ID equal to
//               the expected property ID; the value is loaded in this case
// @return GREATER when the property that was currently read has a greater
//                 property ID than the expected property; the value isn't
//                 loaded in this case
//
// @sa DecodeAnyProperty
// @sa CompareExpectedProperty
[[nodiscard]] DecodeExpectedPropertyStatus DecodeExpectedProperty(Reader *reader, PropertyId expected_property,
                                                                  PropertyValue *value) {
  auto metadata = reader->ReadMetadata();
  if (!metadata) return DecodeExpectedPropertyStatus::MISSING_DATA;

  auto property_id = reader->ReadUint(metadata->id_size);
  if (!property_id) return DecodeExpectedPropertyStatus::MISSING_DATA;

  // Don't load the value if this isn't the expected property.
  if (*property_id != expected_property.AsUint()) {
    value = nullptr;
  }

  if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, value))
    return DecodeExpectedPropertyStatus::MISSING_DATA;

  if (*property_id < expected_property.AsUint()) {
    return DecodeExpectedPropertyStatus::SMALLER;
  } else if (*property_id == expected_property.AsUint()) {
    return DecodeExpectedPropertyStatus::EQUAL;
  } else {
    return DecodeExpectedPropertyStatus::GREATER;
  }
}

// Function used to decode a property (PropertyId, PropertyValue) from a byte
// stream. It can either decode or skip the encoded PropertyValue, depending on
// the provided PropertyValue.
//
// @sa DecodeExpectedProperty
// @sa CompareExpectedProperty
[[nodiscard]] std::optional<PropertyId> DecodeAnyProperty(Reader *reader, PropertyValue *value) {
  auto metadata = reader->ReadMetadata();
  if (!metadata) return std::nullopt;

  auto property_id = reader->ReadUint(metadata->id_size);
  if (!property_id) return std::nullopt;

  if (!DecodePropertyValue(reader, metadata->type, metadata->payload_size, value)) return std::nullopt;

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
[[nodiscard]] DecodeExpectedPropertyStatus FindSpecificProperty(Reader *reader, PropertyId property,
                                                                PropertyValue *value) {
  while (true) {
    auto ret = DecodeExpectedProperty(reader, property, value);
    // Because the properties are sorted in the buffer, we only need to
    // continue searching for the property while this function returns a
    // `SMALLER` value indicating that the ID of the found property is smaller
    // than the seeked ID. All other return values (`MISSING_DATA`, `EQUAL` and
    // `GREATER`) terminate the search.
    if (ret != DecodeExpectedPropertyStatus::SMALLER) {
      return ret;
    }
  }
}

// Struct used to return info about the property position and buffer size.
struct SpecificPropertyAndBufferInfo {
  uint64_t property_begin;
  uint64_t property_end;
  uint64_t property_size;
  uint64_t all_begin;
  uint64_t all_end;
  uint64_t all_size;
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
  uint64_t property_begin = reader->GetPosition();
  uint64_t property_end = reader->GetPosition();
  uint64_t all_begin = reader->GetPosition();
  uint64_t all_end = reader->GetPosition();
  while (true) {
    auto ret = DecodeExpectedProperty(reader, property, nullptr);
    if (ret == DecodeExpectedPropertyStatus::MISSING_DATA) {
      break;
    } else if (ret == DecodeExpectedPropertyStatus::SMALLER) {
      property_begin = reader->GetPosition();
      property_end = reader->GetPosition();
    } else if (ret == DecodeExpectedPropertyStatus::EQUAL) {
      property_end = reader->GetPosition();
    }
    all_end = reader->GetPosition();
  }
  return {property_begin, property_end, property_end - property_begin, all_begin, all_end, all_end - all_begin};
}

// All data buffers will be allocated to a power of 8 size.
uint64_t ToPowerOf8(uint64_t size) {
  uint64_t mod = size % 8;
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

const uint8_t kUseLocalBuffer = 0x01;

// Helper functions used to retrieve/store `size` and `data` from/into the
// `buffer_`.

std::pair<uint64_t, uint8_t *> GetSizeData(const uint8_t *buffer) {
  uint64_t size;
  uint8_t *data;
  memcpy(&size, buffer, sizeof(uint64_t));
  memcpy(&data, buffer + sizeof(uint64_t), sizeof(uint8_t *));
  return {size, data};
}

void SetSizeData(uint8_t *buffer, uint64_t size, uint8_t *data) {
  memcpy(buffer, &size, sizeof(uint64_t));
  memcpy(buffer + sizeof(uint64_t), &data, sizeof(uint8_t *));
}

}  // namespace

PropertyStore::PropertyStore() { memset(buffer_, 0, sizeof(buffer_)); }

PropertyStore::PropertyStore(PropertyStore &&other) noexcept {
  memcpy(buffer_, other.buffer_, sizeof(buffer_));
  memset(other.buffer_, 0, sizeof(other.buffer_));
}

PropertyStore &PropertyStore::operator=(PropertyStore &&other) noexcept {
  uint64_t size;
  uint8_t *data;
  std::tie(size, data) = GetSizeData(buffer_);
  if (size % 8 == 0) {
    // We are storing the data in an external buffer.
    delete[] data;
  }

  memcpy(buffer_, other.buffer_, sizeof(buffer_));
  memset(other.buffer_, 0, sizeof(other.buffer_));

  return *this;
}

PropertyStore::~PropertyStore() {
  uint64_t size;
  uint8_t *data;
  std::tie(size, data) = GetSizeData(buffer_);
  if (size % 8 == 0) {
    // We are storing the data in an external buffer.
    delete[] data;
  }
}

PropertyValue PropertyStore::GetProperty(PropertyId property) const {
  uint64_t size;
  const uint8_t *data;
  std::tie(size, data) = GetSizeData(buffer_);
  if (size % 8 != 0) {
    // We are storing the data in the local buffer.
    size = sizeof(buffer_) - 1;
    data = &buffer_[1];
  }
  Reader reader(data, size);
  PropertyValue value;
  if (FindSpecificProperty(&reader, property, &value) != DecodeExpectedPropertyStatus::EQUAL) return PropertyValue();
  return value;
}

bool PropertyStore::HasProperty(PropertyId property) const {
  uint64_t size;
  const uint8_t *data;
  std::tie(size, data) = GetSizeData(buffer_);
  if (size % 8 != 0) {
    // We are storing the data in the local buffer.
    size = sizeof(buffer_) - 1;
    data = &buffer_[1];
  }
  Reader reader(data, size);
  return FindSpecificProperty(&reader, property, nullptr) == DecodeExpectedPropertyStatus::EQUAL;
}

bool PropertyStore::IsPropertyEqual(PropertyId property, const PropertyValue &value) const {
  uint64_t size;
  const uint8_t *data;
  std::tie(size, data) = GetSizeData(buffer_);
  if (size % 8 != 0) {
    // We are storing the data in the local buffer.
    size = sizeof(buffer_) - 1;
    data = &buffer_[1];
  }
  Reader reader(data, size);
  auto info = FindSpecificPropertyAndBufferInfo(&reader, property);
  if (info.property_size == 0) return value.IsNull();
  Reader prop_reader(data + info.property_begin, info.property_size);
  if (!CompareExpectedProperty(&prop_reader, property, value)) return false;
  return prop_reader.GetPosition() == info.property_size;
}

std::map<PropertyId, PropertyValue> PropertyStore::Properties() const {
  uint64_t size;
  const uint8_t *data;
  std::tie(size, data) = GetSizeData(buffer_);
  if (size % 8 != 0) {
    // We are storing the data in the local buffer.
    size = sizeof(buffer_) - 1;
    data = &buffer_[1];
  }
  Reader reader(data, size);
  std::map<PropertyId, PropertyValue> props;
  while (true) {
    PropertyValue value;
    auto prop = DecodeAnyProperty(&reader, &value);
    if (!prop) break;
    props.emplace(*prop, std::move(value));
  }
  return props;
}

bool PropertyStore::SetProperty(PropertyId property, const PropertyValue &value) {
  uint64_t property_size = 0;
  if (!value.IsNull()) {
    Writer writer;
    EncodeProperty(&writer, property, value);
    property_size = writer.Written();
  }

  bool in_local_buffer = false;
  uint64_t size;
  uint8_t *data;
  std::tie(size, data) = GetSizeData(buffer_);
  if (size % 8 != 0) {
    // We are storing the data in the local buffer.
    size = sizeof(buffer_) - 1;
    data = &buffer_[1];
    in_local_buffer = true;
  }

  bool existed = false;
  if (!size) {
    if (!value.IsNull()) {
      // We don't have a data buffer. Allocate a new one.
      auto property_size_to_power_of_8 = ToPowerOf8(property_size);
      if (property_size <= sizeof(buffer_) - 1) {
        // Use the local buffer.
        buffer_[0] = kUseLocalBuffer;
        size = sizeof(buffer_) - 1;
        data = &buffer_[1];
        in_local_buffer = true;
      } else {
        // Allocate a new external buffer.
        auto *alloc_data = new uint8_t[property_size_to_power_of_8];
        auto alloc_size = property_size_to_power_of_8;

        SetSizeData(buffer_, alloc_size, alloc_data);

        size = alloc_size;
        data = alloc_data;
        in_local_buffer = false;
      }

      // Encode the property into the data buffer.
      Writer writer(data, size);
      MG_ASSERT(EncodeProperty(&writer, property, value), "Invalid database state!");
      auto metadata = writer.WriteMetadata();
      if (metadata) {
        // If there is any space left in the buffer we add a tombstone to
        // indicate that there are no more properties to be decoded.
        metadata->Set({Type::EMPTY});
      }
    } else {
      // We don't have to do anything. We don't have a buffer and we are trying
      // to set a property to `Null` (we are trying to remove the property).
    }
  } else {
    Reader reader(data, size);
    auto info = FindSpecificPropertyAndBufferInfo(&reader, property);
    existed = info.property_size != 0;
    auto new_size = info.all_size - info.property_size + property_size;
    auto new_size_to_power_of_8 = ToPowerOf8(new_size);
    if (new_size_to_power_of_8 == 0) {
      // We don't have any data to encode anymore.
      if (!in_local_buffer) delete[] data;
      SetSizeData(buffer_, 0, nullptr);
      data = nullptr;
      size = 0;
    } else if (new_size_to_power_of_8 > size || new_size_to_power_of_8 <= size * 2 / 3) {
      // We need to enlarge/shrink the buffer.
      bool current_in_local_buffer = false;
      uint8_t *current_data = nullptr;
      uint64_t current_size = 0;
      if (new_size <= sizeof(buffer_) - 1) {
        // Use the local buffer.
        buffer_[0] = kUseLocalBuffer;
        current_size = sizeof(buffer_) - 1;
        current_data = &buffer_[1];
        current_in_local_buffer = true;
      } else {
        // Allocate a new external buffer.
        current_data = new uint8_t[new_size_to_power_of_8];
        current_size = new_size_to_power_of_8;
        current_in_local_buffer = false;
      }
      // Copy everything before the property to the new buffer.
      memmove(current_data, data, info.property_begin);
      // Copy everything after the property to the new buffer.
      memmove(current_data + info.property_begin + property_size, data + info.property_end,
              info.all_end - info.property_end);
      // Free the old buffer.
      if (!in_local_buffer) delete[] data;
      // Permanently remember the new buffer.
      if (!current_in_local_buffer) {
        SetSizeData(buffer_, current_size, current_data);
      }
      // Set the proxy variables.
      data = current_data;
      size = current_size;
      in_local_buffer = current_in_local_buffer;
    } else if (property_size != info.property_size) {
      // We can keep the data in the same buffer, but the new property is
      // larger/smaller than the old property. We need to move the following
      // properties to the right/left.
      memmove(data + info.property_begin + property_size, data + info.property_end, info.all_end - info.property_end);
    }

    if (!value.IsNull()) {
      // We need to encode the new value.
      Writer writer(data + info.property_begin, property_size);
      MG_ASSERT(EncodeProperty(&writer, property, value), "Invalid database state!");
    }

    // We need to recreate the tombstone (if possible).
    Writer writer(data + new_size, size - new_size);
    auto metadata = writer.WriteMetadata();
    if (metadata) {
      metadata->Set({Type::EMPTY});
    }
  }

  return !existed;
}

template <typename TContainer>
bool PropertyStore::DoInitProperties(const TContainer &properties) {
  uint64_t size = 0;
  uint8_t *data = nullptr;
  std::tie(size, data) = GetSizeData(buffer_);
  if (size != 0) {
    return false;
  }

  uint64_t property_size = 0;
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

  auto property_size_to_power_of_8 = ToPowerOf8(property_size);
  if (property_size <= sizeof(buffer_) - 1) {
    // Use the local buffer.
    buffer_[0] = kUseLocalBuffer;
    size = sizeof(buffer_) - 1;
    data = &buffer_[1];
  } else {
    // Allocate a new external buffer.
    auto *alloc_data = new uint8_t[property_size_to_power_of_8];
    auto alloc_size = property_size_to_power_of_8;

    SetSizeData(buffer_, alloc_size, alloc_data);

    size = alloc_size;
    data = alloc_data;
  }

  // Encode the property into the data buffer.
  Writer writer(data, size);

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

  return true;
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
  bool in_local_buffer = false;
  uint64_t size;
  uint8_t *data;
  std::tie(size, data) = GetSizeData(buffer_);
  if (size % 8 != 0) {
    // We are storing the data in the local buffer.
    size = sizeof(buffer_) - 1;
    data = &buffer_[1];
    in_local_buffer = true;
  }
  if (!size) return false;
  if (!in_local_buffer) delete[] data;
  SetSizeData(buffer_, 0, nullptr);
  return true;
}

}  // namespace memgraph::storage
