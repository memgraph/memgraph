#include "storage/v2/property_store.hpp"

#include <cstring>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

#include <glog/logging.h>

#include "utils/cast.hpp"

namespace storage {

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
// stores a list or a map of `PropertyValues` because each of the internal
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
      value |= static_cast<uint8_t>(static_cast<uint8_t>(metadata.id_size)
                                    << kShiftIdSize);
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

  std::optional<Size> WriteDouble(double value) {
    return WriteUint(utils::MemcpyCast<uint64_t>(value));
  }

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
    static_assert(std::numeric_limits<T>::is_signed ==
                  std::numeric_limits<V>::is_signed);
    if (value < std::numeric_limits<T>::min() ||
        value > std::numeric_limits<T>::max())
      return false;
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
  Reader(const uint8_t *data, uint64_t size)
      : data_(data), size_(size), pos_(0) {}

  std::optional<Metadata> ReadMetadata() {
    if (pos_ + 1 > size_) return std::nullopt;
    uint8_t value = data_[pos_++];
    Metadata metadata;
    metadata.type = static_cast<Type>(value & kMaskType);
    metadata.id_size = static_cast<Size>(
        static_cast<uint8_t>(value & kMaskIdSize) >> kShiftIdSize);
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

  bool ReadBytes(char *data, uint64_t size) {
    return ReadBytes(reinterpret_cast<uint8_t *>(data), size);
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
std::optional<std::pair<Type, Size>> EncodePropertyValue(
    Writer *writer, const PropertyValue &value) {
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
        if (!writer->WriteBytes(item.first.data(), item.first.size()))
          return std::nullopt;
        auto ret = EncodePropertyValue(writer, item.second);
        if (!ret) return std::nullopt;
        metadata->Set({ret->first, *key_size, ret->second});
      }
      return {{Type::MAP, *size}};
    }
  }
}

// Function used to decode a PropertyValue from a byte stream. It can either
// decode or skip the encoded PropertyValue, depending on the supplied template
// parameter.
template <bool read_data>
std::optional<PropertyValue> DecodePropertyValue(Reader *reader, Type type,
                                                 Size payload_size) {
  switch (type) {
    case Type::EMPTY:
      return std::nullopt;
    case Type::NONE:
      return PropertyValue();
    case Type::BOOL: {
      if (payload_size == Size::INT64) {
        return PropertyValue(true);
      } else {
        return PropertyValue(false);
      }
    }
    case Type::INT: {
      auto value = reader->ReadInt(payload_size);
      if (!value) return std::nullopt;
      return PropertyValue(*value);
    }
    case Type::DOUBLE: {
      auto value = reader->ReadDouble(payload_size);
      if (!value) return std::nullopt;
      return PropertyValue(*value);
    }
    case Type::STRING: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return std::nullopt;
      if constexpr (read_data) {
        std::string value(*size, '\0');
        if (!reader->ReadBytes(value.data(), *size)) return std::nullopt;
        return PropertyValue(std::move(value));
      } else {
        if (!reader->SkipBytes(*size)) return std::nullopt;
        return PropertyValue();
      }
    }
    case Type::LIST: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return std::nullopt;
      if constexpr (read_data) {
        std::vector<PropertyValue> list;
        list.reserve(*size);
        for (uint64_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return std::nullopt;
          auto ret = DecodePropertyValue<read_data>(reader, metadata->type,
                                                    metadata->payload_size);
          if (!ret) return std::nullopt;
          list.emplace_back(std::move(*ret));
        }
        return PropertyValue(std::move(list));
      } else {
        for (uint64_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return std::nullopt;
          auto ret = DecodePropertyValue<read_data>(reader, metadata->type,
                                                    metadata->payload_size);
          if (!ret) return std::nullopt;
        }
        return PropertyValue();
      }
    }
    case Type::MAP: {
      auto size = reader->ReadUint(payload_size);
      if (!size) return std::nullopt;
      if constexpr (read_data) {
        std::map<std::string, PropertyValue> map;
        for (uint64_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return std::nullopt;
          auto key_size = reader->ReadUint(metadata->id_size);
          if (!key_size) return std::nullopt;
          std::string key(*key_size, '\0');
          if (!reader->ReadBytes(key.data(), *key_size)) return std::nullopt;
          auto ret = DecodePropertyValue<read_data>(reader, metadata->type,
                                                    metadata->payload_size);
          if (!ret) return std::nullopt;
          map.emplace(std::move(key), std::move(*ret));
        }
        return PropertyValue(std::move(map));
      } else {
        for (uint64_t i = 0; i < *size; ++i) {
          auto metadata = reader->ReadMetadata();
          if (!metadata) return std::nullopt;
          auto key_size = reader->ReadUint(metadata->id_size);
          if (!key_size) return std::nullopt;
          if (!reader->SkipBytes(*key_size)) return std::nullopt;
          auto ret = DecodePropertyValue<read_data>(reader, metadata->type,
                                                    metadata->payload_size);
          if (!ret) return std::nullopt;
        }
        return PropertyValue();
      }
    }
  }
}

// Function used to encode a property (PropertyId, PropertyValue) into a byte
// stream.
bool EncodeProperty(Writer *writer, PropertyId property,
                    const PropertyValue &value) {
  auto metadata = writer->WriteMetadata();
  if (!metadata) return false;

  auto id_size = writer->WriteUint(property.AsUint());
  if (!id_size) return false;

  auto type_property_size = EncodePropertyValue(writer, value);
  if (!type_property_size) return false;

  metadata->Set(
      {type_property_size->first, *id_size, type_property_size->second});
  return true;
}

// Function used to decode a property (PropertyId, PropertyValue) from a byte
// stream. It can either decode or skip the encoded PropertyValue, depending on
// the supplied template parameter.
template <bool read_data>
std::optional<std::pair<PropertyId, PropertyValue>> DecodeSkipProperty(
    Reader *reader) {
  auto metadata = reader->ReadMetadata();
  if (!metadata) return std::nullopt;

  auto property = reader->ReadUint(metadata->id_size);
  if (!property) return std::nullopt;

  auto value = DecodePropertyValue<read_data>(reader, metadata->type,
                                              metadata->payload_size);
  if (!value) return std::nullopt;

  return {{PropertyId::FromUint(*property), std::move(*value)}};
}

// Helper function used to decode a property.
std::optional<std::pair<PropertyId, PropertyValue>> DecodeProperty(
    Reader *reader) {
  return DecodeSkipProperty<true>(reader);
}

// Helper function used to skip a property.
std::optional<PropertyId> SkipProperty(Reader *reader) {
  auto ret = DecodeSkipProperty<false>(reader);
  if (!ret) return std::nullopt;
  return ret->first;
}

// Struct used to return info about the property buffer.
struct PropertyBufferInfo {
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
PropertyBufferInfo FindProperty(Reader *reader, PropertyId property) {
  uint64_t property_begin = reader->GetPosition();
  uint64_t property_end = reader->GetPosition();
  uint64_t all_begin = reader->GetPosition();
  uint64_t all_end = reader->GetPosition();
  while (true) {
    auto ret = SkipProperty(reader);
    if (!ret) break;
    if (*ret < property) {
      property_begin = reader->GetPosition();
      property_end = reader->GetPosition();
    } else if (*ret == property) {
      property_end = reader->GetPosition();
    }
    all_end = reader->GetPosition();
  }
  return {property_begin, property_end, property_end - property_begin,
          all_begin,      all_end,      all_end - all_begin};
}

// All data buffers will be allocated to a power of 8 size.
uint64_t ToPowerOf8(uint64_t size) {
  uint64_t mod = size % 8;
  if (mod == 0) return size;
  return size - mod + 8;
}

}  // namespace

PropertyStore::PropertyStore() {}

PropertyStore::PropertyStore(PropertyStore &&other) noexcept
    : data_(other.data_), size_(other.size_) {
  other.data_ = nullptr;
  other.size_ = 0;
}

PropertyStore &PropertyStore::operator=(PropertyStore &&other) noexcept {
  delete[] data_;

  data_ = other.data_;
  size_ = other.size_;

  other.data_ = nullptr;
  other.size_ = 0;

  return *this;
}

PropertyStore::~PropertyStore() {
  delete[] data_;
  size_ = 0;
}

PropertyValue PropertyStore::GetProperty(PropertyId property) const {
  Reader reader(data_, size_);
  auto info = FindProperty(&reader, property);
  if (info.property_size == 0) return PropertyValue();
  Reader prop_reader(data_ + info.property_begin, info.property_size);
  auto prop = DecodeProperty(&prop_reader);
  CHECK(prop) << "Invalid database state!";
  CHECK(prop->first == property) << "Invalid database state!";
  return std::move(prop->second);
}

bool PropertyStore::HasProperty(PropertyId property) const {
  Reader reader(data_, size_);
  auto info = FindProperty(&reader, property);
  return info.property_size != 0;
}

std::map<PropertyId, PropertyValue> PropertyStore::Properties() const {
  Reader reader(data_, size_);
  std::map<PropertyId, PropertyValue> props;
  while (true) {
    auto ret = DecodeProperty(&reader);
    if (!ret) break;
    props.emplace(ret->first, std::move(ret->second));
  }
  return props;
}

bool PropertyStore::SetProperty(PropertyId property,
                                const PropertyValue &value) {
  uint64_t property_size = 0;
  if (!value.IsNull()) {
    Writer writer;
    EncodeProperty(&writer, property, value);
    property_size = writer.Written();
  }

  bool existed = false;
  if (!data_) {
    if (!value.IsNull()) {
      // We don't have a data buffer. Allocate a new one.
      auto size = ToPowerOf8(property_size);
      data_ = new uint8_t[size];
      size_ = size;

      // Encode the property into the data buffer.
      Writer writer(data_, size_);
      CHECK(EncodeProperty(&writer, property, value))
          << "Invalid database state!";
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
    Reader reader(data_, size_);
    auto info = FindProperty(&reader, property);
    existed = info.property_size != 0;
    auto new_size = info.all_size - info.property_size + property_size;
    auto new_size_to_power_of_8 = ToPowerOf8(new_size);
    if (new_size_to_power_of_8 == 0) {
      // We don't have any data to encode anymore.
      delete[] data_;
      data_ = nullptr;
      size_ = 0;
    } else if (new_size_to_power_of_8 > size_ ||
               new_size_to_power_of_8 <= size_ * 2 / 3) {
      // We need to enlarge/shrink the buffer.
      auto buffer = new uint8_t[new_size_to_power_of_8];
      // Copy everything before the property to the new buffer.
      memcpy(buffer, data_, info.property_begin);
      // Copy everything after the property to the new buffer.
      memcpy(buffer + info.property_begin + property_size,
             data_ + info.property_end, info.all_end - info.property_end);
      // Replace the current buffer with the new buffer.
      delete[] data_;
      data_ = buffer;
      size_ = new_size_to_power_of_8;
    } else if (property_size != info.property_size) {
      // We can keep the data in the same buffer, but the new property is
      // larger/smaller than the old property. We need to move the following
      // properties to the right/left.
      memmove(data_ + info.property_begin + property_size,
              data_ + info.property_end, info.all_end - info.property_end);
    }

    if (!value.IsNull()) {
      // We need to encode the new value.
      Writer writer(data_ + info.property_begin, property_size);
      CHECK(EncodeProperty(&writer, property, value))
          << "Invalid database state!";
    }

    // We need to recreate the tombstone (if possible).
    Writer writer(data_ + new_size, size_ - new_size);
    auto metadata = writer.WriteMetadata();
    if (metadata) {
      metadata->Set({Type::EMPTY});
    }
  }

  return !existed;
}

bool PropertyStore::ClearProperties() {
  if (!data_) return false;
  delete[] data_;
  data_ = nullptr;
  size_ = 0;
  return true;
}

}  // namespace storage
