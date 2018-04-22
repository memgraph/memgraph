#pragma once

#include <string>

#include "communication/bolt/v1/codes.hpp"
#include "storage/property_value.hpp"
#include "utils/bswap.hpp"

namespace communication::bolt {

/**
 * Bolt PrimitiveEncoder. Has public interfaces for writing Bolt encoded data.
 * Supported types are: Null, Bool, Int, Double, String and PropertyValue.
 *
 * Bolt encoding is used both for streaming data to network clients and for
 * database durability.
 *
 * @tparam Buffer the output buffer that should be used
 */
template <typename Buffer>
class PrimitiveEncoder {
 public:
  explicit PrimitiveEncoder(Buffer &buffer) : buffer_(buffer) {}

  void WriteRAW(const uint8_t *data, uint64_t len) { buffer_.Write(data, len); }

  void WriteRAW(const char *data, uint64_t len) {
    WriteRAW((const uint8_t *)data, len);
  }

  void WriteRAW(const uint8_t data) { WriteRAW(&data, 1); }

  template <class T>
  void WriteValue(T value) {
    value = utils::Bswap(value);
    WriteRAW(reinterpret_cast<const uint8_t *>(&value), sizeof(value));
  }

  void WriteNull() { WriteRAW(utils::UnderlyingCast(Marker::Null)); }

  void WriteBool(const bool &value) {
    if (value)
      WriteRAW(utils::UnderlyingCast(Marker::True));
    else
      WriteRAW(utils::UnderlyingCast(Marker::False));
  }

  void WriteInt(const int64_t &value) {
    if (value >= -16L && value < 128L) {
      WriteRAW(static_cast<uint8_t>(value));
    } else if (value >= -128L && value < -16L) {
      WriteRAW(utils::UnderlyingCast(Marker::Int8));
      WriteRAW(static_cast<uint8_t>(value));
    } else if (value >= -32768L && value < 32768L) {
      WriteRAW(utils::UnderlyingCast(Marker::Int16));
      WriteValue(static_cast<int16_t>(value));
    } else if (value >= -2147483648L && value < 2147483648L) {
      WriteRAW(utils::UnderlyingCast(Marker::Int32));
      WriteValue(static_cast<int32_t>(value));
    } else {
      WriteRAW(utils::UnderlyingCast(Marker::Int64));
      WriteValue(value);
    }
  }

  void WriteDouble(const double &value) {
    WriteRAW(utils::UnderlyingCast(Marker::Float64));
    WriteValue(*reinterpret_cast<const int64_t *>(&value));
  }

  void WriteTypeSize(const size_t size, const uint8_t typ) {
    if (size <= 15) {
      uint8_t len = size;
      len &= 0x0F;
      WriteRAW(utils::UnderlyingCast(MarkerTiny[typ]) + len);
    } else if (size <= 255) {
      uint8_t len = size;
      WriteRAW(utils::UnderlyingCast(Marker8[typ]));
      WriteRAW(len);
    } else if (size <= 65535) {
      uint16_t len = size;
      WriteRAW(utils::UnderlyingCast(Marker16[typ]));
      WriteValue(len);
    } else {
      uint32_t len = size;
      WriteRAW(utils::UnderlyingCast(Marker32[typ]));
      WriteValue(len);
    }
  }

  void WriteString(const std::string &value) {
    WriteTypeSize(value.size(), MarkerString);
    WriteRAW(value.c_str(), value.size());
  }

  void WritePropertyValue(const PropertyValue &value) {
    auto write_list = [this](const std::vector<PropertyValue> &value) {
      WriteTypeSize(value.size(), MarkerList);
      for (auto &x : value) WritePropertyValue(x);
    };

    auto write_map = [this](const std::map<std::string, PropertyValue> &value) {
      WriteTypeSize(value.size(), MarkerMap);
      for (auto &x : value) {
        WriteString(x.first);
        WritePropertyValue(x.second);
      }
    };
    switch (value.type()) {
      case PropertyValue::Type::Null:
        WriteNull();
        break;
      case PropertyValue::Type::Bool:
        WriteBool(value.Value<bool>());
        break;
      case PropertyValue::Type::Int:
        WriteInt(value.Value<int64_t>());
        break;
      case PropertyValue::Type::Double:
        WriteDouble(value.Value<double>());
        break;
      case PropertyValue::Type::String:
        WriteString(value.Value<std::string>());
        break;
      case PropertyValue::Type::List:
        write_list(value.Value<std::vector<PropertyValue>>());
        break;
      case PropertyValue::Type::Map:
        write_map(value.Value<std::map<std::string, PropertyValue>>());
        break;
    }
  }

 protected:
  Buffer &buffer_;
};
}  // namespace communication::bolt
