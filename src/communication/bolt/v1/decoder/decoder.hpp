#pragma once

#include <string>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/value.hpp"
#include "utils/cast.hpp"
#include "utils/endian.hpp"
#include "utils/logging.hpp"

namespace communication::bolt {

/**
 * Bolt Decoder.
 * Has public interfaces for reading Bolt encoded data.
 *
 * @tparam Buffer the input buffer that should be used
 */
template <typename Buffer>
class Decoder {
 public:
  explicit Decoder(Buffer &buffer) : buffer_(buffer) {}

  /**
   * Reads a Value from the available data in the buffer.
   * This function tries to read a Value from the available data.
   *
   * @param data pointer to a Value where the read data should be stored
   * @returns true if data has been written to the data pointer,
   *          false otherwise
   */
  bool ReadValue(Value *data) {
    uint8_t value;

    if (!buffer_.Read(&value, 1)) {
      return false;
    }

    Marker marker = (Marker)value;

    switch (marker) {
      case Marker::Null:
        return ReadNull(marker, data);

      case Marker::True:
      case Marker::False:
        return ReadBool(marker, data);

      case Marker::Int8:
      case Marker::Int16:
      case Marker::Int32:
      case Marker::Int64:
        return ReadInt(marker, data);

      case Marker::Float64:
        return ReadDouble(marker, data);

      case Marker::String8:
      case Marker::String16:
      case Marker::String32:
        return ReadString(marker, data);

      case Marker::List8:
      case Marker::List16:
      case Marker::List32:
        return ReadList(marker, data);

      case Marker::Map8:
      case Marker::Map16:
      case Marker::Map32:
        return ReadMap(marker, data);

      case Marker::TinyStruct3: {
        // For tiny struct 3 we will also read the Signature to switch between
        // vertex, unbounded_edge and path. Note that in those functions we
        // won't perform an additional signature read.
        uint8_t signature;
        if (!buffer_.Read(&signature, 1)) {
          return false;
        }
        switch (static_cast<Signature>(signature)) {
          case Signature::Node:
            return ReadVertex(data);
          case Signature::UnboundRelationship:
            return ReadUnboundedEdge(data);
          case Signature::Path:
            return ReadPath(data);
          default:
            return false;
        }
      }

      case Marker::TinyStruct5:
        return ReadEdge(marker, data);

      default:
        if ((value & 0xF0) == utils::UnderlyingCast(Marker::TinyString)) {
          return ReadString(marker, data);
        } else if ((value & 0xF0) == utils::UnderlyingCast(Marker::TinyList)) {
          return ReadList(marker, data);
        } else if ((value & 0xF0) == utils::UnderlyingCast(Marker::TinyMap)) {
          return ReadMap(marker, data);
        } else {
          return ReadInt(marker, data);
        }
        break;
    }
  }

  /**
   * Reads a Value from the available data in the buffer and checks
   * whether the read data type matches the supplied data type.
   *
   * @param data pointer to a Value where the read data should be stored
   * @param type the expected type that should be read
   * @returns true if data has been written to the data pointer and the type
   *          matches the expected type, false otherwise
   */
  bool ReadValue(Value *data, Value::Type type) {
    if (!ReadValue(data)) {
      return false;
    }
    if (data->type() != type) {
      return false;
    }
    return true;
  }

  /**
   * Reads a Message header from the available data in the buffer.
   *
   * @param signature pointer to a Signature where the signature should be
   *                  stored
   * @param marker pointer to a Signature where the marker should be stored
   * @returns true if data has been written into the data pointers,
   *          false otherwise
   */
  bool ReadMessageHeader(Signature *signature, Marker *marker) {
    uint8_t values[2];

    if (!buffer_.Read(values, 2)) {
      return false;
    }

    *marker = (Marker)values[0];
    *signature = (Signature)values[1];
    return true;
  }

 protected:
  Buffer &buffer_;

 private:
  bool ReadNull(const Marker &marker, Value *data) {
    DMG_ASSERT(marker == Marker::Null, "Received invalid marker!");
    *data = Value();
    return true;
  }

  bool ReadBool(const Marker &marker, Value *data) {
    DMG_ASSERT(marker == Marker::False || marker == Marker::True, "Received invalid marker!");
    if (marker == Marker::False) {
      *data = Value(false);
    } else {
      *data = Value(true);
    }
    return true;
  }

  bool ReadInt(const Marker &marker, Value *data) {
    uint8_t value = utils::UnderlyingCast(marker);
    int64_t ret;
    if (value >= 240 || value <= 127) {
      ret = value;
      if (value >= 240) ret -= 256;
    } else if (marker == Marker::Int8) {
      int8_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        return false;
      }
      ret = tmp;
    } else if (marker == Marker::Int16) {
      int16_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        return false;
      }
      ret = utils::BigEndianToHost(tmp);
    } else if (marker == Marker::Int32) {
      int32_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        return false;
      }
      ret = utils::BigEndianToHost(tmp);
    } else if (marker == Marker::Int64) {
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&ret), sizeof(ret))) {
        return false;
      }
      ret = utils::BigEndianToHost(ret);
    } else {
      return false;
    }
    *data = Value(ret);
    return true;
  }

  bool ReadDouble(const Marker marker, Value *data) {
    uint64_t value;
    double ret;
    DMG_ASSERT(marker == Marker::Float64, "Received invalid marker!");
    if (!buffer_.Read(reinterpret_cast<uint8_t *>(&value), sizeof(value))) {
      return false;
    }
    value = utils::BigEndianToHost(value);
    ret = utils::MemcpyCast<double>(value);
    *data = Value(ret);
    return true;
  }

  int64_t ReadTypeSize(const Marker &marker, const uint8_t type) {
    uint8_t value = utils::UnderlyingCast(marker);
    if ((value & 0xF0) == utils::UnderlyingCast(MarkerTiny[type])) {
      return value & 0x0F;
    } else if (marker == Marker8[type]) {
      uint8_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        return -1;
      }
      return tmp;
    } else if (marker == Marker16[type]) {
      uint16_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        return -1;
      }
      tmp = utils::BigEndianToHost(tmp);
      return tmp;
    } else if (marker == Marker32[type]) {
      uint32_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        return -1;
      }
      tmp = utils::BigEndianToHost(tmp);
      return tmp;
    } else {
      return -1;
    }
  }

  bool ReadString(const Marker &marker, Value *data) {
    const int kMaxStackBuffer = 8192;
    uint8_t buffer[kMaxStackBuffer];
    auto size = ReadTypeSize(marker, MarkerString);
    if (size == -1) {
      return false;
    }
    // Here we use a temporary buffer on the stack to prevent temporary
    // allocations. Most of strings that are decoded are small so it makes no
    // sense to allocate a temporary buffer every time we decode a string. This
    // way we allocate a temporary buffer only when the string is large. This
    // wouldn't be necessary if we had full C++17 support. In C++17 we could
    // preallocate the `buffer[size]` in the destination string `*data =
    // Value(std::string('\0', size))` and just call
    // `buffer_.Read(data->ValueString().data())`.
    if (size < kMaxStackBuffer) {
      if (!buffer_.Read(buffer, size)) {
        SPDLOG_WARN("[ReadString] Missing data!");
        return false;
      }
      *data = Value(std::string(reinterpret_cast<char *>(buffer), size));
    } else {
      std::unique_ptr<uint8_t[]> ret(new uint8_t[size]);
      if (!buffer_.Read(ret.get(), size)) {
        SPDLOG_WARN("[ReadString] Missing data!");
        return false;
      }
      *data = Value(std::string(reinterpret_cast<char *>(ret.get()), size));
    }
    return true;
  }

  bool ReadList(const Marker &marker, Value *data) {
    auto size = ReadTypeSize(marker, MarkerList);
    if (size == -1) {
      return false;
    }
    *data = Value(std::vector<Value>(size));
    auto &ret = data->ValueList();
    for (int64_t i = 0; i < size; ++i) {
      if (!ReadValue(&ret[i])) {
        return false;
      }
    }
    return true;
  }

  bool ReadMap(const Marker &marker, Value *data) {
    auto size = ReadTypeSize(marker, MarkerMap);
    if (size == -1) {
      return false;
    }

    Value dv_key, dv_val;

    *data = Value(std::map<std::string, Value>());
    auto &ret = data->ValueMap();
    for (int64_t i = 0; i < size; ++i) {
      if (!ReadValue(&dv_key, Value::Type::String)) {
        return false;
      }
      if (!ReadValue(&dv_val)) {
        return false;
      }
      ret.emplace(std::move(dv_key.ValueString()), std::move(dv_val));
    }
    if (ret.size() != size) {
      return false;
    }

    return true;
  }

  bool ReadVertex(Value *data) {
    Value dv;
    *data = Value(Vertex());
    auto &vertex = data->ValueVertex();

    // read ID
    if (!ReadValue(&dv, Value::Type::Int)) {
      return false;
    }
    vertex.id = Id::FromInt(dv.ValueInt());

    // read labels
    if (!ReadValue(&dv, Value::Type::List)) {
      return false;
    }
    auto &labels = dv.ValueList();
    vertex.labels.reserve(labels.size());
    for (size_t i = 0; i < labels.size(); ++i) {
      if (labels[i].type() != Value::Type::String) {
        return false;
      }
      vertex.labels.emplace_back(std::move(labels[i].ValueString()));
    }

    // read properties
    if (!ReadValue(&dv, Value::Type::Map)) {
      return false;
    }
    vertex.properties = std::move(dv.ValueMap());

    return true;
  }

  bool ReadEdge(const Marker &marker, Value *data) {
    uint8_t value;
    Value dv;
    *data = Value(Edge());
    auto &edge = data->ValueEdge();

    if (!buffer_.Read(&value, 1)) {
      return false;
    }

    // check header
    if (marker != Marker::TinyStruct5) {
      return false;
    }
    if (value != utils::UnderlyingCast(Signature::Relationship)) {
      return false;
    }

    // read ID
    if (!ReadValue(&dv, Value::Type::Int)) {
      return false;
    }
    edge.id = Id::FromInt(dv.ValueInt());

    // read from
    if (!ReadValue(&dv, Value::Type::Int)) {
      return false;
    }
    edge.from = Id::FromInt(dv.ValueInt());

    // read to
    if (!ReadValue(&dv, Value::Type::Int)) {
      return false;
    }
    edge.to = Id::FromInt(dv.ValueInt());

    // read type
    if (!ReadValue(&dv, Value::Type::String)) {
      return false;
    }
    edge.type = std::move(dv.ValueString());

    // read properties
    if (!ReadValue(&dv, Value::Type::Map)) {
      return false;
    }
    edge.properties = std::move(dv.ValueMap());

    return true;
  }

  bool ReadUnboundedEdge(Value *data) {
    Value dv;
    *data = Value(UnboundedEdge());
    auto &edge = data->ValueUnboundedEdge();

    // read ID
    if (!ReadValue(&dv, Value::Type::Int)) {
      return false;
    }
    edge.id = Id::FromInt(dv.ValueInt());

    // read type
    if (!ReadValue(&dv, Value::Type::String)) {
      return false;
    }
    edge.type = std::move(dv.ValueString());

    // read properties
    if (!ReadValue(&dv, Value::Type::Map)) {
      return false;
    }
    edge.properties = std::move(dv.ValueMap());

    return true;
  }

  bool ReadPath(Value *data) {
    Value dv;
    *data = Value(Path());
    auto &path = data->ValuePath();

    // vertices
    if (!ReadValue(&dv, Value::Type::List)) {
      return false;
    }
    for (const auto &vertex : dv.ValueList()) {
      if (vertex.type() != Value::Type::Vertex) {
        return false;
      }
      path.vertices.emplace_back(std::move(vertex.ValueVertex()));
    }

    // edges
    if (!ReadValue(&dv, Value::Type::List)) {
      return false;
    }
    for (const auto &edge : dv.ValueList()) {
      if (edge.type() != Value::Type::UnboundedEdge) {
        return false;
      }
      path.edges.emplace_back(std::move(edge.ValueUnboundedEdge()));
    }

    // indices
    if (!ReadValue(&dv, Value::Type::List)) {
      return false;
    }
    for (const auto &index : dv.ValueList()) {
      if (index.type() != Value::Type::Int) {
        return false;
      }
      path.indices.emplace_back(index.ValueInt());
    }

    return true;
  }
};
}  // namespace communication::bolt
