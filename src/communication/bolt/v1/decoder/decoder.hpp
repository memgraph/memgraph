#pragma once

#include <string>

#include <glog/logging.h>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "database/graph_db_accessor.hpp"
#include "utils/bswap.hpp"
#include "utils/underlying_cast.hpp"

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
  Decoder(Buffer &buffer) : buffer_(buffer) {}

  /**
   * Reads a DecodedValue from the available data in the buffer.
   * This function tries to read a DecodedValue from the available data.
   *
   * @param data pointer to a DecodedValue where the read data should be stored
   * @returns true if data has been written to the data pointer,
   *          false otherwise
   */
  bool ReadValue(DecodedValue *data) {
    uint8_t value;

    DLOG(INFO) << "[ReadValue] Start";

    if (!buffer_.Read(&value, 1)) {
      DLOG(WARNING) << "[ReadValue] Marker data missing!";
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
          DLOG(WARNING) << "[ReadVertex] Missing marker and/or signature data!";
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
            DLOG(WARNING) << "[ReadValue] Expected [node | unbounded_ege | "
                             "path] signature, received "
                          << signature;
            return false;
        }
      }

      case Marker::TinyStruct5:
        return ReadEdge(marker, data);

      default:
        if ((value & 0xF0) == underlying_cast(Marker::TinyString)) {
          return ReadString(marker, data);
        } else if ((value & 0xF0) == underlying_cast(Marker::TinyList)) {
          return ReadList(marker, data);
        } else if ((value & 0xF0) == underlying_cast(Marker::TinyMap)) {
          return ReadMap(marker, data);
        } else {
          return ReadInt(marker, data);
        }
        break;
    }
  }

  /**
   * Reads a DecodedValue from the available data in the buffer and checks
   * whether the read data type matches the supplied data type.
   *
   * @param data pointer to a DecodedValue where the read data should be stored
   * @param type the expected type that should be read
   * @returns true if data has been written to the data pointer and the type
   *          matches the expected type, false otherwise
   */
  bool ReadValue(DecodedValue *data, DecodedValue::Type type) {
    if (!ReadValue(data)) {
      DLOG(WARNING) << "[ReadValue] ReadValue call failed!";
      return false;
    }
    if (data->type() != type) {
      DLOG(WARNING) << "[ReadValue] Decoded value has wrong type!";
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

    DLOG(INFO) << "[ReadMessageHeader] Start";

    if (!buffer_.Read(values, 2)) {
      DLOG(WARNING) << "[ReadMessageHeader] Marker data missing!";
      return false;
    }

    *marker = (Marker)values[0];
    *signature = (Signature)values[1];
    DLOG(WARNING) << "[ReadMessageHeader] Success";
    return true;
  }

 protected:
  Buffer &buffer_;

 private:
  bool ReadNull(const Marker &marker, DecodedValue *data) {
    DLOG(INFO) << "[ReadNull] Start";
    debug_assert(marker == Marker::Null, "Received invalid marker!");
    *data = DecodedValue();
    DLOG(INFO) << "[ReadNull] Success";
    return true;
  }

  bool ReadBool(const Marker &marker, DecodedValue *data) {
    DLOG(INFO) << "[ReadBool] Start";
    debug_assert(marker == Marker::False || marker == Marker::True,
                 "Received invalid marker!");
    if (marker == Marker::False) {
      *data = DecodedValue(false);
    } else {
      *data = DecodedValue(true);
    }
    DLOG(INFO) << "[ReadBool] Success";
    return true;
  }

  bool ReadInt(const Marker &marker, DecodedValue *data) {
    uint8_t value = underlying_cast(marker);
    bool success = true;
    int64_t ret;
    DLOG(INFO) << "[ReadInt] Start";
    if (value >= 240 || value <= 127) {
      DLOG(INFO) << "[ReadInt] Found a TinyInt";
      ret = value;
      if (value >= 240) ret -= 256;
    } else if (marker == Marker::Int8) {
      DLOG(INFO) << "[ReadInt] Found an Int8";
      int8_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        DLOG(WARNING) << "[ReadInt] Int8 missing data!";
        return false;
      }
      ret = tmp;
    } else if (marker == Marker::Int16) {
      DLOG(INFO) << "[ReadInt] Found an Int16";
      int16_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        DLOG(WARNING) << "[ReadInt] Int16 missing data!";
        return false;
      }
      ret = bswap(tmp);
    } else if (marker == Marker::Int32) {
      DLOG(INFO) << "[ReadInt] Found an Int32";
      int32_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        DLOG(WARNING) << "[ReadInt] Int32 missing data!";
        return false;
      }
      ret = bswap(tmp);
    } else if (marker == Marker::Int64) {
      DLOG(INFO) << "[ReadInt] Found an Int64";
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&ret), sizeof(ret))) {
        DLOG(WARNING) << "[ReadInt] Int64 missing data!";
        return false;
      }
      ret = bswap(ret);
    } else {
      DLOG(WARNING) << "[ReadInt] Received invalid marker "
                    << underlying_cast(marker);
      return false;
    }
    if (success) {
      *data = DecodedValue(ret);
      DLOG(INFO) << "[ReadInt] Success";
    }
    return success;
  }

  bool ReadDouble(const Marker marker, DecodedValue *data) {
    uint64_t value;
    double ret;
    DLOG(INFO) << "[ReadDouble] Start";
    debug_assert(marker == Marker::Float64, "Received invalid marker!");
    if (!buffer_.Read(reinterpret_cast<uint8_t *>(&value), sizeof(value))) {
      DLOG(WARNING) << "[ReadDouble] Missing data!";
      return false;
    }
    value = bswap(value);
    ret = *reinterpret_cast<double *>(&value);
    *data = DecodedValue(ret);
    DLOG(INFO) << "[ReadDouble] Success";
    return true;
  }

  int64_t ReadTypeSize(const Marker &marker, const uint8_t type) {
    uint8_t value = underlying_cast(marker);
    if ((value & 0xF0) == underlying_cast(MarkerTiny[type])) {
      DLOG(INFO) << "[ReadTypeSize] Found a TinyType";
      return value & 0x0F;
    } else if (marker == Marker8[type]) {
      DLOG(INFO) << "[ReadTypeSize] Found a Type8";
      uint8_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        DLOG(WARNING) << "[ReadTypeSize] Type8 missing data!";
        return -1;
      }
      return tmp;
    } else if (marker == Marker16[type]) {
      DLOG(INFO) << "[ReadTypeSize] Found a Type16";
      uint16_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        DLOG(WARNING) << "[ReadTypeSize] Type16 missing data!";
        return -1;
      }
      tmp = bswap(tmp);
      return tmp;
    } else if (marker == Marker32[type]) {
      DLOG(INFO) << "[ReadTypeSize] Found a Type32";
      uint32_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        DLOG(WARNING) << "[ReadTypeSize] Type32 missing data!";
        return -1;
      }
      tmp = bswap(tmp);
      return tmp;
    } else {
      DLOG(WARNING) << "[ReadTypeSize] Received invalid marker "
                    << underlying_cast(marker);
      return -1;
    }
  }

  bool ReadString(const Marker &marker, DecodedValue *data) {
    DLOG(INFO) << "[ReadString] Start";
    auto size = ReadTypeSize(marker, MarkerString);
    if (size == -1) {
      DLOG(WARNING) << "[ReadString] Couldn't get size!";
      return false;
    }
    std::unique_ptr<uint8_t[]> ret(new uint8_t[size]);
    if (!buffer_.Read(ret.get(), size)) {
      DLOG(WARNING) << "[ReadString] Missing data!";
      return false;
    }
    *data =
        DecodedValue(std::string(reinterpret_cast<char *>(ret.get()), size));
    DLOG(INFO) << "[ReadString] Success";
    return true;
  }

  bool ReadList(const Marker &marker, DecodedValue *data) {
    DLOG(INFO) << "[ReadList] Start";
    auto size = ReadTypeSize(marker, MarkerList);
    if (size == -1) {
      DLOG(WARNING) << "[ReadList] Couldn't get size!";
      return false;
    }
    std::vector<DecodedValue> ret(size);
    for (int64_t i = 0; i < size; ++i) {
      if (!ReadValue(&ret[i])) {
        DLOG(WARNING) << "[ReadList] Couldn't read element " << i;
        return false;
      }
    }
    *data = DecodedValue(ret);
    DLOG(INFO) << "[ReadList] Success";
    return true;
  }

  bool ReadMap(const Marker &marker, DecodedValue *data) {
    DLOG(INFO) << "[ReadMap] Start";
    auto size = ReadTypeSize(marker, MarkerMap);
    if (size == -1) {
      DLOG(WARNING) << "[ReadMap] Couldn't get size!";
      return false;
    }

    DecodedValue dv;
    std::string str;
    std::map<std::string, DecodedValue> ret;
    for (int64_t i = 0; i < size; ++i) {
      if (!ReadValue(&dv)) {
        DLOG(WARNING) << "[ReadMap] Couldn't read index " << i;
        return false;
      }
      if (dv.type() != DecodedValue::Type::String) {
        DLOG(WARNING) << "[ReadMap] Index " << i << " isn't a string!";
        return false;
      }
      str = dv.ValueString();

      if (!ReadValue(&dv)) {
        DLOG(WARNING) << "[ReadMap] Couldn't read element " << i;
        return false;
      }
      ret.insert(std::make_pair(str, dv));
    }
    if (ret.size() != size) {
      DLOG(WARNING)
          << "[ReadMap] The client sent multiple objects with same indexes!";
      return false;
    }

    *data = DecodedValue(ret);
    DLOG(INFO) << "[ReadMap] Success";
    return true;
  }

  bool ReadVertex(DecodedValue *data) {
    DecodedValue dv;
    DecodedVertex vertex;

    DLOG(INFO) << "[ReadVertex] Start";

    // read ID
    if (!ReadValue(&dv, DecodedValue::Type::Int)) {
      DLOG(WARNING) << "[ReadVertex] Couldn't read ID!";
      return false;
    }
    vertex.id = dv.ValueInt();

    // read labels
    if (!ReadValue(&dv, DecodedValue::Type::List)) {
      DLOG(WARNING) << "[ReadVertex] Couldn't read labels!";
      return false;
    }
    auto &labels = dv.ValueList();
    vertex.labels.resize(labels.size());
    for (size_t i = 0; i < labels.size(); ++i) {
      if (labels[i].type() != DecodedValue::Type::String) {
        DLOG(WARNING) << "[ReadVertex] Label has wrong type!";
        return false;
      }
      vertex.labels[i] = labels[i].ValueString();
    }

    // read properties
    if (!ReadValue(&dv, DecodedValue::Type::Map)) {
      DLOG(WARNING) << "[ReadVertex] Couldn't read properties!";
      return false;
    }
    vertex.properties = dv.ValueMap();

    *data = DecodedValue(vertex);

    DLOG(INFO) << "[ReadVertex] Success";

    return true;
  }

  bool ReadEdge(const Marker &marker, DecodedValue *data) {
    uint8_t value;
    DecodedValue dv;
    DecodedEdge edge;

    DLOG(INFO) << "[ReadEdge] Start";

    if (!buffer_.Read(&value, 1)) {
      DLOG(WARNING) << "[ReadEdge] Missing marker and/or signature data!";
      return false;
    }

    // check header
    if (marker != Marker::TinyStruct5) {
      DLOG(WARNING) << "[ReadEdge] Received invalid marker "
                    << (uint64_t)underlying_cast(marker);
      return false;
    }
    if (value != underlying_cast(Signature::Relationship)) {
      DLOG(WARNING) << "[ReadEdge] Received invalid signature " << value;
      return false;
    }

    // read ID
    if (!ReadValue(&dv, DecodedValue::Type::Int)) {
      DLOG(WARNING) << "[ReadEdge] Couldn't read ID!";
      return false;
    }
    edge.id = dv.ValueInt();

    // read from
    if (!ReadValue(&dv, DecodedValue::Type::Int)) {
      DLOG(WARNING) << "[ReadEdge] Couldn't read from_id!";
      return false;
    }
    edge.from = dv.ValueInt();

    // read to
    if (!ReadValue(&dv, DecodedValue::Type::Int)) {
      DLOG(WARNING) << "[ReadEdge] Couldn't read to_id!";
      return false;
    }
    edge.to = dv.ValueInt();

    // read type
    if (!ReadValue(&dv, DecodedValue::Type::String)) {
      DLOG(WARNING) << "[ReadEdge] Couldn't read type!";
      return false;
    }
    edge.type = dv.ValueString();

    // read properties
    if (!ReadValue(&dv, DecodedValue::Type::Map)) {
      DLOG(WARNING) << "[ReadEdge] Couldn't read properties!";
      return false;
    }
    edge.properties = dv.ValueMap();

    *data = DecodedValue(edge);

    DLOG(INFO) << "[ReadEdge] Success";

    return true;
  }

  bool ReadUnboundedEdge(DecodedValue *data) {
    DecodedValue dv;
    DecodedUnboundedEdge edge;

    DLOG(INFO) << "[ReadUnboundedEdge] Start";

    // read ID
    if (!ReadValue(&dv, DecodedValue::Type::Int)) {
      DLOG(WARNING) << "[ReadUnboundedEdge] Couldn't read ID!";
      return false;
    }
    edge.id = dv.ValueInt();

    // read type
    if (!ReadValue(&dv, DecodedValue::Type::String)) {
      DLOG(WARNING) << "[ReadUnboundedEdge] Couldn't read type!";
      return false;
    }
    edge.type = dv.ValueString();

    // read properties
    if (!ReadValue(&dv, DecodedValue::Type::Map)) {
      DLOG(WARNING) << "[ReadUnboundedEdge] Couldn't read properties!";
      return false;
    }
    edge.properties = dv.ValueMap();

    *data = DecodedValue(edge);

    DLOG(INFO) << "[ReadUnboundedEdge] Success";

    return true;
  }

  bool ReadPath(DecodedValue *data) {
    DecodedValue dv;
    DecodedPath path;

    DLOG(INFO) << "[ReadPath] Start";

    // vertices
    if (!ReadValue(&dv, DecodedValue::Type::List)) {
      DLOG(WARNING) << "[ReadPath] Couldn't read vertices!";
      return false;
    }
    for (const auto &vertex : dv.ValueList()) {
      if (vertex.type() != DecodedValue::Type::Vertex) {
        DLOG(WARNING) << "[ReadPath] Received a '" << vertex.type() << "' element in the vertices list!";
        return false;
      }
      path.vertices.emplace_back(vertex.ValueVertex());
    }

    // edges
    if (!ReadValue(&dv, DecodedValue::Type::List)) {
      DLOG(WARNING) << "[ReadPath] Couldn't read edges!";
      return false;
    }
    for (const auto &edge : dv.ValueList()) {
      if (edge.type() != DecodedValue::Type::UnboundedEdge) {
        DLOG(WARNING) << "[ReadPath] Received a '" << edge.type() << "' element in the edges list!";
        return false;
      }
      path.edges.emplace_back(edge.ValueUnboundedEdge());
    }

    // indices
    if (!ReadValue(&dv, DecodedValue::Type::List)) {
      DLOG(WARNING) << "[ReadPath] Couldn't read indices!";
      return false;
    }
    for (const auto &index : dv.ValueList()) {
      if (index.type() != DecodedValue::Type::Int) {
        DLOG(WARNING) << "[ReadPath] Received a '" << index.type() << "' element in the indices list (expected an int)!";
        return false;
      }
      path.indices.emplace_back(index.ValueInt());
    }

    *data = DecodedValue(path);

    DLOG(INFO) << "[ReadPath] Success";

    return true;
  }
};
}
