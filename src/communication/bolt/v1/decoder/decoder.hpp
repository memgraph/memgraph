#pragma once

#include "communication/bolt/v1/codes.hpp"
#include "database/graph_db_accessor.hpp"
#include "logging/default.hpp"
#include "logging/logger.hpp"
#include "query/typed_value.hpp"
#include "utils/bswap.hpp"
#include "utils/underlying_cast.hpp"

#include <string>

namespace communication::bolt {

/**
 * Structure used when reading a Vertex with the decoder.
 * The decoder writes data into this structure.
 */
struct DecodedVertex {
  int64_t id;
  std::vector<std::string> labels;
  std::map<std::string, query::TypedValue> properties;
};

/**
 * Structure used when reading an Edge with the decoder.
 * The decoder writes data into this structure.
 */
struct DecodedEdge {
  int64_t id;
  int64_t from;
  int64_t to;
  std::string type;
  std::map<std::string, query::TypedValue> properties;
};

/**
 * Bolt Decoder.
 * Has public interfaces for reading Bolt encoded data.
 * Supports reading: TypedValue (without Vertex, Edge and Path),
 *                   Vertex, Edge
 *
 * @tparam Buffer the input buffer that should be used
 */
template <typename Buffer>
class Decoder : public Loggable {
 public:
  Decoder(Buffer &buffer)
      : Loggable("communication::bolt::Decoder"), buffer_(buffer) {}

  /**
   * Reads a TypedValue from the available data in the buffer.
   * This function tries to read a TypedValue from the available data.
   *
   * @param data pointer to a TypedValue where the read data should be stored
   * @returns true if data has been written to the data pointer,
   *          false otherwise
   */
  bool ReadTypedValue(query::TypedValue *data) {
    uint8_t value;

    logger.trace("[ReadTypedValue] Start");

    if (!buffer_.Read(&value, 1)) {
      logger.debug("[ReadTypedValue] Marker data missing!");
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
   * Reads a TypedValue from the available data in the buffer and checks
   * whether the read data type matches the supplied data type.
   *
   * @param data pointer to a TypedValue where the read data should be stored
   * @param type the expected type that should be read
   * @returns true if data has been written to the data pointer and the type
   *          matches the expected type, false otherwise
   */
  bool ReadTypedValue(query::TypedValue *data, query::TypedValue::Type type) {
    if (!ReadTypedValue(data)) {
      logger.debug("[ReadTypedValue] ReadTypedValue call failed!");
      return false;
    }
    if (data->type() != type) {
      logger.debug("[ReadTypedValue] Typed value has wrong type!");
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

    logger.trace("[ReadMessageHeader] Start");

    if (!buffer_.Read(values, 2)) {
      logger.debug("[ReadMessageHeader] Marker data missing!");
      return false;
    }

    *marker = (Marker)values[0];
    *signature = (Signature)values[1];
    logger.trace("[ReadMessageHeader] Success");
    return true;
  }

  /**
   * Reads a Vertex from the available data in the buffer.
   * This function tries to read a Vertex from the available data.
   *
   * @param data pointer to a DecodedVertex where the data should be stored
   * @returns true if data has been written into the data pointer,
   *          false otherwise
   */
  bool ReadVertex(DecodedVertex *data) {
    uint8_t value[2];
    query::TypedValue tv;

    logger.trace("[ReadVertex] Start");

    if (!buffer_.Read(value, 2)) {
      logger.debug("[ReadVertex] Missing marker and/or signature data!");
      return false;
    }

    // check header
    if (value[0] != underlying_cast(Marker::TinyStruct) + 3) {
      logger.debug("[ReadVertex] Received invalid marker ({})!", value[0]);
      return false;
    }
    if (value[1] != underlying_cast(Signature::Node)) {
      logger.debug("[ReadVertex] Received invalid signature ({})!", value[1]);
      return false;
    }

    // read ID
    if (!ReadTypedValue(&tv, query::TypedValue::Type::Int)) {
      logger.debug("[ReadVertex] Couldn't read ID!");
      return false;
    }
    data->id = tv.Value<int64_t>();

    // read labels
    if (!ReadTypedValue(&tv, query::TypedValue::Type::List)) {
      logger.debug("[ReadVertex] Couldn't read labels!");
      return false;
    }
    auto &labels = tv.Value<std::vector<query::TypedValue>>();
    data->labels.resize(labels.size());
    for (size_t i = 0; i < labels.size(); ++i) {
      if (labels[i].type() != query::TypedValue::Type::String) {
        logger.debug("[ReadVertex] Label has wrong type!");
        return false;
      }
      data->labels[i] = labels[i].Value<std::string>();
    }

    // read properties
    if (!ReadTypedValue(&tv, query::TypedValue::Type::Map)) {
      logger.debug("[ReadVertex] Couldn't read properties!");
      return false;
    }
    data->properties = tv.Value<std::map<std::string, query::TypedValue>>();

    logger.trace("[ReadVertex] Success");

    return true;
  }

  /**
   * Reads an Edge from the available data in the buffer.
   * This function tries to read an Edge from the available data.
   *
   * @param data pointer to a DecodedEdge where the data should be stored
   * @returns true if data has been written into the data pointer,
   *          false otherwise
   */
  bool ReadEdge(DecodedEdge *data) {
    uint8_t value[2];
    query::TypedValue tv;

    logger.trace("[ReadEdge] Start");

    if (!buffer_.Read(value, 2)) {
      logger.debug("[ReadEdge] Missing marker and/or signature data!");
      return false;
    }

    // check header
    if (value[0] != underlying_cast(Marker::TinyStruct) + 5) {
      logger.debug("[ReadEdge] Received invalid marker ({})!", value[0]);
      return false;
    }
    if (value[1] != underlying_cast(Signature::Relationship)) {
      logger.debug("[ReadEdge] Received invalid signature ({})!", value[1]);
      return false;
    }

    // read ID
    if (!ReadTypedValue(&tv, query::TypedValue::Type::Int)) {
      logger.debug("[ReadEdge] couldn't read ID!");
      return false;
    }
    data->id = tv.Value<int64_t>();

    // read from
    if (!ReadTypedValue(&tv, query::TypedValue::Type::Int)) {
      logger.debug("[ReadEdge] Couldn't read from_id!");
      return false;
    }
    data->from = tv.Value<int64_t>();

    // read to
    if (!ReadTypedValue(&tv, query::TypedValue::Type::Int)) {
      logger.debug("[ReadEdge] Couldn't read to_id!");
      return false;
    }
    data->to = tv.Value<int64_t>();

    // read type
    if (!ReadTypedValue(&tv, query::TypedValue::Type::String)) {
      logger.debug("[ReadEdge] Couldn't read type!");
      return false;
    }
    data->type = tv.Value<std::string>();

    // read properties
    if (!ReadTypedValue(&tv, query::TypedValue::Type::Map)) {
      logger.debug("[ReadEdge] Couldn't read properties!");
      return false;
    }
    data->properties = tv.Value<std::map<std::string, query::TypedValue>>();

    logger.trace("[ReadEdge] Success");

    return true;
  }

 protected:
  Buffer &buffer_;

 private:
  bool ReadNull(const Marker &marker, query::TypedValue *data) {
    logger.trace("[ReadNull] Start");
    debug_assert(marker == Marker::Null, "Received invalid marker!");
    *data = query::TypedValue::Null;
    logger.trace("[ReadNull] Success");
    return true;
  }

  bool ReadBool(const Marker &marker, query::TypedValue *data) {
    logger.trace("[ReadBool] Start");
    debug_assert(marker == Marker::False || marker == Marker::True,
                 "Received invalid marker!");
    if (marker == Marker::False) {
      *data = query::TypedValue(false);
    } else {
      *data = query::TypedValue(true);
    }
    logger.trace("[ReadBool] Success");
    return true;
  }

  bool ReadInt(const Marker &marker, query::TypedValue *data) {
    uint8_t value = underlying_cast(marker);
    bool success = true;
    int64_t ret;
    logger.trace("[ReadInt] Start");
    if (value >= 240 || value <= 127) {
      logger.trace("[ReadInt] Found a TinyInt");
      ret = value;
      if (value >= 240) ret -= 256;
    } else if (marker == Marker::Int8) {
      logger.trace("[ReadInt] Found an Int8");
      int8_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        logger.debug("[ReadInt] Int8 missing data!");
        return false;
      }
      ret = tmp;
    } else if (marker == Marker::Int16) {
      logger.trace("[ReadInt] Found an Int16");
      int16_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        logger.debug("[ReadInt] Int16 missing data!");
        return false;
      }
      ret = bswap(tmp);
    } else if (marker == Marker::Int32) {
      logger.trace("[ReadInt] Found an Int32");
      int32_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        logger.debug("[ReadInt] Int32 missing data!");
        return false;
      }
      ret = bswap(tmp);
    } else if (marker == Marker::Int64) {
      logger.trace("[ReadInt] Found an Int64");
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&ret), sizeof(ret))) {
        logger.debug("[ReadInt] Int64 missing data!");
        return false;
      }
      ret = bswap(ret);
    } else {
      logger.debug("[ReadInt] Received invalid marker ({})!",
                   underlying_cast(marker));
      return false;
    }
    if (success) {
      *data = query::TypedValue(ret);
      logger.trace("[ReadInt] Success");
    }
    return success;
  }

  bool ReadDouble(const Marker marker, query::TypedValue *data) {
    uint64_t value;
    double ret;
    logger.trace("[ReadDouble] Start");
    debug_assert(marker == Marker::Float64, "Received invalid marker!");
    if (!buffer_.Read(reinterpret_cast<uint8_t *>(&value), sizeof(value))) {
      logger.debug("[ReadDouble] Missing data!");
      return false;
    }
    value = bswap(value);
    ret = *reinterpret_cast<double *>(&value);
    *data = query::TypedValue(ret);
    logger.trace("[ReadDouble] Success");
    return true;
  }

  int64_t ReadTypeSize(const Marker &marker, const uint8_t type) {
    uint8_t value = underlying_cast(marker);
    if ((value & 0xF0) == underlying_cast(MarkerTiny[type])) {
      logger.trace("[ReadTypeSize] Found a TinyType");
      return value & 0x0F;
    } else if (marker == Marker8[type]) {
      logger.trace("[ReadTypeSize] Found a Type8");
      uint8_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        logger.debug("[ReadTypeSize] Type8 missing data!");
        return -1;
      }
      return tmp;
    } else if (marker == Marker16[type]) {
      logger.trace("[ReadTypeSize] Found a Type16");
      uint16_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        logger.debug("[ReadTypeSize] Type16 missing data!");
        return -1;
      }
      tmp = bswap(tmp);
      return tmp;
    } else if (marker == Marker32[type]) {
      logger.trace("[ReadTypeSize] Found a Type32");
      uint32_t tmp;
      if (!buffer_.Read(reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp))) {
        logger.debug("[ReadTypeSize] Type32 missing data!");
        return -1;
      }
      tmp = bswap(tmp);
      return tmp;
    } else {
      logger.debug("[ReadTypeSize] Received invalid marker ({})!",
                   underlying_cast(marker));
      return -1;
    }
  }

  bool ReadString(const Marker &marker, query::TypedValue *data) {
    logger.trace("[ReadString] Start");
    auto size = ReadTypeSize(marker, MarkerString);
    if (size == -1) {
      logger.debug("[ReadString] Couldn't get size!");
      return false;
    }
    std::unique_ptr<uint8_t[]> ret(new uint8_t[size]);
    if (!buffer_.Read(ret.get(), size)) {
      logger.debug("[ReadString] Missing data!");
      return false;
    }
    *data = query::TypedValue(
        std::string(reinterpret_cast<char *>(ret.get()), size));
    logger.trace("[ReadString] Success");
    return true;
  }

  bool ReadList(const Marker &marker, query::TypedValue *data) {
    logger.trace("[ReadList] Start");
    auto size = ReadTypeSize(marker, MarkerList);
    if (size == -1) {
      logger.debug("[ReadList] Couldn't get size!");
      return false;
    }
    std::vector<query::TypedValue> ret(size);
    for (int64_t i = 0; i < size; ++i) {
      if (!ReadTypedValue(&ret[i])) {
        logger.debug("[ReadList] Couldn't read element {}", i);
        return false;
      }
    }
    *data = query::TypedValue(ret);
    logger.trace("[ReadList] Success");
    return true;
  }

  bool ReadMap(const Marker &marker, query::TypedValue *data) {
    logger.trace("[ReadMap] Start");
    auto size = ReadTypeSize(marker, MarkerMap);
    if (size == -1) {
      logger.debug("[ReadMap] Couldn't get size!");
      return false;
    }

    query::TypedValue tv;
    std::string str;
    std::map<std::string, query::TypedValue> ret;
    for (int64_t i = 0; i < size; ++i) {
      if (!ReadTypedValue(&tv)) {
        logger.debug("[ReadMap] Couldn't read index {}", i);
        return false;
      }
      if (tv.type() != query::TypedValue::Type::String) {
        logger.debug("[ReadMap] Index {} isn't a string!", i);
        return false;
      }
      str = tv.Value<std::string>();

      if (!ReadTypedValue(&tv)) {
        logger.debug("[ReadMap] Couldn't read element {}", i);
        return false;
      }
      ret.insert(std::make_pair(str, tv));
    }
    if (ret.size() != size) {
      logger.debug(
          "[ReadMap] The client sent multiple objects with same indexes!");
      return false;
    }

    *data = query::TypedValue(ret);
    logger.trace("[ReadMap] Success");
    return true;
  }
};
}
