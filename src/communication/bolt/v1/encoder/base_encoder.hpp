#pragma once

#include "database/graph_db_accessor.hpp"
#include "logging/default.hpp"
#include "logging/logger.hpp"
#include "query/backend/cpp/typed_value.hpp"
#include "utils/bswap.hpp"

#include <string>

namespace communication::bolt {

static constexpr uint8_t TSTRING = 0, TLIST = 1, TMAP = 2;
static constexpr uint8_t type_tiny_marker[3] = {0x80, 0x90, 0xA0};
static constexpr uint8_t type_8_marker[3] = {0xD0, 0xD4, 0xD8};
static constexpr uint8_t type_16_marker[3] = {0xD1, 0xD5, 0xD9};
static constexpr uint8_t type_32_marker[3] = {0xD2, 0xD6, 0xDA};

/**
 * Bolt BaseEncoder.
 * Has public interfaces for writing Bolt encoded data.
 * Supported types are: Null, Bool, Int, Double,
 * String, List, Map, Vertex, Edge
 *
 * This class has a dual purpose. The first is streaming of bolt data to
 * network clients. The second is streaming to disk in the database snapshotter.
 * The two usages are changed depending on the encode_ids boolean flag.
 * If encode_ids is set to false (it's default value) then the encoder encodes
 * normal bolt network client data. If encode_ids is set to true then the
 * encoder encodes data for the database snapshotter.
 * In the normal mode (encode_ids == false) the encoder doesn't output object
 * IDs but instead it outputs fixed zeros. In the snapshotter mode it outputs
 * temporary IDs obtained from the objects.
 *
 * Also note that currently expansion across the graph is not allowed during
 * streaming, if an update has happened in the same transaction command.
 * Attempting to do so crashes the DB. That's another reason why we encode
 * IDs (expansion from edges) only in the snapshotter.
 *
 * @tparam Buffer the output buffer that should be used
 */
template <typename Buffer>
class BaseEncoder : public Loggable {
 public:
  BaseEncoder(Buffer &buffer, bool encode_ids = false)
      : Loggable("communication::bolt::BaseEncoder"),
        buffer_(buffer),
        encode_ids_(encode_ids) {}

  void WriteRAW(const uint8_t *data, uint64_t len) { buffer_.Write(data, len); }

  void WriteRAW(const char *data, uint64_t len) {
    WriteRAW((const uint8_t *)data, len);
  }

  void WriteRAW(const uint8_t data) { WriteRAW(&data, 1); }

  template <class T>
  void WriteValue(T value) {
    value = bswap(value);
    WriteRAW(reinterpret_cast<const uint8_t *>(&value), sizeof(value));
  }

  void WriteNull() {
    // 0xC0 = null marker
    WriteRAW(0xC0);
  }

  void WriteBool(const bool &value) {
    if (value) {
      // 0xC3 = true marker
      WriteRAW(0xC3);
    } else {
      // 0xC2 = false marker
      WriteRAW(0xC2);
    }
  }

  void WriteInt(const int64_t &value) {
    if (value >= -16L && value < 128L) {
      WriteRAW(static_cast<uint8_t>(value));
    } else if (value >= -128L && value < -16L) {
      // 0xC8 = int8 marker
      WriteRAW(0xC8);
      WriteRAW(static_cast<uint8_t>(value));
    } else if (value >= -32768L && value < 32768L) {
      // 0xC9 = int16 marker
      WriteRAW(0xC9);
      WriteValue(static_cast<int16_t>(value));
    } else if (value >= -2147483648L && value < 2147483648L) {
      // 0xCA = int32 marker
      WriteRAW(0xCA);
      WriteValue(static_cast<int32_t>(value));
    } else {
      // 0xCB = int64 marker
      WriteRAW(0xCB);
      WriteValue(value);
    }
  }

  void WriteDouble(const double &value) {
    // 0xC1 = float64 marker
    WriteRAW(0xC1);
    WriteValue(*reinterpret_cast<const int64_t *>(&value));
  }

  void WriteTypeSize(const size_t size, const uint8_t typ) {
    if (size <= 15) {
      uint8_t len = size;
      len &= 0x0F;
      // tiny marker (+len)
      WriteRAW(type_tiny_marker[typ] + len);
    } else if (size <= 255) {
      uint8_t len = size;
      // 8 marker
      WriteRAW(type_8_marker[typ]);
      WriteRAW(len);
    } else if (size <= 65536) {
      uint16_t len = size;
      // 16 marker
      WriteRAW(type_16_marker[typ]);
      WriteValue(len);
    } else {
      uint32_t len = size;
      // 32 marker
      WriteRAW(type_32_marker[typ]);
      WriteValue(len);
    }
  }

  void WriteString(const std::string &value) {
    WriteTypeSize(value.size(), TSTRING);
    WriteRAW(value.c_str(), value.size());
  }

  void WriteList(const std::vector<TypedValue> &value) {
    WriteTypeSize(value.size(), TLIST);
    for (auto &x : value) WriteTypedValue(x);
  }

  void WriteMap(const std::map<std::string, TypedValue> &value) {
    WriteTypeSize(value.size(), TMAP);
    for (auto &x : value) {
      WriteString(x.first);
      WriteTypedValue(x.second);
    }
  }

  void WriteVertex(const VertexAccessor &vertex) {
    // 0xB3 = struct 3; 0x4E = vertex signature
    WriteRAW("\xB3\x4E", 2);

    if (encode_ids_) {
      // IMPORTANT: this is used only in the database snapshotter!
      WriteUInt(vertex.temporary_id());
    } else {
      // IMPORTANT: here we write a hardcoded 0 because we don't
      // use internal IDs, but need to give something to Bolt
      // note that OpenCypher has no id(x) function, so the client
      // should not be able to do anything with this value anyway
      WriteInt(0);
    }

    // write labels
    const auto &labels = vertex.labels();
    WriteTypeSize(labels.size(), TLIST);
    for (const auto &label : labels)
      WriteString(vertex.db_accessor().label_name(label));

    // write properties
    const auto &props = vertex.Properties();
    WriteTypeSize(props.size(), TMAP);
    for (const auto &prop : props) {
      WriteString(vertex.db_accessor().property_name(prop.first));
      WriteTypedValue(prop.second);
    }
  }

  void WriteEdge(const EdgeAccessor &edge) {
    // 0xB5 = struct 5; 0x52 = edge signature
    WriteRAW("\xB5\x52", 2);

    if (encode_ids_) {
      // IMPORTANT: this is used only in the database snapshotter!
      WriteUInt(edge.temporary_id());
      WriteUInt(edge.from().temporary_id());
      WriteUInt(edge.to().temporary_id());
    } else {
      // IMPORTANT: here we write a hardcoded 0 because we don't
      // use internal IDs, but need to give something to Bolt
      // note that OpenCypher has no id(x) function, so the client
      // should not be able to do anything with this value anyway
      WriteInt(0);
      WriteInt(0);
      WriteInt(0);
    }

    // write type
    WriteString(edge.db_accessor().edge_type_name(edge.edge_type()));

    // write properties
    const auto &props = edge.Properties();
    WriteTypeSize(props.size(), TMAP);
    for (const auto &prop : props) {
      WriteString(edge.db_accessor().property_name(prop.first));
      WriteTypedValue(prop.second);
    }
  }

  void WritePath() {
    // TODO: this isn't implemented in the backend!
  }

  void WriteTypedValue(const TypedValue &value) {
    switch (value.type()) {
      case TypedValue::Type::Null:
        WriteNull();
        break;
      case TypedValue::Type::Bool:
        WriteBool(value.Value<bool>());
        break;
      case TypedValue::Type::Int:
        WriteInt(value.Value<int64_t>());
        break;
      case TypedValue::Type::Double:
        WriteDouble(value.Value<double>());
        break;
      case TypedValue::Type::String:
        WriteString(value.Value<std::string>());
        break;
      case TypedValue::Type::List:
        WriteList(value.Value<std::vector<TypedValue>>());
        break;
      case TypedValue::Type::Map:
        WriteMap(value.Value<std::map<std::string, TypedValue>>());
        break;
      case TypedValue::Type::Vertex:
        WriteVertex(value.Value<VertexAccessor>());
        break;
      case TypedValue::Type::Edge:
        WriteEdge(value.Value<EdgeAccessor>());
        break;
      case TypedValue::Type::Path:
        // TODO: this is not implemeted yet!
        WritePath();
        break;
    }
  }

 protected:
  Buffer &buffer_;
  bool encode_ids_;

 private:
  void WriteUInt(const uint64_t &value) {
    WriteInt(*reinterpret_cast<const int64_t *>(&value));
  }
};
}
