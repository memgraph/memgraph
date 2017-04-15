#pragma once

#include "communication/bolt/v1/codes.hpp"
#include "database/graph_db_accessor.hpp"
#include "logging/default.hpp"
#include "logging/logger.hpp"
#include "query/typed_value.hpp"
#include "utils/bswap.hpp"

#include <string>

namespace communication::bolt {

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

  void WriteNull() { WriteRAW(underlying_cast(Marker::Null)); }

  void WriteBool(const bool &value) {
    if (value)
      WriteRAW(underlying_cast(Marker::True));
    else
      WriteRAW(underlying_cast(Marker::False));
  }

  void WriteInt(const int64_t &value) {
    if (value >= -16L && value < 128L) {
      WriteRAW(static_cast<uint8_t>(value));
    } else if (value >= -128L && value < -16L) {
      WriteRAW(underlying_cast(Marker::Int8));
      WriteRAW(static_cast<uint8_t>(value));
    } else if (value >= -32768L && value < 32768L) {
      WriteRAW(underlying_cast(Marker::Int16));
      WriteValue(static_cast<int16_t>(value));
    } else if (value >= -2147483648L && value < 2147483648L) {
      WriteRAW(underlying_cast(Marker::Int32));
      WriteValue(static_cast<int32_t>(value));
    } else {
      WriteRAW(underlying_cast(Marker::Int64));
      WriteValue(value);
    }
  }

  void WriteDouble(const double &value) {
    WriteRAW(underlying_cast(Marker::Float64));
    WriteValue(*reinterpret_cast<const int64_t *>(&value));
  }

  void WriteTypeSize(const size_t size, const uint8_t typ) {
    if (size <= 15) {
      uint8_t len = size;
      len &= 0x0F;
      WriteRAW(underlying_cast(MarkerTiny[typ]) + len);
    } else if (size <= 255) {
      uint8_t len = size;
      WriteRAW(underlying_cast(Marker8[typ]));
      WriteRAW(len);
    } else if (size <= 65536) {
      uint16_t len = size;
      WriteRAW(underlying_cast(Marker16[typ]));
      WriteValue(len);
    } else {
      uint32_t len = size;
      WriteRAW(underlying_cast(Marker32[typ]));
      WriteValue(len);
    }
  }

  void WriteString(const std::string &value) {
    WriteTypeSize(value.size(), MarkerString);
    WriteRAW(value.c_str(), value.size());
  }

  void WriteList(const std::vector<query::TypedValue> &value) {
    WriteTypeSize(value.size(), MarkerList);
    for (auto &x : value) WriteTypedValue(x);
  }

  void WriteMap(const std::map<std::string, query::TypedValue> &value) {
    WriteTypeSize(value.size(), MarkerMap);
    for (auto &x : value) {
      WriteString(x.first);
      WriteTypedValue(x.second);
    }
  }

  void WriteVertex(const VertexAccessor &vertex) {
    WriteRAW(underlying_cast(Marker::TinyStruct) + 3);
    WriteRAW(underlying_cast(Signature::Node));

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
    WriteTypeSize(labels.size(), MarkerList);
    for (const auto &label : labels)
      WriteString(vertex.db_accessor().label_name(label));

    // write properties
    const auto &props = vertex.Properties();
    WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      WriteString(vertex.db_accessor().property_name(prop.first));
      WriteTypedValue(prop.second);
    }
  }

  void WriteEdge(const EdgeAccessor &edge) {
    WriteRAW(underlying_cast(Marker::TinyStruct) + 5);
    WriteRAW(underlying_cast(Signature::Relationship));

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
    WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      WriteString(edge.db_accessor().property_name(prop.first));
      WriteTypedValue(prop.second);
    }
  }

  void WritePath() {
    // TODO: this isn't implemented in the backend!
  }

  void WriteTypedValue(const query::TypedValue &value) {
    switch (value.type()) {
      case query::TypedValue::Type::Null:
        WriteNull();
        break;
      case query::TypedValue::Type::Bool:
        WriteBool(value.Value<bool>());
        break;
      case query::TypedValue::Type::Int:
        WriteInt(value.Value<int64_t>());
        break;
      case query::TypedValue::Type::Double:
        WriteDouble(value.Value<double>());
        break;
      case query::TypedValue::Type::String:
        WriteString(value.Value<std::string>());
        break;
      case query::TypedValue::Type::List:
        WriteList(value.Value<std::vector<query::TypedValue>>());
        break;
      case query::TypedValue::Type::Map:
        WriteMap(value.Value<std::map<std::string, query::TypedValue>>());
        break;
      case query::TypedValue::Type::Vertex:
        WriteVertex(value.Value<VertexAccessor>());
        break;
      case query::TypedValue::Type::Edge:
        WriteEdge(value.Value<EdgeAccessor>());
        break;
      case query::TypedValue::Type::Path:
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
