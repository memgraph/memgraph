#pragma once

#include "database/graph_db_accessor.hpp"
#include "logging/default.hpp"
#include "query/backend/cpp/typed_value.hpp"
#include "utils/bswap.hpp"

#include <string>

namespace communication::bolt {

static constexpr uint8_t TSTRING = 0, TLIST = 1, TMAP = 2;
static constexpr uint8_t type_tiny_marker[3] = { 0x80, 0x90, 0xA0 };
static constexpr uint8_t type_8_marker[3] = { 0xD0, 0xD4, 0xD8 };
static constexpr uint8_t type_16_marker[3] = { 0xD1, 0xD5, 0xD9 };
static constexpr uint8_t type_32_marker[3] = { 0xD2, 0xD6, 0xDA };


/**
 * Bolt Encoder.
 * Has public interfaces for writing Bolt specific response messages.
 * Supported messages are: Record, Success, Failure and Ignored.
 *
 * @tparam Buffer the output buffer that should be used
 * @tparam Socket the output socket that should be used
 */
template <typename Buffer, typename Socket>
class Encoder {

 public:
  Encoder(Socket& socket) : socket_(socket), buffer_(socket), logger_(logging::log->logger("communication::bolt::Encoder")) {}

  /**
   * Sends a Record message.
   *
   * From the Bolt v1 documentation:
   *   RecordMessage (signature=0x71) {
   *     List<Value> fields
   *   }
   *
   * @param values the fields list object that should be sent
   */
  void MessageRecord(const std::vector<TypedValue>& values) {
    // 0xB1 = struct 1; 0x71 = record signature
    WriteRAW("\xB1\x71", 2);
    WriteList(values);
    buffer_.Flush();
  }

  /**
   * Sends a Success message.
   *
   * From the Bolt v1 documentation:
   *   SuccessMessage (signature=0x70) {
   *     Map<String,Value> metadata
   *   }
   *
   * @param metadata the metadata map object that should be sent
   */
  void MessageSuccess(const std::map<std::string, TypedValue>& metadata) {
    // 0xB1 = struct 1; 0x70 = success signature
    WriteRAW("\xB1\x70", 2);
    WriteMap(metadata);
    buffer_.Flush();
  }

  /**
   * Sends a Failure message.
   *
   * From the Bolt v1 documentation:
   *   FailureMessage (signature=0x7F) {
   *     Map<String,Value> metadata
   *   }
   *
   * @param metadata the metadata map object that should be sent
   */
  void MessageFailure(const std::map<std::string, TypedValue>& metadata) {
    // 0xB1 = struct 1; 0x7F = failure signature
    WriteRAW("\xB1\x7F", 2);
    WriteMap(metadata);
    buffer_.Flush();
  }

  /**
   * Sends an Ignored message.
   *
   * From the bolt v1 documentation:
   *   IgnoredMessage (signature=0x7E) {
   *     Map<String,Value> metadata
   *   }
   *
   * @param metadata the metadata map object that should be sent
   */
  void MessageIgnored(const std::map<std::string, TypedValue>& metadata) {
    // 0xB1 = struct 1; 0x7E = ignored signature
    WriteRAW("\xB1\x7E", 2);
    WriteMap(metadata);
    buffer_.Flush();
  }

  /**
   * Sends an Ignored message.
   *
   * This function sends an ignored message without additional metadata.
   */
  void MessageIgnored() {
    // 0xB0 = struct 0; 0x7E = ignored signature
    WriteRAW("\xB0\x7E", 2);
    buffer_.Flush();
  }


 private:
  Socket& socket_;
  Buffer buffer_;
  Logger logger_;


  void WriteRAW(const uint8_t* data, uint64_t len) {
    buffer_.Write(data, len);
  }

  void WriteRAW(const char* data, uint64_t len) {
    WriteRAW((const uint8_t*) data, len);
  }

  void WriteRAW(const uint8_t data) {
    WriteRAW(&data, 1);
  }

  template <class T>
  void WriteValue(T value) {
    value = bswap(value);
    WriteRAW(reinterpret_cast<const uint8_t *>(&value), sizeof(value));
  }

  void WriteNull() {
    // 0xC0 = null marker
    WriteRAW(0xC0);
  }

  void WriteBool(const bool& value) {
    if (value) {
      // 0xC3 = true marker
      WriteRAW(0xC3);
    } else {
      // 0xC2 = false marker
      WriteRAW(0xC2);
    }
  }

  void WriteInt(const int64_t& value) {
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

  void WriteDouble(const double& value) {
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

  void WriteString(const std::string& value) {
    WriteTypeSize(value.size(), TSTRING);
    WriteRAW(value.c_str(), value.size());
  }

  void WriteList(const std::vector<TypedValue>& value) {
    WriteTypeSize(value.size(), TLIST);
    for (auto& x: value) WriteTypedValue(x);
  }

  void WriteMap(const std::map<std::string, TypedValue>& value) {
    WriteTypeSize(value.size(), TMAP);
    for (auto& x: value) {
      WriteString(x.first);
      WriteTypedValue(x.second);
    }
  }

  void WriteVertex(const VertexAccessor& vertex) {
    // 0xB3 = struct 3; 0x4E = vertex signature
    WriteRAW("\xB3\x4E", 2);

    // IMPORTANT: here we write a hardcoded 0 because we don't
    // use internal IDs, but need to give something to Bolt
    // note that OpenCypher has no id(x) function, so the client
    // should not be able to do anything with this value anyway
    WriteInt(0);

    // write labels
    const auto& labels = vertex.labels();
    WriteTypeSize(labels.size(), TLIST);
    for (const auto& label : labels)
      WriteString(vertex.db_accessor().label_name(label));

    // write properties
    const auto& props = vertex.Properties();
    WriteTypeSize(props.size(), TMAP);
    for (const auto& prop : props) {
      WriteString(vertex.db_accessor().property_name(prop.first));
      WriteTypedValue(prop.second);
    }
  }


  void WriteEdge(const EdgeAccessor& edge) {
    // 0xB5 = struct 5; 0x52 = edge signature
    WriteRAW("\xB5\x52", 2);

    // IMPORTANT: here we write a hardcoded 0 because we don't
		// use internal IDs, but need to give something to Bolt
		// note that OpenCypher has no id(x) function, so the client
		// should not be able to do anything with this value anyway
		WriteInt(0);
		WriteInt(0);
		WriteInt(0);

		// write type
		WriteString(edge.db_accessor().edge_type_name(edge.edge_type()));

		// write properties
		const auto& props = edge.Properties();
		WriteTypeSize(props.size(), TMAP);
		for (const auto& prop : props) {
      WriteString(edge.db_accessor().property_name(prop.first));
      WriteTypedValue(prop.second);
		}
  }

  void WritePath() {
    // TODO: this isn't implemented in the backend!
  }

  void WriteTypedValue(const TypedValue& value) {
    switch(value.type()) {
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
};
}
