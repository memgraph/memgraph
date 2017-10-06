#pragma once

#include "communication/bolt/v1/codes.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/typed_value.hpp"
#include "utils/bswap.hpp"

#include <string>

namespace communication::bolt {

/**
 * Bolt BaseEncoder. Has public interfaces for writing Bolt encoded data.
 * Supported types are: Null, Bool, Int, Double, String, List, Map, Vertex, Edge
 *
 * This class has a dual purpose. The first is streaming of bolt data to network
 * clients. The second is streaming to disk in the database snapshotter.
 *
 * @tparam Buffer the output buffer that should be used
 */
template <typename Buffer>
class BaseEncoder {
 public:
  BaseEncoder(Buffer &buffer) : buffer_(buffer) {}

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
    } else if (size <= 65535) {
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

  /**
   * Writes a map value.
   *
   * @tparam TMap - an iterable of (std::string, TypedValue) pairs.
   */
  template <typename TMap>
  void WriteMap(const TMap &value) {
    WriteTypeSize(value.size(), MarkerMap);
    for (auto &x : value) {
      WriteString(x.first);
      WriteTypedValue(x.second);
    }
  }

  void WriteVertex(const VertexAccessor &vertex) {
    WriteRAW(underlying_cast(Marker::TinyStruct) + 3);
    WriteRAW(underlying_cast(Signature::Node));
    WriteUInt(vertex.temporary_id());

    // write labels
    const auto &labels = vertex.labels();
    WriteTypeSize(labels.size(), MarkerList);
    for (const auto &label : labels)
      WriteString(vertex.db_accessor().LabelName(label));

    // write properties
    const auto &props = vertex.Properties();
    WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      WriteString(vertex.db_accessor().PropertyName(prop.first));
      WriteTypedValue(prop.second);
    }
  }

  void WriteEdge(const EdgeAccessor &edge, bool unbound = false) {
    WriteRAW(underlying_cast(Marker::TinyStruct) + (unbound ? 3 : 5));
    WriteRAW(underlying_cast(unbound ? Signature::UnboundRelationship
                                     : Signature::Relationship));

    WriteUInt(edge.temporary_id());
    if (!unbound) {
      WriteUInt(edge.from().temporary_id());
      WriteUInt(edge.to().temporary_id());
    }

    // write type
    WriteString(edge.db_accessor().EdgeTypeName(edge.EdgeType()));

    // write properties
    const auto &props = edge.Properties();
    WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      WriteString(edge.db_accessor().PropertyName(prop.first));
      WriteTypedValue(prop.second);
    }
  }

  void WritePath(const query::Path &path) {
    // Prepare the data structures to be written.
    //
    // Unique vertices in the path.
    std::vector<VertexAccessor> vertices;
    // Unique edges in the path.
    std::vector<EdgeAccessor> edges;
    // Indices that map path positions to vertices/edges elements. Positive
    // indices for left-to-right directionality and negative for right-to-left.
    std::vector<int> indices;

    // Helper function. Looks for the given element in the collection. If found
    // it puts it's index into `indices`. Otherwise emplaces the given element
    // into the collection and puts that index into `indices`. A multiplier is
    // added to switch between positive and negative indices (that define edge
    // direction).
    auto add_element = [&indices](auto &collection, const auto &element,
                                  int multiplier, int offset) {
      auto found = std::find(collection.begin(), collection.end(), element);
      indices.emplace_back(multiplier *
                           (std::distance(collection.begin(), found) + offset));
      if (found == collection.end()) collection.emplace_back(element);
    };

    vertices.emplace_back(path.vertices()[0]);
    for (uint i = 0; i < path.size(); i++) {
      const auto &e = path.edges()[i];
      const auto &v = path.vertices()[i + 1];
      add_element(edges, e, e.to_is(v) ? 1 : -1, 1);
      add_element(vertices, v, 1, 0);
    }

    // Write data.
    WriteRAW(underlying_cast(Marker::TinyStruct) + 3);
    WriteRAW(underlying_cast(Signature::Path));
    WriteTypeSize(vertices.size(), MarkerList);
    for (auto &v : vertices) WriteVertex(v);
    WriteTypeSize(edges.size(), MarkerList);
    for (auto &e : edges) WriteEdge(e, true);
    WriteTypeSize(indices.size(), MarkerList);
    for (auto &i : indices) WriteInt(i);
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
        WritePath(value.ValuePath());
        break;
    }
  }

 protected:
  Buffer &buffer_;

 private:
  void WriteUInt(const uint64_t &value) {
    WriteInt(*reinterpret_cast<const int64_t *>(&value));
  }
};
}
