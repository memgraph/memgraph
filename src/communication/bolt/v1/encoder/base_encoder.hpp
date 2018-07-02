#pragma once

#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "communication/bolt/v1/encoder/primitive_encoder.hpp"

namespace communication::bolt {

/**
 * Bolt BaseEncoder. Subclass of PrimitiveEncoder. Extends it with the
 * capability to encode DecodedValues (as well as lists and maps of
 * DecodedValues), Edges, Vertices and Paths.
 *
 * @tparam Buffer the output buffer that should be used
 */
template <typename Buffer>
class BaseEncoder : public PrimitiveEncoder<Buffer> {
 public:
  explicit BaseEncoder(Buffer &buffer) : PrimitiveEncoder<Buffer>(buffer) {}

  void WriteList(const std::vector<DecodedValue> &value) {
    this->WriteTypeSize(value.size(), MarkerList);
    for (auto &x : value) WriteDecodedValue(x);
  }

  /**
   * Writes a map value.
   *
   * @tparam TMap - an iterable of (std::string, DecodedValue) pairs.
   */
  template <typename TMap>
  void WriteMap(const TMap &value) {
    this->WriteTypeSize(value.size(), MarkerMap);
    for (auto &x : value) {
      this->WriteString(x.first);
      WriteDecodedValue(x.second);
    }
  }

  void WriteVertex(const DecodedVertex &vertex) {
    this->WriteRAW(utils::UnderlyingCast(Marker::TinyStruct) + 3);
    this->WriteRAW(utils::UnderlyingCast(Signature::Node));
    this->WriteInt(vertex.id.AsInt());

    // write labels
    const auto &labels = vertex.labels;
    this->WriteTypeSize(labels.size(), MarkerList);
    for (const auto &label : labels) this->WriteString(label);

    // write properties
    const auto &props = vertex.properties;
    this->WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      this->WriteString(prop.first);
      WriteDecodedValue(prop.second);
    }
  }

  void WriteEdge(const DecodedEdge &edge, bool unbound = false) {
    this->WriteRAW(utils::UnderlyingCast(Marker::TinyStruct) +
                   (unbound ? 3 : 5));
    this->WriteRAW(utils::UnderlyingCast(
        unbound ? Signature::UnboundRelationship : Signature::Relationship));

    this->WriteInt(edge.id.AsInt());
    if (!unbound) {
      this->WriteInt(edge.from.AsInt());
      this->WriteInt(edge.to.AsInt());
    }

    this->WriteString(edge.type);

    const auto &props = edge.properties;
    this->WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      this->WriteString(prop.first);
      WriteDecodedValue(prop.second);
    }
  }

  void WriteEdge(const DecodedUnboundedEdge &edge) {
    this->WriteRAW(utils::UnderlyingCast(Marker::TinyStruct) + 3);
    this->WriteRAW(utils::UnderlyingCast(Signature::UnboundRelationship));

    this->WriteInt(edge.id.AsInt());

    this->WriteString(edge.type);

    const auto &props = edge.properties;
    this->WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      this->WriteString(prop.first);
      WriteDecodedValue(prop.second);
    }
  }

  void WritePath(const DecodedPath &path) {
    this->WriteRAW(utils::UnderlyingCast(Marker::TinyStruct) + 3);
    this->WriteRAW(utils::UnderlyingCast(Signature::Path));
    this->WriteTypeSize(path.vertices.size(), MarkerList);
    for (auto &v : path.vertices) WriteVertex(v);
    this->WriteTypeSize(path.edges.size(), MarkerList);
    for (auto &e : path.edges) WriteEdge(e);
    this->WriteTypeSize(path.indices.size(), MarkerList);
    for (auto &i : path.indices) this->WriteInt(i);
  }

  void WriteDecodedValue(const DecodedValue &value) {
    switch (value.type()) {
      case DecodedValue::Type::Null:
        this->WriteNull();
        break;
      case DecodedValue::Type::Bool:
        this->WriteBool(value.ValueBool());
        break;
      case DecodedValue::Type::Int:
        this->WriteInt(value.ValueInt());
        break;
      case DecodedValue::Type::Double:
        this->WriteDouble(value.ValueDouble());
        break;
      case DecodedValue::Type::String:
        this->WriteString(value.ValueString());
        break;
      case DecodedValue::Type::List:
        WriteList(value.ValueList());
        break;
      case DecodedValue::Type::Map:
        WriteMap(value.ValueMap());
        break;
      case DecodedValue::Type::Vertex:
        WriteVertex(value.ValueVertex());
        break;
      case DecodedValue::Type::Edge:
        WriteEdge(value.ValueEdge());
        break;
      case DecodedValue::Type::UnboundedEdge:
        WriteEdge(value.ValueUnboundedEdge());
        break;
      case DecodedValue::Type::Path:
        WritePath(value.ValuePath());
        break;
    }
  }
};

}  // namespace communication::bolt
