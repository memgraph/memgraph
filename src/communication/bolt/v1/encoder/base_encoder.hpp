#pragma once

#include "communication/bolt/v1/encoder/primitive_encoder.hpp"
#include "communication/bolt/v1/value.hpp"

namespace communication::bolt {

/**
 * Bolt BaseEncoder. Subclass of PrimitiveEncoder. Extends it with the
 * capability to encode Values (as well as lists and maps of Values), Edges,
 * Vertices and Paths.
 *
 * @tparam Buffer the output buffer that should be used
 */
template <typename Buffer>
class BaseEncoder : public PrimitiveEncoder<Buffer> {
 public:
  explicit BaseEncoder(Buffer &buffer) : PrimitiveEncoder<Buffer>(buffer) {}

  void WriteList(const std::vector<Value> &value) {
    this->WriteTypeSize(value.size(), MarkerList);
    for (auto &x : value) WriteValue(x);
  }

  /**
   * Writes a map value.
   *
   * @tparam TMap - an iterable of (std::string, Value) pairs.
   */
  template <typename TMap>
  void WriteMap(const TMap &value) {
    this->WriteTypeSize(value.size(), MarkerMap);
    for (auto &x : value) {
      this->WriteString(x.first);
      WriteValue(x.second);
    }
  }

  void WriteVertex(const Vertex &vertex) {
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
      WriteValue(prop.second);
    }
  }

  void WriteEdge(const Edge &edge, bool unbound = false) {
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
      WriteValue(prop.second);
    }
  }

  void WriteEdge(const UnboundedEdge &edge) {
    this->WriteRAW(utils::UnderlyingCast(Marker::TinyStruct) + 3);
    this->WriteRAW(utils::UnderlyingCast(Signature::UnboundRelationship));

    this->WriteInt(edge.id.AsInt());

    this->WriteString(edge.type);

    const auto &props = edge.properties;
    this->WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      this->WriteString(prop.first);
      WriteValue(prop.second);
    }
  }

  void WritePath(const Path &path) {
    this->WriteRAW(utils::UnderlyingCast(Marker::TinyStruct) + 3);
    this->WriteRAW(utils::UnderlyingCast(Signature::Path));
    this->WriteTypeSize(path.vertices.size(), MarkerList);
    for (auto &v : path.vertices) WriteVertex(v);
    this->WriteTypeSize(path.edges.size(), MarkerList);
    for (auto &e : path.edges) WriteEdge(e);
    this->WriteTypeSize(path.indices.size(), MarkerList);
    for (auto &i : path.indices) this->WriteInt(i);
  }

  void WriteValue(const Value &value) {
    switch (value.type()) {
      case Value::Type::Null:
        this->WriteNull();
        break;
      case Value::Type::Bool:
        this->WriteBool(value.ValueBool());
        break;
      case Value::Type::Int:
        this->WriteInt(value.ValueInt());
        break;
      case Value::Type::Double:
        this->WriteDouble(value.ValueDouble());
        break;
      case Value::Type::String:
        this->WriteString(value.ValueString());
        break;
      case Value::Type::List:
        WriteList(value.ValueList());
        break;
      case Value::Type::Map:
        WriteMap(value.ValueMap());
        break;
      case Value::Type::Vertex:
        WriteVertex(value.ValueVertex());
        break;
      case Value::Type::Edge:
        WriteEdge(value.ValueEdge());
        break;
      case Value::Type::UnboundedEdge:
        WriteEdge(value.ValueUnboundedEdge());
        break;
      case Value::Type::Path:
        WritePath(value.ValuePath());
        break;
    }
  }
};

}  // namespace communication::bolt
