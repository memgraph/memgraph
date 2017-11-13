#pragma once

#include "communication/bolt/v1/encoder/primitive_encoder.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/typed_value.hpp"

namespace communication::bolt {

/**
 * Bolt BaseEncoder. Subclass of PrimitiveEncoder. Extends it with the
 * capability to encode TypedValues (as well as lists and maps of TypedValues),
 * Edges, Vertices and Paths.
 *
 * @tparam Buffer the output buffer that should be used
 */
template <typename Buffer>
class BaseEncoder : public PrimitiveEncoder<Buffer> {
 public:
  explicit BaseEncoder(Buffer &buffer) : PrimitiveEncoder<Buffer>(buffer) {}

  void WriteList(const std::vector<query::TypedValue> &value) {
    this->WriteTypeSize(value.size(), MarkerList);
    for (auto &x : value) WriteTypedValue(x);
  }

  /**
   * Writes a map value.
   *
   * @tparam TMap - an iterable of (std::string, TypedValue) pairs.
   */
  template <typename TMap>
  void WriteMap(const TMap &value) {
    this->WriteTypeSize(value.size(), MarkerMap);
    for (auto &x : value) {
      this->WriteString(x.first);
      WriteTypedValue(x.second);
    }
  }

  void WriteVertex(const VertexAccessor &vertex) {
    this->WriteRAW(underlying_cast(Marker::TinyStruct) + 3);
    this->WriteRAW(underlying_cast(Signature::Node));
    WriteUInt(vertex.id());

    // write labels
    const auto &labels = vertex.labels();
    this->WriteTypeSize(labels.size(), MarkerList);
    for (const auto &label : labels)
      this->WriteString(vertex.db_accessor().LabelName(label));

    // write properties
    const auto &props = vertex.Properties();
    this->WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      this->WriteString(vertex.db_accessor().PropertyName(prop.first));
      WriteTypedValue(prop.second);
    }
  }

  void WriteEdge(const EdgeAccessor &edge, bool unbound = false) {
    this->WriteRAW(underlying_cast(Marker::TinyStruct) + (unbound ? 3 : 5));
    this->WriteRAW(underlying_cast(unbound ? Signature::UnboundRelationship
                                           : Signature::Relationship));

    WriteUInt(edge.id());
    if (!unbound) {
      WriteUInt(edge.from().id());
      WriteUInt(edge.to().id());
    }

    // write type
    this->WriteString(edge.db_accessor().EdgeTypeName(edge.EdgeType()));

    // write properties
    const auto &props = edge.Properties();
    this->WriteTypeSize(props.size(), MarkerMap);
    for (const auto &prop : props) {
      this->WriteString(edge.db_accessor().PropertyName(prop.first));
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
    this->WriteRAW(underlying_cast(Marker::TinyStruct) + 3);
    this->WriteRAW(underlying_cast(Signature::Path));
    this->WriteTypeSize(vertices.size(), MarkerList);
    for (auto &v : vertices) WriteVertex(v);
    this->WriteTypeSize(edges.size(), MarkerList);
    for (auto &e : edges) WriteEdge(e, true);
    this->WriteTypeSize(indices.size(), MarkerList);
    for (auto &i : indices) this->WriteInt(i);
  }

  void WriteTypedValue(const query::TypedValue &value) {
    switch (value.type()) {
      case query::TypedValue::Type::Null:
        this->WriteNull();
        break;
      case query::TypedValue::Type::Bool:
        this->WriteBool(value.Value<bool>());
        break;
      case query::TypedValue::Type::Int:
        this->WriteInt(value.Value<int64_t>());
        break;
      case query::TypedValue::Type::Double:
        this->WriteDouble(value.Value<double>());
        break;
      case query::TypedValue::Type::String:
        this->WriteString(value.Value<std::string>());
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

 private:
  void WriteUInt(const uint64_t &value) {
    this->WriteInt(*reinterpret_cast<const int64_t *>(&value));
  }
};
}  // namespace communication::bolt
