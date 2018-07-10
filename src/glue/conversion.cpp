#include "glue/conversion.hpp"

#include <map>
#include <string>
#include <vector>

#include "database/graph_db_accessor.hpp"

using communication::bolt::DecodedValue;

namespace glue {

query::TypedValue ToTypedValue(const DecodedValue &value) {
  switch (value.type()) {
    case DecodedValue::Type::Null:
      return query::TypedValue::Null;
    case DecodedValue::Type::Bool:
      return query::TypedValue(value.ValueBool());
    case DecodedValue::Type::Int:
      return query::TypedValue(value.ValueInt());
    case DecodedValue::Type::Double:
      return query::TypedValue(value.ValueDouble());
    case DecodedValue::Type::String:
      return query::TypedValue(value.ValueString());
    case DecodedValue::Type::List: {
      std::vector<query::TypedValue> list;
      list.reserve(value.ValueList().size());
      for (const auto &v : value.ValueList()) list.push_back(ToTypedValue(v));
      return query::TypedValue(list);
    }
    case DecodedValue::Type::Map: {
      std::map<std::string, query::TypedValue> map;
      for (const auto &kv : value.ValueMap())
        map.emplace(kv.first, ToTypedValue(kv.second));
      return query::TypedValue(map);
    }
    case DecodedValue::Type::Vertex:
    case DecodedValue::Type::Edge:
    case DecodedValue::Type::UnboundedEdge:
    case DecodedValue::Type::Path:
      throw communication::bolt::DecodedValueException(
          "Unsupported conversion from DecodedValue to TypedValue");
  }
}

DecodedValue ToDecodedValue(const query::TypedValue &value) {
  switch (value.type()) {
    case query::TypedValue::Type::Null:
      return DecodedValue();
    case query::TypedValue::Type::Bool:
      return DecodedValue(value.ValueBool());
    case query::TypedValue::Type::Int:
      return DecodedValue(value.ValueInt());
    case query::TypedValue::Type::Double:
      return DecodedValue(value.ValueDouble());
    case query::TypedValue::Type::String:
      return DecodedValue(value.ValueString());
    case query::TypedValue::Type::List: {
      std::vector<DecodedValue> values;
      values.reserve(value.ValueList().size());
      for (const auto &v : value.ValueList()) {
        values.push_back(ToDecodedValue(v));
      }
      return DecodedValue(values);
    }
    case query::TypedValue::Type::Map: {
      std::map<std::string, DecodedValue> map;
      for (const auto &kv : value.ValueMap()) {
        map.emplace(kv.first, ToDecodedValue(kv.second));
      }
      return DecodedValue(map);
    }
    case query::TypedValue::Type::Vertex:
      return DecodedValue(ToDecodedVertex(value.ValueVertex()));
    case query::TypedValue::Type::Edge:
      return DecodedValue(ToDecodedEdge(value.ValueEdge()));
    case query::TypedValue::Type::Path:
      return DecodedValue(ToDecodedPath(value.ValuePath()));
  }
}

communication::bolt::DecodedVertex ToDecodedVertex(
    const VertexAccessor &vertex) {
  auto id = communication::bolt::Id::FromUint(vertex.gid());
  std::vector<std::string> labels;
  labels.reserve(vertex.labels().size());
  for (const auto &label : vertex.labels()) {
    labels.push_back(vertex.db_accessor().LabelName(label));
  }
  std::map<std::string, DecodedValue> properties;
  for (const auto &prop : vertex.Properties()) {
    properties[vertex.db_accessor().PropertyName(prop.first)] =
        ToDecodedValue(prop.second);
  }
  return communication::bolt::DecodedVertex{id, labels, properties};
}

communication::bolt::DecodedEdge ToDecodedEdge(const EdgeAccessor &edge) {
  auto id = communication::bolt::Id::FromUint(edge.gid());
  auto from = communication::bolt::Id::FromUint(edge.from().gid());
  auto to = communication::bolt::Id::FromUint(edge.to().gid());
  auto type = edge.db_accessor().EdgeTypeName(edge.EdgeType());
  std::map<std::string, DecodedValue> properties;
  for (const auto &prop : edge.Properties()) {
    properties[edge.db_accessor().PropertyName(prop.first)] =
        ToDecodedValue(prop.second);
  }
  return communication::bolt::DecodedEdge{id, from, to, type, properties};
}

communication::bolt::DecodedPath ToDecodedPath(const query::Path &path) {
  std::vector<communication::bolt::DecodedVertex> vertices;
  vertices.reserve(path.vertices().size());
  for (const auto &v : path.vertices()) {
    vertices.push_back(ToDecodedVertex(v));
  }
  std::vector<communication::bolt::DecodedEdge> edges;
  edges.reserve(path.edges().size());
  for (const auto &e : path.edges()) {
    edges.push_back(ToDecodedEdge(e));
  }
  return communication::bolt::DecodedPath(vertices, edges);
}

PropertyValue ToPropertyValue(const DecodedValue &value) {
  switch (value.type()) {
    case DecodedValue::Type::Null:
      return PropertyValue::Null;
    case DecodedValue::Type::Bool:
      return PropertyValue(value.ValueBool());
    case DecodedValue::Type::Int:
      return PropertyValue(value.ValueInt());
    case DecodedValue::Type::Double:
      return PropertyValue(value.ValueDouble());
    case DecodedValue::Type::String:
      return PropertyValue(value.ValueString());
    case DecodedValue::Type::List: {
      std::vector<PropertyValue> vec;
      vec.reserve(value.ValueList().size());
      for (const auto &value : value.ValueList())
        vec.emplace_back(ToPropertyValue(value));
      return PropertyValue(std::move(vec));
    }
    case DecodedValue::Type::Map: {
      std::map<std::string, PropertyValue> map;
      for (const auto &kv : value.ValueMap())
        map.emplace(kv.first, ToPropertyValue(kv.second));
      return PropertyValue(std::move(map));
    }
    case DecodedValue::Type::Vertex:
    case DecodedValue::Type::Edge:
    case DecodedValue::Type::UnboundedEdge:
    case DecodedValue::Type::Path:
      throw communication::bolt::DecodedValueException(
          "Unsupported conversion from DecodedValue to PropertyValue");
  }
}

DecodedValue ToDecodedValue(const PropertyValue &value) {
  switch (value.type()) {
    case PropertyValue::Type::Null:
      return DecodedValue();
    case PropertyValue::Type::Bool:
      return DecodedValue(value.Value<bool>());
    case PropertyValue::Type::Int:
      return DecodedValue(value.Value<int64_t>());
      break;
    case PropertyValue::Type::Double:
      return DecodedValue(value.Value<double>());
    case PropertyValue::Type::String:
      return DecodedValue(value.Value<std::string>());
    case PropertyValue::Type::List: {
      const auto &values = value.Value<std::vector<PropertyValue>>();
      std::vector<DecodedValue> vec;
      vec.reserve(values.size());
      for (const auto &v : values) {
        vec.push_back(ToDecodedValue(v));
      }
      return DecodedValue(vec);
    }
    case PropertyValue::Type::Map: {
      const auto &map = value.Value<std::map<std::string, PropertyValue>>();
      std::map<std::string, DecodedValue> dv_map;
      for (const auto &kv : map) {
        dv_map.emplace(kv.first, ToDecodedValue(kv.second));
      }
      return DecodedValue(dv_map);
    }
  }
}

}  // namespace glue
