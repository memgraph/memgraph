#pragma once

#include <cstdint>
#include <memory>

#include "distributed/serialization.capnp.h"
#include "query/typed_value.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"
#include "utils/exceptions.hpp"

namespace distributed {

void SaveVertex(const Vertex &vertex, capnp::Vertex::Builder *builder,
                int16_t worker_id);

void SaveEdge(const Edge &edge, capnp::Edge::Builder *builder,
              int16_t worker_id);

/// Alias for `SaveEdge` allowing for param type resolution.
inline void SaveElement(const Edge &record, capnp::Edge::Builder *builder,
                        int16_t worker_id) {
  return SaveEdge(record, builder, worker_id);
}

/// Alias for `SaveVertex` allowing for param type resolution.
inline void SaveElement(const Vertex &record, capnp::Vertex::Builder *builder,
                        int16_t worker_id) {
  return SaveVertex(record, builder, worker_id);
}

std::unique_ptr<Vertex> LoadVertex(const capnp::Vertex::Reader &reader);

std::unique_ptr<Edge> LoadEdge(const capnp::Edge::Reader &reader);

inline void SaveCapnpTypedValue(
    const query::TypedValue &value, capnp::TypedValue::Builder *builder,
    std::function<void(const query::TypedValue &, capnp::TypedValue::Builder *)>
        save_graph_element = nullptr) {
  switch (value.type()) {
    case query::TypedValue::Type::Null:
      builder->setNullType();
      return;
    case query::TypedValue::Type::Bool:
      builder->setBool(value.Value<bool>());
      return;
    case query::TypedValue::Type::Int:
      builder->setInteger(value.Value<int64_t>());
      return;
    case query::TypedValue::Type::Double:
      builder->setDouble(value.Value<double>());
      return;
    case query::TypedValue::Type::String:
      builder->setString(value.Value<std::string>());
      return;
    case query::TypedValue::Type::List: {
      const auto &values = value.Value<std::vector<query::TypedValue>>();
      auto list_builder = builder->initList(values.size());
      for (size_t i = 0; i < values.size(); ++i) {
        auto value_builder = list_builder[i];
        SaveCapnpTypedValue(values[i], &value_builder, save_graph_element);
      }
      return;
    }
    case query::TypedValue::Type::Map: {
      const auto &map = value.Value<std::map<std::string, query::TypedValue>>();
      auto map_builder = builder->initMap(map.size());
      size_t i = 0;
      for (const auto &kv : map) {
        auto kv_builder = map_builder[i];
        kv_builder.setKey(kv.first);
        auto value_builder = kv_builder.initValue();
        SaveCapnpTypedValue(kv.second, &value_builder, save_graph_element);
        ++i;
      }
      return;
    }
    case query::TypedValue::Type::Vertex:
    case query::TypedValue::Type::Edge:
    case query::TypedValue::Type::Path:
      if (save_graph_element) {
        save_graph_element(value, builder);
      } else {
        throw utils::BasicException(
            "Unable to serialize TypedValue of type: {}", value.type());
      }
  }
}

inline void LoadCapnpTypedValue(
    const capnp::TypedValue::Reader &reader, query::TypedValue *value,
    std::function<void(const capnp::TypedValue::Reader &, query::TypedValue *)>
        load_graph_element = nullptr) {
  switch (reader.which()) {
    case distributed::capnp::TypedValue::NULL_TYPE:
      *value = query::TypedValue::Null;
      return;
    case distributed::capnp::TypedValue::BOOL:
      *value = reader.getBool();
      return;
    case distributed::capnp::TypedValue::INTEGER:
      *value = reader.getInteger();
      return;
    case distributed::capnp::TypedValue::DOUBLE:
      *value = reader.getDouble();
      return;
    case distributed::capnp::TypedValue::STRING:
      *value = reader.getString().cStr();
      return;
    case distributed::capnp::TypedValue::LIST: {
      std::vector<query::TypedValue> list;
      list.reserve(reader.getList().size());
      for (const auto &value_reader : reader.getList()) {
        list.emplace_back();
        LoadCapnpTypedValue(value_reader, &list.back(), load_graph_element);
      }
      *value = list;
      return;
    }
    case distributed::capnp::TypedValue::MAP: {
      std::map<std::string, query::TypedValue> map;
      for (const auto &kv_reader : reader.getMap()) {
        auto key = kv_reader.getKey().cStr();
        LoadCapnpTypedValue(kv_reader.getValue(), &map[key],
                            load_graph_element);
      }
      *value = map;
      return;
    }
    case distributed::capnp::TypedValue::VERTEX:
    case distributed::capnp::TypedValue::EDGE:
    case distributed::capnp::TypedValue::PATH:
      if (load_graph_element) {
        load_graph_element(reader, value);
      } else {
        throw utils::BasicException(
            "Unexpected TypedValue type '{}' when loading from archive",
            reader.which());
      }
  }
}

}  // namespace distributed
