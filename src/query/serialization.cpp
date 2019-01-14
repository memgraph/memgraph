#include "query/serialization.hpp"

#include "distributed/data_manager.hpp"
#include "query/frontend/ast/ast_serialization.hpp"
#include "utils/serialization.hpp"

namespace query {

void SaveCapnpTypedValue(const TypedValue &value,
                         capnp::TypedValue::Builder *builder,
                         storage::SendVersions versions, int worker_id) {
  switch (value.type()) {
    case TypedValue::Type::Null:
      builder->setNullType();
      return;
    case TypedValue::Type::Bool:
      builder->setBool(value.Value<bool>());
      return;
    case TypedValue::Type::Int:
      builder->setInteger(value.Value<int64_t>());
      return;
    case TypedValue::Type::Double:
      builder->setDouble(value.Value<double>());
      return;
    case TypedValue::Type::String:
      builder->setString(value.Value<std::string>());
      return;
    case TypedValue::Type::List: {
      const auto &values = value.Value<std::vector<TypedValue>>();
      auto list_builder = builder->initList(values.size());
      for (size_t i = 0; i < values.size(); ++i) {
        auto value_builder = list_builder[i];
        SaveCapnpTypedValue(values[i], &value_builder, versions, worker_id);
      }
      return;
    }
    case TypedValue::Type::Map: {
      const auto &map = value.Value<std::map<std::string, TypedValue>>();
      auto map_builder = builder->initMap(map.size());
      size_t i = 0;
      for (const auto &kv : map) {
        auto kv_builder = map_builder[i];
        kv_builder.setKey(kv.first);
        auto value_builder = kv_builder.initValue();
        SaveCapnpTypedValue(kv.second, &value_builder, versions, worker_id);
        ++i;
      }
      return;
    }
    case TypedValue::Type::Vertex: {
      auto vertex_builder = builder->initVertex();
      storage::SaveVertexAccessor(value.ValueVertex(), &vertex_builder,
                                  versions, worker_id);
      return;
    }
    case TypedValue::Type::Edge: {
      auto edge_builder = builder->initEdge();
      storage::SaveEdgeAccessor(value.ValueEdge(), &edge_builder, versions,
                                worker_id);
      return;
    }
    case TypedValue::Type::Path: {
      auto path_builder = builder->initPath();
      const auto &path = value.ValuePath();
      auto vertices_builder = path_builder.initVertices(path.vertices().size());
      for (size_t i = 0; i < path.vertices().size(); ++i) {
        auto vertex_builder = vertices_builder[i];
        storage::SaveVertexAccessor(path.vertices()[i], &vertex_builder,
                                    versions, worker_id);
      }
      auto edges_builder = path_builder.initEdges(path.edges().size());
      for (size_t i = 0; i < path.edges().size(); ++i) {
        auto edge_builder = edges_builder[i];
        storage::SaveEdgeAccessor(path.edges()[i], &edge_builder, versions,
                                  worker_id);
      }
      return;
    }
  }
}

void LoadCapnpTypedValue(const capnp::TypedValue::Reader &reader,
                         TypedValue *value, database::GraphDbAccessor *dba,
                         distributed::DataManager *data_manager) {
  switch (reader.which()) {
    case capnp::TypedValue::NULL_TYPE:
      *value = TypedValue::Null;
      return;
    case capnp::TypedValue::BOOL:
      *value = reader.getBool();
      return;
    case capnp::TypedValue::INTEGER:
      *value = reader.getInteger();
      return;
    case capnp::TypedValue::DOUBLE:
      *value = reader.getDouble();
      return;
    case capnp::TypedValue::STRING:
      *value = reader.getString().cStr();
      return;
    case capnp::TypedValue::LIST: {
      std::vector<TypedValue> list;
      list.reserve(reader.getList().size());
      for (const auto &value_reader : reader.getList()) {
        list.emplace_back();
        LoadCapnpTypedValue(value_reader, &list.back(), dba, data_manager);
      }
      *value = list;
      return;
    }
    case capnp::TypedValue::MAP: {
      std::map<std::string, TypedValue> map;
      for (const auto &kv_reader : reader.getMap()) {
        auto key = kv_reader.getKey().cStr();
        LoadCapnpTypedValue(kv_reader.getValue(), &map[key], dba, data_manager);
      }
      *value = map;
      return;
    }
    case capnp::TypedValue::VERTEX:
      *value =
          storage::LoadVertexAccessor(reader.getVertex(), dba, data_manager);
      return;
    case capnp::TypedValue::EDGE:
      *value = storage::LoadEdgeAccessor(reader.getEdge(), dba, data_manager);
      return;
    case capnp::TypedValue::PATH: {
      auto vertices_reader = reader.getPath().getVertices();
      auto edges_reader = reader.getPath().getEdges();
      query::Path path(
          storage::LoadVertexAccessor(vertices_reader[0], dba, data_manager));
      for (size_t i = 0; i < edges_reader.size(); ++i) {
        path.Expand(
            storage::LoadEdgeAccessor(edges_reader[i], dba, data_manager));
        path.Expand(storage::LoadVertexAccessor(vertices_reader[i + 1], dba,
                                                data_manager));
      }
      *value = path;
      return;
    }
  }
}

void Save(const Parameters &parameters,
          utils::capnp::Map<utils::capnp::BoxInt64,
                            storage::capnp::PropertyValue>::Builder *builder) {
  auto params_builder = builder->initEntries(parameters.size());
  size_t i = 0;
  for (auto &entry : parameters) {
    auto builder = params_builder[i];
    auto key_builder = builder.initKey();
    key_builder.setValue(entry.first);
    auto value_builder = builder.initValue();
    storage::SaveCapnpPropertyValue(entry.second, &value_builder);
    ++i;
  }
}

void Load(
    Parameters *parameters,
    const utils::capnp::Map<utils::capnp::BoxInt64,
                            storage::capnp::PropertyValue>::Reader &reader) {
  for (const auto &entry_reader : reader.getEntries()) {
    PropertyValue value;
    storage::LoadCapnpPropertyValue(entry_reader.getValue(), &value);
    parameters->Add(entry_reader.getKey().getValue(), value);
  }
}

void Save(const TypedValueVectorCompare &comparator,
          capnp::TypedValueVectorCompare::Builder *builder) {
  auto ordering_builder = builder->initOrdering(comparator.ordering().size());
  for (size_t i = 0; i < comparator.ordering().size(); ++i) {
    ordering_builder.set(i, comparator.ordering()[i] == Ordering::ASC
                                ? capnp::Ordering::ASC
                                : capnp::Ordering::DESC);
  }
}

void Load(TypedValueVectorCompare *comparator,
          const capnp::TypedValueVectorCompare::Reader &reader) {
  std::vector<Ordering> ordering;
  ordering.reserve(reader.getOrdering().size());
  for (auto ordering_reader : reader.getOrdering()) {
    ordering.push_back(ordering_reader == capnp::Ordering::ASC
                           ? Ordering::ASC
                           : Ordering::DESC);
  }
  comparator->ordering_ = ordering;
}

}  // namespace query

namespace slk {

void Save(const query::TypedValue &value, slk::Builder *builder,
          storage::SendVersions versions, int16_t worker_id) {
  switch (value.type()) {
    case query::TypedValue::Type::Null:
      slk::Save(static_cast<uint8_t>(0), builder);
      return;
    case query::TypedValue::Type::Bool:
      slk::Save(static_cast<uint8_t>(1), builder);
      slk::Save(value.Value<bool>(), builder);
      return;
    case query::TypedValue::Type::Int:
      slk::Save(static_cast<uint8_t>(2), builder);
      slk::Save(value.Value<int64_t>(), builder);
      return;
    case query::TypedValue::Type::Double:
      slk::Save(static_cast<uint8_t>(3), builder);
      slk::Save(value.Value<double>(), builder);
      return;
    case query::TypedValue::Type::String:
      slk::Save(static_cast<uint8_t>(4), builder);
      slk::Save(value.Value<std::string>(), builder);
      return;
    case query::TypedValue::Type::List: {
      slk::Save(static_cast<uint8_t>(5), builder);
      const auto &values = value.Value<std::vector<query::TypedValue>>();
      size_t size = values.size();
      slk::Save(size, builder);
      for (const auto &v : values) {
        slk::Save(v, builder, versions, worker_id);
      }
      return;
    }
    case query::TypedValue::Type::Map: {
      slk::Save(static_cast<uint8_t>(6), builder);
      const auto &map = value.Value<std::map<std::string, query::TypedValue>>();
      size_t size = map.size();
      slk::Save(size, builder);
      for (const auto &kv : map) {
        slk::Save(kv.first, builder);
        slk::Save(kv.second, builder, versions, worker_id);
      }
      return;
    }
    case query::TypedValue::Type::Vertex: {
      slk::Save(static_cast<uint8_t>(7), builder);
      slk::Save(value.ValueVertex(), builder, versions, worker_id);
      return;
    }
    case query::TypedValue::Type::Edge: {
      slk::Save(static_cast<uint8_t>(8), builder);
      slk::Save(value.ValueEdge(), builder, versions, worker_id);
      return;
    }
    case query::TypedValue::Type::Path: {
      slk::Save(static_cast<uint8_t>(9), builder);
      const auto &path = value.ValuePath();
      size_t v_size = path.vertices().size();
      slk::Save(v_size, builder);
      for (const auto &v : path.vertices()) {
        slk::Save(v, builder, versions, worker_id);
      }
      size_t e_size = path.edges().size();
      slk::Save(e_size, builder);
      for (const auto &e : path.edges()) {
        slk::Save(e, builder, versions, worker_id);
      }
      return;
    }
  }
}

void Load(query::TypedValue *value, slk::Reader *reader,
          database::GraphDbAccessor *dba,
          distributed::DataManager *data_manager) {
  uint8_t type;
  slk::Load(&type, reader);
  switch (type) {
    case static_cast<uint8_t>(0):
      *value = query::TypedValue::Null;
      return;
    case static_cast<uint8_t>(1): {
      bool v;
      slk::Load(&v, reader);
      *value = v;
      return;
    }
    case static_cast<uint8_t>(2): {
      int64_t v;
      slk::Load(&v, reader);
      *value = v;
      return;
    }
    case static_cast<uint8_t>(3): {
      double v;
      slk::Load(&v, reader);
      *value = v;
      return;
    }
    case static_cast<uint8_t>(4): {
      std::string v;
      slk::Load(&v, reader);
      *value = std::move(v);
      return;
    }
    case static_cast<uint8_t>(5): {
      size_t size;
      slk::Load(&size, reader);
      std::vector<query::TypedValue> list;
      list.resize(size);
      for (size_t i = 0; i < size; ++i) {
        slk::Load(&list[i], reader, dba, data_manager);
      }
      *value = std::move(list);
      return;
    }
    case static_cast<uint8_t>(6): {
      size_t size;
      slk::Load(&size, reader);
      std::map<std::string, query::TypedValue> map;
      for (size_t i = 0; i < size; ++i) {
        std::string key;
        slk::Load(&key, reader);
        slk::Load(&map[key], reader, dba, data_manager);
      }
      *value = std::move(map);
      return;
    }
    case static_cast<uint8_t>(7):
      *value = slk::LoadVertexAccessor(reader, dba, data_manager);
      return;
    case static_cast<uint8_t>(8):
      *value = slk::LoadEdgeAccessor(reader, dba, data_manager);
      return;
    case static_cast<uint8_t>(9): {
      size_t v_size;
      slk::Load(&v_size, reader);
      std::vector<VertexAccessor> vertices;
      vertices.reserve(v_size);
      for (size_t i = 0; i < v_size; ++i) {
        vertices.push_back(slk::LoadVertexAccessor(reader, dba, data_manager));
      }
      size_t e_size;
      slk::Load(&e_size, reader);
      std::vector<EdgeAccessor> edges;
      edges.reserve(e_size);
      for (size_t i = 0; i < e_size; ++i) {
        edges.push_back(slk::LoadEdgeAccessor(reader, dba, data_manager));
      }
      query::Path path(vertices[0]);
      path.vertices() = std::move(vertices);
      path.edges() = std::move(edges);
      *value = std::move(path);
      return;
    }
    default:
      throw slk::SlkDecodeException("Trying to load unknown TypedValue!");
  }
}

void Save(const query::Parameters &parameters, slk::Builder *builder) {
  slk::Save(parameters.size(), builder);
  for (auto &entry : parameters) {
    slk::Save(entry, builder);
  }
}

void Load(query::Parameters *parameters, slk::Reader *reader) {
  size_t size = 0;
  slk::Load(&size, reader);
  for (size_t i = 0; i < size; ++i) {
    std::pair<int, PropertyValue> entry;
    slk::Load(&entry, reader);
    parameters->Add(entry.first, entry.second);
  }
}

void Save(const query::TypedValueVectorCompare &comparator,
          slk::Builder *builder) {
  slk::Save(comparator.ordering_, builder);
}

void Load(query::TypedValueVectorCompare *comparator, slk::Reader *reader) {
  slk::Load(&comparator->ordering_, reader);
}


void Save(const query::GraphView &graph_view, slk::Builder *builder) {
  uint8_t enum_value;
  switch (graph_view) {
    case query::GraphView::OLD:
      enum_value = 0;
      break;
    case query::GraphView::NEW:
      enum_value = 1;
      break;
  }
  slk::Save(enum_value, builder);
}

void Load(query::GraphView *graph_view, slk::Reader *reader) {
  uint8_t enum_value;
  slk::Load(&enum_value, reader);
  switch (enum_value) {
    case static_cast<uint8_t>(0):
      *graph_view = query::GraphView::OLD;
      break;
    case static_cast<uint8_t>(1):
      *graph_view = query::GraphView::NEW;
      break;
    default:
      throw slk::SlkDecodeException("Trying to load unknown enum value!");
  }
}

}  // namespace slk
