#include "query/serialization.hpp"

#include "distributed/data_manager.hpp"

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

void SaveEvaluationContext(const EvaluationContext &ctx,
                           capnp::EvaluationContext::Builder *builder) {
  builder->setTimestamp(ctx.timestamp);
  auto params_builder =
      builder->initParams().initEntries(ctx.parameters.size());
  size_t i = 0;
  for (auto &entry : ctx.parameters) {
    auto builder = params_builder[i];
    auto key_builder = builder.initKey();
    key_builder.setValue(entry.first);
    auto value_builder = builder.initValue();
    storage::SaveCapnpPropertyValue(entry.second, &value_builder);
    ++i;
  }
}

void LoadEvaluationContext(const capnp::EvaluationContext::Reader &reader,
                           EvaluationContext *ctx) {
  ctx->timestamp = reader.getTimestamp();
  for (const auto &entry_reader : reader.getParams().getEntries()) {
    PropertyValue value;
    storage::LoadCapnpPropertyValue(entry_reader.getValue(), &value);
    ctx->parameters.Add(entry_reader.getKey().getValue(), value);
  }
}

}  // namespace query
