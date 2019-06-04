#include "query/distributed/serialization.hpp"

#include "distributed/data_manager.hpp"
#include "query/distributed/frontend/ast/ast_serialization.hpp"

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
      slk::Save(std::string(value.ValueString()), builder);
      return;
    case query::TypedValue::Type::List: {
      slk::Save(static_cast<uint8_t>(5), builder);
      const auto &values = value.ValueList();
      size_t size = values.size();
      slk::Save(size, builder);
      for (const auto &v : values) {
        slk::Save(v, builder, versions, worker_id);
      }
      return;
    }
    case query::TypedValue::Type::Map: {
      slk::Save(static_cast<uint8_t>(6), builder);
      const auto &map = value.ValueMap();
      size_t size = map.size();
      slk::Save(size, builder);
      for (const auto &kv : map) {
        slk::Save(std::string(kv.first), builder);
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
      auto *memory = value->GetMemoryResource();
      std::vector<VertexAccessor, utils::Allocator<VertexAccessor>> vertices(
          memory);
      vertices.reserve(v_size);
      for (size_t i = 0; i < v_size; ++i) {
        vertices.push_back(slk::LoadVertexAccessor(reader, dba, data_manager));
      }
      size_t e_size;
      slk::Load(&e_size, reader);
      std::vector<EdgeAccessor, utils::Allocator<EdgeAccessor>> edges(memory);
      edges.reserve(e_size);
      for (size_t i = 0; i < e_size; ++i) {
        edges.push_back(slk::LoadEdgeAccessor(reader, dba, data_manager));
      }
      query::Path path(vertices[0], memory);
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
  uint8_t enum_value = 0;
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
