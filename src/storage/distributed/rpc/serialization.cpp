#include "storage/distributed/rpc/serialization.hpp"

#include "database/distributed/graph_db_accessor.hpp"
#include "distributed/data_manager.hpp"

namespace storage {

void Save(const Label &label, capnp::Label::Builder *builder) {
  builder->setStorage(label.id_);
}

void Load(Label *label, const capnp::Label::Reader &reader) {
  label->id_ = reader.getStorage();
}

void Save(const EdgeType &edge_type, capnp::EdgeType::Builder *builder) {
  builder->setStorage(edge_type.id_);
}

void Load(EdgeType *edge_type, const capnp::EdgeType::Reader &reader) {
  edge_type->id_ = reader.getStorage();
}

void Save(const Property &property, capnp::Property::Builder *builder) {
  builder->setStorage(property.id_);
}

void Load(Property *property, const capnp::Property::Reader &reader) {
  property->id_ = reader.getStorage();
}

void SaveCapnpPropertyValue(const PropertyValue &value,
                            capnp::PropertyValue::Builder *builder) {
  switch (value.type()) {
    case PropertyValue::Type::Null:
      builder->setNullType();
      return;
    case PropertyValue::Type::Bool:
      builder->setBool(value.Value<bool>());
      return;
    case PropertyValue::Type::Int:
      builder->setInteger(value.Value<int64_t>());
      return;
    case PropertyValue::Type::Double:
      builder->setDouble(value.Value<double>());
      return;
    case PropertyValue::Type::String:
      builder->setString(value.Value<std::string>());
      return;
    case PropertyValue::Type::List: {
      const auto &values = value.Value<std::vector<PropertyValue>>();
      auto list_builder = builder->initList(values.size());
      for (size_t i = 0; i < values.size(); ++i) {
        auto value_builder = list_builder[i];
        SaveCapnpPropertyValue(values[i], &value_builder);
      }
      return;
    }
    case PropertyValue::Type::Map: {
      const auto &map = value.Value<std::map<std::string, PropertyValue>>();
      auto map_builder = builder->initMap(map.size());
      size_t i = 0;
      for (const auto &kv : map) {
        auto kv_builder = map_builder[i];
        kv_builder.setKey(kv.first);
        auto value_builder = kv_builder.initValue();
        SaveCapnpPropertyValue(kv.second, &value_builder);
        ++i;
      }
      return;
    }
  }
}

void LoadCapnpPropertyValue(const capnp::PropertyValue::Reader &reader,
                            PropertyValue *value) {
  switch (reader.which()) {
    case capnp::PropertyValue::NULL_TYPE:
      *value = PropertyValue::Null;
      return;
    case capnp::PropertyValue::BOOL:
      *value = reader.getBool();
      return;
    case capnp::PropertyValue::INTEGER:
      *value = reader.getInteger();
      return;
    case capnp::PropertyValue::DOUBLE:
      *value = reader.getDouble();
      return;
    case capnp::PropertyValue::STRING:
      *value = reader.getString().cStr();
      return;
    case capnp::PropertyValue::LIST: {
      std::vector<PropertyValue> list;
      list.reserve(reader.getList().size());
      for (const auto &value_reader : reader.getList()) {
        list.emplace_back();
        LoadCapnpPropertyValue(value_reader, &list.back());
      }
      *value = list;
      return;
    }
    case capnp::PropertyValue::MAP: {
      std::map<std::string, PropertyValue> map;
      for (const auto &kv_reader : reader.getMap()) {
        auto key = kv_reader.getKey();
        LoadCapnpPropertyValue(kv_reader.getValue(), &map[key]);
      }
      *value = map;
      return;
    }
  }
}

void SaveProperties(const PropertyValueStore &properties,
                    capnp::PropertyValueStore::Builder *builder) {
  size_t i = 0;
  auto props_builder = builder->initProperties(properties.size());
  for (const auto &kv : properties) {
    auto kv_builder = props_builder[i++];
    auto id_builder = kv_builder.initId();
    Save(kv.first, &id_builder);
    auto value_builder = kv_builder.initValue();
    SaveCapnpPropertyValue(kv.second, &value_builder);
  }
}

void LoadProperties(const capnp::PropertyValueStore::Reader &reader,
                    PropertyValueStore *properties) {
  properties->clear();
  auto props_reader = reader.getProperties();
  for (const auto &kv_reader : props_reader) {
    storage::Property id;
    storage::Load(&id, kv_reader.getId());
    PropertyValue value;
    LoadCapnpPropertyValue(kv_reader.getValue(), &value);
    properties->set(id, value);
  }
}

template <class TAddress>
void SaveAddress(TAddress address, capnp::Address::Builder *builder,
                 int16_t worker_id) {
  TAddress global_address =
      address.is_local() ? TAddress(address.local()->gid_, worker_id) : address;
  builder->setStorage(global_address.raw());
}

EdgeAddress LoadEdgeAddress(const capnp::Address::Reader &reader) {
  return EdgeAddress(reader.getStorage());
}

VertexAddress LoadVertexAddress(const capnp::Address::Reader &reader) {
  return VertexAddress(reader.getStorage());
}

void SaveVertex(const Vertex &vertex, capnp::Vertex::Builder *builder,
                int16_t worker_id) {
  auto save_edges = [worker_id](const auto &edges, auto *edges_builder) {
    int64_t i = 0;
    for (const auto &edge : edges) {
      auto edge_builder = (*edges_builder)[i];
      auto vertex_addr_builder = edge_builder.initVertexAddress();
      SaveAddress(edge.vertex, &vertex_addr_builder, worker_id);
      auto edge_addr_builder = edge_builder.initEdgeAddress();
      SaveAddress(edge.edge, &edge_addr_builder, worker_id);
      edge_builder.setEdgeTypeId(edge.edge_type.Id());
      ++i;
    }
  };
  auto out_builder = builder->initOutEdges(vertex.out_.size());
  save_edges(vertex.out_, &out_builder);
  auto in_builder = builder->initInEdges(vertex.in_.size());
  save_edges(vertex.in_, &in_builder);
  auto labels_builder = builder->initLabelIds(vertex.labels_.size());
  for (size_t i = 0; i < vertex.labels_.size(); ++i) {
    labels_builder.set(i, vertex.labels_[i].Id());
  }
  auto properties_builder = builder->initProperties();
  storage::SaveProperties(vertex.properties_, &properties_builder);
}

void SaveEdge(const Edge &edge, capnp::Edge::Builder *builder,
              int16_t worker_id) {
  auto from_builder = builder->initFrom();
  SaveAddress(edge.from_, &from_builder, worker_id);
  auto to_builder = builder->initTo();
  SaveAddress(edge.to_, &to_builder, worker_id);
  builder->setTypeId(edge.edge_type_.Id());
  auto properties_builder = builder->initProperties();
  storage::SaveProperties(edge.properties_, &properties_builder);
}

/// Alias for `SaveEdge` allowing for param type resolution.
void SaveElement(const Edge &record, capnp::Edge::Builder *builder,
                 int16_t worker_id) {
  return SaveEdge(record, builder, worker_id);
}

/// Alias for `SaveVertex` allowing for param type resolution.
void SaveElement(const Vertex &record, capnp::Vertex::Builder *builder,
                 int16_t worker_id) {
  return SaveVertex(record, builder, worker_id);
}

std::unique_ptr<Vertex> LoadVertex(const capnp::Vertex::Reader &reader) {
  auto vertex = std::make_unique<Vertex>();
  auto load_edges = [](const auto &edges_reader) {
    Edges edges;
    for (const auto &edge_reader : edges_reader) {
      auto vertex_address = LoadVertexAddress(edge_reader.getVertexAddress());
      auto edge_address = LoadEdgeAddress(edge_reader.getEdgeAddress());
      storage::EdgeType edge_type(edge_reader.getEdgeTypeId());
      edges.emplace(vertex_address, edge_address, edge_type);
    }
    return edges;
  };
  vertex->out_ = load_edges(reader.getOutEdges());
  vertex->in_ = load_edges(reader.getInEdges());
  for (const auto &label_id : reader.getLabelIds()) {
    vertex->labels_.emplace_back(label_id);
  }
  storage::LoadProperties(reader.getProperties(), &vertex->properties_);
  return vertex;
}

std::unique_ptr<Edge> LoadEdge(const capnp::Edge::Reader &reader) {
  auto from = LoadVertexAddress(reader.getFrom());
  auto to = LoadVertexAddress(reader.getTo());
  auto edge =
      std::make_unique<Edge>(from, to, storage::EdgeType{reader.getTypeId()});
  storage::LoadProperties(reader.getProperties(), &edge->properties_);
  return edge;
}

template <class TRecord, class TCapnpRecord>
void SaveRecordAccessor(const RecordAccessor<TRecord> &accessor,
                        typename TCapnpRecord::Builder *builder,
                        SendVersions versions, int worker_id) {
  builder->setCypherId(accessor.CypherId());
  builder->setAddress(accessor.GlobalAddress().raw());

  bool reconstructed = false;
  auto guard = storage::GetDataLock(accessor);
  if (!accessor.GetOld() && !accessor.GetNew()) {
    reconstructed = true;
    bool result = accessor.Reconstruct();
    CHECK(result) << "Attempting to serialize an element not visible to "
                     "current transaction.";
  }
  auto old_rec = accessor.GetOld();
  if (old_rec && versions != SendVersions::ONLY_NEW) {
    auto old_builder = builder->initOld();
    storage::SaveElement(*old_rec, &old_builder, worker_id);
  }
  if (versions != SendVersions::ONLY_OLD) {
    if (!reconstructed && !accessor.GetNew()) {
      bool result = accessor.Reconstruct();
      CHECK(result) << "Attempting to serialize an element not visible to "
                       "current transaction.";
    }
    auto *new_rec = accessor.GetNew();
    if (new_rec) {
      auto new_builder = builder->initNew();
      storage::SaveElement(*new_rec, &new_builder, worker_id);
    }
  }
}

void SaveVertexAccessor(const VertexAccessor &vertex_accessor,
                        capnp::VertexAccessor::Builder *builder,
                        SendVersions versions, int worker_id) {
  SaveRecordAccessor<Vertex, capnp::VertexAccessor>(vertex_accessor, builder,
                                                    versions, worker_id);
}

void SaveEdgeAccessor(const EdgeAccessor &edge_accessor,
                      capnp::EdgeAccessor::Builder *builder,
                      SendVersions versions, int worker_id) {
  SaveRecordAccessor<Edge, capnp::EdgeAccessor>(edge_accessor, builder,
                                                versions, worker_id);
}

VertexAccessor LoadVertexAccessor(const capnp::VertexAccessor::Reader &reader,
                                  database::GraphDbAccessor *dba,
                                  distributed::DataManager *data_manager) {
  int64_t cypher_id = reader.getCypherId();
  storage::VertexAddress global_address(reader.getAddress());
  auto old_record =
      reader.hasOld() ? storage::LoadVertex(reader.getOld()) : nullptr;
  auto new_record =
      reader.hasNew() ? storage::LoadVertex(reader.getNew()) : nullptr;
  data_manager->Emplace(
      dba->transaction_id(), global_address.gid(),
      distributed::CachedRecordData<Vertex>(cypher_id, std::move(old_record),
                                            std::move(new_record)));
  return VertexAccessor(global_address, *dba);
}

EdgeAccessor LoadEdgeAccessor(const capnp::EdgeAccessor::Reader &reader,
                              database::GraphDbAccessor *dba,
                              distributed::DataManager *data_manager) {
  int64_t cypher_id = reader.getCypherId();
  storage::EdgeAddress global_address(reader.getAddress());
  auto old_record =
      reader.hasOld() ? storage::LoadEdge(reader.getOld()) : nullptr;
  auto new_record =
      reader.hasNew() ? storage::LoadEdge(reader.getNew()) : nullptr;
  data_manager->Emplace(
      dba->transaction_id(), global_address.gid(),
      distributed::CachedRecordData<Edge>(cypher_id, std::move(old_record),
                                          std::move(new_record)));
  return EdgeAccessor(global_address, *dba);
}

}  // namespace storage

namespace slk {

void Save(const storage::SendVersions &versions, slk::Builder *builder) {
  uint8_t enum_value;
  switch (versions) {
    case storage::SendVersions::BOTH:
      enum_value = 0;
      break;
    case storage::SendVersions::ONLY_OLD:
      enum_value = 1;
      break;
    case storage::SendVersions::ONLY_NEW:
      enum_value = 2;
      break;
  }
  slk::Save(enum_value, builder);
}

void Load(storage::SendVersions *versions, slk::Reader *reader) {
  uint8_t enum_value;
  slk::Load(&enum_value, reader);
  switch (enum_value) {
    case static_cast<uint8_t>(0):
      *versions = storage::SendVersions::BOTH;
      break;
    case static_cast<uint8_t>(1):
      *versions = storage::SendVersions::ONLY_OLD;
      break;
    case static_cast<uint8_t>(2):
      *versions = storage::SendVersions::ONLY_NEW;
      break;
    default:
      throw slk::SlkDecodeException("Trying to load unknown enum value!");
  }
}

namespace {

template <class TRecordAccessor>
void SaveRecordAccessor(const TRecordAccessor &accessor, slk::Builder *builder,
                        storage::SendVersions versions, int16_t worker_id) {
  bool reconstructed = false;
  if (!accessor.GetOld() && !accessor.GetNew()) {
    reconstructed = true;
    bool result = accessor.Reconstruct();
    CHECK(result) << "Attempting to serialize an element not visible to "
                     "current transaction.";
  }
  auto save_optional_record = [builder, worker_id](const auto *rec) {
    slk::Save(static_cast<bool>(rec), builder);
    if (rec) {
      slk::Save(*rec, builder, worker_id);
    }
  };
  slk::Save(accessor.CypherId(), builder);
  slk::Save(accessor.GlobalAddress(), builder);
  // Save old record first
  if (versions != storage::SendVersions::ONLY_NEW) {
    save_optional_record(accessor.GetOld());
  } else {
    // Mark old record as empty
    slk::Save(false, builder);
  }
  // Save new record
  if (versions != storage::SendVersions::ONLY_OLD) {
    if (!reconstructed && !accessor.GetNew()) {
      bool result = accessor.Reconstruct();
      CHECK(result) << "Attempting to serialize an element not visible to "
                       "current transaction.";
    }
    save_optional_record(accessor.GetNew());
  } else {
    // Mark new record as empty
    slk::Save(false, builder);
  }
}

}  // namespace

void Save(const VertexAccessor &value, slk::Builder *builder,
          storage::SendVersions versions, int16_t worker_id) {
  SaveRecordAccessor(value, builder, versions, worker_id);
}

VertexAccessor LoadVertexAccessor(slk::Reader *reader,
                                  database::GraphDbAccessor *dba,
                                  distributed::DataManager *data_manager) {
  auto load_optional_record = [reader]() {
    std::unique_ptr<Vertex> rec;
    bool has_ptr;
    slk::Load(&has_ptr, reader);
    if (has_ptr) {
      rec = std::make_unique<Vertex>();
      slk::Load(rec.get(), reader);
    }
    return rec;
  };
  int64_t cypher_id;
  slk::Load(&cypher_id, reader);
  storage::VertexAddress global_address;
  slk::Load(&global_address, reader);
  auto old_record = load_optional_record();
  auto new_record = load_optional_record();
  data_manager->Emplace(
      dba->transaction_id(), global_address.gid(),
      distributed::CachedRecordData<Vertex>(cypher_id, std::move(old_record),
                                            std::move(new_record)));
  return VertexAccessor(global_address, *dba);
}

void Save(const EdgeAccessor &value, slk::Builder *builder,
          storage::SendVersions versions, int16_t worker_id) {
  SaveRecordAccessor(value, builder, versions, worker_id);
}

EdgeAccessor LoadEdgeAccessor(slk::Reader *reader,
                              database::GraphDbAccessor *dba,
                              distributed::DataManager *data_manager) {
  auto load_optional_record = [reader]() {
    std::unique_ptr<Edge> rec;
    bool has_ptr;
    slk::Load(&has_ptr, reader);
    if (has_ptr) {
      slk::Load(&rec, reader);
    }
    return rec;
  };
  int64_t cypher_id;
  slk::Load(&cypher_id, reader);
  storage::EdgeAddress global_address;
  slk::Load(&global_address, reader);
  auto old_record = load_optional_record();
  auto new_record = load_optional_record();
  data_manager->Emplace(
      dba->transaction_id(), global_address.gid(),
      distributed::CachedRecordData<Edge>(cypher_id, std::move(old_record),
                                          std::move(new_record)));
  return EdgeAccessor(global_address, *dba);
}

namespace {

// TODO: WTF is with saving adress in a special way?
template <class TAddress>
void SaveAddress(TAddress address, slk::Builder *builder, int16_t worker_id) {
  TAddress global_address =
      address.is_local() ? TAddress(address.local()->gid_, worker_id) : address;
  slk::Save(global_address.raw(), builder);
}

}  // namespace

void Save(const Vertex &vertex, slk::Builder *builder, int16_t worker_id) {
  auto save_edges = [builder, worker_id](const auto &edges) {
    size_t size = edges.size();
    slk::Save(size, builder);
    for (const auto &edge : edges) {
      SaveAddress(edge.vertex, builder, worker_id);
      SaveAddress(edge.edge, builder, worker_id);
      slk::Save(edge.edge_type, builder);
    }
  };
  save_edges(vertex.out_);
  save_edges(vertex.in_);
  slk::Save(vertex.labels_, builder);
  slk::Save(vertex.properties_, builder);
}

void Load(Vertex *vertex, slk::Reader *reader) {
  auto load_edges = [reader]() {
    Edges edges;
    size_t size;
    slk::Load(&size, reader);
    for (size_t i = 0; i < size; ++i) {
      storage::VertexAddress vertex_address;
      slk::Load(&vertex_address, reader);
      storage::EdgeAddress edge_address;
      slk::Load(&edge_address, reader);
      storage::EdgeType edge_type;
      slk::Load(&edge_type, reader);
      edges.emplace(vertex_address, edge_address, edge_type);
    }
    return edges;
  };
  vertex->out_ = load_edges();
  vertex->in_ = load_edges();
  slk::Load(&vertex->labels_, reader);
  slk::Load(&vertex->properties_, reader);
}

void Save(const Edge &edge, slk::Builder *builder, int16_t worker_id) {
  SaveAddress(edge.from_, builder, worker_id);
  SaveAddress(edge.to_, builder, worker_id);
  slk::Save(edge.edge_type_, builder);
  slk::Save(edge.properties_, builder);
}

void Load(std::unique_ptr<Edge> *edge, slk::Reader *reader) {
  storage::VertexAddress from;
  slk::Load(&from, reader);
  storage::VertexAddress to;
  slk::Load(&to, reader);
  storage::EdgeType edge_type;
  slk::Load(&edge_type, reader);
  *edge = std::make_unique<Edge>(from, to, edge_type);
  slk::Load(&(*edge)->properties_, reader);
}

}  // namespace slk
