#include "storage/distributed/rpc/serialization.hpp"

#include "database/distributed/graph_db_accessor.hpp"
#include "distributed/data_manager.hpp"

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
  auto guard = storage::GetDataLock(accessor);
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
