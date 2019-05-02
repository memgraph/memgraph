#pragma once

#include "slk/serialization.hpp"
#include "storage/common/types/property_value.hpp"
#include "storage/common/types/property_value_store.hpp"
#include "storage/common/types/slk.hpp"
#include "storage/distributed/edge.hpp"
#include "storage/distributed/edge_accessor.hpp"
#include "storage/distributed/vertex.hpp"
#include "storage/distributed/vertex_accessor.hpp"

namespace database {
class GraphDbAccessor;
}

namespace distributed {
class DataManager;
}

namespace storage {

enum class SendVersions { BOTH, ONLY_OLD, ONLY_NEW };

}  // namespace storage

namespace slk {

template <typename TLocalObj>
void Save(const storage::Address<TLocalObj> &address, slk::Builder *builder) {
  // TODO: Is this even correct w.r.t. local and global address?
  slk::Save(address.storage_, builder);
}

template <typename TLocalObj>
void Load(storage::Address<TLocalObj> *address, slk::Reader *reader) {
  // TODO: Is this even correct w.r.t. local and global address?
  slk::Load(&address->storage_, reader);
}

void Save(const storage::SendVersions &versions, slk::Builder *builder);

void Load(storage::SendVersions *versions, slk::Reader *reader);

void Save(const VertexAccessor &value, slk::Builder *builder,
          storage::SendVersions versions, int16_t worker_id);

VertexAccessor LoadVertexAccessor(slk::Reader *reader,
                                  database::GraphDbAccessor *dba,
                                  distributed::DataManager *data_manager);

void Save(const EdgeAccessor &value, slk::Builder *builder,
          storage::SendVersions versions, int16_t worker_id);

EdgeAccessor LoadEdgeAccessor(slk::Reader *reader,
                              database::GraphDbAccessor *dba,
                              distributed::DataManager *data_manager);

void Save(const Vertex &vertex, slk::Builder *builder, int16_t worker_id);

void Load(Vertex *vertex, slk::Reader *reader);

void Save(const Edge &edge, slk::Builder *builder, int16_t worker_id);

void Load(std::unique_ptr<Edge> *edge, slk::Reader *reader);

}  // namespace slk
