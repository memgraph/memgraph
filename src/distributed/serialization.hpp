#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "database/types.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"
#include "utils/serialization.hpp"

namespace distributed {

namespace impl {

// Saves the given address into the given archive. Converts a local address to a
// global one, using the given worker_id.
template <typename TArchive, typename TAddress>
void SaveAddress(TArchive &ar, TAddress address, int worker_id) {
  auto gid = address.is_remote() ? address.global_id() : address.local()->gid_;
  ar << gid;
  ar << worker_id;
};

// Saves the given properties into the given archive.
template <typename TArchive>
void SaveProperties(TArchive &ar, const PropertyValueStore &props) {
  ar << props.size();
  for (auto &kv : props) {
    ar << kv.first.storage();
    utils::SaveTypedValue(ar, kv.second);
  }
}
}  // namespace impl

/**
 * Saves the given vertex into the given Boost archive.
 *
 * @param ar - Archive into which to serialize.
 * @param vertex - Getting serialized.
 * @param worker_id - ID of the worker this is happening on. Necessary for local
 * to global address conversion.
 * @tparam TArchive - type of archive.
 */
template <typename TArchive>
void SaveVertex(TArchive &ar, const Vertex &vertex, int worker_id) {
  auto save_edges = [&ar, worker_id](auto &edges) {
    ar << edges.size();
    for (auto &edge_struct : edges) {
      impl::SaveAddress(ar, edge_struct.vertex, worker_id);
      impl::SaveAddress(ar, edge_struct.edge, worker_id);
      ar << edge_struct.edge_type.storage();
    }
  };
  save_edges(vertex.out_);
  save_edges(vertex.in_);

  ar << vertex.labels_.size();
  for (auto &label : vertex.labels_) {
    ar << label.storage();
  }

  impl::SaveProperties(ar, vertex.properties_);
}

/**
 * Saves the given edge into the given Boost archive.
 *
 * @param - Archive into which to serialize.
 * @param edge - Getting serialized.
 * @param worker_id - ID of the worker this is happening on. Necessary for local
 * to global address conversion.
 * @tparam TArchive - type of archive.
 */
template <typename TArchive>
void SaveEdge(TArchive &ar, const Edge &edge, int worker_id) {
  impl::SaveAddress(ar, edge.from_, worker_id);
  impl::SaveAddress(ar, edge.to_, worker_id);
  ar << edge.edge_type_.storage();
  impl::SaveProperties(ar, edge.properties_);
}

namespace impl {

template <typename TArchive>
Edges::VertexAddress LoadVertexAddress(TArchive &ar) {
  gid::Gid vertex_id;
  ar >> vertex_id;
  int worker_id;
  ar >> worker_id;
  return {vertex_id, worker_id};
}

template <typename TArchive>
void LoadProperties(TArchive &ar, PropertyValueStore &store) {
  size_t count;
  ar >> count;
  for (size_t i = 0; i < count; ++i) {
    database::Property::StorageT prop;
    ar >> prop;
    query::TypedValue value;
    utils::LoadTypedValue(ar, value);
    store.set(database::Property(prop), static_cast<PropertyValue>(value));
  }
}

}  // namespace impl

/**
 * Loads a Vertex from the given archive and returns it.
 *
 * @param ar - The archive to load from.
 * @tparam TArchive - archive type.
 */
template <typename TArchive>
std::unique_ptr<Vertex> LoadVertex(TArchive &ar) {
  auto vertex = std::make_unique<Vertex>();

  auto decode_edges = [&ar](Edges &edges) {
    size_t count;
    ar >> count;
    for (size_t i = 0; i < count; ++i) {
      auto vertex_address = impl::LoadVertexAddress(ar);
      database::EdgeType::StorageT edge_type;
      gid::Gid edge_id;
      ar >> edge_id;
      int edge_worker_id;
      ar >> edge_worker_id;
      ar >> edge_type;
      edges.emplace(vertex_address, {edge_id, edge_worker_id},
                    database::EdgeType(edge_type));
    }
  };
  decode_edges(vertex->out_);
  decode_edges(vertex->in_);

  size_t count;
  ar >> count;
  for (size_t i = 0; i < count; ++i) {
    database::Label::StorageT label;
    ar >> label;
    vertex->labels_.emplace_back(label);
  }
  impl::LoadProperties(ar, vertex->properties_);

  return vertex;
}

/**
 * Loads an Edge from the given archive and returns it.
 *
 * @param ar - The archive to load from.
 * @tparam TArchive - archive type.
 */
template <typename TArchive>
std::unique_ptr<Edge> LoadEdge(TArchive &ar) {
  auto from = impl::LoadVertexAddress(ar);
  auto to = impl::LoadVertexAddress(ar);
  database::EdgeType::StorageT edge_type;
  ar >> edge_type;
  auto edge = std::make_unique<Edge>(from, to, database::EdgeType{edge_type});
  impl::LoadProperties(ar, edge->properties_);

  return edge;
}
}  // namespace distributed
