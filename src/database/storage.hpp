#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "data_structures/concurrent/concurrent_set.hpp"
#include "database/indexes/key_index.hpp"
#include "database/indexes/label_property_index.hpp"
#include "mvcc/version_list.hpp"
#include "storage/edge.hpp"
#include "storage/types.hpp"
#include "storage/vertex.hpp"

namespace database {

/** A data structure containing the main data members of a graph database. */
class Storage {
 public:
  explicit Storage(int worker_id)
      : vertex_generator_{worker_id}, edge_generator_{worker_id} {}

 public:
  ~Storage() {
    // Delete vertices and edges which weren't collected before, also deletes
    // records inside version list
    for (auto &id_vlist : vertices_.access()) delete id_vlist.second;
    for (auto &id_vlist : edges_.access()) delete id_vlist.second;
  }

  Storage(const Storage &) = delete;
  Storage(Storage &&) = delete;
  Storage &operator=(const Storage &) = delete;
  Storage &operator=(Storage &&) = delete;

  gid::Generator &VertexGenerator() { return vertex_generator_; }
  gid::Generator &EdgeGenerator() { return edge_generator_; }

 private:
  friend class GraphDbAccessor;
  friend class StorageGc;

  gid::Generator vertex_generator_;
  gid::Generator edge_generator_;

  // main storage for the graph
  ConcurrentMap<gid::Gid, mvcc::VersionList<Vertex> *> vertices_;
  ConcurrentMap<gid::Gid, mvcc::VersionList<Edge> *> edges_;

  // indexes
  KeyIndex<storage::Label, Vertex> labels_index_;
  LabelPropertyIndex label_property_index_;

  // Set of transactions ids which are building indexes currently
  ConcurrentSet<tx::transaction_id_t> index_build_tx_in_progress_;

  // DB level global counters, used in the "counter" function.
  ConcurrentMap<std::string, std::atomic<int64_t>> counters_;
};
}  // namespace database
