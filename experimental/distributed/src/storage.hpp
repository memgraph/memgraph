#pragma once

#include <string>
#include <unordered_map>

#include "uid.hpp"

/**
 * Mock-up storage to test basic hardcoded queries.
 *
 * Current code is taken from graph.hpp. It will grow as needed to support
 * new queries, but not more. Once all functionality from graph.hpp is 
 * transfered here, this file will be included in graph.hpp.
 */

class Vertex {};

/**
 * Storage that is split over multiple nodes.
 */
class ShardedStorage {
 public:
  ShardedStorage(int64_t mnid) : mnid_(mnid) {}

  /** Returns number of vertices on this node. */
  int64_t VertexCount() const { return vertices_.size(); }

  /** Creates a new vertex on this node. Returns its global id */
  const UniqueVid &MakeVertex() {
    UniqueVid new_id(mnid_, next_vertex_sequence_++);
    auto new_vertex = vertices_.emplace(std::make_pair(new_id, Vertex()));
    return new_vertex.first->first;
  };

 private:
  // unique Memgraph node ID
  // uniqueness is ensured by the (distributed) system
  const int64_t mnid_;

  // counter of vertices created on this node.
  int64_t next_vertex_sequence_{0};

  // vertex storage of this node
  std::unordered_map<UniqueVid, Vertex> vertices_;
};