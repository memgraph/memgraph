#pragma once

#include <cassert>
#include <unordered_map>
#include <vector>

#include "uid.hpp"

enum class EdgeType { OUTGOING, INCOMING };

/** A vertex in the graph. Has incoming and outgoing edges which
 * are defined as global addresses of other vertices */
class Vertex {
 public:
  Vertex(const UniqueVid &id) : id_(id) {}

  const auto &id() const { return id_; };
  const auto &edges_out() const { return edges_out_; }
  const auto &edges_in() const { return edges_in_; }

  void AddConnection(EdgeType edge_type, const GlobalVertAddress &gad) {
    (edge_type == EdgeType::INCOMING ? edges_in_ : edges_out_)
        .emplace_back(gad);
  }

  /** Changes all old_address edges to have the new Memgraph node id */
  void RedirectEdges(const GlobalVertAddress& old_address, int64_t new_mnid) {
    for (auto &address : edges_in_)
      if (address == old_address) address.cur_mnid_ = new_mnid;
    for (auto &address : edges_out_)
      if (address == old_address) address.cur_mnid_ = new_mnid;
  }

 private:
  UniqueVid id_;

  // global addresses of vertices this vertex is connected to
  std::vector<GlobalVertAddress> edges_out_;
  std::vector<GlobalVertAddress> edges_in_;
};

/**
 * A storage that doesn't assume everything is in-memory.
 */
class ShardedStorage {
 public:
  // Unique Memgraph node ID. Uniqueness is ensured by the (distributed) system.
  const int64_t mnid_;

  ShardedStorage(int64_t mnid) : mnid_(mnid) {}

  int64_t VertexCount() const { return vertices_.size(); }

  /** Gets a vertex. */
  Vertex &GetVertex(const UniqueVid &gid) {
    auto found = vertices_.find(gid);
    assert(found != vertices_.end());
    return found->second;
  }

  /**
   * Returns the number of edges that cross from this
   * node into another one
   */
  int64_t BoundaryEdgeCount() const {
    int64_t count = 0;
    auto count_f = [this, &count](const auto &edges) {
      for (const GlobalVertAddress &address : edges)
        if (address.cur_mnid_ != mnid_) count++;
    };
    for (const auto &vertex : vertices_) {
      count_f(vertex.second.edges_out());
      count_f(vertex.second.edges_in());
    }

    return count;
  }

  /** Creates a new vertex on this node. Returns its global id */
  const UniqueVid &MakeVertex() {
    UniqueVid new_id(mnid_, next_vertex_sequence_++);
    auto new_vertex = vertices_.emplace(std::make_pair(new_id, Vertex(new_id)));
    return new_vertex.first->first;
  };

  /** Places the existing vertex on this node */
  void PlaceVertex(const UniqueVid &gid, const Vertex &vertex) {
    vertices_.emplace(gid, vertex);
  }

  /** Removes the vertex with the given ID from this node */
  void RemoveVertex(const UniqueVid &gid) { vertices_.erase(gid); }

  auto begin() const { return vertices_.begin(); }

  auto end() const { return vertices_.end(); }

 private:
  // counter of sequences numbers of vertices created on this node
  int64_t next_vertex_sequence_{0};

  // vertex storage of this node
  std::unordered_map<UniqueVid, Vertex> vertices_;
};

/**
 * A distributed system consisting of mulitple nodes.
 * For the time being it's not modelling a distributed
 * system correctly in terms of message passing (as opposed
 * to operating on nodes and their data directly).
 */
class Distributed {
 public:
  /** Creates a distributed with the given number of nodes */
  Distributed(int initial_mnode_count = 0) {
    for (int mnode_id = 0; mnode_id < initial_mnode_count; mnode_id++)
      AddMnode();
  }

  int64_t AddMnode() {
    int64_t new_mnode_id = mnodes_.size();
    mnodes_.emplace_back(new_mnode_id);
    return new_mnode_id;
  }

  int MnodeCount() const { return mnodes_.size(); }

  auto &GetMnode(int64_t mnode_id) { return mnodes_[mnode_id]; }

  GlobalVertAddress MakeVertex(int64_t mnid) {
    return {mnid, mnodes_[mnid].MakeVertex()};
  }

  Vertex &GetVertex(const GlobalVertAddress &address) {
    return mnodes_[address.cur_mnid_].GetVertex(address.uvid_);
  }

  /** Moves a vertex with the given global id to the given mnode */
  void MoveVertex(const GlobalVertAddress &gad, int64_t destination) {
    const Vertex &vertex = GetVertex(gad);

    // make sure that all edges to and from the vertex are updated
    for (auto &edge : vertex.edges_in())
      GetVertex(edge).RedirectEdges(gad, destination);
    for (auto &edge : vertex.edges_out())
      GetVertex(edge).RedirectEdges(gad, destination);

    // change vertex destination
    mnodes_[destination].PlaceVertex(gad.uvid_, vertex);
    mnodes_[gad.cur_mnid_].RemoveVertex(gad.uvid_);
  }

  void MakeEdge(const GlobalVertAddress &from, const GlobalVertAddress &to) {
    GetVertex(from).AddConnection(EdgeType::OUTGOING, to);
    GetVertex(to).AddConnection(EdgeType::INCOMING, from);
  }

  auto begin() const { return mnodes_.begin(); }

  auto end() const { return mnodes_.end(); }

 private:
  std::vector<ShardedStorage> mnodes_;
};
