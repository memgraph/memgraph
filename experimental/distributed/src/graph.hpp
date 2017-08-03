#pragma once

#include <cassert>
#include <unordered_map>
#include <vector>

#include "uid.hpp"

enum class EdgeType { OUTGOING, INCOMING };

/** A node in the graph. Has incoming and outgoing edges which
 * are defined as global addresses of other nodes */
class Node {
 public:
  Node(const GlobalId &id) : id_(id) {}

  const auto &id() const { return id_; };
  const auto &edges_out() const { return edges_out_; }
  const auto &edges_in() const { return edges_in_; }

  void AddConnection(EdgeType edge_type, const GlobalAddress &gad) {
    (edge_type == EdgeType::INCOMING ? edges_in_ : edges_out_)
        .emplace_back(gad);
  }

  /** Changes all old_address edges to have the new_worker */
  void RedirectEdges(const GlobalAddress old_address, size_t new_worker) {
    for (auto &address : edges_in_)
      if (address == old_address) address.worker_id_ = new_worker;
    for (auto &address : edges_out_)
      if (address == old_address) address.worker_id_ = new_worker;
  }

 private:
  // TODO remove id_ from Node if not necessary
  GlobalId id_;

  // global addresses of nodes this node is connected to
  std::vector<GlobalAddress> edges_out_;
  std::vector<GlobalAddress> edges_in_;
};

/** A worker / shard in the distributed system */
class Worker {
 public:
  // unique worker ID. uniqueness is ensured by the worker
  // owner (the Distributed class)
  const int64_t id_;

  Worker(int64_t id) : id_(id) {}

  int64_t NodeCount() const { return nodes_.size(); }

  /** Gets a node. */
  Node &GetNode(const GlobalId &gid) {
    auto found = nodes_.find(gid);
    assert(found != nodes_.end());
    return found->second;
  }

  /** Returns the number of edges that cross from this
   * graph / worker into another one */
  int64_t BoundaryEdgeCount() const {
    int64_t count = 0;
    auto count_f = [this, &count](const auto &edges) {
      for (const GlobalAddress &address : edges)
        if (address.worker_id_ != id_) count++;
    };
    for (const auto &node : nodes_) {
      count_f(node.second.edges_out());
      count_f(node.second.edges_in());
    }

    return count;
  }

  /** Creates a new node on this worker. Returns it's global id */
  const GlobalId &MakeNode() {
    GlobalId new_id(id_, next_node_sequence_++);
    auto new_node = nodes_.emplace(std::make_pair(new_id, Node(new_id)));
    return new_node.first->first;
  };

  /** Places the existing node on this worker */
  void PlaceNode(const GlobalId &gid, const Node &node) {
    nodes_.emplace(gid, node);
  }

  /** Removes the node with the given ID from this worker */
  void RemoveNode(const GlobalId &gid) { nodes_.erase(gid); }

  auto begin() const { return nodes_.begin(); }

  auto end() const { return nodes_.end(); }

 private:
  // counter of sequences numbers of nodes created on this worker
  int64_t next_node_sequence_{0};

  // node storage of this worker
  std::unordered_map<GlobalId, Node> nodes_;
};

/**
 * A distributed system consisting of mulitple workers.
 * For the time being it's not modelling a distributed
 * system correctly in terms of message passing (as opposed
 * to operating on workers and their data directly).
 */
class Distributed {
 public:
  /** Creates a distributed with the given number of workers */
  Distributed(int initial_worker_count = 0) {
    for (int worker_id = 0; worker_id < initial_worker_count; worker_id++)
      AddWorker();
  }

  int64_t AddWorker() {
    int64_t new_worker_id = workers_.size();
    workers_.emplace_back(new_worker_id);
    return new_worker_id;
  }

  int WorkerCount() const { return workers_.size(); }

  auto &GetWorker(int64_t worker_id) { return workers_[worker_id]; }

  GlobalAddress MakeNode(int64_t worker_id) {
    return {worker_id, workers_[worker_id].MakeNode()};
  }

  Node &GetNode(const GlobalAddress &address) {
    return workers_[address.worker_id_].GetNode(address.id_);
  }

  /** Moves a node with the given global id to the given worker */
  void MoveNode(const GlobalAddress &gid, int64_t destination) {
    const Node &node = GetNode(gid);

    // make sure that all edges to and from the node are updated
    for (auto &edge : node.edges_in())
      GetNode(edge).RedirectEdges(gid, destination);
    for (auto &edge : node.edges_out())
      GetNode(edge).RedirectEdges(gid, destination);

    // change node destination
    workers_[destination].PlaceNode(gid.id_, node);
    workers_[gid.worker_id_].RemoveNode(gid.id_);
  }

  void MakeEdge(const GlobalAddress &from, const GlobalAddress &to) {
    GetNode(from).AddConnection(EdgeType::OUTGOING, to);
    GetNode(to).AddConnection(EdgeType::INCOMING, from);
  }

  auto begin() const { return workers_.begin(); }

  auto end() const { return workers_.end(); }

 private:
  std::vector<Worker> workers_;
};
