/// @file

#pragma once

#include <cstdint>
#include <set>
#include <vector>

namespace comdata {

struct Neighbour {
  uint32_t dest;
  double weight;
  Neighbour(uint32_t d, double w) : dest(d), weight(w) {}
};

/// Class which models a weighted, undirected graph with necessary
/// functionalities for community detection algorithms.
class Graph {
public:
  /// Constructs a new graph with a given number of nodes and no edges between
  /// them.
  ///
  /// The implementation assumes (and enforces) that all nodes
  /// are indexed from 0 to n_nodes.
  ///
  /// @param n_nodes Number of nodes in the graph.
  explicit Graph(uint32_t n_nodes);

  /// @return number of nodes in the graph.
  uint32_t Size() const;

  /// Adds a bidirectional, weighted edge to the graph between the given
  /// nodes. If both given nodes are the same, the method inserts a weighted
  /// self-loop.
  ///
  /// There should be no edges between the given nodes when before invoking
  /// this method.
  ///
  /// @param node1 index of an incident node.
  /// @param node2 index of an incident node.
  /// @param weight real value which represents the weight of the edge.
  ///
  /// @throw std::out_of_range
  /// @throw std::invalid_argument
  void AddEdge(uint32_t node1, uint32_t node2, double weight);

  /// @param node index of node.
  ///
  /// @return community where the node belongs to.
  ///
  /// @throw std::out_of_range
  uint32_t Community(uint32_t node) const;

  /// Adds a given node to a given community.
  ///
  /// @param node index of node.
  /// @param c community where the given node should go in.
  ///
  /// @throw std::out_of_range
  void SetCommunity(uint32_t node, uint32_t c);

  /// Normalizes the values of communities. More precisely, after invoking this
  /// method communities will be indexed by successive integers starting from 0.
  ///
  /// Note: this method is computationally expensive and takes O(|V|)
  ///       time, i.e., it traverses all nodes in the graph.
  ///
  /// @return number of communities in the graph
  uint32_t NormalizeCommunities();

  /// Returns the number of incident edges to a given node. Self-loops
  /// contribute a single edge to the degree.
  ///
  /// @param node index of node.
  ///
  /// @return degree of given node.
  ///
  /// @throw std::out_of_range
  uint32_t Degree(uint32_t node) const;

  /// Returns the total weight of incident edges to a given node. Weight
  /// of a self loop contributes once to the total sum.
  ///
  /// @param node index of node.
  ///
  /// @return total incident weight of a given node.
  ///
  /// @throw std::out_of_range
  double IncidentWeight(uint32_t node) const;

  /// @return total weight of all edges in a graph.
  double TotalWeight() const;

  /// Calculates the modularity of the graph which is defined as a real value
  /// between -1 and 1 that measures the density of links inside communities
  /// compared to links between communities.
  ///
  /// Note: this method is computationally expensive and takes O(|V| + |E|)
  ///       time, i.e., it traverses the entire graph.
  ///
  /// @return modularity of the graph.
  double Modularity() const;

  /// Returns nodes adjacent to a given node.
  ///
  /// @param node index of node.
  ///
  /// @return list of neighbouring nodes.
  ///
  /// @throw std::out_of_range
  const std::vector<Neighbour>& Neighbours(uint32_t node) const;

private:
  uint32_t n_nodes_;
  double total_w_;

  std::vector<std::vector<Neighbour>> adj_list_;
  std::set<std::pair<uint32_t, uint32_t>> edges_;

  std::vector<double> inc_w_;
  std::vector<uint32_t> community_;
};

} // namespace comdata
