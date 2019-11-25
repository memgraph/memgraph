#pragma once

#include <chrono>
#include <random>
#include <set>
#include <tuple>

#include "data_structures/graph.hpp"

/// This class is threadsafe
class Timer {
 public:
  Timer() : start_time_(std::chrono::steady_clock::now()) {}

  template <typename TDuration = std::chrono::duration<double>>
  TDuration Elapsed() const {
    return std::chrono::duration_cast<TDuration>(
        std::chrono::steady_clock::now() - start_time_);
  }

 private:
  std::chrono::steady_clock::time_point start_time_;
};

/// Builds the graph from a given number of nodes and a list of edges.
/// Nodes should be 0-indexed and each edge should be provided only once.
comdata::Graph BuildGraph(
    uint32_t nodes, std::vector<std::tuple<uint32_t, uint32_t, double>> edges);

/// Generates random undirected graph with a given number of nodes and edges.
/// The generated graph is not picked out of a uniform distribution. All weights
/// are the same and equal to one.
comdata::Graph GenRandomUnweightedGraph(uint32_t nodes, uint32_t edges);
