#pragma once

#include <chrono>
#include <random>
#include <set>
#include <tuple>

#include "data_structures/graph.hpp"

/// Builds the graph from a given number of nodes and a list of edges.
/// Nodes should be 0-indexed and each edge should be provided only once.
comdata::Graph BuildGraph(
    uint32_t nodes, std::vector<std::tuple<uint32_t, uint32_t, double>> edges);

/// Generates random undirected graph with a given number of nodes and edges.
/// The generated graph is not picked out of a uniform distribution. All weights
/// are the same and equal to one.
comdata::Graph GenRandomUnweightedGraph(uint32_t nodes, uint32_t edges);
