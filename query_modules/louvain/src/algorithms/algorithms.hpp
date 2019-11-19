/// @file
///
/// The file contains function declarations of several community-detection
/// graph algorithms.

#pragma once

#include "data_structures/graph.hpp"

namespace algorithms {
  /// Detects communities of an unidrected, weighted graph using the Louvain
  /// algorithm. The algorithm attempts to maximze the modularity of a weighted
  /// graph.
  ///
  /// @param G pointer to an undirected, weighted graph which may contain
  ///          self-loops.
  void Louvain(comdata::Graph *G);
} // namespace algorithms
