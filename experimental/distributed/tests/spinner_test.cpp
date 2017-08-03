#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "graph.hpp"
#include "spinner.hpp"

void PrintStatistics(const Distributed &distributed) {
  using std::cout;
  using std::endl;
  for (const Worker &worker : distributed) {
    cout << "  Worker " << worker.id_ << ":";
    cout << " #nodes = " << worker.NodeCount();
    int64_t edge_count{0};
    for (const auto &gid_node_pair : worker) {
      edge_count += gid_node_pair.second.edges_in().size();
      edge_count += gid_node_pair.second.edges_out().size();
    }
    cout << ", #edges = " << edge_count;
    cout << ", #cuts = " << worker.BoundaryEdgeCount() << endl;
  }
}

/**
 * Reads an undirected graph from file.
 *   - first line of the file: vertices_count, edges_count
 *   - next edges_count lines contain vertices that form an edge
 * example:
 *   https://snap.stanford.edu/data/facebook_combined.txt.gz
 *   add number of vertices and edges in the first line of that file
 */
Distributed ReadGraph(std::string filename, int worker_count) {
  Distributed distributed(worker_count);

  std::fstream fs;
  fs.open(filename, std::fstream::in);
  if (fs.fail()) return distributed;

  int vertex_count, edge_count;
  fs >> vertex_count >> edge_count;

  // assign vertices to random workers
  std::vector<GlobalAddress> nodes;
  for (int i = 0; i < vertex_count; ++i)
    nodes.emplace_back(distributed.MakeNode(rand() % worker_count));

  // add edges
  for (int i = 0; i < edge_count; ++i) {
    size_t u, v;
    fs >> u >> v;
    assert(u < nodes.size() && v < nodes.size());
    distributed.MakeEdge(nodes[u], nodes[v]);
  }
  fs.close();
  return distributed;
}

int main(int argc, const char *argv[]) {
  srand(time(NULL));

  if (argc == 1) {
    std::cout << "Usage:" << std::endl;
    std::cout << argv[0] << " filename partitions iterations" << std::endl;
    return 0;
  }

  std::cout << "Memgraph spinner test " << std::endl;
  std::string filename(argv[1]);
  int partitions = std::max(1, atoi(argv[2]));
  int iterations = std::max(1, atoi(argv[3]));

  Distributed distributed = ReadGraph(filename, partitions);
  PrintStatistics(distributed);
  for (int iter = 0; iter < iterations; ++iter) {
    spinner::PerformSpinnerStep(distributed);
    std::cout << "Iteration " << iter << std::endl;
    PrintStatistics(distributed);
  }

  return 0;
}
