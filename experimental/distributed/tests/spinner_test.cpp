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
  for (const ShardedStorage &mnode : distributed) {
    cout << "  ShardedStorage " << mnode.mnid_ << ":";
    cout << " #vertices = " << mnode.VertexCount();
    int64_t edge_count{0};
    for (const auto &gid_vertex_pair : mnode) {
      edge_count += gid_vertex_pair.second.edges_in().size();
      edge_count += gid_vertex_pair.second.edges_out().size();
    }
    cout << ", #edges = " << edge_count;
    cout << ", #cuts = " << mnode.BoundaryEdgeCount() << endl;
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
Distributed ReadGraph(std::string filename, int mnode_count) {
  Distributed distributed(mnode_count);

  std::fstream fs;
  fs.open(filename, std::fstream::in);
  if (fs.fail()) return distributed;

  int vertex_count, edge_count;
  fs >> vertex_count >> edge_count;

  // assign vertices to random mnodes
  std::vector<GlobalVertAddress> vertices;
  for (int i = 0; i < vertex_count; ++i)
    vertices.emplace_back(distributed.MakeVertex(rand() % mnode_count));

  // add edges
  for (int i = 0; i < edge_count; ++i) {
    size_t u, v;
    fs >> u >> v;
    assert(u < vertices.size() && v < vertices.size());
    distributed.MakeEdge(vertices[u], vertices[v]);
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
