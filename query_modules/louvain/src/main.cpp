#include <iostream>

#include "algorithms/algorithms.hpp"
#include "data_structures/graph.hpp"

// A simple program that reads the graph from STDIN and
// outputs the detected communities from louvain along with
// its modularity measure on STDOUT.
int main() {
  int n;
  int m;
  std::cin >> n >> m;
  comdata::Graph G(n);
  for (int i = 0; i < m; ++i) {
    int a;
    int b;
    double c;
    std::cin >> a >> b >> c;
    G.AddEdge(a, b, c);
  }

  algorithms::Louvain(&G);

  for (int i = 0; i < n; ++i)
    std::cout << i << G.Community(i) << "\n";
  std::cout << G.Modularity() << "\n";
  return 0;
}
