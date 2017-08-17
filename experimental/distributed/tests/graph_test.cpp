#include <cassert>
#include <iostream>
#include <iterator>

#include "graph.hpp"

void test_global_id() {
  UniqueVid a(1, 1);
  assert(a == UniqueVid(1, 1));
  assert(a != UniqueVid(1, 2));
  assert(a != UniqueVid(2, 1));
}

void test_global_address() {
  GlobalVertAddress a(1, {1, 1});
  assert(a == GlobalVertAddress(1, {1, 1}));
  assert(a != GlobalVertAddress(2, {1, 1}));
  assert(a != GlobalVertAddress(1, {2, 1}));
}

void test_mnode() {
  ShardedStorage mnode0{0};
  assert(mnode0.VertexCount() == 0);
  UniqueVid n0 = mnode0.MakeVertex();
  assert(mnode0.VertexCount() == 1);

  ShardedStorage mnode1{1};
  mnode1.PlaceVertex(n0, mnode0.GetVertex(n0));
  mnode0.RemoveVertex(n0);
  assert(mnode0.VertexCount() == 0);
  assert(mnode1.VertexCount() == 1);

  mnode1.MakeVertex();
  assert(mnode1.VertexCount() == 2);
  assert(std::distance(mnode1.begin(), mnode1.end()) == 2);
}

void test_distributed() {
  Distributed d;
  assert(d.MnodeCount() == 0);
  auto w0 = d.AddMnode();
  assert(d.MnodeCount() == 1);
  auto w1 = d.AddMnode();
  assert(d.MnodeCount() == 2);

  GlobalVertAddress n0 = d.MakeVertex(w0);
  assert(d.GetMnode(w0).VertexCount() == 1);
  GlobalVertAddress n1 = d.MakeVertex(w1);

  assert(d.GetVertex(n0).edges_out().size() == 0);
  assert(d.GetVertex(n0).edges_in().size() == 0);
  assert(d.GetVertex(n1).edges_out().size() == 0);
  assert(d.GetVertex(n1).edges_in().size() == 0);
  d.MakeEdge(n0, n1);
  assert(d.GetVertex(n0).edges_out().size() == 1);
  assert(d.GetVertex(n0).edges_in().size() == 0);
  assert(d.GetVertex(n1).edges_out().size() == 0);
  assert(d.GetVertex(n1).edges_in().size() == 1);
}

int main() {
  test_global_id();
  test_global_address();
  test_mnode();
  test_distributed();
  std::cout << "All tests  passed" << std::endl;
}
