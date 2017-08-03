#include <cassert>
#include <iostream>
#include <iterator>

#include "graph.hpp"

void test_global_id() {
  GlobalId a(1, 1);
  assert(a == GlobalId(1, 1));
  assert(a != GlobalId(1, 2));
  assert(a != GlobalId(2, 1));
}

void test_global_address() {
  GlobalAddress a(1, {1, 1});
  assert(a == GlobalAddress(1, {1, 1}));
  assert(a != GlobalAddress(2, {1, 1}));
  assert(a != GlobalAddress(1, {2, 1}));
}

void test_worker() {
  Worker worker0{0};
  assert(worker0.NodeCount() == 0);
  GlobalId n0 = worker0.MakeNode();
  assert(worker0.NodeCount() == 1);

  Worker worker1{1};
  worker1.PlaceNode(n0, worker0.GetNode(n0));
  worker0.RemoveNode(n0);
  assert(worker0.NodeCount() == 0);
  assert(worker1.NodeCount() == 1);

  worker1.MakeNode();
  assert(worker1.NodeCount() == 2);
  assert(std::distance(worker1.begin(), worker1.end()) == 2);
}

void test_distributed() {
  Distributed d;
  assert(d.WorkerCount() == 0);
  auto w0 = d.AddWorker();
  assert(d.WorkerCount() == 1);
  auto w1 = d.AddWorker();
  assert(d.WorkerCount() == 2);

  GlobalAddress n0 = d.MakeNode(w0);
  assert(d.GetWorker(w0).NodeCount() == 1);
  GlobalAddress n1 = d.MakeNode(w1);

  assert(d.GetNode(n0).edges_out().size() == 0);
  assert(d.GetNode(n0).edges_in().size() == 0);
  assert(d.GetNode(n1).edges_out().size() == 0);
  assert(d.GetNode(n1).edges_in().size() == 0);
  d.MakeEdge(n0, n1);
  assert(d.GetNode(n0).edges_out().size() == 1);
  assert(d.GetNode(n0).edges_in().size() == 0);
  assert(d.GetNode(n1).edges_out().size() == 0);
  assert(d.GetNode(n1).edges_in().size() == 1);
}

int main() {
  test_global_id();
  test_global_address();
  test_worker();
  test_distributed();
  std::cout << "All tests  passed" << std::endl;
}
