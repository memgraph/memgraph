#pragma once

#include <algorithm>
#include <functional>
#include <vector>

#include "kdnode.hpp"
#include "math.hpp"

namespace kd {

template <class T, class U>
using Nodes = std::vector<KdNode<T, U>*>;

template <class T, class U>
KdNode<T, U>* build(Nodes<T, U>& nodes, byte axis = 0) {
  // if there are no elements left, we've completed building of this branch
  if (nodes.empty()) return nullptr;

  // comparison function to use for sorting the elements
  auto fsort = [axis](KdNode<T, U>* a, KdNode<T, U>* b) -> bool {
    return kd::math::axial_distance(a->coord, b->coord, axis) < 0;
  };

  size_t median = nodes.size() / 2;

  // partial sort nodes vector to compute median and ensure that elements
  // less than median are positioned before the median so we can slice it
  // nicely

  // internal implementation is O(n) worst case
  // tl;dr http://en.wikipedia.org/wiki/Introselect
  std::nth_element(nodes.begin(), nodes.begin() + median, nodes.end(), fsort);

  // set axis for the node
  auto node = nodes.at(median);
  node->axis = axis;

  // slice the vector into two halves
  auto left = Nodes<T, U>(nodes.begin(), nodes.begin() + median);
  auto right = Nodes<T, U>(nodes.begin() + median + 1, nodes.end());

  // recursively build left and right branches
  node->left = build(left, axis ^ 1);
  node->right = build(right, axis ^ 1);

  return node;
}

template <class T, class U, class It>
KdNode<T, U>* build(It first, It last) {
  Nodes<T, U> kdnodes;

  std::transform(first, last, std::back_inserter(kdnodes),
                 [&](const std::pair<Point<T>, U>& element) {
                   auto key = element.first;
                   auto data = element.second;
                   return new KdNode<T, U>(key, data);
                 });

  // build the tree from the kdnodes and return the root node
  return build(kdnodes);
}
}
