#pragma once

#include "kdnode.hpp"
#include "math.hpp"
#include "point.hpp"

namespace kd {

// helper class for calculating the nearest neighbour in a kdtree
template <class T, class U>
struct Result {
  Result() : node(nullptr), distance_sq(std::numeric_limits<T>::infinity()) {}

  Result(const KdNode<T, U>* node, T distance_sq)
      : node(node), distance_sq(distance_sq) {}

  const KdNode<T, U>* node;
  T distance_sq;
};

// a recursive implementation for the kdtree nearest neighbour search
// \param p    the point for which we search for the nearest neighbour
// \param node the root of the subtree during recursive descent
// \param best the place to save the best result so far
template <class T, class U>
void nns(const Point<T>& p, const KdNode<T, U>* const node,
         Result<T, U>& best) {
  if (node == nullptr) return;

  T d = math::distance_sq(p, node->coord);

  // keep record of the closest point C found so far
  if (d < best.distance_sq) {
    best.node = node;
    best.distance_sq = d;
  }

  // where to traverse next?
  // what to prune?

  //            |
  //  possible  |
  //  prune     *
  //  area      | - - - - -* P
  //            |
  //
  //            |----------|
  //                 dx
  //

  //        possible prune
  // RIGHT    area
  //
  //  --------*------   ---
  //          |          |
  // LEFT                |
  //          |          |  dy
  //                     |
  //          |          |
  //          * p       ---

  T axd = math::axial_distance(p, node->coord, node->axis);

  // traverse the subtree in order that
  // maximizes the probability for pruning
  auto near = axd > 0 ? node->right : node->left;
  auto far = axd > 0 ? node->left : node->right;

  // try near first
  nns(p, near, best);

  // prune subtrees once their bounding boxes say
  // that they can't contain any point closer than C
  if (axd * axd >= best.distance_sq) return;

  // try other subtree
  nns(p, far, best);
}

// an implementation for the kdtree nearest neighbour search
// \param  p    the point for which we search for the nearest neighbour
// \param  root the root of the tree
// \return the nearest neighbour for the point p
template <class T, class U>
const KdNode<T, U>* nns(const Point<T>& p, const KdNode<T, U>* root) {
  Result<T, U> best;

  // begin recursive search
  nns(p, root, best);

  return best.node;
}
}
