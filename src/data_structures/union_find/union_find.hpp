#pragma once

#include <memory>
#include <vector>

template <class uintXX_t = uint32_t>
/**
 * UnionFind data structure. Provides means of connectivity
 * setting and checking in O(alpha(n)) amortized complexity. Memory
 * complexity is linear.
 */
class UnionFind {
 public:
  /**
   * Constructor, creates a UnionFind structure of fixed size.
   *
   * @param n Number of elements in the data structure.
   */
  UnionFind(uintXX_t n) : set_count(n), rank(n), parent(n) {
    for (auto i = 0; i < n; ++i) rank[i] = 0, parent[i] = i;
  }

  /**
   * Connects two elements (and thereby the sets they belong
   * to). If they are already connected the function has no effect.
   *
   * Has O(alpha(n)) amortized time complexity.
   *
   * @param p First element.
   * @param q Second element.
   */
  void connect(uintXX_t p, uintXX_t q) {
    auto rp = root(p);
    auto rq = root(q);

    // if roots are equal, we don't have to do anything
    if (rp == rq) return;

    // merge the subtree with the smaller rank to the root of the subtree with
    // the larger rank
    if (rank[rp] < rank[rq])
      parent[rp] = rq;
    else if (rank[rp] > rank[rq])
      parent[rq] = rp;
    else
      parent[rq] = rp, rank[rp] += 1;

    // update the number of groups
    set_count--;
  }

  /**
   * Indicates if two elements are connected. Has amortized O(alpha(n)) time
   * complexity.
   *
   * @param p First element.
   * @param q Second element.
   * @return See above.
   */
  bool find(uintXX_t p, uintXX_t q) { return root(p) == root(q); }

  /**
   * Returns the number of disjoint sets in this UnionFind.
   *
   * @return See above.
   */
  uintXX_t size() const { return set_count; }

 private:
  uintXX_t set_count;

  // array of subtree ranks
  std::vector<uintXX_t> rank;

  // array of tree indices
  std::vector<uintXX_t> parent;

  uintXX_t root(uintXX_t p) {
    auto r = p;
    auto newp = p;

    // find the node connected to itself, that's the root
    while (parent[r] != r) r = parent[r];

    // do some path compression to enable faster searches
    while (p != r) newp = parent[p], parent[p] = r, p = newp;

    return r;
  }
};
