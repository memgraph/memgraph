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
  explicit UnionFind(uintXX_t n) : set_count_(n), rank_(n), parent_(n) {
    for (auto i = 0; i < n; ++i) rank_[i] = 0, parent_[i] = i;
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
  void Connect(uintXX_t p, uintXX_t q) {
    auto rp = Root(p);
    auto rq = Root(q);

    // if roots are equal, we don't have to do anything
    if (rp == rq) return;

    // merge the subtree with the smaller rank to the root of the subtree with
    // the larger rank
    if (rank_[rp] < rank_[rq])
      parent_[rp] = rq;
    else if (rank_[rp] > rank_[rq])
      parent_[rq] = rp;
    else
      parent_[rq] = rp, rank_[rp] += 1;

    // update the number of groups
    set_count_--;
  }

  /**
   * Indicates if two elements are connected. Has amortized O(alpha(n)) time
   * complexity.
   *
   * @param p First element.
   * @param q Second element.
   * @return See above.
   */
  bool Find(uintXX_t p, uintXX_t q) { return Root(p) == Root(q); }

  /**
   * Returns the number of disjoint sets in this UnionFind.
   *
   * @return See above.
   */
  uintXX_t Size() const { return set_count_; }

 private:
  uintXX_t set_count_;

  // array of subtree ranks
  std::vector<uintXX_t> rank_;

  // array of tree indices
  std::vector<uintXX_t> parent_;

  uintXX_t Root(uintXX_t p) {
    auto r = p;
    auto newp = p;

    // find the node connected to itself, that's the root
    while (parent_[r] != r) r = parent_[r];

    // do some path compression to enable faster searches
    while (p != r) newp = parent_[p], parent_[p] = r, p = newp;

    return r;
  }
};
