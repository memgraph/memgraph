#pragma once

#include <algorithm>
#include <cassert>
#include <memory>

#include "utils/placeholder.hpp"
#include "utils/random/fast_binomial.hpp"

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

#include "data_structures/concurrent/skiplist_gc.hpp"

/* @brief Concurrent lock-based skiplist with fine grained locking
 *
 * From Wikipedia:
 *    "A skip list is a data structure that allows fast search within an
 *     ordered sequence of elements. Fast search is made possible by
 *     maintaining a linked hierarchy of subsequences, each skipping over
 *     fewer elements. Searching starts in the sparsest subsequence until
 *     two consecutive elements have been found, one smaller and one
 *     larger than or equal to the element searched for."
 *
 *  [_]---------------->[+]----------->[_]
 *  [_]->[+]----------->[+]------>[+]->[_]
 *  [_]->[+]------>[+]->[+]------>[+]->[_]
 *  [_]->[+]->[+]->[+]->[+]->[+]->[+]->[_]
 *  head  1    2    4    5    8    9   nil
 *
 * The logarithmic properties are maintained by randomizing the height for
 * every new node using the binomial distribution
 * p(k) = (1/2)^k for k in [1...H].
 *
 * The implementation is based on the work described in the paper
 * "A Provably Correct Scalable Concurrent Skip List"
 * URL: https://www.cs.tau.ac.il/~shanir/nir-pubs-web/Papers/OPODIS2006-BA.pdf
 *
 * The proposed implementation is in Java so the authors don't worry about
 * garbage collection, but obviously we have to. This implementation uses
 * lazy garbage collection. When all clients stop using the skiplist, we can
 * be sure that all logically removed nodes are not visible to anyone so
 * we can safely remove them. The idea of counting active clients implies
 * the use of a intermediary structure (called Accessor) when accessing the
 * skiplist.
 *
 * The implementation has an interface which closely resembles the functions
 * with arguments and returned types frequently used by the STL.
 *
 * Example usage:
 *   Skiplist<T> skiplist;
 *
 *   {
 *       auto accessor = skiplist.access();
 *
 *       // inserts item into the skiplist and returns
 *       // <iterator, bool> pair. iterator points to the newly created
 *       // node and the boolean member evaluates to true denoting that the
 *       // insertion was successful
 *       accessor.insert(item1);
 *
 *       // nothing gets inserted because item1 already exist in the skiplist
 *       // returned iterator points to the existing element and the return
 *       // boolean evaluates to false denoting the failed insertion
 *       accessor.insert(item1);
 *
 *       // returns an iterator to the element item1
 *       auto it = accessor.find(item1);
 *
 *       // returns an empty iterator. it == accessor.end()
 *       auto it = accessor.find(item2);
 *
 *       // iterate over all items
 *       for(auto it = accessor.begin(); it != accessor.end(); ++it)
 *           cout << *it << endl;
 *
 *       // range based for loops also work
 *       for(auto& e : accessor)
 *          cout << e << endl;
 *
 *       accessor.remove(item1); // returns true
 *       accessor.remove(item1); // returns false because key1 doesn't exist
 *   }
 *
 *   // accessor out of scope, garbage collection might occur
 *
 * For detailed operations available, please refer to the Accessor class
 * inside the public section of the SkipList class.
 *
 * @tparam T Type to use as the item
 * @tparam H Maximum node height. Determines the effective number of nodes
 *           the skiplist can hold in order to preserve it's log2 properties
 * @tparam lock_t Lock type used when locking is needed during the creation
 *                and deletion of nodes.
 */
template <class T, size_t H = 32, class lock_t = SpinLock>
class SkipList : private Lockable<lock_t> {
public:
  // computes the height for the new node from the interval [1...H]
  // with p(k) = (1/2)^k for all k from the interval
  static thread_local FastBinomial<H> rnd;

  /* @brief Wrapper class for flags used in the implementation
   *
   * MARKED flag is used to logically delete a node.
   * FULLY_LINKED is used to mark the node as fully inserted, i.e. linked
   * at all layers in the skiplist up to the node height
   */
  struct Flags {
    enum node_flags : uint8_t {
      MARKED = 0x01,
      FULLY_LINKED = 0x10,
    };

    bool is_marked() const { return flags.load() & MARKED; }

    void set_marked() { flags.fetch_or(MARKED); }

    bool is_fully_linked() const { return flags.load() & FULLY_LINKED; }

    void set_fully_linked() { flags.fetch_or(FULLY_LINKED); }

  private:
    std::atomic<uint8_t> flags{0};
  };

  class Node : Lockable<lock_t> {
  public:
    friend class SkipList;

    const uint8_t height;
    Flags flags;

    T &value() { return data.get(); }

    const T &value() const { return data.get(); }

    static Node *sentinel(uint8_t height) {
      // we have raw memory and we need to construct an object
      // of type Node on it
      return new (allocate(height)) Node(height);
    }

    static Node *create(const T &item, uint8_t height) {
      return create(item, height);
    }

    static Node *create(T &&item, uint8_t height) {
      auto node = allocate(height);

      // we have raw memory and we need to construct an object
      // of type Node on it
      return new (node) Node(std::forward<T>(item), height);
    }

    static void destroy(Node *node) {
      node->~Node();
      std::free(node);
    }

    Node *forward(size_t level) const { return tower[level].load(); }

    void forward(size_t level, Node *next) { tower[level].store(next); }

  private:
    Node(uint8_t height) : height(height) {
      // here we assume, that the memory for N towers (N = height) has
      // been allocated right after the Node structure so we need to
      // initialize that memory
      for (auto i = 0; i < height; ++i)
        new (&tower[i]) std::atomic<Node *>{nullptr};
    }

    Node(T &&data, uint8_t height) : Node(height) {
      this->data.set(std::forward<T>(data));
    }

    ~Node() {
      for (auto i = 0; i < height; ++i)
        tower[i].~atomic();
    }

    static Node *allocate(uint8_t height) {
      // [      Node      ][Node*][Node*][Node*]...[Node*]
      //         |            |      |      |         |
      //         |            0      1      2      height-1
      // |----------------||-----------------------------|
      //   space for Node     space for tower pointers
      //     structure          right after the Node
      //                             structure
      auto size = sizeof(Node) + height * sizeof(std::atomic<Node *>);
      auto node = static_cast<Node *>(std::malloc(size));

      return node;
    }

    Placeholder<T> data;

    // this creates an array of the size zero. we can't put any sensible
    // value here since we don't know what size it will be untill the
    // node is allocated. we could make it a Node** but then we would
    // have two memory allocations, one for node and one for the forward
    // list. this way we avoid expensive malloc/free calls and also cache
    // thrashing when following a pointer on the heap
    std::atomic<Node *> tower[0];
  };

public:
  template <class It> class IteratorBase : public Crtp<It> {
  protected:
    IteratorBase(Node *node) : node(node) {}

    Node *node{nullptr};

  public:
    IteratorBase() = default;
    IteratorBase(const IteratorBase &) = default;

    T &operator*() {
      assert(node != nullptr);
      return node->value();
    }

    T *operator->() {
      assert(node != nullptr);
      return &node->value();
    }

    operator T &() {
      assert(node != nullptr);
      return node->value();
    }

    It &operator++() {
      assert(node != nullptr);
      node = node->forward(0);
      return this->derived();
    }

    It &operator++(int) { return operator++(); }

    friend bool operator==(const It &a, const It &b) {
      return a.node == b.node;
    }

    friend bool operator!=(const It &a, const It &b) { return !(a == b); }
  };

  class ConstIterator : public IteratorBase<ConstIterator> {
    friend class SkipList;
    ConstIterator(Node *node) : IteratorBase<ConstIterator>(node) {}

  public:
    ConstIterator() = default;
    ConstIterator(const ConstIterator &) = default;

    const T &operator*() { return IteratorBase<ConstIterator>::operator*(); }

    const T *operator->() { return IteratorBase<ConstIterator>::operator->(); }

    operator const T &() { return IteratorBase<ConstIterator>::operator T &(); }
  };

  class Iterator : public IteratorBase<Iterator> {
    friend class SkipList;
    Iterator(Node *node) : IteratorBase<Iterator>(node) {}

  public:
    Iterator() = default;
    Iterator(const Iterator &) = default;
  };

  SkipList() : header(Node::sentinel(H)) {}

  ~SkipList() {
    // Someone could be using this map through an Accessor.
    Node *now = header;
    header = nullptr;

    while (now != nullptr) {
      Node *next = now->forward(0);
      Node::destroy(now);
      now = next;
    }
  }

  friend class Accessor;

  class Accessor {
    friend class SkipList;

    Accessor(SkipList *skiplist) : skiplist(skiplist) {
      assert(skiplist != nullptr);

      skiplist->gc.add_ref();
    }

  public:
    Accessor(const Accessor &) = delete;

    Accessor(Accessor &&other) : skiplist(other.skiplist) {
      other.skiplist = nullptr;
    }

    ~Accessor() {
      if (skiplist == nullptr)
        return;

      skiplist->gc.release_ref();
    }

    Iterator begin() { return skiplist->begin(); }

    ConstIterator begin() const { return skiplist->cbegin(); }

    ConstIterator cbegin() const { return skiplist->cbegin(); }

    Iterator end() { return skiplist->end(); }

    ConstIterator end() const { return skiplist->cend(); }

    ConstIterator cend() const { return skiplist->cend(); }

    std::pair<Iterator, bool> insert(const T &item) {
      return skiplist->insert(item, preds, succs);
    }

    std::pair<Iterator, bool> insert(T &&item) {
      return skiplist->insert(std::forward<T>(item), preds, succs);
    }

    template <class K> ConstIterator find(const K &item) const {
      return static_cast<const SkipList &>(*skiplist).find(item);
    }

    template <class K> Iterator find(const K &item) {
      return skiplist->find(item);
    }

    template <class K> bool contains(const K &item) const {
      return this->find(item) != this->end();
    }

    template <class K> bool remove(const K &item) {
      return skiplist->remove(item, preds, succs);
    }

    size_t size() const { return skiplist->size(); }

  private:
    SkipList *skiplist;
    Node *preds[H], *succs[H];
  };

  Accessor access() { return Accessor(this); }

  const Accessor access() const { return Accessor(this); }

private:
  using guard_t = std::unique_lock<lock_t>;

  Iterator begin() { return Iterator(header->forward(0)); }

  ConstIterator begin() const { return ConstIterator(header->forward(0)); }

  ConstIterator cbegin() const { return ConstIterator(header->forward(0)); }

  Iterator end() { return Iterator(); }

  ConstIterator end() const { return ConstIterator(); }

  ConstIterator cend() const { return ConstIterator(); }

  size_t size() const { return count.load(); }

  template <class K> bool greater(const K &item, const Node *const node) {
    return node && item > node->value();
  }

  template <class K> bool less(const K &item, const Node *const node) {
    return (node == nullptr) || item < node->value();
  }

  template <class K> ConstIterator find(const K &item) const {
    return const_cast<SkipList *>(this)->find_node<ConstIterator, K>(item);
  }

  template <class K> Iterator find(const K &item) {
    return find_node<Iterator, K>(item);
  }

  template <class It, class K> It find_node(const K &item) {
    Node *node, *pred = header;
    int h = static_cast<int>(pred->height) - 1;

    while (true) {
      // try to descend down first the next key on this layer overshoots
      for (; h >= 0 && less(item, node = pred->forward(h)); --h) {
      }

      // if we overshoot at every layer, item doesn't exist
      if (h < 0)
        return It();

      // the item is farther to the right, continue going right as long
      // as the key is greater than the current node's key
      while (greater(item, node))
        pred = node, node = node->forward(h);

      // check if we have a hit. if not, we need to descend down again
      if (!less(item, node) && !node->flags.is_marked())
        return It(node);
    }
  }

  template <class K>
  int find_path(Node *from, int start, const K &item, Node *preds[],
                Node *succs[]) {
    int level_found = -1;
    Node *pred = from;

    for (int level = start; level >= 0; --level) {
      Node *node = pred->forward(level);

      while (greater(item, node))
        pred = node, node = pred->forward(level);

      if (level_found == -1 && !less(item, node))
        level_found = level;

      preds[level] = pred;
      succs[level] = node;
    }

    return level_found;
  }

  template <bool ADDING>
  bool lock_nodes(uint8_t height, guard_t guards[], Node *preds[],
                  Node *succs[]) {
    Node *prepred, *pred, *succ = nullptr;
    bool valid = true;

    for (int level = 0; valid && level < height; ++level) {
      pred = preds[level], succ = succs[level];

      if (pred != prepred)
        guards[level] = pred->acquire_unique(), prepred = pred;

      valid = !pred->flags.is_marked() && pred->forward(level) == succ;

      if (ADDING)
        valid = valid && (succ == nullptr || !succ->flags.is_marked());
    }

    return valid;
  }

  std::pair<Iterator, bool> insert(T &&data, Node *preds[], Node *succs[]) {
    while (true) {
      // TODO: before here was data.first
      auto level = find_path(header, H - 1, data, preds, succs);

      if (level != -1) {
        auto found = succs[level];

        if (found->flags.is_marked())
          continue;

        while (!found->flags.is_fully_linked())
          usleep(250);

        return {Iterator{succs[level]}, false};
      }

      auto height = rnd();
      guard_t guards[H];

      // try to acquire the locks for predecessors up to the height of
      // the new node. release the locks and try again if someone else
      // has the locks
      if (!lock_nodes<true>(height, guards, preds, succs))
        continue;

      // you have the locks, create a new node
      auto new_node = Node::create(std::forward<T>(data), height);

      // link the predecessors and successors, e.g.
      //
      // 4 HEAD ... P ------------------------> S ... NULL
      // 3 HEAD ... ... P -----> NEW ---------> S ... NULL
      // 2 HEAD ... ... P -----> NEW -----> S ... ... NULL
      // 1 HEAD ... ... ... P -> NEW -> S ... ... ... NULL
      for (uint8_t level = 0; level < height; ++level) {
        new_node->forward(level, succs[level]);
        preds[level]->forward(level, new_node);
      }

      new_node->flags.set_fully_linked();
      count.fetch_add(1);

      return {Iterator{new_node}, true};
    }
  }

  bool ok_delete(Node *node, int level) {
    return node->flags.is_fully_linked() && node->height - 1 == level &&
           !node->flags.is_marked();
  }

  template <class K> bool remove(const K &item, Node *preds[], Node *succs[]) {
    Node *node = nullptr;
    guard_t node_guard;
    bool marked = false;
    int height = 0;

    while (true) {
      auto level = find_path(header, H - 1, item, preds, succs);

      if (!marked && (level == -1 || !ok_delete(succs[level], level)))
        return false;

      if (!marked) {
        node = succs[level];
        height = node->height;
        node_guard = node->acquire_unique();

        if (node->flags.is_marked())
          return false;

        node->flags.set_marked();
        marked = true;
      }

      guard_t guards[H];

      if (!lock_nodes<false>(height, guards, preds, succs))
        continue;

      for (int level = height - 1; level >= 0; --level)
        preds[level]->forward(level, node->forward(level));

      // TODO: review and test
      gc.collect(node);

      count.fetch_sub(1);
      return true;
    }
  }

  // number of elements
  std::atomic<size_t> count{0};
  Node *header;
  SkiplistGC<Node> gc;
};

template <class T, size_t H, class lock_t>
thread_local FastBinomial<H> SkipList<T, H, lock_t>::rnd;
