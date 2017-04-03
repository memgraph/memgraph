#pragma once

#include <algorithm>
#include <cmath>
#include <memory>

#include "utils/assert.hpp"
#include "utils/placeholder.hpp"
#include "utils/random/fast_binomial.hpp"

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

#include "data_structures/concurrent/skiplist_gc.hpp"

/**
 * computes the height for the new node from the interval [1...H]
 * with p(k) = (1/2)^k for all k from the interval
 */
static thread_local FastBinomial<> rnd;

/** @brief Concurrent lock-based skiplist with fine grained locking
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
  /** @brief Wrapper class for flags used in the implementation
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
      auto node = allocate(height);

      // we have raw memory and we need to construct an object
      // of type Node on it
      return new (node) Node(item, height);
    }

    static Node *create(T &&item, uint8_t height) {
      auto node = allocate(height);

      // we have raw memory and we need to construct an object
      // of type Node on it
      return new (node) Node(std::move(item), height);
    }

    template <class... Args>
    static Node *emplace(uint8_t height, Args &&... args) {
      auto node = allocate(height);

      // we have raw memory and we need to construct an object
      // of type Node on it
      return new (node) Node(height, std::forward<Args>(args)...);
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

    template <class... Args>
    Node(uint8_t height, Args &&... args) : Node(height) {
      this->data.emplace(std::forward<Args>(args)...);
    }

    Node(const T &data, uint8_t height) : Node(height) { this->data.set(data); }

    Node(T &&data, uint8_t height) : Node(height) {
      this->data.set(std::move(data));
    }

    ~Node() {
      for (auto i = 0; i < height; ++i) tower[i].~atomic();
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

    /**
     * this creates an array of the size zero. we can't put any sensible
     * value here since we don't know what size it will be untill the
     * node is allocated. we could make it a Node** but then we would
     * have two memory allocations, one for node and one for the forward
     * list. this way we avoid expensive malloc/free calls and also cache
     * thrashing when following a pointer on the heap
     */
    std::atomic<Node *> tower[0];
  };

 public:
  template <class It>
  class IteratorBase : public Crtp<It> {
   protected:
    IteratorBase(Node *node) : node(node) {}

    Node *node{nullptr};

   public:
    IteratorBase() = default;
    IteratorBase(const IteratorBase &) = default;

    T &operator*() {
      debug_assert(node != nullptr, "Node is nullptr.");
      return node->value();
    }

    T *operator->() {
      debug_assert(node != nullptr, "Node is nullptr.");
      return &node->value();
    }

    operator T &() {
      debug_assert(node != nullptr, "Node is nullptr.");
      return node->value();
    }

    It &operator++() {
      debug_assert(node != nullptr, "Node is nullptr.");
      node = node->forward(0);
      return this->derived();
    }

    bool has_next() {
      debug_assert(node != nullptr, "Node is nullptr.");
      return node->forward(0) != nullptr;
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

  /**
    @class ReverseIterator
    @author Sandi Fatic

    @brief
    ReverseIterator is used to iterate the skiplist in backwards. The current
    implementation complexity is M*C*Log(N) where M is the number of elements
    we are iterating, N the size of the skiplist and some constant C. The time
    is better then using find M times. Look the benchmark for this class.

    The performance of the reverse iterator is similar to M finds, but faster
    because it stores the preds and shortens the search space.

    @todo
    Research possible better and faster more optimized traversals.
  */
  class ReverseIterator : public Crtp<ReverseIterator> {
    friend class SkipList;

    ReverseIterator(Node *node) : node_(node) {}

   public:
    ReverseIterator(SkipList *skiplist, Node *node, Node *preds[])
        : skiplist_(skiplist), node_(node) {
      for (int i = 0; i < H; i++) preds_[i] = preds[i];
      if (node_ == skiplist_->header) node_ = node_->forward(0);
    }

    T &operator*() {
      debug_assert(node_ != nullptr, "Node is nullptr.");
      return node_->value();
    }

    T *operator->() {
      debug_assert(node_ != nullptr, "Node is nullptr.");
      return &node_->value();
    }

    operator T &() {
      debug_assert(node_ != nullptr, "Node is nullptr.");
      return node_->value();
    }

    ReverseIterator &operator++() {
      debug_assert(node_ != nullptr, "Node is nullptr.");
      do {
        next();
      } while (node_->flags.is_marked());
      return this->derived();
    }

    friend bool operator==(const ReverseIterator &a, const ReverseIterator &b) {
      return a.node_ == b.node_;
    }

    ReverseIterator &operator++(int) { return operator++(); }

    /**
      @brief
      The next() function generates the previous element in skiplist if exists.

      It uses the stored preds to find the previous element and updates the
      preds to optimize the search.
    */
    void next() {
      int level_found = -1, curr = 0;
      auto prev = preds_[0]->value();

      Node *pred = preds_[curr];

      // finds the level from which to start the search
      while (curr < H) {
        curr++;
        if (pred != preds_[curr]) break;
      }

      // goes on level up if possible for better performance
      // not always the optimal but benchmarks are better
      if (curr + 1 < H) curr++;

      while (level_found == -1 && curr < H) {
        Node *pred = preds_[curr];
        for (int level = curr; level >= 0; --level) {
          Node *node = pred->forward(level);

          while (greater(prev, node)) {
            pred = node, node = pred->forward(level);
          }

          if (level_found == -1 && !less(prev, node)) level_found = level;

          preds_[level] = pred;
        }
        curr++;
      }

      node_ = preds_[0];
      if (node_ == skiplist_->header) node_ = node_->forward(0);
    }

    bool has_next() { return node_ != skiplist_->header; }

   private:
    SkipList *skiplist_;
    Node *node_{nullptr};
    Node *preds_[H];
  };

  template <class K>
  class MultiIterator : public Crtp<MultiIterator<K>> {
    friend class SkipList;

    MultiIterator(const K &data) : data(data), skiplist(nullptr) {
      succs[0] = nullptr;
    };
    MultiIterator(SkipList *skiplist, const K &data)
        : data(data), skiplist(skiplist) {
      // We must find the first element with K key.
      // All of logic in this loop was taken from insert method.
      while (true) {
        auto level = find_path(skiplist, H - 1, data, preds, succs);
        if (level == -1) {
          succs[0] = nullptr;
        } else if (succs[0] != succs[succs[0]->height - 1] ||
                   !succs[level]->flags.is_fully_linked()) {
          usleep(250);
          continue;
        }
        break;
      }
    }

   public:
    MultiIterator(const MultiIterator &) = default;

    T &operator*() {
      debug_assert(succs[0] != nullptr, "Node is nullptr.");
      return succs[0]->value();
    }

    T *operator->() {
      debug_assert(succs[0] != nullptr, "Node is nullptr.");
      return &succs[0]->value();
    }

    operator T &() {
      debug_assert(succs[0] != nullptr, "Node is nullptr.");
      return succs[0]->value();
    }

    bool has_value() { return succs[0] != nullptr; }

    MultiIterator &operator++() {
      debug_assert(succs[0] != nullptr, "Node is nullptr.");
      // NOTE: This whole method can be optimized if it's valid to expect
      // height
      // of 1 on same key elements.

      // First update preds and succs.
      for (int i = succs[0]->height - 1; i >= 0; i--) {
        preds[i] = succs[i];
        succs[i] = preds[i]->forward(i);
      }

      // If there exists current value then check if it is equal to our
      // data.
      if (succs[0] != nullptr) {
        if (succs[0]->value() != data) {
          // Current data isn't equal to our data that means that this
          // is the end of list of same values.
          succs[0] = nullptr;
        } else {
          // Current value is same as our data but we must check that
          // it is valid data and if not we must wait for it to
          // become valid.
          while (succs[0] != succs[succs[0]->height - 1] ||
                 !succs[0]->flags.is_fully_linked()) {
            usleep(250);  // Wait to become linked
            // Reget succs.
            for (int i = succs[0]->height - 1; i >= 0; i--) {
              succs[i] = preds[i]->forward(i);
            }
          }
        }
      }

      return this->derived();
    }

    MultiIterator &operator++(int) { return operator++(); }

    friend bool operator==(const MultiIterator &a, const MultiIterator &b) {
      return a.succs[0] == b.succs[0];
    }

    friend bool operator!=(const MultiIterator &a, const MultiIterator &b) {
      return !(a == b);
    }

    bool is_removed() {
      debug_assert(succs[0] != nullptr, "Node is nullptr.");
      return succs[0]->flags.is_marked();
    }

    // True if this call successfuly removed value. ITERATOR IS'T ADVANCED.
    // False may mean that data has already been removed.
    bool remove() {
      debug_assert(succs[0] != nullptr, "Node is nullptr.");
      // Calls skiplist remove method.

      return skiplist->template remove<K>(
          data, preds, succs,
          SkipList<T>::template MultiIterator<K>::update_path);
    }

   private:
    // TODO: figure why start is unused
    static int update_path(SkipList *skiplist, int start, const K &item,
                           Node *preds[], Node *succs[]) {
      // NOTE: This could be done more efficent than serching for item
      // element again. Whe just need to use infromation already present
      // in preds and succ because he know that this method is used
      // exclusively by passing it into skiplist remove method from
      // MultiIterator remove method.

      // One optimization here would be to wait for is_fully_linked to be
      // true. That way that doesnt have to be done in constructor and
      // ++ operator.
      int level_found = succs[0]->height - 1;

      debug_assert(succs[0] == succs[level_found], "Node is initialized.");

      for (auto it = MultiIterator<K>(skiplist, item); it.has_value(); it++) {
        if (it.succs[0] == succs[0]) {  // Found it
          std::copy(it.preds, it.preds + H, preds);
          std::copy(it.succs, it.succs + H, succs);
          return level_found;
        }
      }
      // Someone removed it
      return -1;
    }

    const K &data;
    SkipList *skiplist;
    Node *preds[H], *succs[H];
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
      debug_assert(skiplist != nullptr, "Skiplist is nullptr.");

      skiplist->gc.add_ref();
    }

   public:
    Accessor(const Accessor &) = delete;

    Accessor(Accessor &&other) : skiplist(other.skiplist) {
      other.skiplist = nullptr;
    }

    ~Accessor() {
      if (skiplist == nullptr) return;

      skiplist->gc.release_ref();
    }

    Iterator begin() { return skiplist->begin(); }

    ConstIterator begin() const { return skiplist->cbegin(); }

    ConstIterator cbegin() const { return skiplist->cbegin(); }

    Iterator end() { return skiplist->end(); }

    ConstIterator end() const { return skiplist->cend(); }

    ConstIterator cend() const { return skiplist->cend(); }

    ReverseIterator rbegin() { return skiplist->rbegin(); }

    ReverseIterator rend() { return skiplist->rend(); }

    template <class K>
    MultiIterator<K> end(const K &data) {
      return skiplist->mend(data);
    }

    template <class K>
    MultiIterator<K> mend(const K &data) {
      return skiplist->template mend<K>(data);
    }

    std::pair<Iterator, bool> insert(const T &item) {
      return skiplist->insert(preds, succs, item);
    }

    std::pair<Iterator, bool> insert(T &&item) {
      return skiplist->insert(preds, succs, std::move(item));
    }

    template <class K, class... Args>
    std::pair<Iterator, bool> emplace(K &key, Args &&... args) {
      return skiplist->emplace(preds, succs, key, std::forward<Args>(args)...);
    }

    Iterator insert_non_unique(const T &item) {
      return skiplist->insert_non_unique(item, preds, succs);
    }

    Iterator insert_non_unique(T &&item) {
      return skiplist->insert_non_unique(std::forward<T>(item), preds, succs);
    }

    template <class K>
    MultiIterator<K> find_multi(const K &item) const {
      return MultiIterator<K>(this->skiplist, item);
    }

    template <class K>
    ConstIterator find(const K &item) const {
      return static_cast<const SkipList &>(*skiplist).find(item);
    }

    template <class K>
    Iterator find(const K &item) {
      return skiplist->find(item);
    }

    template <class K>
    std::pair<ReverseIterator, bool> reverse(const K &item) {
      return skiplist->reverse(item);
    }

    template <class K>
    ConstIterator find_or_larger(const K &item) const {
      return static_cast<const SkipList &>(*skiplist).find_or_larger(item);
    }

    template <class It, class K>
    It find_or_larger(const K &item) {
      return skiplist->find_or_larger<It, K>(item);
    }

    template <class K>
    bool contains(const K &item) const {
      return this->find(item) != this->end();
    }

    template <class K>
    bool remove(const K &item) {
      return skiplist->remove(item, preds, succs,
                              SkipList<T>::template find_path<K>);
    }

    size_t size() const { return skiplist->size(); }

    template <class K>
    size_t distance(const K &first, const K &second) {
      return skiplist->distance(first, second);
    }

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

  ReverseIterator rend() { return ReverseIterator(header->forward(0)); }

  ReverseIterator rbegin() {
    Node *begin = header;
    Node *preds[H];
    for (int level = H - 1; level >= 0; level--) {
      preds[level] = begin;
      while (begin->forward(level) != nullptr) {
        begin = begin->forward(level);
        preds[level] = begin;
      }
    }
    return ReverseIterator(this, begin, preds);
  }

  ConstIterator end() const { return ConstIterator(); }

  ConstIterator cend() const { return ConstIterator(); }

  template <class K>
  MultiIterator<K> end(const K &data) {
    return MultiIterator<K>(data);
  }

  template <class K>
  MultiIterator<K> mend(const K &data) {
    return MultiIterator<K>(data);
  }

  size_t size() const { return count.load(); }

  template <class K>
  static bool equal(const K *item, const Node *const node) {
    return node && item == node->value();
  }

  template <class K>
  static bool greater(const K &item, const Node *const node) {
    return node && item > node->value();
  }

  template <class K>
  static bool less(const K &item, const Node *const node) {
    return (node == nullptr) || item < node->value();
  }

  /**
   * Returns first occurence of item if there exists one.
   */
  template <class K>
  ConstIterator find(const K &item) const {
    return const_cast<SkipList *>(this)->find_node<ConstIterator, K>(item);
  }

  /**
   * Returns first occurence of item if there exists one.
   */
  template <class K>
  Iterator find(const K &item) {
    return find_node<Iterator, K>(item);
  }

  template <class K>
  std::pair<ReverseIterator, bool> reverse(const K &item) {
    auto it = find(item);
    if (it == end()) {
      return std::make_pair(rend(), false);
    }

    Node *preds[H];
    find_path(this, H - 1, item, preds);
    return std::make_pair(ReverseIterator(this, preds[0], preds), true);
  }

  template <class It, class K>
  It find_node(const K &item) {
    auto it = find_or_larger<It, K>(item);
    if (it.node == nullptr || item == *it) {
      return std::move(it);
    } else {
      return It();
    }
  }

  /**
   * Returns iterator on searched element or the first larger element.
   */
  template <class It, class K>
  It find_or_larger(const K &item) {
    Node *node, *pred = header;
    int h = static_cast<int>(pred->height) - 1;

    while (true) {
      // try to descend down first the next key on this layer overshoots
      for (; h >= 0 && less(item, node = pred->forward(h)); --h) {
      }

      // if we overshoot at every layer, item doesn't exist
      if (h < 0) return It(node);

      // the item is farther to the right, continue going right as long
      // as the key is greater than the current node's key
      while (greater(item, node)) pred = node, node = node->forward(h);

      // check if we have a hit. if not, we need to descend down again
      if (!less(item, node) && !node->flags.is_marked()) return It(node);
    }
  }

  template <class K>
  static int find_path(SkipList *skiplist, int start, const K &item,
                       Node *preds[] = nullptr, Node *succs[] = nullptr) {
    int level_found = -1;
    Node *pred = skiplist->header;

    for (int level = start; level >= 0; --level) {
      Node *node = pred->forward(level);

      while (greater(item, node)) pred = node, node = pred->forward(level);

      if (level_found == -1 && !less(item, node)) level_found = level;

      if (preds != nullptr) preds[level] = pred;
      if (succs != nullptr) succs[level] = node;
    }

    return level_found;
  }

  /**
    @brief
    Distance method approximates the distance between two elements.

    General idea of the skiplist distance function is to find the preds which
    are the same for two elements and based on that calculate the distance.
    With every pred which is the same the skiplist should be 2 times smaller.

    @todo
    Current implementation is trivial. The mistake is quite big (up to x8)
    times in some cases.
  */
  template <class K>
  size_t distance(const K &first, const K &second) {
    auto skiplist_size = size();

    // finds the max level of the skiplist based on the size (simple math).
    auto level = static_cast<size_t>(std::round(std::log2(skiplist_size)));

    Node *first_preds[32];
    Node *second_preds[32];

    find_path(this, H - 1, first, first_preds, nullptr);
    find_path(this, H - 1, second, second_preds, nullptr);

    for (int i = level; i >= 0; i--)
      if (first_preds[i] == second_preds[i]) skiplist_size /= 2;

    return skiplist_size;
  }

  template <bool ADDING>
  static bool lock_nodes(uint8_t height, guard_t guards[], Node *preds[],
                         Node *succs[]) {
    Node *prepred = nullptr, *pred = nullptr, *succ = nullptr;
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

  /**
   * Inserts non unique data into list.
   *
   * NOTE: Uses modified logic from insert method.
   */
  Iterator insert_non_unique(T &&data, Node *preds[], Node *succs[]) {
    while (true) {
      auto level = find_path(this, H - 1, data, preds, succs);

      auto height = 1;
      if (level != -1) {
        auto found = succs[level];

        if (found->flags.is_marked()) continue;

        if (!found->flags.is_fully_linked()) {
          usleep(250);
          continue;
        }

        // TODO Optimization for avoiding degrading of skiplist to list.
        // Somehow this operation is errornus.
        // if (found->height == 1) { // To avoid linearization new
        // element
        //                           // will be at least 2 height and
        //                           will
        //                           // be added in front.
        //     height = rnd(H);
        //     //   if (height == 1) height = 2;
        // } else {

        //   Only level 0 will be used so the rest is irrelevant.
        preds[0] = found;
        succs[0] = found->forward(0);

        // This maybe isn't necessary
        auto next = succs[0];
        if (next != nullptr) {
          if (next->flags.is_marked()) continue;

          if (!next->flags.is_fully_linked()) {
            usleep(250);
            continue;
          }
        }
      } else {
        height = rnd(H);
        // Optimization which doesn't add any extra locking.
        if (height == 1)
          height = 2;  // Same key list will be skipped more often.
      }

      guard_t guards[H];

      // try to acquire the locks for predecessors up to the height of
      // the new node. release the locks and try again if someone else
      // has the locks
      if (!lock_nodes<true>(height, guards, preds, succs)) continue;

      return insert_here(Node::create(std::move(data), height), preds, succs,
                         height, guards);
    }
  }

  /**
   * Insert unique data
   *
   * F - type of funct which will create new node if needed. Recieves height
   * of node.
   */
  // TODO this code is not DRY w.r.t. the other insert function (rvalue ref)
  std::pair<Iterator, bool> insert(Node *preds[], Node *succs[],
                                   const T &data) {
    while (true) {
      // TODO: before here was data.first
      auto level = find_path(this, H - 1, data, preds, succs);

      if (level != -1) {
        auto found = succs[level];

        if (found->flags.is_marked()) continue;

        while (!found->flags.is_fully_linked()) usleep(250);

        return {Iterator{succs[level]}, false};
      }

      auto height = rnd(H);
      guard_t guards[H];

      // try to acquire the locks for predecessors up to the height of
      // the new node. release the locks and try again if someone else
      // has the locks
      if (!lock_nodes<true>(height, guards, preds, succs)) continue;

      return {
          insert_here(Node::create(data, height), preds, succs, height, guards),
          true};
    }
  }

  /**
   * Insert unique data
   *
   * F - type of funct which will create new node if needed. Recieves height
   * of node.
   */
  std::pair<Iterator, bool> insert(Node *preds[], Node *succs[], T &&data) {
    while (true) {
      // TODO: before here was data.first
      auto level = find_path(this, H - 1, data, preds, succs);

      if (level != -1) {
        auto found = succs[level];

        if (found->flags.is_marked()) continue;

        while (!found->flags.is_fully_linked()) usleep(250);

        return {Iterator{succs[level]}, false};
      }

      auto height = rnd(H);
      guard_t guards[H];

      // try to acquire the locks for predecessors up to the height of
      // the new node. release the locks and try again if someone else
      // has the locks
      if (!lock_nodes<true>(height, guards, preds, succs)) continue;

      return {insert_here(Node::create(std::move(data), height), preds, succs,
                          height, guards),
              true};
    }
  }

  /**
   * Insert unique data
   *
   * NOTE: This is almost all duplicate code from insert.
   */
  template <class K, class... Args>
  std::pair<Iterator, bool> emplace(Node *preds[], Node *succs[], K &key,
                                    Args &&... args) {
    while (true) {
      // TODO: before here was data.first
      auto level = find_path(this, H - 1, key, preds, succs);

      if (level != -1) {
        auto found = succs[level];

        if (found->flags.is_marked()) continue;

        while (!found->flags.is_fully_linked()) usleep(250);

        return {Iterator{succs[level]}, false};
      }

      auto height = rnd(H);
      guard_t guards[H];

      // try to acquire the locks for predecessors up to the height of
      // the new node. release the locks and try again if someone else
      // has the locks
      if (!lock_nodes<true>(height, guards, preds, succs)) continue;

      return {insert_here(Node::emplace(height, std::forward<Args>(args)...),
                          preds, succs, height, guards),
              true};
    }
  }

  /**
   * Inserts data to specified locked location.
   */
  Iterator insert_here(Node *new_node, Node *preds[], Node *succs[], int height,
                       guard_t guards[])  // TODO: querds unused
  {
    // Node::create(std::move(data), height)
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

    return Iterator{new_node};
  }

  static bool ok_delete(Node *node, int level) {
    return node->flags.is_fully_linked() && node->height - 1 == level &&
           !node->flags.is_marked();
  }

  /**
   * Removes item found with fp with arguments skiplist, preds and succs.
   * fp has to fill preds and succs which reflect location of item or return
   * -1 as in not found otherwise returns level on which the item was first
   * found.
   */
  template <class K>
  bool remove(const K &item, Node *preds[], Node *succs[],
              int (*fp)(SkipList *, int, const K &, Node *[], Node *[])) {
    Node *node = nullptr;
    guard_t node_guard;
    bool marked = false;
    int height = 0;

    while (true) {
      auto level = fp(this, H - 1, item, preds, succs);

      if (!marked && (level == -1 || !ok_delete(succs[level], level)))
        return false;

      if (!marked) {
        node = succs[level];
        height = node->height;
        node_guard = node->acquire_unique();

        if (node->flags.is_marked()) return false;

        node->flags.set_marked();
        marked = true;
      }

      guard_t guards[H];

      if (!lock_nodes<false>(height, guards, preds, succs)) continue;

      for (int level = height - 1; level >= 0; --level)
        preds[level]->forward(level, node->forward(level));

      // TODO: review and test
      gc.collect(node);

      count.fetch_sub(1);
      return true;
    }
  }

  /**
   * number of elements
   */
  std::atomic<size_t> count{0};
  Node *header;
  SkiplistGC<Node> gc;
};
