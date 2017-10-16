#pragma once

#include <algorithm>
#include <cmath>
#include <memory>
#include <type_traits>

#include "glog/logging.h"
#include "utils/crtp.hpp"
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

    /** Waits until the these flags don't get the "fully linked" status. */
    void wait_fully_linked() const {
      while (!is_fully_linked()) usleep(250);
    }

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

    /**
     * Creates a new node for the given item.
     *
     * @param item - The item that is being inserted into the skiplist.
     * @param height - Node height.
     * @tparam TItem - This function is templatized so it can accept
     *  a universal reference to the item. TItem should be the same
     *  as T.
     */
    template <typename TItem>
    static Node *create(TItem &&item, uint8_t height) {
      auto node = allocate(height);

      // we have raw memory and we need to construct an object
      // of type Node on it
      return new (node) Node(std::forward<TItem>(item), height);
    }

    template <class... Args>
    static Node *emplace(uint8_t height, Args &&... args) {
      auto node = allocate(height);

      // we have raw memory and we need to construct an object
      // of type Node on it
      return new (node) Node(height, std::forward<Args>(args)...);
    }

    bool ok_delete(int level) const {
      return flags.is_fully_linked() && height - 1 == level &&
             !flags.is_marked();
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

    const T &operator*() const {
      DCHECK(node != nullptr) << "Node is nullptr.";
      return node->value();
    }

    const T *operator->() const {
      DCHECK(node != nullptr) << "Node is nullptr.";
      return &node->value();
    }

    T &operator*() {
      DCHECK(node != nullptr) << "Node is nullptr.";
      return node->value();
    }

    T *operator->() {
      DCHECK(node != nullptr) << "Node is nullptr.";
      return &node->value();
    }

    operator T &() {
      DCHECK(node != nullptr) << "Node is nullptr.";
      return node->value();
    }

    It &operator++() {
      DCHECK(node != nullptr) << "Node is nullptr.";
      node = node->forward(0);
      return this->derived();
    }

    bool has_next() {
      DCHECK(node != nullptr) << "Node is nullptr.";
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

    const T &operator*() const {
      return IteratorBase<ConstIterator>::operator*();
    }

    const T *operator->() const {
      return IteratorBase<ConstIterator>::operator->();
    }

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
      DCHECK(node_ != nullptr) << "Node is nullptr.";
      return node_->value();
    }

    T *operator->() {
      DCHECK(node_ != nullptr) << "Node is nullptr.";
      return &node_->value();
    }

    operator T &() {
      DCHECK(node_ != nullptr) << "Node is nullptr.";
      return node_->value();
    }

    ReverseIterator &operator++() {
      DCHECK(node_ != nullptr) << "Node is nullptr.";
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

  template <typename TSkipList>
  class Accessor {
    friend class SkipList;

    Accessor(TSkipList *skiplist)
        : skiplist(skiplist), status_(skiplist->gc.CreateNewAccessor()) {
      DCHECK(skiplist != nullptr) << "Skiplist is nullptr.";
    }

   public:
    Accessor(const Accessor &) = delete;

    Accessor(Accessor &&other)
        : skiplist(other.skiplist), status_(other.status_) {
      other.skiplist = nullptr;
    }

    ~Accessor() {
      if (skiplist == nullptr) return;

      status_.alive_ = false;
    }

    auto begin() const { return skiplist->begin(); }

    ConstIterator cbegin() const { return skiplist->cbegin(); }

    auto end() const { return skiplist->end(); }

    ConstIterator cend() const { return skiplist->cend(); }

    ReverseIterator rbegin() { return skiplist->rbegin(); }

    ReverseIterator rend() { return skiplist->rend(); }

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

    /**
     * Returns an iterator pointing to the element equal to
     * item, or the first larger element.
     *
     * @param item An item that is comparable to skiplist element type.
     * @tparam TItem item type
     */
    template <class TItem>
    Iterator find_or_larger(const TItem &item) {
      return skiplist->template find_or_larger<Iterator, TItem>(item);
    }

    /**
     * Returns an iterator pointing to the element equal to
     * item, or the first larger element.
     *
     * @param item An item that is comparable to skiplist element type.
     * @tparam TItem item type
     */
    template <class TItem>
    ConstIterator find_or_larger(const TItem &item) const {
      return static_cast<const SkipList &>(*skiplist)
          .find_or_larger<ConstIterator, TItem>(item);
    }

    /**
     * Position and count estimation. Gives estimates
     * on the position of the given item in this skiplist, and
     * the number of identical items according to 'less'.
     *
     * If `item` is not contained in the skiplist,
     * then the position where it would be inserted is returned
     * as the position estimate, and 0 as count estimate.
     *
     * Position and count detection works by iterating over the
     * list at a certain level. These levels can be tuned as
     * a performance vs precision optimization. Lower levels mean
     * higher precision, higher levels mean better performance.
     * TODO: tune the levels once benchmarks are available.
     *
     * @param item The item for which the position is estimated.
     * @param less Comparison function. It must be partially
     *  consistent with natural comparison of Skiplist elements:
     *  if `less` indicates that X is less than
     *  Y, then natural comparison must indicate the same. The
     *  reverse does not have to hold.
     * @param greater Comparsion function, analogue to less.
     * @param position_level_reduction - Defines at which level
     *  item position is estimated. Position level is defined
     *  as log2(skiplist->size()) - position_level_reduction.
     * @param count_max_level - Defines the max level at which
     *  item count is estimated.
     * @tparam TItem - type of item skiplist elements are compared
     * to. Does not have to be the same type as skiplist element.
     * @tparam TLess - less comparison function type.
     * @tparam TEqual - equality comparison function type.
     * @return A pair of ints where the first element is the estimated
     *  position of item, and the second is the estimated number
     *  of items that are the same according to `less`.
     */
    template <typename TItem, typename TLess = std::less<T>,
              typename TEqual = std::equal_to<T>>
    std::pair<int64_t, int64_t> position_and_count(
        const TItem &item, TLess less = TLess(), TEqual equal = TEqual(),
        int position_level_reduction = 10, int count_max_level = 3) {
      // the level at which position will be sought
      int position_level = std::max(
          0, static_cast<int>(std::lround(std::log2(skiplist->size()))) -
                 position_level_reduction);

      Node *pred = skiplist->header;
      Node *succ = nullptr;

      int position = 0;
      for (int i = position_level; i >= 0; i--) {
        // count how many towers we pass on this level,
        // used for calculating item position
        int tower_count = 0;

        // on the current height (i) find the last tower
        // whose value is lesser than item, store it in pred
        // while succ will be either skiplist end or the
        // first element greater than item
        succ = pred->forward(i);
        while (succ &&
               !(less(item, succ->value()) || equal(item, succ->value()))) {
          pred = succ;
          succ = succ->forward(i);
          tower_count++;
        }

        // in the succs field we'll keep track of successors
        // that are equal to item, or nullptr otherwise
        succs[i] = (!succ || less(item, succ->value())) ? nullptr : succ;

        position += (1 << i) * tower_count;
      }

      // if succ is nullptr, then item is greater than all elements in the list
      if (succ == nullptr) return std::make_pair(size(), 0);

      // now we need to estimate the count of elements equal to item
      // we'll do that by looking for the first element that is greater
      // than item, and counting how far we have to look

      // first find the rightmost (highest) succ that has value == item
      int count_level = 0;
      for (int i = position_level; i >= 0; i--)
        if (succs[i]) {
          count_level = i;
          break;
        }
      count_level = std::min(count_level, count_max_level);
      succ = succs[count_level];

      // it is possible that succ just became null (even though before
      // it wasn't). that happens when item is lesser then all list elems
      if (!succ) return std::make_pair(0, 0);

      // now expand to the right as long as element value == item
      // at the same time accumulate count
      int count = 1 << count_level;
      for (; count_level >= 0; count_level--) {
        Node *next = succ->forward(count_level);
        while (next && !less(item, next->value())) {
          succ = next;
          next = next->forward(count_level);
          count += 1 << count_level;
        }
      }

      return std::make_pair(position, count);
    }

    template <class K>
    bool contains(const K &item) const {
      return this->find(item) != this->end();
    }

    template <class K>
    bool remove(const K &item) {
      return skiplist->remove(item, preds, succs);
    }

    size_t size() const { return skiplist->size(); }

    template <class K>
    size_t distance(const K &first, const K &second) {
      return skiplist->distance(first, second);
    }

   private:
    TSkipList *skiplist;
    Node *preds[H], *succs[H];
    typename SkipListGC<Node>::AccessorStatus &status_;
  };

  Accessor<SkipList> access() { return Accessor<SkipList>(this); }

  Accessor<const SkipList> access() const {
    return Accessor<const SkipList>(this);
  }

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

  size_t size() const { return count.load(); }

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

    // TODO why are preds created here and not reused from accessor?
    Node *preds[H];
    find_path(item, preds);
    return std::make_pair(ReverseIterator(this, preds[0], preds), true);
  }

  template <class It, class K>
  It find_node(const K &item) const {
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
  It find_or_larger(const K &item) const {
    Node *node, *pred = header;
    int h = static_cast<int>(pred->height) - 1;

    while (true) {
      // try to descend down first while the next key on this layer overshoots
      // or the next key is marked for deletion
      for (; h >= 0 && less(item, node = pred->forward(h)); --h) {
      }

      // if we overshoot at every layer, item doesn't exist
      if (h < 0) return It(node);

      // the item is farther to the right, continue going right as long
      // as the key is greater than the current node's key
      while (greater(item, node)) pred = node, node = node->forward(h);

      // check if we have a hit. if not, we need to descend down again
      if (!less(item, node)) {
        if (!node->flags.is_marked()) return It(node);
        return It(nullptr);
      }
    }
  }

  /**
   * Finds the location in the skiplist for the given item.
   * Fills up the predecessor and successor nodes if pointers
   * are given.
   *
   * @param item - the item for which to find the location.
   * @param preds - An array of predecessor nodes. Filled up with
   *  towers that would link to the new tower. If nullptr, it is
   *  ignored.
   * @param succs - Like preds, for successor nodes.
   * @tparam K - type of item that must be comparable to the
   *  type of item <T> stored in the skiplist.
   * @return - The height of the node already present in the
   *  skiplist, that matches the given item (is equal to it).
   *  Returns -1 if there is no matching item in the skiplist.
   */
  template <typename K>
  int find_path(const K &item, Node *preds[] = nullptr,
                Node *succs[] = nullptr) const {
    int level_found = -1;
    Node *pred = header;

    for (int level = H - 1; level >= 0; --level) {
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

    // TODO
    // inconsistent design, it seems that Accessor is trying to reuse nodes
    // and pass the same ones to SkipList functions, why is this function
    // doing it differently?
    // also, why is 32 hardcoded?
    Node *first_preds[32];
    Node *second_preds[32];

    find_path(first, first_preds, nullptr);
    find_path(second, second_preds, nullptr);

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
   * Insert an element into the skiplist.
   *
   * @param preds - Predecessor nodes
   * @param succs - Successor nodes
   * @param data - Item to insert into the skiplist
   * @tparam TItem - Item type. Must be the same as skiplist item
   *  type <T>. This function is templatized so it can accept
   *  universal references and keep the code dry.
   */
  template <typename TItem>
  std::pair<Iterator, bool> insert(Node *preds[], Node *succs[], TItem &&data) {
    while (true) {
      auto level = find_path(data, preds, succs);

      if (level != -1) {
        auto found = succs[level];

        if (found->flags.is_marked()) continue;
        found->flags.wait_fully_linked();

        return {Iterator{succs[level]}, false};
      }

      auto height = rnd(H);
      guard_t guards[H];

      // try to acquire the locks for predecessors up to the height of
      // the new node. release the locks and try again if someone else
      // has the locks
      if (!lock_nodes<true>(height, guards, preds, succs)) continue;

      return {insert_here(Node::create(std::forward<TItem>(data), height),
                          preds, succs, height),
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
      auto level = find_path(key, preds, succs);

      if (level != -1) {
        auto found = succs[level];

        if (found->flags.is_marked()) continue;
        found->flags.wait_fully_linked();

        return {Iterator{succs[level]}, false};
      }

      auto height = rnd(H);
      guard_t guards[H];

      // try to acquire the locks for predecessors up to the height of
      // the new node. release the locks and try again if someone else
      // has the locks
      if (!lock_nodes<true>(height, guards, preds, succs)) continue;

      return {insert_here(Node::emplace(height, std::forward<Args>(args)...),
                          preds, succs, height),
              true};
    }
  }

  /**
   * Inserts data to specified locked location.
   */
  Iterator insert_here(Node *new_node, Node *preds[], Node *succs[],
                       int height) {
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
    count++;

    return Iterator{new_node};
  }

  /**
   * Removes item found with fp with arguments skiplist, preds and succs.
   */
  template <class K>
  bool remove(const K &item, Node *preds[], Node *succs[]) {
    Node *node = nullptr;
    guard_t node_guard;
    bool marked = false;
    int height = 0;

    while (true) {
      auto level = find_path(item, preds, succs);

      if (!marked && (level == -1 || !succs[level]->ok_delete(level)))
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

      gc.Collect(node);

      count--;
      return true;
    }
  }

  /**
   * number of elements
   */
  std::atomic<size_t> count{0};
  Node *header;
  mutable SkipListGC<Node> gc;
};
