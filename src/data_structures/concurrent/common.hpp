#pragma once

#include "data_structures/concurrent/skiplist.hpp"
#include "utils/total_ordering.hpp"

using std::pair;

// Item stored in skiplist. Used by ConcurrentMap to
// store key and value but to make ordering on keys.
template <typename K, typename T>
class Item : public TotalOrdering<Item<K, T>>,
             public TotalOrdering<K, Item<K, T>>,
             public TotalOrdering<Item<K, T>, K>,
             public pair<const K, T> {
 public:
  using pair<const K, T>::pair;

  friend constexpr bool operator<(const Item &lhs, const Item &rhs) {
    return lhs.first < rhs.first;
  }

  friend constexpr bool operator==(const Item &lhs, const Item &rhs) {
    return lhs.first == rhs.first;
  }

  friend constexpr bool operator<(const K &lhs, const Item &rhs) {
    return lhs < rhs.first;
  }

  friend constexpr bool operator==(const K &lhs, const Item &rhs) {
    return lhs == rhs.first;
  }

  friend constexpr bool operator<(const Item &lhs, const K &rhs) {
    return lhs.first < rhs;
  }

  friend constexpr bool operator==(const Item &lhs, const K &rhs) {
    return lhs.first == rhs;
  }
};

// Common base for accessor of all derived containers(ConcurrentMap,
// ConcurrentSet, ...) from SkipList.
template <typename T, bool IsConst>
class AccessorBase {
  typedef
      typename std::conditional<IsConst, const SkipList<T>, SkipList<T>>::type
          list;
  typedef typename SkipList<T>::Iterator list_it;
  typedef typename SkipList<T>::ConstIterator list_it_con;

 protected:
  AccessorBase(list *skiplist) : accessor(skiplist->access()) {}

 public:
  AccessorBase(const AccessorBase &) = delete;

  AccessorBase(AccessorBase &&other) : accessor(std::move(other.accessor)) {}

  ~AccessorBase() {}

  size_t size() { return accessor.size(); };

  auto begin() { return accessor.begin(); }

  auto begin() const { return accessor.cbegin(); }

  list_it_con cbegin() const { return accessor.cbegin(); }

  auto end() { return accessor.end(); }

  auto end() const { return accessor.cend(); }

  list_it_con cend() const { return accessor.cend(); }

  size_t size() const { return accessor.size(); }

 protected:
  decltype(std::declval<list>().access()) accessor;
};
