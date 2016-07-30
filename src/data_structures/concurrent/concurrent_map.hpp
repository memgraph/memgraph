#pragma once

#include "data_structures/concurrent/skiplist.hpp"
#include "utils/total_ordering.hpp"

using std::pair;

template <typename K, typename T> class ConcurrentMap {

  class Item : public TotalOrdering<Item>,
               public TotalOrdering<K, Item>,
               public TotalOrdering<Item, K>,
               public pair<const K, T> {
  public:
    using pair<const K, T>::pair;

    friend constexpr bool operator<(const Item &lhs, const Item &rhs) {
      std::pair<const K, T> *a;
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

  typedef SkipList<Item> list;
  typedef typename SkipList<Item>::Iterator list_it;
  typedef typename SkipList<Item>::ConstIterator list_it_con;

public:
  ConcurrentMap() {}

  friend class Accessor;
  class Accessor {
    friend class ConcurrentMap;

    Accessor(list *skiplist) : accessor(skiplist->access()) {}

  public:
    Accessor(const Accessor &) = delete;

    Accessor(Accessor &&other) : accessor(std::move(other.accessor)) {}

    ~Accessor() {}

    list_it begin() { return accessor.begin(); }

    list_it_con begin() const { return accessor.cbegin(); }

    list_it_con cbegin() const { return accessor.cbegin(); }

    list_it end() { return accessor.end(); }

    list_it_con end() const { return accessor.cend(); }

    list_it_con cend() const { return accessor.cend(); }

    std::pair<list_it, bool> insert(const K &key, const T &data) {
      return accessor.insert(Item(key, data));
    }

    std::pair<list_it, bool> insert(const K &key, T &&data) {
      return accessor.insert(Item(key, std::forward<T>(data)));
    }

    std::pair<list_it, bool> insert(K &&key, T &&data) {
      return accessor.insert(Item(std::forward<K>(key), std::forward<T>(data)));
    }

    list_it_con find(const K &key) const { return accessor.find(key); }

    list_it find(const K &key) { return accessor.find(key); }

    bool contains(const K &key) const { return this->find(key) != this->end(); }

    bool remove(const K &key) { return accessor.remove(key); }

    size_t size() const { return accessor.size(); }

  private:
    typename list::Accessor accessor;
  };

  Accessor access() { return Accessor(&skiplist); }

  const Accessor access() const { return Accessor(&skiplist); }

private:
  list skiplist;
};
