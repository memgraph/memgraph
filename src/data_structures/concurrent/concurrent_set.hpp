#pragma once

#include "data_structures/concurrent/common.hpp"
#include "data_structures/concurrent/skiplist.hpp"

// Multi thread safe set based on skiplist.
// T - type of data.
template <class T>
class ConcurrentSet {
  typedef SkipList<T> list;
  typedef typename SkipList<T>::Iterator list_it;
  typedef typename SkipList<T>::ConstIterator list_it_con;

 public:
  ConcurrentSet() {}

  template <bool IsConst = false>
  class Accessor : public AccessorBase<T, IsConst> {
    friend class ConcurrentSet;

    using AccessorBase<T, IsConst>::AccessorBase;

   private:
    using AccessorBase<T, IsConst>::accessor;

   public:
    std::pair<list_it, bool> insert(const T &item) {
      return accessor.insert(item);
    }

    std::pair<list_it, bool> insert(T &&item) {
      return accessor.insert(std::move(item));
    }

    list_it_con find(const T &item) const { return accessor.find(item); }

    list_it find(const T &item) { return accessor.find(item); }

    // Returns iterator to item or first larger if it doesn't exist.
    template <class K>
    list_it_con find_or_larger(const K &item) const {
      return accessor.find_or_larger(item);
    }

    // Returns iterator to item or first larger if it doesn't exist.
    template <class K>
    list_it find_or_larger(const K &item) {
      return accessor.find_or_larger(item);
    }

    // Returns iterator to item or first larger if it doesn't exist.
    template <class K>
    list_it_con cfind_or_larger(const K &item) {
      return accessor.template find_or_larger<list_it_con, K>(item);
    }

    bool contains(const T &item) const {
      return this->find(item) != this->end();
    }

    bool remove(const T &item) { return accessor.remove(item); }
  };

  auto access() { return Accessor<false>(&skiplist); }
  auto access() const { return Accessor<true>(&skiplist); }

 private:
  list skiplist;
};
