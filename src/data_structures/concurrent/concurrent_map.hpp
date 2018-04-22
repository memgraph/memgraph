#pragma once

#include "data_structures/concurrent/skiplist.hpp"
#include "utils/total_ordering.hpp"

/// Thread-safe map intended for high concurrent throughput.
///
/// @tparam TKey is a type of key.
/// @tparam TValue is a type of data.
template <typename TKey, typename TValue>
class ConcurrentMap {
  /// At item in the concurrent map. A pair of <TKey, TValue> that compares on
  /// the first value (key). Comparable to another Item, or only to the TKey.
  class Item : public utils::TotalOrdering<Item>,
               public utils::TotalOrdering<TKey, Item>,
               public utils::TotalOrdering<Item, TKey>,
               public std::pair<const TKey, TValue> {
   public:
    using std::pair<const TKey, TValue>::pair;

    friend constexpr bool operator<(const Item &lhs, const Item &rhs) {
      return lhs.first < rhs.first;
    }

    friend constexpr bool operator==(const Item &lhs, const Item &rhs) {
      return lhs.first == rhs.first;
    }

    friend constexpr bool operator<(const TKey &lhs, const Item &rhs) {
      return lhs < rhs.first;
    }

    friend constexpr bool operator==(const TKey &lhs, const Item &rhs) {
      return lhs == rhs.first;
    }

    friend constexpr bool operator<(const Item &lhs, const TKey &rhs) {
      return lhs.first < rhs;
    }

    friend constexpr bool operator==(const Item &lhs, const TKey &rhs) {
      return lhs.first == rhs;
    }
  };

  using Iterator = typename SkipList<Item>::Iterator;

 public:
  ConcurrentMap() {}

  template <typename TSkipList>
  class Accessor : public SkipList<Item>::template Accessor<TSkipList> {
   public:
    friend class ConcurrentMap;

    using SkipList<Item>::template Accessor<TSkipList>::Accessor;

    std::pair<Iterator, bool> insert(const TKey &key, const TValue &data) {
      return SkipList<Item>::template Accessor<TSkipList>::insert(
          Item(key, data));
    }

    std::pair<Iterator, bool> insert(const TKey &key, TValue &&data) {
      return SkipList<Item>::template Accessor<TSkipList>::insert(
          Item(key, std::move(data)));
    }

    std::pair<Iterator, bool> insert(TKey &&key, TValue &&data) {
      return SkipList<Item>::template Accessor<TSkipList>::insert(
          Item(std::forward<TKey>(key), std::forward<TValue>(data)));
    }

    template <class... Args1, class... Args2>
    std::pair<Iterator, bool> emplace(const TKey &key,
                                      std::tuple<Args1...> first_args,
                                      std::tuple<Args2...> second_args) {
      return SkipList<Item>::template Accessor<TSkipList>::emplace(
          key, std::piecewise_construct,
          std::forward<std::tuple<Args1...>>(first_args),
          std::forward<std::tuple<Args2...>>(second_args));
    }
  };

  auto access() { return Accessor<SkipList<Item>>(&skiplist); }
  auto access() const { return Accessor<const SkipList<Item>>(&skiplist); }

 private:
  SkipList<Item> skiplist;
};
