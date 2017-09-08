#pragma once

#include <cppitertools/reversed.hpp>
#include "cppitertools/imap.hpp"

/**
 * Converts a (beginning, end) pair of iterators into an iterable that can be
 * passed on to itertools. */
template <typename TIterator>
class Iterable {
 public:
  Iterable(TIterator &&begin, TIterator &&end)
      : begin_(std::forward<TIterator>(begin)),
        end_(std::forward<TIterator>(end)) {}

  auto begin() { return begin_; };
  auto end() { return end_; };

 private:
  TIterator begin_;
  TIterator end_;
};

/**
 * Creates an iterator over record accessors (Edge or Vertex).
 *
 * @param begin Start iterator over (vertex_vlist_ptr, edge_vlist_ptr) pairs.
 * @param end End iterator over (vertex_vlist_ptr, edge_vlist_ptr) pairs.
 * @param db_accessor A database accessor to create the record accessors with.
 *
 * @tparam TAccessor The exact type of accessor.
 * @tparam TIterable An iterable of pointers to version list objects.
 */
template <typename TAccessor, typename TIterator>
auto MakeAccessorIterator(TIterator &&begin, TIterator &&end,
                          GraphDbAccessor &db_accessor) {
  return iter::imap(
      [&db_accessor](auto &edges_element) {
        return TAccessor(*edges_element.edge, db_accessor);
      },
      Iterable<TIterator>(std::forward<TIterator>(begin),
                          std::forward<TIterator>(end)));
}
