// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <algorithm>
#include <chrono>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "utils/exceptions.hpp"

namespace memgraph::utils {

/**
 * Outputs a collection of items as a string, separating them with the given delimiter.
 *
 * @param first Starting iterator of collection which items are going to be
 *  printed.
 * @param last Ending iterator of the collection.
 * @param delim Delimiter that is put between items.
 * @param transformation Function which accepts an item and returns a derived value.
 */
template <typename TIterator, typename TTransformation>
inline std::string IterableToString(TIterator first, TIterator last, const std::string_view delim = ", ",
                                    TTransformation transformation = {}) {
  std::string representation;
  if (first != last) {
    representation.append(transformation(*first));
    ++first;
  }
  for (; first != last; ++first) {
    representation.append(delim);
    representation.append(transformation(*first));
  }

  return representation;
}

/**
 * Outputs a collection of items as a string, separating them with the given delimiter.
 *
 * @param iterable An iterable collection of items.
 * @param delim Delimiter that is put between items.
 * @param transformation Function which accepts an item and returns a derived value.
 */
template <typename TIterable, typename TTransformation>
inline std::string IterableToString(const TIterable &iterable, const std::string_view delim = ", ",
                                    TTransformation transformation = {}) {
  return IterableToString(iterable.begin(), iterable.end(), delim, transformation);
}

/**
 * Outputs a collection of items as a string, separating them with the given delimiter.
 *
 * @param iterable An iterable collection of items.
 * @param delim Delimiter that is put between items.
 */
template <typename TIterable>
inline std::string IterableToString(const TIterable &iterable, const std::string_view delim = ", ") {
  return IterableToString(iterable, delim, [](const auto &item) { return item; });
}

/**
 * Outputs a collection of items to the given stream, separating them with the
 * given delimiter.
 *
 * @param stream Destination stream.
 * @param first Starting iterator of collection which items are going to be
 *  printed.
 * @param last Ending iterator of the collection.
 * @param delim Delimiter that is put between items.
 * @param streamer Function which accepts a TStream and an item and streams the
 *  item to the stream.
 */
template <typename TStream, typename TIterator, typename TStreamer>
inline void PrintIterable(TStream *stream, TIterator first, TIterator last, const std::string &delim = ", ",
                          TStreamer streamer = {}) {
  if (first != last) {
    streamer(*stream, *first);
    ++first;
  }
  for (; first != last; ++first) {
    *stream << delim;
    streamer(*stream, *first);
  }
}

/**
 * Outputs a collection of items to the given stream, separating them with the
 * given delimiter.
 *
 * @param stream Destination stream.
 * @param iterable An iterable collection of items.
 * @param delim Delimiter that is put between items.
 * @param streamer Function which accepts a TStream and an item and
 *  streams the item to the stream.
 */
template <typename TStream, typename TIterable, typename TStreamer>
inline void PrintIterable(TStream &stream, const TIterable &iterable, const std::string &delim = ", ",
                          TStreamer streamer = {}) {
  PrintIterable(&stream, iterable.begin(), iterable.end(), delim, streamer);
}

/**
 * Outputs a collection of items to the given stream, separating them with the
 * given delimiter.
 *
 * @param stream Destination stream.
 * @param iterable An iterable collection of items.
 * @param delim Delimiter that is put between items.
 */
template <typename TStream, typename TIterable>
inline void PrintIterable(TStream &stream, const TIterable &iterable, const std::string &delim = ", ") {
  PrintIterable(stream, iterable, delim, [](auto &stream, const auto &item) { stream << item; });
}

/**
 * Finds a mapping if it exists for given key or returns the provided value.
 *
 * @param map A map-like container which has the `find` method.
 * @param key Key to look for in the map.
 * @param or_value Value to use if the key is not in the map.
 * @return Pair of value and a boolean indicating whether the key was in the
 * map.
 */
template <class TMap, class TKey, class TVal>
inline std::pair<TVal, bool> FindOr(const TMap &map, const TKey &key, TVal &&or_value) {
  auto it = map.find(key);
  if (it != map.end()) return {it->second, true};
  return {std::forward<TVal>(or_value), false};
}

/**
 * Returns the *copy* of the first element from an iterable.
 *
 * @param iterable An iterable collection of values.
 * @return The first element of the `iterable`.
 * @exception BasicException is thrown if the `iterable` is empty.
 */
template <class TIterable>
inline auto First(TIterable &&iterable) {
  if (iterable.begin() != iterable.end()) return *iterable.begin();
  throw utils::BasicException("Empty iterable");
}

/**
 * Returns the *copy* of the first element from an iterable.
 *
 * @param iterable An iterable collection of values.
 * @param empty_value Value to return if the `iterable` is empty.
 * @return The first element of the `iterable` or the `empty_value`.
 */
template <class TVal, class TIterable>
inline TVal First(TIterable &&iterable, TVal &&empty_value) {
  if (iterable.begin() != iterable.end()) return *iterable.begin();
  return empty_value;
}

template <class TElement, class THash, class TEqual, class TAllocator>
bool Contains(const std::unordered_set<TElement, THash, TEqual, TAllocator> &iterable, const TElement &element) {
  return iterable.find(element) != iterable.end();
}

template <class TKey, class TValue, class THash, class TKeyEqual, class TAllocator>
bool Contains(const std::unordered_map<TKey, TValue, THash, TKeyEqual, TAllocator> &iterable, const TKey &key) {
  return iterable.find(key) != iterable.end();
}

/**
 * Returns `true` if the given iterable contains the given element.
 *
 * @param iterable An iterable collection of values.
 * @param element The sought element.
 * @return `true` if element is contained in iterable.
 * @tparam TIiterable type of iterable.
 * @tparam TElement type of element.
 */
template <typename TIterable, typename TElement>
inline bool Contains(const TIterable &iterable, const TElement &element) {
  return std::find(iterable.begin(), iterable.end(), element) != iterable.end();
}

/**
 * Return a reversed copy of the given collection.
 * The copy is allocated using the default allocator.
 */
template <class TCollection>
TCollection Reversed(const TCollection &collection) {
  return TCollection(std::rbegin(collection), std::rend(collection));
}

/**
 * Return a reversed copy of the given collection.
 * The copy is allocated with the given `alloc`.
 */
template <class TCollection, class TAllocator>
TCollection Reversed(const TCollection &collection, const TAllocator &alloc) {
  return TCollection(std::rbegin(collection), std::rend(collection), alloc);
}

/**
 * Converts a (beginning, end) pair of iterators into an iterable that can be
 * passed on to itertools.
 */
template <typename TIterator>
class Iterable {
 public:
  Iterable(TIterator &&begin, TIterator &&end)
      : begin_(std::forward<TIterator>(begin)), end_(std::forward<TIterator>(end)) {}

  auto begin() { return begin_; };
  auto end() { return end_; };

 private:
  TIterator begin_;
  TIterator end_;
};

}  // namespace memgraph::utils
