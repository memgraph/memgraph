#pragma once

#include <algorithm>
#include <string>
#include <utility>

#include "utils/exceptions.hpp"

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
void PrintIterable(TStream &stream, const TIterable &iterable,
                   const std::string &delim = ", ", TStreamer streamer = {}) {
  bool first = true;
  for (const auto &item : iterable) {
    if (first)
      first = false;
    else
      stream << delim;
    streamer(stream, item);
  }
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
void PrintIterable(TStream &stream, const TIterable &iterable,
                   const std::string &delim = ", ") {
  PrintIterable(stream, iterable, delim,
                [](auto &stream, const auto &item) { stream << item; });
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
std::pair<TVal, bool> FindOr(const TMap &map, const TKey &key,
                             TVal &&or_value) {
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
auto First(TIterable &&iterable) {
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
TVal First(TIterable &&iterable, TVal &&empty_value) {
  if (iterable.begin() != iterable.end()) return *iterable.begin();
  return empty_value;
}
