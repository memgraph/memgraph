#pragma once

#include <algorithm>
#include <string>
#include <utility>

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
  if (it != map.end()) {
    return {it->second, true};
  }
  return {std::forward<TVal>(or_value), false};
}
