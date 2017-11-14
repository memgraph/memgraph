#pragma once

#include <algorithm>
#include <string>
#include <utility>

#include "utils/exceptions.hpp"

namespace utils {

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
inline void PrintIterable(TStream &stream, const TIterable &iterable,
                          const std::string &delim = ", ",
                          TStreamer streamer = {}) {
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
inline void PrintIterable(TStream &stream, const TIterable &iterable,
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
inline std::pair<TVal, bool> FindOr(const TMap &map, const TKey &key,
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
 * Converts a (beginning, end) pair of iterators into an iterable that can be
 * passed on to itertools.
 */
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
}  // namespace utils
