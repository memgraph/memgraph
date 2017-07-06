#pragma once

#include <algorithm>
#include <string>

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
