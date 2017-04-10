#pragma once

#include <algorithm>

#include "logging/default.hpp"

/**
 * Goes from first to last item in a container, if an element satisfying the
 * predicate then the action is going to be executed and the element is going
 * to be shifted to the end of the container.
 *
 * @tparam ForwardIt type of forward iterator
 * @tparam UnaryPredicate type of predicate
 * @tparam Action type of action
 *
 * @return a past-the-end iterator for the new end of the range
 */
template <class ForwardIt, class UnaryPredicate, class Action>
ForwardIt action_remove_if(ForwardIt first, ForwardIt last, UnaryPredicate p,
                           Action a) {
  auto it = std::remove_if(first, last, p);
  if (it == last) return it;
  std::for_each(it, last, a);
  return it;
}

/**
 * Outputs a collection of items to the given stream, separating them with the
 * given delimiter.
 *
 * @param stream Destination stream.
 * @param iterable An iterable collection of items.
 * @param delim Delimiter that is put between items.
 * @param converter Function which converts an item to a type which supports
 *        @c operator<<.
 */
template <typename TStream, typename TIterable, typename TConverter>
void PrintIterable(TStream &stream, const TIterable &iterable,
                   const std::string &delim = ", ", TConverter converter = {}) {
  bool first = true;
  for (const auto &item : iterable) {
    if (first)
      first = false;
    else
      stream << delim;
    stream << converter(item);
  }
}

