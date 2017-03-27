#pragma once

#include <cppitertools/reversed.hpp>
#include "cppitertools/imap.hpp"

/**
 * Creates an iterator over record accessors (Edge or Vertex).
 *
 * @tparam TAccessor The exact type of accessor.
 * @tparam TIterable An iterable of pointers to version list objects.
 *
 * @param records An iterable of version list pointers for which accessors
 *  need to be created.
 * @param db_accessor A database accessor to create the record accessors with.
 */
template <typename TAccessor, typename TIterable>
auto make_accessor_iterator(const TIterable &records, GraphDbAccessor &db_accessor) {
  return iter::imap([&db_accessor](auto vlist) {
    return TAccessor(*vlist, db_accessor);
    // note that here we iterate over records in REVERSED order
    // this is necessary for DETACH DELETE (see GraphDbAccessor)
    // which deletes items from relationship collections in a
    // vertex accessor
  }, iter::reversed(records));
}
