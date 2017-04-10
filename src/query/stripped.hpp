#pragma once

#include <map>

#include "parameters.hpp"
#include "storage/property_value_store.hpp"
#include "utils/assert.hpp"
#include "utils/hashing/fnv.hpp"

/*
* StrippedQuery contains:
*     * stripped query
*     * plan arguments stripped from query
*     * hash of stripped query
*/
struct StrippedQuery {
  StrippedQuery(const std::string &unstripped_query, const std::string &&query,
                const Parameters &arguments, HashType hash)
      : unstripped_query(unstripped_query),
        query(query),
        arguments(arguments),
        hash(hash) {}

  /**
   * Copy constructor is deleted because we don't want to make unnecessary
   * copies of this object (copying of string and vector could be expensive)
   */
  StrippedQuery(const StrippedQuery &other) = delete;
  StrippedQuery &operator=(const StrippedQuery &other) = delete;

  /**
   * Move is allowed operation because it is not expensive and we can
   * move the object after it was created.
   */
  StrippedQuery(StrippedQuery &&other) = default;
  StrippedQuery &operator=(StrippedQuery &&other) = default;

  // original, unstripped query
  const std::string unstripped_query;

  // stripped query
  const std::string query;

  // striped arguments
  const Parameters arguments;

  // hash based on the stripped query
  const HashType hash;
};
