#pragma once

#include <map>

#include "query/backend/cpp/typed_value.hpp"
#include "storage/property_value_store.hpp"

#include "utils/assert.hpp"
#include "utils/hashing/fnv.hpp"
#include "parameters.hpp"

/*
* StrippedQuery contains:
*     * stripped query
*     * plan arguments stripped from query
*     * hash of stripped query
*/
struct StrippedQuery {
  StrippedQuery(const std::string &&query,
                const Parameters &arguments,
                HashType hash)
      : query(query), arguments(arguments), hash(hash) {}

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

  /**
   * Stripped query
   */
  const std::string query;

  /**
   * Stripped arguments
   */
  const Parameters arguments;

  /**
   * Hash based on stripped query.
   */
  const HashType hash;
};
