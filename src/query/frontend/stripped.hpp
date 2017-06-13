#pragma once

#include <map>

#include "logging/loggable.hpp"
#include "query/parameters.hpp"
#include "storage/property_value_store.hpp"
#include "utils/assert.hpp"
#include "utils/hashing/fnv.hpp"

namespace query {

/*
* StrippedQuery contains:
*     * stripped query
*     * plan arguments stripped from query
*     * hash of stripped query
*/
class StrippedQuery : Loggable {
 public:
  /**
   * Strips the input query and stores stripped query, stripped arguments and
   * stripped query hash.
   *
   * @param query input query
   */
  explicit StrippedQuery(const std::string &query);

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

  const std::string &query() const { return query_; }
  const Parameters &parameters() const { return parameters_; }
  HashType hash() const { return hash_; }

 private:
  // stripped query
  std::string query_;

  // striped arguments
  Parameters parameters_;

  // hash based on the stripped query
  HashType hash_;
};
}
