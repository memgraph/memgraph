#pragma once

#include "logging/loggable.hpp"
#include "query/parameters.hpp"
#include "query/typed_value.hpp"
#include "utils/assert.hpp"
#include "utils/hashing/fnv.hpp"

namespace query {

// Strings used to replace original tokens. Different types are replaced with
// different token.
const std::string kStrippedIntToken = "0";
const std::string kStrippedDoubleToken = "0.0";
const std::string kStrippedStringToken = "\"a\"";
const std::string kStrippedBooleanToken = "true";

/**
 * StrippedQuery contains:
 *     * stripped query
 *     * literals stripped from query
 *     * hash of stripped query
 */
class StrippedQuery {
 public:
  /**
   * Strips the input query and stores stripped query, stripped arguments and
   * stripped query hash.
   *
   * @param query Input query.
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
  auto &literals() const { return literals_; }
  HashType hash() const { return hash_; }

 private:
  // Stripped query.
  std::string query_;

  // Token positions of stripped out literals mapped to their values.
  Parameters literals_;

  // Hash based on the stripped query.
  HashType hash_;
};
}
