#pragma once

#include "logging/loggable.hpp"
#include "query/stripper.hpp"

/*
 * Query preprocessing contains:
 *     * query stripping
 *
 * This class is here because conceptually process of query preprocessing
 * might have more than one step + in current version of C++ standard
 * isn't trivial to instantiate QueryStripper because of template arguments +
 * it depends on underlying lexical analyser.
 *
 * The preprocessing results are:
 *     * stripped query      |
 *     * stripped arguments  |-> StrippedQuery
 *     * stripped query hash |
 */
class QueryPreprocessor : public Loggable {
 public:
  QueryPreprocessor() : Loggable("QueryPreprocessor") {}

  /**
   * Preprocess the query:
   *     * strip parameters
   *     * calculate query hash
   *
   * @param query that is going to be stripped
   *
   * @return QueryStripped object
   */
  auto preprocess(const std::string &query) {
    auto preprocessed = stripper.strip(query);

    logger.info("stripped_query = {}", preprocessed.query);
    logger.info("query_hash = {}", preprocessed.hash);

    return preprocessed;
  }

 private:
  QueryStripper stripper;
};
