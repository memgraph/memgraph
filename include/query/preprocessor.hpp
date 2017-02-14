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
class QueryPreprocessor : public Loggable
{
private:
    // In C++17 the ints will be removed.
    // as far as I understand in C++17 class template parameters
    // can be deduced just like function template parameters
    // TODO: once C++ 17 will be well suported by comilers
    // refactor this piece of code because it is hard to maintain.
    using QueryStripperT = QueryStripper<int, int, int, int>;

public:
    using HashT = QueryStripperT::HashT;

    QueryPreprocessor() : Loggable("QueryPreprocessor"),
        stripper(make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL))
    {
    }

    /**
     * Preprocess the query:
     *     * strip parameters
     *     * calculate query hash
     *
     * @param query that is going to be stripped
     *
     * @return QueryStripped object
     */
    auto preprocess(const std::string &query)
    {
        auto preprocessed = stripper.strip(query);

        logger.info("stripped_query = {}", preprocessed.query);
        logger.info("query_hash = {}", preprocessed.hash);

        return stripper.strip(query);
    }

private:
    QueryStripperT stripper;
};
