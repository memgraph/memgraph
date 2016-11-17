#pragma once

#include "query/strip/stripper.hpp"

/*
 * Query preprocessing contains:
 *     * query stripping
 *
 * The preprocessing results are:
 *     * stripped query      |
 *     * stripped arguments  |-> QueryStripped
 *     * stripped query hash |
 */
class QueryPreprocessor
{
public:
    QueryPreprocessor()
        : stripper(make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL))
    {
    }

    auto strip_space(const std::string& query)
    {
        return stripper.strip_space(query);
    }

    auto preprocess(const std::string &query) { return stripper.strip(query); }

private:
    // In C++17 the ints should be unnecessary?
    // as far as I understand in C++17 class template parameters
    // can be deduced just like function template parameters
    // TODO: once C++ 17 will be well suported by comilers
    // refactor this piece of code
    QueryStripper<int, int, int, int> stripper;
};
