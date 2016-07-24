#pragma once

#include "cypher/common.hpp"
#include "query_stripper.hpp"
#include "utils/hashing/fnv.hpp"

class QueryHasher
{
public:
    QueryHasher()
        : stripper(make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL)) {}

    std::string hash(std::string &query)
    {
        auto stripped = stripper.strip(query);
        auto stripped_hash = fnv(stripped.query);
        auto hash_string = std::to_string(stripped_hash);
        return hash_string;
    }
    
private:
    QueryStripper<int, int, int, int> stripper;
};
