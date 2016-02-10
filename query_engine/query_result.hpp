#pragma once

#include <string>
#include <memory>

class QueryResult
{
public:
    using sptr = std::shared_ptr<QueryResult>;

    QueryResult(std::string result) :
        result(result) {}

    std::string result;
};
