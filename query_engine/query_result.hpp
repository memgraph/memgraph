#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

#include "storage/model/properties/properties.hpp"

struct ResultList
{
    using sptr = std::shared_ptr<ResultList>;
    using data_t = std::vector<const Properties*>;

    ResultList() = delete;
    ResultList(ResultList& other) = delete;
    ResultList(ResultList&& other) = default;

    ResultList(data_t&& data) :
        data(std::forward<data_t>(data)) {}

    std::vector<const Properties*> data;
};

struct QueryResult
{
    using sptr = std::shared_ptr<QueryResult>;
    using data_t = std::unordered_map<std::string, ResultList::sptr>;

    QueryResult() = delete;
    QueryResult(QueryResult& other) = delete;
    QueryResult(QueryResult&& other) = default;

    QueryResult(data_t&& data) :
        data(std::forward<data_t>(data)) {}

    data_t data;
};
