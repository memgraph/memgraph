#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/model/properties/properties.hpp"

struct ResultList
{
    using sptr = std::shared_ptr<ResultList>;
    using data_t = std::vector<const Properties *>;

    ResultList() = default;
    ResultList(ResultList &other) = delete;
    ResultList(ResultList &&other) = default;
    ResultList(data_t &&data) : data(std::forward<data_t>(data)) {}

    explicit operator bool() const { return data.size() > 0; }

    std::vector<const Properties *> data;
};

struct QueryResult
{
    using sptr = std::shared_ptr<QueryResult>;
    using data_t = std::unordered_map<std::string, ResultList::sptr>;

    QueryResult() = default;
    QueryResult(QueryResult &other) = delete;
    QueryResult(QueryResult &&other) = default;
    QueryResult(data_t &&data) : data(std::forward<data_t>(data)) {}

    explicit operator bool() const { return data.size() > 0; }

    data_t data;
};
