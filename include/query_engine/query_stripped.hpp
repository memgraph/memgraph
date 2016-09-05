#pragma once

#include <vector>

#include "storage/model/properties/property.hpp"

using code_args_t = std::vector<Property>;

struct QueryStripped
{
    QueryStripped(const std::string &&query, uint64_t hash,
                  code_args_t &&arguments)
        : query(std::forward<const std::string>(query)), hash(hash),
          arguments(std::forward<code_args_t>(arguments))
    {
    }

    QueryStripped(QueryStripped &other) = delete;
    QueryStripped(QueryStripped &&other) = default;

    std::string query;
    uint64_t hash;
    code_args_t arguments;
};
