#pragma once

#include <vector>

#include "storage/model/properties/property.hpp"

/*
 * Query Plan Arguments Type
 */
using plan_args_t = std::vector<Property>;

/*
 * QueryStripped contains:
 *     * stripped query
 *     * plan arguments stripped from query
 *     * hash of stripped query
 */
struct QueryStripped
{
    QueryStripped(const std::string &&query, plan_args_t &&arguments,
                  uint64_t hash)
        : query(std::forward<const std::string>(query)),
          arguments(std::forward<plan_args_t>(arguments)), hash(hash)
    {
    }
    QueryStripped(QueryStripped &other) = delete;
    QueryStripped(QueryStripped &&other) = default;

    std::string query;
    plan_args_t arguments;
    uint64_t hash;
};
