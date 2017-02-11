#pragma once

#include <vector>
#include "storage/typed_value_store.hpp"


/*
 * QueryStripped contains:
 *     * stripped query
 *     * plan arguments stripped from query
 *     * hash of stripped query
 */
struct QueryStripped {

  QueryStripped(const std::string &&query, const TypedValueStore &&arguments, uint64_t hash)
      : query(std::forward<const std::string>(query)),
        arguments(std::forward<const TypedValueStore>(arguments)),
        hash(hash) {}

  QueryStripped(QueryStripped &other) = delete;
  QueryStripped(QueryStripped &&other) = default;

  std::string query;
  TypedValueStore arguments;
  uint64_t hash;
};
