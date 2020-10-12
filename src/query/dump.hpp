#pragma once

#include <ostream>

#include "query/db_accessor.hpp"
#include "query/stream.hpp"
#include "storage/v2/storage.hpp"

namespace query {

void DumpDatabaseToCypherQueries(query::DbAccessor *dba, AnyStream *stream);

struct PullPlanDump {
  explicit PullPlanDump(query::DbAccessor *dba);

  /// Pull the dump results lazily
  /// @return true if all results were returned, false otherwise
  bool Pull(AnyStream *stream, std::optional<int> n);

 private:
  query::DbAccessor *dba_ = nullptr;

  std::optional<storage::IndicesInfo> indices_info_ = std::nullopt;
  std::optional<storage::ConstraintsInfo> constraints_info_ = std::nullopt;

  using VertexAccessorIterable =
      decltype(std::declval<query::DbAccessor>().Vertices(storage::View::OLD));
  using VertexAccessorIterableIterator =
      decltype(std::declval<VertexAccessorIterable>().begin());

  using EdgeAcessorIterableIterator = decltype(std::declval<VertexAccessor>()
                                                   .OutEdges(storage::View::OLD)
                                                   .GetValue()
                                                   .begin());

  VertexAccessorIterable vertices_iterable_;
  bool internal_index_created_ = false;

  size_t current_function_index_ = 0;

  // We define every part of the dump query in a self contained function.
  // Each functions is responsible of keeping track of its execution status.
  // If a function did finish its execution, it should return number of results
  // it streamed so we know how many rows should be pulled from the next
  // function, otherwise std::nullopt is returned.
  std::vector<std::function<std::optional<size_t>(AnyStream *stream,
                                                  std::optional<int> n)>>
      pull_functions_;
};
}  // namespace query
