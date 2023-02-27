// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <ostream>

#include "query/db_accessor.hpp"
#include "query/stream.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::query {

struct PullPlanIndexStatistics {
  explicit PullPlanIndexStatistics(query::DbAccessor *dba);

  /// Pull the dump results lazily
  /// @return true if all results were returned, false otherwise
  bool Pull(AnyStream *stream, std::optional<int> n);

 private:
  query::DbAccessor *dba_ = nullptr;

  std::optional<storage::IndicesInfo> indices_info_ = std::nullopt;

  using VertexAccessorIterable = decltype(std::declval<query::DbAccessor>().Vertices(storage::View::OLD));
  using VertexAccessorIterableIterator = decltype(std::declval<VertexAccessorIterable>().begin());

  using EdgeAccessorIterable = decltype(std::declval<VertexAccessor>().OutEdges(storage::View::OLD));
  using EdgeAccessorIterableIterator = decltype(std::declval<EdgeAccessorIterable>().GetValue().begin());

  VertexAccessorIterable vertices_iterable_;
  bool internal_index_created_ = false;

  size_t current_chunk_index_ = 0;

  void CollectStatistics();
};
}  // namespace memgraph::query
