// Copyright 2022 Memgraph Ltd.
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

#include "query/v2/db_accessor.hpp"
#include "query/v2/stream.hpp"
#include "storage/v3/storage.hpp"

namespace memgraph::query::v2 {

void DumpDatabaseToCypherQueries(query::v2::DbAccessor *dba, AnyStream *stream);

struct PullPlanDump {
  explicit PullPlanDump(query::v2::DbAccessor *dba);

  /// Pull the dump results lazily
  /// @return true if all results were returned, false otherwise
  bool Pull(AnyStream *stream, std::optional<int> n);

 private:
  query::v2::DbAccessor *dba_ = nullptr;

  std::optional<storage::v3::IndicesInfo> indices_info_ = std::nullopt;
  std::optional<storage::v3::ConstraintsInfo> constraints_info_ = std::nullopt;

  using VertexAccessorIterable = decltype(std::declval<query::v2::DbAccessor>().Vertices(storage::v3::View::OLD));
  using VertexAccessorIterableIterator = decltype(std::declval<VertexAccessorIterable>().begin());

  using EdgeAccessorIterable = decltype(std::declval<VertexAccessor>().OutEdges(storage::v3::View::OLD));
  using EdgeAccessorIterableIterator = decltype(std::declval<EdgeAccessorIterable>().GetValue().begin());

  VertexAccessorIterable vertices_iterable_;
  bool internal_index_created_ = false;

  size_t current_chunk_index_ = 0;

  using PullChunk = std::function<std::optional<size_t>(AnyStream *stream, std::optional<int> n)>;
  // We define every part of the dump query in a self contained function.
  // Each functions is responsible of keeping track of its execution status.
  // If a function did finish its execution, it should return number of results
  // it streamed so we know how many rows should be pulled from the next
  // function, otherwise std::nullopt is returned.
  std::vector<PullChunk> pull_chunks_;

  PullChunk CreateLabelIndicesPullChunk();
  PullChunk CreateLabelPropertyIndicesPullChunk();
  PullChunk CreateExistenceConstraintsPullChunk();
  PullChunk CreateUniqueConstraintsPullChunk();
  PullChunk CreateInternalIndexPullChunk();
  PullChunk CreateVertexPullChunk();
  PullChunk CreateEdgePullChunk();
  PullChunk CreateDropInternalIndexPullChunk();
  PullChunk CreateInternalIndexCleanupPullChunk();
};
}  // namespace memgraph::query::v2
