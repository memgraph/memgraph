// Copyright 2024 Memgraph Ltd.
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

#include "dbms/database.hpp"
#include "query/db_accessor.hpp"
#include "query/stream.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::query {

void DumpDatabaseToCypherQueries(query::DbAccessor *dba, AnyStream *stream, dbms::DatabaseAccess db_acc);

struct PullPlanDump {
  explicit PullPlanDump(query::DbAccessor *dba, dbms::DatabaseAccess db_acc);

  /// Pull the dump results lazily
  /// @return true if all results were returned, false otherwise
  bool Pull(AnyStream *stream, std::optional<int> n);

 private:
  query::DbAccessor *dba_ = nullptr;
  dbms::DatabaseAccess db_acc_;

  std::optional<storage::IndicesInfo> indices_info_ = std::nullopt;
  std::optional<storage::ConstraintsInfo> constraints_info_ = std::nullopt;

  using VertexAccessorIterable = decltype(std::declval<query::DbAccessor>().Vertices(storage::View::OLD));
  using VertexAccessorIterableIterator = decltype(std::declval<VertexAccessorIterable>().begin());

  using EdgeAccessorIterable = decltype(std::declval<VertexAccessor>().OutEdges(storage::View::OLD));
  using EdgeAccessorIterableIterator = decltype(std::declval<EdgeAccessorIterable>().GetValue().edges.begin());

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
  PullChunk CreateTextIndicesPullChunk();
  PullChunk CreateExistenceConstraintsPullChunk();
  PullChunk CreateUniqueConstraintsPullChunk();
  PullChunk CreateInternalIndexPullChunk();
  PullChunk CreateVertexPullChunk();
  PullChunk CreateEdgePullChunk();
  PullChunk CreateDropInternalIndexPullChunk();
  PullChunk CreateInternalIndexCleanupPullChunk();
  PullChunk CreateTriggersPullChunk();
};
}  // namespace memgraph::query
