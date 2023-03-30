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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>

#include "coordinator/hybrid_logical_clock.hpp"
#include "storage/v3/config.hpp"
#include "storage/v3/delta.hpp"
#include "storage/v3/edge.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/indices.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/name_id_mapper.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex.hpp"
#include "utils/concepts.hpp"

namespace memgraph::storage::v3 {

// If edge properties-on-edges is false then we don't need to send edges but
// only vertices, since they will contain those edges
struct SplitData {
  LabelId primary_label;
  PrimaryKey min_primary_key;
  std::optional<PrimaryKey> max_primary_key;
  std::vector<SchemaProperty> schema;
  Config config;
  std::unordered_map<uint64_t, std::string> id_to_name;
  coordinator::Hlc shard_version;

  VertexContainer vertices;
  std::optional<EdgeContainer> edges;
  std::map<uint64_t, std::unique_ptr<Transaction>> transactions;
  std::map<LabelId, LabelIndex::IndexContainer> label_indices;
  std::map<std::pair<LabelId, PropertyId>, LabelPropertyIndex::IndexContainer> label_property_indices;
};

// TODO(jbajic) Handle deleted_vertices_ and deleted_edges_ after the finishing GC
class Splitter final {
 public:
  Splitter(LabelId primary_label, VertexContainer &vertices, EdgeContainer &edges,
           std::map<uint64_t, std::unique_ptr<Transaction>> &start_logical_id_to_transaction, Indices &indices,
           const Config &config, const std::vector<SchemaProperty> &schema, const NameIdMapper &name_id_mapper_);

  Splitter(const Splitter &) = delete;
  Splitter(Splitter &&) noexcept = delete;
  Splitter &operator=(const Splitter &) = delete;
  Splitter operator=(Splitter &&) noexcept = delete;
  ~Splitter() = default;

  SplitData SplitShard(const PrimaryKey &split_key, const std::optional<PrimaryKey> &max_primary_key,
                       coordinator::Hlc shard_version);

 private:
  VertexContainer CollectVertices(SplitData &data, std::set<uint64_t> &collected_transactions_start_id,
                                  const PrimaryKey &split_key);

  std::optional<EdgeContainer> CollectEdges(std::set<uint64_t> &collected_transactions_start_id,
                                            const VertexContainer &split_vertices, const PrimaryKey &split_key);

  std::map<uint64_t, std::unique_ptr<Transaction>> CollectTransactions(
      const std::set<uint64_t> &collected_transactions_start_id, EdgeContainer &cloned_edges,
      const PrimaryKey &split_key);

  static void ScanDeltas(std::set<uint64_t> &collected_transactions_start_id, const Delta *delta);

  void PruneOriginalDeltas(Transaction &transaction, std::map<uint64_t, std::unique_ptr<Transaction>> &transactions,
                           const PrimaryKey &split_key);

  void AdjustClonedTransaction(Transaction &cloned_transaction, const Transaction &transaction,
                               std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions,
                               EdgeContainer &cloned_edges);

  void AdjustClonedTransactions(std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions,
                                EdgeContainer &cloned_edges, const PrimaryKey &split_key);

  void AdjustEdgeRef(Delta &cloned_delta, EdgeContainer &cloned_edges) const;

  static void AdjustDeltaNextAndPrev(const Delta &original, Delta &cloned,
                                     std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions);

  static void AdjustDeltaPrevPtr(const Delta &original, Delta &cloned,
                                 std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions,
                                 EdgeContainer &cloned_edges);

  const LabelId primary_label_;
  VertexContainer &vertices_;
  EdgeContainer &edges_;
  std::map<uint64_t, std::unique_ptr<Transaction>> &start_logical_id_to_transaction_;
  Indices &indices_;
  const Config &config_;
  const std::vector<SchemaProperty> schema_;
  const NameIdMapper &name_id_mapper_;
};

}  // namespace memgraph::storage::v3
