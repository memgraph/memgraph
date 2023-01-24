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

#include <map>
#include <memory>
#include <optional>
#include <set>

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

  VertexContainer vertices;
  std::optional<EdgeContainer> edges;
  std::map<uint64_t, std::unique_ptr<Transaction>> transactions;
  std::map<LabelId, LabelIndex::IndexContainer> label_indices;
  std::map<std::pair<LabelId, PropertyId>, LabelPropertyIndex::IndexContainer> label_property_indices;
};

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

  SplitData SplitShard(const PrimaryKey &split_key, const std::optional<PrimaryKey> &max_primary_key);

 private:
  VertexContainer CollectVertices(SplitData &data, std::set<uint64_t> &collected_transactions_start_id,
                                  const PrimaryKey &split_key);

  std::optional<EdgeContainer> CollectEdges(std::set<uint64_t> &collected_transactions_start_id,
                                            const VertexContainer &split_vertices, const PrimaryKey &split_key);

  std::map<uint64_t, std::unique_ptr<Transaction>> CollectTransactions(
      const std::set<uint64_t> &collected_transactions_start_id, VertexContainer &cloned_vertices,
      EdgeContainer &cloned_edges);

  template <typename IndexMap, typename IndexType>
  requires utils::SameAsAnyOf<IndexMap, LabelPropertyIndex, LabelIndex>
      std::map<IndexType, typename IndexMap::IndexContainer> CollectIndexEntries(
          IndexMap &index, const PrimaryKey &split_key,
          std::map<IndexType, std::multimap<const Vertex *, const typename IndexMap::IndexContainer::iterator>>
              &vertex_entry_map) {
    if (index.Empty()) {
      return {};
    }

    std::map<IndexType, typename IndexMap::IndexContainer> cloned_indices;
    for (auto &[index_type_val, index] : index.GetIndex()) {
      // cloned_indices[index_type_val] = typename IndexMap::IndexContainer{};

      auto entry_it = index.begin();
      while (entry_it != index.end()) {
        // We need to save the next pointer since the current one will be
        // invalidated after extract
        auto next_entry_it = std::next(entry_it);
        if (entry_it->vertex->first > split_key) {
          // We get this entry
          [[maybe_unused]] const auto &[inserted_entry_it, inserted, node] =
              cloned_indices[index_type_val].insert(index.extract(entry_it));
          MG_ASSERT(inserted, "Failed to extract index entry!");

          vertex_entry_map[index_type_val].insert({inserted_entry_it->vertex, inserted_entry_it});
        }
        entry_it = next_entry_it;
      }
    }

    return cloned_indices;
  }

  static void ScanDeltas(std::set<uint64_t> &collected_transactions_start_id, Delta *delta);

  static void AdjustClonedTransaction(Transaction &cloned_transaction, const Transaction &transaction,
                                      std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions,
                                      VertexContainer &cloned_vertices, EdgeContainer &cloned_edges);

  void AdjustClonedTransactions(std::map<uint64_t, std::unique_ptr<Transaction>> &cloned_transactions,
                                VertexContainer &cloned_vertices, EdgeContainer &cloned_edges);

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
