// Copyright 2025 Memgraph Ltd.
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

#include <mutex>
#include <nlohmann/json_fwd.hpp>

#include "mg_procedure.h"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertices_iterable.hpp"

namespace memgraph::storage {

struct TextEdgeIndexData {
  mgcxx::text_search::Context context_;
  EdgeTypeId scope_;
  std::vector<PropertyId> properties_;
  std::mutex write_mutex_;  // Only used for exclusive locking during writes. IndexReader and IndexWriter are
                            // independent, so no lock is required when reading.

  TextEdgeIndexData(mgcxx::text_search::Context context, EdgeTypeId scope, std::vector<PropertyId> properties)
      : context_(std::move(context)), scope_(scope), properties_(std::move(properties)) {}
};

class TextEdgeIndex {
 private:
  std::filesystem::path text_index_storage_dir_;

  void CreateTantivyIndex(const std::string &index_path, const TextEdgeIndexSpec &index_info);

  std::vector<TextEdgeIndexData *> GetApplicableTextIndices(EdgeTypeId edge_type,
                                                            std::span<PropertyId const> properties);

  mgcxx::text_search::SearchOutput SearchGivenProperties(const std::string &index_name,
                                                         const std::string &search_query);

  mgcxx::text_search::SearchOutput RegexSearch(const std::string &index_name, const std::string &search_query);

  mgcxx::text_search::SearchOutput SearchAllProperties(const std::string &index_name, const std::string &search_query);

 public:
  explicit TextEdgeIndex(const std::filesystem::path &storage_dir)
      : text_index_storage_dir_(storage_dir / kTextIndicesDirectory) {}

  TextEdgeIndex(const TextEdgeIndex &) = delete;
  TextEdgeIndex(TextEdgeIndex &&) = delete;
  TextEdgeIndex &operator=(const TextEdgeIndex &) = delete;
  TextEdgeIndex &operator=(TextEdgeIndex &&) = delete;

  ~TextEdgeIndex() = default;

  std::map<std::string, TextEdgeIndexData> index_;

  void UpdateOnEdgeCreation(Edge *edge, EdgeTypeId edge_type, Transaction &tx);

  void RemoveEdge(Edge *edge, EdgeTypeId edge_type, Transaction &tx);

  void UpdateOnSetProperty(Edge *edge, EdgeTypeId edge_type, Transaction &tx);

  void CreateIndex(const TextEdgeIndexSpec &index_info, VerticesIterable vertices, NameIdMapper *name_id_mapper);

  void RecoverIndex(const TextEdgeIndexSpec &index_info,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  void DropIndex(const std::string &index_name);

  bool IndexExists(const std::string &index_name) const;

  std::vector<Gid> Search(const std::string &index_name, const std::string &search_query, text_search_mode search_mode);

  std::string Aggregate(const std::string &index_name, const std::string &search_query,
                        const std::string &aggregation_query);

  static void ApplyTrackedChanges(Transaction &tx, NameIdMapper *name_id_mapper);

  std::vector<TextEdgeIndexSpec> ListIndices() const;

  void Clear();
};

}  // namespace memgraph::storage
