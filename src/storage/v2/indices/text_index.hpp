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
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "text_search.hpp"

namespace memgraph::storage {

struct TextIndexData {
  mutable mgcxx::text_search::Context context_;
  LabelId scope_;
  std::vector<PropertyId> properties_;
  std::mutex write_mutex_;  // Only used for exclusive locking during writes. IndexReader and IndexWriter are
                            // independent, so no lock is required when reading.

  TextIndexData(mgcxx::text_search::Context context, LabelId scope, std::vector<PropertyId> properties)
      : context_(std::move(context)), scope_(scope), properties_(std::move(properties)) {}
};

class TextIndex {
 private:
  std::filesystem::path text_index_storage_dir_;

  void CreateTantivyIndex(const std::string &index_path, const TextIndexSpec &index_info);

  std::vector<TextIndexData *> GetApplicableTextIndices(std::span<storage::LabelId const> labels,
                                                        std::span<PropertyId const> properties);

  static void AddNodeToTextIndex(std::int64_t gid, const nlohmann::json &properties,
                                 const std::string &property_values_as_str, mgcxx::text_search::Context &context);

  static std::map<PropertyId, PropertyValue> ExtractVertexProperties(const PropertyStore &property_store,
                                                                     std::span<PropertyId const> properties);

 public:
  explicit TextIndex(const std::filesystem::path &storage_dir)
      : text_index_storage_dir_(storage_dir / kTextIndicesDirectory) {}

  TextIndex(const TextIndex &) = delete;
  TextIndex(TextIndex &&) = delete;
  TextIndex &operator=(const TextIndex &) = delete;
  TextIndex &operator=(TextIndex &&) = delete;

  ~TextIndex() = default;

  void UpdateOnAddLabel(LabelId label, const Vertex *vertex, Transaction &tx);

  void UpdateOnRemoveLabel(LabelId label, const Vertex *vertex, Transaction &tx);

  void UpdateOnSetProperty(const Vertex *vertex, Transaction &tx);

  void RemoveNode(const Vertex *vertex_after_update, Transaction &tx);

  void CreateIndex(const TextIndexSpec &index_info, VerticesIterable vertices, NameIdMapper *name_id_mapper);

  void RecoverIndex(const TextIndexSpec &index_info,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  void DropIndex(const std::string &index_name);

  bool IndexExists(const std::string &index_name) const;

  std::vector<Gid> Search(const std::string &index_name, const std::string &search_query, text_search_mode search_mode);

  std::string Aggregate(const std::string &index_name, const std::string &search_query,
                        const std::string &aggregation_query);

  static void ApplyTrackedChanges(Transaction &tx, NameIdMapper *name_id_mapper);

  std::vector<TextIndexSpec> ListIndices() const;

  std::optional<uint64_t> ApproximateVerticesTextCount(std::string_view index_name) const;

  void Clear();

  std::map<std::string, TextIndexData, std::less<>> index_;
};

}  // namespace memgraph::storage
