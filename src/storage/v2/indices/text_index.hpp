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

#include <nlohmann/json.hpp>
#include "mg_procedure.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "text_search.hpp"

namespace memgraph::query {
class DbAccessor;
}

namespace memgraph::storage {
struct TextIndexData {
  mgcxx::text_search::Context context_;
  LabelId scope_;
};

class TextIndex {
 private:
  static constexpr bool kDoSkipCommit = true;
  static constexpr std::string_view kTextIndicesDirectory = "text_indices";
  std::filesystem::path text_index_storage_dir_;

  inline std::string MakeIndexPath(const std::string &index_name);

  void CreateEmptyIndex(const std::string &index_name, LabelId label);

  template <typename T>
  nlohmann::json SerializeProperties(const std::map<PropertyId, PropertyValue> &properties, T *name_resolver);

  static std::string StringifyProperties(const std::map<PropertyId, PropertyValue> &properties);

  std::vector<mgcxx::text_search::Context *> GetApplicableTextIndices(std::span<storage::LabelId const> labels);

  static void LoadNodeToTextIndices(std::int64_t gid, const nlohmann::json &properties,
                                    const std::string &property_values_as_str,
                                    const std::vector<mgcxx::text_search::Context *> &applicable_text_indices);

  static void CommitLoadedNodes(mgcxx::text_search::Context &index_context);

  mgcxx::text_search::SearchOutput SearchGivenProperties(const std::string &index_name,
                                                         const std::string &search_query);

  mgcxx::text_search::SearchOutput RegexSearch(const std::string &index_name, const std::string &search_query);

  mgcxx::text_search::SearchOutput SearchAllProperties(const std::string &index_name, const std::string &search_query);

 public:
  explicit TextIndex(const std::filesystem::path &storage_dir)
      : text_index_storage_dir_(storage_dir / kTextIndicesDirectory) {}

  TextIndex(const TextIndex &) = delete;
  TextIndex(TextIndex &&) = delete;
  TextIndex &operator=(const TextIndex &) = delete;
  TextIndex &operator=(TextIndex &&) = delete;

  ~TextIndex() = default;

  std::map<std::string, TextIndexData> index_;
  std::map<LabelId, std::string> label_to_index_;

  void AddNode(
      Vertex *vertex, NameIdMapper *name_id_mapper,
      const std::optional<std::vector<mgcxx::text_search::Context *>> &maybe_applicable_text_indices = std::nullopt);

  void UpdateNode(Vertex *vertex, NameIdMapper *name_id_mapper, const std::vector<LabelId> &removed_labels = {});

  void RemoveNode(
      Vertex *vertex,
      const std::optional<std::vector<mgcxx::text_search::Context *>> &maybe_applicable_text_indices = std::nullopt);

  void CreateIndex(std::string const &index_name, LabelId label, VerticesIterable vertices, NameIdMapper *nameIdMapper);

  void RecoverIndex(const std::string &index_name, LabelId label, utils::SkipList<Vertex>::Accessor vertices,
                    NameIdMapper *name_id_mapper,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  LabelId DropIndex(const std::string &index_name);

  bool IndexExists(const std::string &index_name) const;

  std::vector<Gid> Search(const std::string &index_name, const std::string &search_query, text_search_mode search_mode);

  std::string Aggregate(const std::string &index_name, const std::string &search_query,
                        const std::string &aggregation_query);

  void Commit();

  void Rollback();

  std::vector<std::pair<std::string, LabelId>> ListIndices() const;

  void Clear();
};

}  // namespace memgraph::storage
