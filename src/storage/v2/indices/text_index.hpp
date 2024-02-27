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

#include <json/json.hpp>
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/vertex.hpp"
#include "text_search.hpp"

namespace memgraph::query {
class DbAccessor;
}

namespace memgraph::storage {
struct TextIndexData {
  mgcxx::text_search::Context context_;
  LabelId scope_;
};

enum class TextSearchMode : uint8_t {
  SPECIFIED_PROPERTIES = 0,
  REGEX = 1,
  ALL_PROPERTIES = 2,
};

class TextIndex {
 private:
  static constexpr bool kDoSkipCommit = true;
  static constexpr std::string_view kTextIndicesDirectory = "text_indices";

  inline std::string MakeIndexPath(const std::filesystem::path &storage_dir, const std::string &index_name);

  void CreateEmptyIndex(const std::filesystem::path &storage_dir, const std::string &index_name, LabelId label);

  template <typename T>
  nlohmann::json SerializeProperties(const std::map<PropertyId, PropertyValue> &properties, T *name_resolver);

  std::string StringifyProperties(const std::map<PropertyId, PropertyValue> &properties);

  std::vector<mgcxx::text_search::Context *> GetApplicableTextIndices(const std::vector<LabelId> &labels);

  void LoadNodeToTextIndices(const std::int64_t gid, const nlohmann::json &properties,
                             const std::string &property_values_as_str,
                             const std::vector<mgcxx::text_search::Context *> &applicable_text_indices);

  void CommitLoadedNodes(mgcxx::text_search::Context &index_context);

  mgcxx::text_search::SearchOutput SearchGivenProperties(const std::string &index_name,
                                                         const std::string &search_query);

  mgcxx::text_search::SearchOutput RegexSearch(const std::string &index_name, const std::string &search_query);

  mgcxx::text_search::SearchOutput SearchAllProperties(const std::string &index_name, const std::string &search_query);

 public:
  TextIndex() = default;

  TextIndex(const TextIndex &) = delete;
  TextIndex(TextIndex &&) = delete;
  TextIndex &operator=(const TextIndex &) = delete;
  TextIndex &operator=(TextIndex &&) = delete;

  ~TextIndex() = default;

  std::map<std::string, TextIndexData> index_;
  std::map<LabelId, std::string> label_to_index_;

  void AddNode(Vertex *vertex, NameIdMapper *name_id_mapper,
               std::optional<std::vector<mgcxx::text_search::Context *>> applicable_text_indices = std::nullopt);

  void UpdateNode(Vertex *vertex, NameIdMapper *name_id_mapper, const std::vector<LabelId> &removed_labels = {});

  void RemoveNode(Vertex *vertex,
                  std::optional<std::vector<mgcxx::text_search::Context *>> applicable_text_indices = std::nullopt);

  void CreateIndex(const std::filesystem::path &storage_dir, const std::string &index_name, LabelId label,
                   memgraph::query::DbAccessor *db);

  void RecoverIndex(const std::filesystem::path &storage_dir, const std::string &index_name, LabelId label,
                    memgraph::utils::SkipList<Vertex>::Accessor vertices, NameIdMapper *name_id_mapper);

  LabelId DropIndex(const std::filesystem::path &storage_dir, const std::string &index_name);

  bool IndexExists(const std::string &index_name) const;

  std::vector<Gid> Search(const std::string &index_name, const std::string &search_query, TextSearchMode search_mode);

  std::string Aggregate(const std::string &index_name, const std::string &search_query,
                        const std::string &aggregation_query);

  void Commit();

  void Rollback();

  std::vector<std::pair<std::string, LabelId>> ListIndices() const;
};

}  // namespace memgraph::storage
