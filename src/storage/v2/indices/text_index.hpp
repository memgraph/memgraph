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

#include <nlohmann/json_fwd.hpp>
#include <shared_mutex>
#include "mg_procedure.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "text_search.hpp"

namespace memgraph::query {
class DbAccessor;
}  // namespace memgraph::query

namespace memgraph::storage {

inline constexpr std::string_view kTextIndicesDirectory = "text_indices";
struct TextIndexData {
  mgcxx::text_search::Context context_;
  LabelId scope_;
};

class TextIndex {
 private:
  static constexpr bool kDoSkipCommit = true;

  // Boolean operators that should be preserved in uppercase for Tantivy
  static constexpr std::string_view kBooleanAnd = "AND";
  static constexpr std::string_view kBooleanOr = "OR";
  static constexpr std::string_view kBooleanNot = "NOT";

  std::filesystem::path text_index_storage_dir_;
  std::shared_mutex
      index_writer_mutex_;  // This mutex is used to protect add_document, remove_document, commit and rollback
                            // operations. Underlying Tantivy IndexWriter requires unique lock for commit and rollback
                            // operations and shared lock for add_document and remove_document operations.
                            // TODO(@DavIvek): Better approach would be to add locking on mgcxx side.

  inline std::string MakeIndexPath(std::string_view index_name) const;

  void CreateTantivyIndex(const std::string &index_name, LabelId label, const std::string &index_path);

  static nlohmann::json SerializeProperties(const std::map<PropertyId, PropertyValue> &properties,
                                            NameIdMapper *name_id_mapper);

  static std::string StringifyProperties(const std::map<PropertyId, PropertyValue> &properties);

  static std::string ToLowerCasePreservingBooleanOperators(std::string_view input);

  std::vector<mgcxx::text_search::Context *> GetApplicableTextIndices(std::span<storage::LabelId const> labels);

  void AddNodeToTextIndices(std::int64_t gid, const nlohmann::json &properties,
                            const std::string &property_values_as_str,
                            const std::vector<mgcxx::text_search::Context *> &applicable_text_indices);

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

  void AddNode(Vertex *vertex, NameIdMapper *name_id_mapper,
               const std::vector<mgcxx::text_search::Context *> &maybe_applicable_text_indices = {});

  void UpdateNode(Vertex *vertex, NameIdMapper *name_id_mapper);

  void RemoveNode(Vertex *vertex, const std::vector<mgcxx::text_search::Context *> &maybe_applicable_text_indices = {});

  void UpdateOnAddLabel(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper);

  void UpdateOnRemoveLabel(LabelId label, Vertex *vertex);

  void UpdateOnSetProperty(Vertex *vertex, NameIdMapper *name_id_mapper);

  void CreateIndex(std::string const &index_name, LabelId label, VerticesIterable vertices,
                   NameIdMapper *name_id_mapper);

  void RecoverIndex(const std::string &index_name, LabelId label,
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
