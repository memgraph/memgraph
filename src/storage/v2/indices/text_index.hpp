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
constexpr bool kDoSkipCommit = true;

struct TextIndexData {
  mgcxx::text_search::Context context_;
  LabelId scope_;
};

class TextIndex {
 private:
  void CreateEmptyIndex(const std::string &index_name, LabelId label);

  template <typename T>
  nlohmann::json SerializeProperties(const std::map<PropertyId, PropertyValue> &properties, T *name_resolver);

  std::vector<mgcxx::text_search::Context *> GetApplicableTextIndices(const std::vector<LabelId> &labels);

  void LoadNodeToTextIndices(const std::int64_t gid, const nlohmann::json &properties,
                             const std::vector<mgcxx::text_search::Context *> &applicable_text_indices);

  void CommitLoadedNodes(mgcxx::text_search::Context &index_context);

  void AddNode(Vertex *vertex, NameIdMapper *name_id_mapper,
               const std::vector<mgcxx::text_search::Context *> &applicable_text_indices);

  void RemoveNode(Vertex *vertex, const std::vector<mgcxx::text_search::Context *> &applicable_text_indices);

 public:
  TextIndex() = default;

  TextIndex(const TextIndex &) = delete;
  TextIndex(TextIndex &&) = delete;
  TextIndex &operator=(const TextIndex &) = delete;
  TextIndex &operator=(TextIndex &&) = delete;

  ~TextIndex() = default;

  std::map<std::string, TextIndexData> index_;
  std::map<LabelId, std::string> label_to_index_;

  void AddNode(Vertex *vertex, NameIdMapper *name_id_mapper);

  void UpdateNode(Vertex *vertex, NameIdMapper *name_id_mapper, const std::vector<LabelId> &removed_labels = {});

  void RemoveNode(Vertex *vertex);

  void CreateIndex(const std::string &index_name, LabelId label, memgraph::query::DbAccessor *db);

  void RecoverIndex(const std::string &index_name, LabelId label, memgraph::utils::SkipList<Vertex>::Accessor vertices,
                    NameIdMapper *name_id_mapper);

  LabelId DropIndex(const std::string &index_name);

  bool IndexExists(const std::string &index_name) const;

  std::vector<Gid> Search(const std::string &index_name, const std::string &search_query);

  void Commit();

  void Rollback();

  std::vector<std::pair<std::string, LabelId>> ListIndices() const;

  std::uint64_t ApproximateVertexCount(const std::string &index_name) const;
};

}  // namespace memgraph::storage
