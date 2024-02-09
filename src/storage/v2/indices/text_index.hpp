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

#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "text_search.hpp"

namespace memgraph::query {
class DbAccessor;
}

namespace memgraph::storage {
class Storage;

constexpr bool kDoSkipCommit = true;

struct TextIndexData {
  mgcxx::text_search::Context context_;
  LabelId scope_;
};

class TextIndex {
 private:
  void AddNode(Vertex *vertex, NameIdMapper *name_id_mapper, const std::uint64_t transaction_start_timestamp,
               const std::vector<mgcxx::text_search::Context *> &applicable_text_indices);

  std::vector<mgcxx::text_search::Context *> GetApplicableTextIndices(const std::vector<LabelId> &labels);

  std::vector<mgcxx::text_search::Context *> GetApplicableTextIndices(Vertex *vertex);

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

  void AddNode(Vertex *vertex, NameIdMapper *name_id_mapper, const std::uint64_t transaction_start_timestamp);

  void UpdateNode(Vertex *vertex, NameIdMapper *name_id_mapper, const std::uint64_t transaction_start_timestamp);

  void UpdateNode(Vertex *vertex, NameIdMapper *name_id_mapper, const std::uint64_t transaction_start_timestamp,
                  const std::vector<LabelId> &removed_labels);

  void RemoveNode(Vertex *vertex);

  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
                        const std::uint64_t transaction_start_timestamp);

  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update,
                           const std::uint64_t transaction_start_timestamp);

  void UpdateOnSetProperty(Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
                           const std::uint64_t transaction_start_timestamp);

  bool CreateIndex(const std::string &index_name, LabelId label, memgraph::query::DbAccessor *db);

  bool RecoverIndex(const std::string &index_name, LabelId label, memgraph::utils::SkipList<Vertex>::Accessor vertices,
                    NameIdMapper *name_id_mapper);

  bool DropIndex(const std::string &index_name);

  bool IndexExists(const std::string &index_name) const;

  std::vector<Gid> Search(const std::string &index_name, const std::string &search_query);

  void Commit();

  void Rollback();

  std::vector<std::pair<std::string, LabelId>> ListIndices() const;

  std::uint64_t ApproximateVertexCount(const std::string &index_name) const;
};

}  // namespace memgraph::storage
