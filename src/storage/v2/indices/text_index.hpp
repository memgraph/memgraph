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
#include "storage/v2/mgcxx_mock.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "text_search.hpp"

namespace memgraph::query {
class DbAccessor;
}

namespace memgraph::storage {

class TextIndex {
 public:
  TextIndex() = default;

  TextIndex(const TextIndex &) = delete;
  TextIndex(TextIndex &&) = delete;
  TextIndex &operator=(const TextIndex &) = delete;
  TextIndex &operator=(TextIndex &&) = delete;

  ~TextIndex() = default;

  std::map<std::string, memcxx::text_search::Context> index_;
  std::map<LabelId, std::string> label_to_index_;

  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) const;

  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) const;

  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                           const Transaction &tx) const;

  std::vector<memcxx::text_search::Context *> GetApplicableTextIndices(Vertex *vertex);

  bool CreateIndex(std::string index_name, LabelId label, memgraph::query::DbAccessor *db);

  bool DropIndex(std::string index_name);

  bool IndexExists(std::string index_name) const;

  std::vector<Gid> Search(std::string index_name, std::string search_query);

  std::vector<std::string> ListIndices() const;

  uint64_t ApproximateVertexCount(std::string index_name) const;
};

}  // namespace memgraph::storage
