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

#include "storage/v2/indices/text_index.hpp"
#include "query/db_accessor.hpp"
#include "storage/v2/mgcxx_mock.hpp"
#include "text_search.hpp"

namespace memgraph::storage {

void TextIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) const {}

void TextIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) const {}

void TextIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                    const Transaction &tx) const {}

std::vector<memcxx::text_search::Context *> TextIndex::GetApplicableTextIndices(Vertex *vertex) {
  std::vector<memcxx::text_search::Context *> applicable_text_indices;
  for (const auto &label : vertex->labels) {
    if (label_to_index_.contains(label)) {
      applicable_text_indices.push_back(&index_.at(label_to_index_.at(label)));
    }
  }
  return applicable_text_indices;
}

bool TextIndex::CreateIndex(std::string index_name, LabelId label, memgraph::query::DbAccessor *db) {
  auto index_config = memcxx::text_search::IndexConfig{
      .mappings = "TODO devise the mapping by reading the indexable nodes' properties"};
  index_.emplace(index_name, memcxx::text_search::create_index(index_name, index_config));
  label_to_index_.emplace(label, index_name);
  return true;

  // TODO add documents (indexable nodes) to index
}

bool TextIndex::DropIndex(std::string index_name) {
  memcxx::text_search::drop_index(index_name);
  index_.erase(index_name);
  std::erase_if(label_to_index_, [index_name](const auto &item) { return item.second == index_name; });
  return true;
}

bool TextIndex::IndexExists(std::string index_name) const { return index_.contains(index_name); }

std::vector<Gid> TextIndex::Search(std::string index_name, std::string search_query) {
  auto input = memcxx::text_search::SearchInput{.search_query = search_query, .return_fields = {"metadata.gid"}};
  // Basic check for search fields in the query (Tantivy syntax delimits them with a `:` to the right)
  if (search_query.find(":") == std::string::npos) {
    input.search_fields = {"data"};
  }

  std::vector<Gid> found_nodes;
  for (const auto &doc : memcxx::text_search::search(index_.at(index_name), input).docs) {
    found_nodes.push_back(storage::Gid::FromString(doc.data.data()));
  }
  return found_nodes;
}

std::vector<std::string> TextIndex::ListIndices() const {
  std::vector<std::string> ret;
  ret.reserve(index_.size());
  for (const auto &item : index_) {
    ret.push_back(item.first);
  }
  return ret;
}
uint64_t TextIndex::ApproximateVertexCount(std::string index_name) const { return 10; }

}  // namespace memgraph::storage
