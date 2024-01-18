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
#include "storage/v2/view.hpp"
#include "text_search.hpp"

namespace memgraph::storage {

void TextIndex::AddNode(Vertex *vertex_after_update, Storage *storage,
                        const std::vector<mgcxx::text_search::Context *> &applicable_text_indices) {
  // NOTE: Text indexes are presently all-property indices. If we allow text indexes restricted to specific properties,
  // an indexable document should be created for each applicable index.
  nlohmann::json document = {};
  nlohmann::json properties = {};
  for (const auto &[prop_id, prop_value] : vertex_after_update->properties.Properties()) {
    if (!prop_value.IsString()) continue;
    properties[storage->PropertyToName(prop_id)] = prop_value.ValueString();
  }

  document["data"] = properties;
  document["metadata"] = {};
  document["metadata"]["gid"] = vertex_after_update->gid.AsInt();
  // TODO add txid
  document["metadata"]["deleted"] = false;
  document["metadata"]["is_node"] = true;

  for (auto *index_context : applicable_text_indices) {
    try {
      mgcxx::text_search::add_document(*index_context, mgcxx::text_search::DocumentInput{.data = document.dump()},
                                       false);
    } catch (const std::exception &e) {
      throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
    }
  }
}

void TextIndex::AddNode(Vertex *vertex_after_update, Storage *storage) {
  auto applicable_text_indices = GetApplicableTextIndices(vertex_after_update);
  if (applicable_text_indices.empty()) return;
  AddNode(vertex_after_update, storage, applicable_text_indices);
}

void TextIndex::UpdateNode(Vertex *vertex_after_update, Storage *storage) {
  auto applicable_text_indices = GetApplicableTextIndices(vertex_after_update);
  if (applicable_text_indices.empty()) return;
  RemoveNode(vertex_after_update, applicable_text_indices);
  AddNode(vertex_after_update, storage, applicable_text_indices);
}

void TextIndex::UpdateNode(Vertex *vertex_after_update, Storage *storage, const std::vector<LabelId> &removed_labels) {
  auto indexes_to_remove_node_from = GetApplicableTextIndices(removed_labels);
  RemoveNode(vertex_after_update, indexes_to_remove_node_from);

  auto indexes_to_update_node = GetApplicableTextIndices(vertex_after_update);
  if (indexes_to_update_node.empty()) return;
  RemoveNode(vertex_after_update, indexes_to_update_node);
  AddNode(vertex_after_update, storage, indexes_to_update_node);
}

void TextIndex::RemoveNode(Vertex *vertex_after_update,
                           const std::vector<mgcxx::text_search::Context *> &applicable_text_indices) {
  auto search_node_to_be_deleted =
      mgcxx::text_search::SearchInput{.search_query = fmt::format("metadata.gid:{}", vertex_after_update->gid.AsInt())};

  for (auto *index_context : applicable_text_indices) {
    try {
      mgcxx::text_search::delete_document(*index_context, search_node_to_be_deleted, false);
    } catch (const std::exception &e) {
      throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
    }
  }
}

void TextIndex::RemoveNode(Vertex *vertex_after_update) {
  auto applicable_text_indices = GetApplicableTextIndices(vertex_after_update);
  if (applicable_text_indices.empty()) return;
  RemoveNode(vertex_after_update, applicable_text_indices);
}

void TextIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, Storage *storage,
                                 const Transaction &tx) {
  if (!label_to_index_.contains(added_label)) {
    return;
  }
  AddNode(vertex_after_update, storage, {&index_.at(label_to_index_.at(added_label))});
}

void TextIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) {
  if (!label_to_index_.contains(removed_label)) {
    return;
  }
  RemoveNode(vertex_after_update, {&index_.at(label_to_index_.at(removed_label))});
}

void TextIndex::UpdateOnSetProperty(Vertex *vertex_after_update, Storage *storage, const Transaction &tx) {
  UpdateNode(vertex_after_update, storage);
}

std::vector<mgcxx::text_search::Context *> TextIndex::GetApplicableTextIndices(const std::vector<LabelId> &labels) {
  std::vector<mgcxx::text_search::Context *> applicable_text_indices;
  for (const auto &label : labels) {
    if (label_to_index_.contains(label)) {
      applicable_text_indices.push_back(&index_.at(label_to_index_.at(label)));
    }
  }
  return applicable_text_indices;
}

std::vector<mgcxx::text_search::Context *> TextIndex::GetApplicableTextIndices(Vertex *vertex) {
  std::vector<mgcxx::text_search::Context *> applicable_text_indices;
  for (const auto &label : vertex->labels) {
    if (label_to_index_.contains(label)) {
      applicable_text_indices.push_back(&index_.at(label_to_index_.at(label)));
    }
  }
  return applicable_text_indices;
}

bool TextIndex::CreateIndex(std::string index_name, LabelId label, memgraph::query::DbAccessor *db) {
  nlohmann::json mappings = {};
  mappings["properties"] = {};
  mappings["properties"]["metadata"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
  mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};

  try {
    index_.emplace(index_name, mgcxx::text_search::create_index(
                                   index_name, mgcxx::text_search::IndexConfig{.mappings = mappings.dump()}));
  } catch (const std::exception &e) {
    throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
  }
  label_to_index_.emplace(label, index_name);

  bool has_schema = false;
  std::vector<std::pair<PropertyId, std::string>> indexed_properties{};
  for (const auto &v : db->Vertices(View::OLD)) {
    if (!v.HasLabel(View::OLD, label).GetValue()) {
      continue;
    }

    if (!has_schema) [[unlikely]] {
      for (const auto &[prop_id, prop_val] : v.Properties(View::OLD).GetValue()) {
        if (prop_val.IsString()) {
          indexed_properties.emplace_back(std::pair<PropertyId, std::string>{prop_id, db->PropertyToName(prop_id)});
        }
      }
      has_schema = true;
    }

    nlohmann::json document = {};
    nlohmann::json properties = {};
    for (const auto &[prop_id, prop_name] : indexed_properties) {
      properties[prop_name] = v.GetProperty(View::OLD, prop_id).GetValue().ValueString();
    }

    document["data"] = properties;
    document["metadata"] = {};
    document["metadata"]["gid"] = v.Gid().AsInt();
    // TODO add txid
    document["metadata"]["deleted"] = false;
    document["metadata"]["is_node"] = true;

    try {
      mgcxx::text_search::add_document(index_.at(index_name),
                                       mgcxx::text_search::DocumentInput{.data = document.dump()}, false);
    } catch (const std::exception &e) {
      throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
    }
  }
  return true;
}

bool TextIndex::DropIndex(std::string index_name) {
  try {
    mgcxx::text_search::drop_index(index_name);
  } catch (const std::exception &e) {
    throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
  }
  index_.erase(index_name);
  std::erase_if(label_to_index_, [index_name](const auto &item) { return item.second == index_name; });
  return true;
}

bool TextIndex::IndexExists(std::string index_name) const { return index_.contains(index_name); }

std::vector<Gid> TextIndex::Search(std::string index_name, std::string search_query) {
  auto input = mgcxx::text_search::SearchInput{.search_query = search_query, .return_fields = {"metadata"}};
  // Basic check for search fields in the query (Tantivy syntax delimits them with a `:` to the right)
  if (search_query.find(":") == std::string::npos) {
    input.search_fields = {"data"};
  }

  std::vector<Gid> found_nodes;

  mgcxx::text_search::SearchOutput search_results;
  try {
    search_results = mgcxx::text_search::search(index_.at(index_name), input);
  } catch (const std::exception &e) {
    throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
  }
  auto docs = search_results.docs;
  for (const auto &doc : docs) {
    auto doc_data = doc.data;
    std::string doc_string = doc_data.data();
    auto doc_len = doc_data.length();
    doc_string.resize(doc_len);
    auto doc_json = nlohmann::json::parse(doc_string);
    found_nodes.push_back(storage::Gid::FromString(doc_json["metadata"]["gid"].dump()));
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

std::uint64_t TextIndex::ApproximateVertexCount(std::string index_name) const { return 10; }

}  // namespace memgraph::storage
