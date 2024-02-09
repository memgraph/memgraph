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
#include "flags/run_time_configurable.hpp"
#include "query/db_accessor.hpp"
#include "storage/v2/view.hpp"
#include "text_search.hpp"

namespace memgraph::storage {

void TextIndex::AddNode(Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
                        const std::uint64_t transaction_start_timestamp,
                        const std::vector<mgcxx::text_search::Context *> &applicable_text_indices) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  // NOTE: Text indexes are presently all-property indices. If we allow text indexes restricted to specific properties,
  // an indexable document should be created for each applicable index.
  nlohmann::json document = {};
  nlohmann::json properties = nlohmann::json::value_t::object;
  for (const auto &[prop_id, prop_value] : vertex_after_update->properties.Properties()) {
    switch (prop_value.type()) {
      case PropertyValue::Type::Bool:
        properties[name_id_mapper->IdToName(prop_id.AsUint())] = prop_value.ValueBool();
        break;
      case PropertyValue::Type::Int:
        properties[name_id_mapper->IdToName(prop_id.AsUint())] = prop_value.ValueInt();
        break;
      case PropertyValue::Type::Double:
        properties[name_id_mapper->IdToName(prop_id.AsUint())] = prop_value.ValueDouble();
        break;
      case PropertyValue::Type::String:
        properties[name_id_mapper->IdToName(prop_id.AsUint())] = prop_value.ValueString();
        break;
      case PropertyValue::Type::Null:
      case PropertyValue::Type::List:
      case PropertyValue::Type::Map:
      case PropertyValue::Type::TemporalData:
      default:
        continue;
    }
  }

  document["data"] = properties;
  document["metadata"] = {};
  document["metadata"]["gid"] = vertex_after_update->gid.AsInt();
  document["metadata"]["txid"] = transaction_start_timestamp;
  document["metadata"]["deleted"] = false;
  document["metadata"]["is_node"] = true;

  for (auto *index_context : applicable_text_indices) {
    try {
      mgcxx::text_search::add_document(
          *index_context,
          mgcxx::text_search::DocumentInput{
              .data = document.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace)},
          kDoSkipCommit);
    } catch (const std::exception &e) {
      throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
    }
  }
}

void TextIndex::AddNode(Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
                        const std::uint64_t transaction_start_timestamp) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  auto applicable_text_indices = GetApplicableTextIndices(vertex_after_update);
  if (applicable_text_indices.empty()) return;
  AddNode(vertex_after_update, std::move(name_id_mapper), transaction_start_timestamp, applicable_text_indices);
}

void TextIndex::UpdateNode(Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
                           const std::uint64_t transaction_start_timestamp) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  auto applicable_text_indices = GetApplicableTextIndices(vertex_after_update);
  if (applicable_text_indices.empty()) return;
  RemoveNode(vertex_after_update, applicable_text_indices);
  AddNode(vertex_after_update, std::move(name_id_mapper), transaction_start_timestamp, applicable_text_indices);
}

void TextIndex::UpdateNode(Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
                           const std::uint64_t transaction_start_timestamp,
                           const std::vector<LabelId> &removed_labels) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  auto indexes_to_remove_node_from = GetApplicableTextIndices(removed_labels);
  RemoveNode(vertex_after_update, indexes_to_remove_node_from);

  auto indexes_to_update_node = GetApplicableTextIndices(vertex_after_update);
  if (indexes_to_update_node.empty()) return;
  RemoveNode(vertex_after_update, indexes_to_update_node);
  AddNode(vertex_after_update, std::move(name_id_mapper), transaction_start_timestamp, indexes_to_update_node);
}

void TextIndex::RemoveNode(Vertex *vertex_after_update,
                           const std::vector<mgcxx::text_search::Context *> &applicable_text_indices) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  auto search_node_to_be_deleted =
      mgcxx::text_search::SearchInput{.search_query = fmt::format("metadata.gid:{}", vertex_after_update->gid.AsInt())};

  for (auto *index_context : applicable_text_indices) {
    try {
      mgcxx::text_search::delete_document(*index_context, search_node_to_be_deleted, kDoSkipCommit);
    } catch (const std::exception &e) {
      throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
    }
  }
}

void TextIndex::RemoveNode(Vertex *vertex_after_update) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  auto applicable_text_indices = GetApplicableTextIndices(vertex_after_update);
  if (applicable_text_indices.empty()) return;
  RemoveNode(vertex_after_update, applicable_text_indices);
}

void TextIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
                                 const std::uint64_t transaction_start_timestamp) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  if (!label_to_index_.contains(added_label)) {
    return;
  }
  AddNode(vertex_after_update, std::move(name_id_mapper), transaction_start_timestamp,
          std::vector<mgcxx::text_search::Context *>{&index_.at(label_to_index_.at(added_label)).context_});
}

void TextIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update,
                                    const std::uint64_t transaction_start_timestamp) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  if (!label_to_index_.contains(removed_label)) {
    return;
  }
  RemoveNode(vertex_after_update, {&index_.at(label_to_index_.at(removed_label)).context_});
}

void TextIndex::UpdateOnSetProperty(Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
                                    std::uint64_t transaction_start_timestamp) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  UpdateNode(vertex_after_update, std::move(name_id_mapper), transaction_start_timestamp);
}

std::vector<mgcxx::text_search::Context *> TextIndex::GetApplicableTextIndices(const std::vector<LabelId> &labels) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  std::vector<mgcxx::text_search::Context *> applicable_text_indices;
  for (const auto &label : labels) {
    if (label_to_index_.contains(label)) {
      applicable_text_indices.push_back(&index_.at(label_to_index_.at(label)).context_);
    }
  }
  return applicable_text_indices;
}

std::vector<mgcxx::text_search::Context *> TextIndex::GetApplicableTextIndices(Vertex *vertex) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  std::vector<mgcxx::text_search::Context *> applicable_text_indices;
  for (const auto &label : vertex->labels) {
    if (label_to_index_.contains(label)) {
      applicable_text_indices.push_back(&index_.at(label_to_index_.at(label)).context_);
    }
  }
  return applicable_text_indices;
}

bool TextIndex::CreateIndex(const std::string &index_name, LabelId label, memgraph::query::DbAccessor *db) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  nlohmann::json mappings = {};
  mappings["properties"] = {};
  mappings["properties"]["metadata"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
  mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};

  try {
    index_.emplace(index_name,
                   TextIndexData{.context_ = mgcxx::text_search::create_index(
                                     index_name, mgcxx::text_search::IndexConfig{.mappings = mappings.dump()}),
                                 .scope_ = label});
  } catch (const std::exception &e) {
    throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
  }
  label_to_index_.emplace(label, index_name);

  bool has_schema = false;
  std::vector<std::pair<PropertyId, std::string>> indexed_properties{};
  auto &index_context = index_.at(index_name).context_;

  // TODO antepusic get nodes with label if there's an adequate label index
  for (const auto &v : db->Vertices(View::NEW)) {
    if (!v.HasLabel(View::NEW, label).GetValue()) {
      continue;
    }

    if (!has_schema) [[unlikely]] {
      auto properties = v.Properties(View::NEW).GetValue();
      for (const auto &[prop_id, prop_val] : properties) {
        if (prop_val.IsBool() || prop_val.IsInt() || prop_val.IsDouble() || prop_val.IsString()) {
          indexed_properties.emplace_back(std::pair<PropertyId, std::string>{prop_id, db->PropertyToName(prop_id)});
        }
      }
      has_schema = true;
    }

    nlohmann::json document = {};
    nlohmann::json properties = nlohmann::json::value_t::object;
    for (const auto &[prop_id, prop_name] : indexed_properties) {
      const auto prop_value = v.GetProperty(View::NEW, prop_id).GetValue();
      switch (prop_value.type()) {
        case PropertyValue::Type::Bool:
          properties[prop_name] = prop_value.ValueBool();
          break;
        case PropertyValue::Type::Int:
          properties[prop_name] = prop_value.ValueInt();
          break;
        case PropertyValue::Type::Double:
          properties[prop_name] = prop_value.ValueDouble();
          break;
        case PropertyValue::Type::String:
          properties[prop_name] = prop_value.ValueString();
          break;
        case PropertyValue::Type::Null:
        case PropertyValue::Type::List:
        case PropertyValue::Type::Map:
        case PropertyValue::Type::TemporalData:
        default:
          continue;
      }
    }

    document["data"] = properties;
    document["metadata"] = {};
    document["metadata"]["gid"] = v.Gid().AsInt();
    document["metadata"]["txid"] = v.impl_.transaction_->start_timestamp;
    document["metadata"]["deleted"] = false;
    document["metadata"]["is_node"] = true;

    try {
      mgcxx::text_search::add_document(
          index_context,
          mgcxx::text_search::DocumentInput{
              .data = document.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace)},
          kDoSkipCommit);
    } catch (const std::exception &e) {
      throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
    }
  }

  // As CREATE TEXT INDEX (...) queries don’t accumulate deltas, db_transactional_accessor_->Commit() does not reach
  // the code area where changes to indices are committed. To get around that without needing to commit text indices
  // after every such query, we commit here.
  try {
    mgcxx::text_search::commit(index_context);
  } catch (const std::exception &e) {
    throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
  }
  return true;
}

bool TextIndex::RecoverIndex(const std::string &index_name, LabelId label,
                             memgraph::utils::SkipList<Vertex>::Accessor vertices, NameIdMapper *name_id_mapper) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  nlohmann::json mappings = {};
  mappings["properties"] = {};
  mappings["properties"]["metadata"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
  mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};

  try {
    index_.emplace(index_name,
                   TextIndexData{.context_ = mgcxx::text_search::create_index(
                                     index_name, mgcxx::text_search::IndexConfig{.mappings = mappings.dump()}),
                                 .scope_ = label});
  } catch (const std::exception &e) {
    throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
  }
  label_to_index_.emplace(label, index_name);

  bool has_schema = false;
  std::vector<std::pair<PropertyId, std::string>> indexed_properties{};
  auto &index_context = index_.at(index_name).context_;
  for (const auto &v : vertices) {
    if (std::find(v.labels.begin(), v.labels.end(), label) == v.labels.end()) {
      continue;
    }

    auto vertex_properties = v.properties.Properties();

    if (!has_schema) [[unlikely]] {
      for (const auto &[prop_id, prop_val] : vertex_properties) {
        if (prop_val.IsBool() || prop_val.IsInt() || prop_val.IsDouble() || prop_val.IsString()) {
          indexed_properties.emplace_back(
              std::pair<PropertyId, std::string>{prop_id, name_id_mapper->IdToName(prop_id.AsUint())});
        }
      }
      has_schema = true;
    }

    nlohmann::json document = {};
    nlohmann::json properties = nlohmann::json::value_t::object;
    for (const auto &[prop_id, prop_name] : indexed_properties) {
      if (!vertex_properties.contains(prop_id)) {
        continue;
      }
      const auto prop_value = vertex_properties.at(prop_id);
      switch (prop_value.type()) {
        case PropertyValue::Type::Bool:
          properties[prop_name] = prop_value.ValueBool();
          break;
        case PropertyValue::Type::Int:
          properties[prop_name] = prop_value.ValueInt();
          break;
        case PropertyValue::Type::Double:
          properties[prop_name] = prop_value.ValueDouble();
          break;
        case PropertyValue::Type::String:
          properties[prop_name] = prop_value.ValueString();
          break;
        case PropertyValue::Type::Null:
        case PropertyValue::Type::List:
        case PropertyValue::Type::Map:
        case PropertyValue::Type::TemporalData:
        default:
          continue;
      }
    }

    document["data"] = properties;
    document["metadata"] = {};
    document["metadata"]["gid"] = v.gid.AsInt();
    document["metadata"]["txid"] = -1;
    document["metadata"]["deleted"] = false;
    document["metadata"]["is_node"] = true;

    try {
      mgcxx::text_search::add_document(
          index_context,
          mgcxx::text_search::DocumentInput{
              .data = document.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace)},
          kDoSkipCommit);
    } catch (const std::exception &e) {
      throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
    }
  }

  // As CREATE TEXT INDEX (...) queries don’t accumulate deltas, db_transactional_accessor_->Commit() does not reach
  // the code area where changes to indices are committed. To get around that without needing to commit text indices
  // after every such query, we commit here.
  try {
    mgcxx::text_search::commit(index_context);
  } catch (const std::exception &e) {
    throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
  }
  return true;
}

bool TextIndex::DropIndex(const std::string &index_name) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  try {
    mgcxx::text_search::drop_index(index_name);
  } catch (const std::exception &e) {
    throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
  }
  index_.erase(index_name);
  std::erase_if(label_to_index_, [index_name](const auto &item) { return item.second == index_name; });
  return true;
}

bool TextIndex::IndexExists(const std::string &index_name) const { return index_.contains(index_name); }

std::vector<Gid> TextIndex::Search(const std::string &index_name, const std::string &search_query) {
  if (!flags::run_time::GetExperimentalTextSearchEnabled()) {
    throw query::QueryException("To use text indices, enable the text search feature.");
  }

  if (!index_.contains(index_name)) {
    throw query::QueryException(fmt::format("Text index \"{}\" doesn’t exist.", index_name));
  }

  auto input = mgcxx::text_search::SearchInput{.search_query = search_query, .return_fields = {"data", "metadata"}};
  // Basic check for search fields in the query (Tantivy syntax delimits them with a `:` to the right)
  if (search_query.find(":") == std::string::npos) {
    input.search_fields = {"data"};
  }

  std::vector<Gid> found_nodes;
  mgcxx::text_search::SearchOutput search_results;

  try {
    search_results = mgcxx::text_search::search(index_.at(index_name).context_, input);
  } catch (const std::exception &e) {
    throw query::QueryException(fmt::format("Tantivy error: {}", e.what()));
  }
  for (const auto &doc : search_results.docs) {
    // The CXX .data() method (https://cxx.rs/binding/string.html) may overestimate string length, causing JSON parsing
    // errors downstream. We prevent this by resizing the converted string with the correctly-working .length() method.
    std::string doc_string = doc.data.data();
    doc_string.resize(doc.data.length());
    auto doc_json = nlohmann::json::parse(doc_string);
    found_nodes.push_back(storage::Gid::FromString(doc_json["metadata"]["gid"].dump()));
  }
  return found_nodes;
}

void TextIndex::Commit() {
  for (auto &[_, index_data] : index_) {
    mgcxx::text_search::commit(index_data.context_);
  }
}

void TextIndex::Rollback() {
  for (auto &[_, index_data] : index_) {
    mgcxx::text_search::rollback(index_data.context_);
  }
}

std::vector<std::pair<std::string, LabelId>> TextIndex::ListIndices() const {
  std::vector<std::pair<std::string, LabelId>> ret;
  ret.reserve(index_.size());
  for (const auto &[index_name, index_data] : index_) {
    ret.emplace_back(index_name, index_data.scope_);
  }
  return ret;
}

std::uint64_t TextIndex::ApproximateVertexCount(const std::string &index_name) const { return 10; }

}  // namespace memgraph::storage
