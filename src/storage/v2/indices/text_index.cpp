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

#include "flags/experimental.hpp"
#include "flags/run_time_configurable.hpp"
#include "mgcxx_text_search.hpp"
#include "query/db_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"

#include <span>
#include <vector>

namespace memgraph::storage {

std::string GetPropertyName(PropertyId prop_id, memgraph::query::DbAccessor *db) { return db->PropertyToName(prop_id); }

std::string GetPropertyName(PropertyId prop_id, NameIdMapper *name_id_mapper) {
  return name_id_mapper->IdToName(prop_id.AsUint());
}

inline std::string TextIndex::MakeIndexPath(const std::filesystem::path &storage_dir, const std::string &index_name) {
  return (storage_dir / kTextIndicesDirectory / index_name).string();
}

void TextIndex::CreateEmptyIndex(const std::filesystem::path &storage_dir, const std::string &index_name,
                                 LabelId label) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  if (index_.contains(index_name)) {
    throw query::TextSearchException("Text index \"{}\" already exists.", index_name);
  }

  try {
    nlohmann::json mappings = {};
    mappings["properties"] = {};
    mappings["properties"]["metadata"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["all"] = {{"type", "text"}, {"fast", true}, {"stored", true}, {"text", true}};

    index_.emplace(index_name, TextIndexData{.context_ = mgcxx::text_search::create_index(
                                                 MakeIndexPath(storage_dir, index_name),
                                                 mgcxx::text_search::IndexConfig{.mappings = mappings.dump()}),
                                             .scope_ = label});
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
  label_to_index_.emplace(label, index_name);
}

template <typename T>
nlohmann::json TextIndex::SerializeProperties(const std::map<PropertyId, PropertyValue> &properties, T *name_resolver) {
  nlohmann::json serialized_properties = nlohmann::json::value_t::object;
  for (const auto &[prop_id, prop_value] : properties) {
    switch (prop_value.type()) {
      case PropertyValue::Type::Bool:
        serialized_properties[GetPropertyName(prop_id, name_resolver)] = prop_value.ValueBool();
        break;
      case PropertyValue::Type::Int:
        serialized_properties[GetPropertyName(prop_id, name_resolver)] = prop_value.ValueInt();
        break;
      case PropertyValue::Type::Double:
        serialized_properties[GetPropertyName(prop_id, name_resolver)] = prop_value.ValueDouble();
        break;
      case PropertyValue::Type::String:
        serialized_properties[GetPropertyName(prop_id, name_resolver)] = prop_value.ValueString();
        break;
      case PropertyValue::Type::Null:
      case PropertyValue::Type::List:
      case PropertyValue::Type::Map:
      case PropertyValue::Type::TemporalData:
      case PropertyValue::Type::ZonedTemporalData:
      default:
        continue;
    }
  }

  return serialized_properties;
}

std::string TextIndex::StringifyProperties(const std::map<PropertyId, PropertyValue> &properties) {
  std::vector<std::string> indexable_properties_as_string;
  for (const auto &[_, prop_value] : properties) {
    switch (prop_value.type()) {
      case PropertyValue::Type::Bool:
        indexable_properties_as_string.push_back(prop_value.ValueBool() ? "true" : "false");
        break;
      case PropertyValue::Type::Int:
        indexable_properties_as_string.push_back(std::to_string(prop_value.ValueInt()));
        break;
      case PropertyValue::Type::Double:
        indexable_properties_as_string.push_back(std::to_string(prop_value.ValueDouble()));
        break;
      case PropertyValue::Type::String:
        indexable_properties_as_string.push_back(prop_value.ValueString());
        break;
      // NOTE: As the following types aren‘t indexed in Tantivy, they don’t appear in the property value string either.
      case PropertyValue::Type::Null:
      case PropertyValue::Type::List:
      case PropertyValue::Type::Map:
      case PropertyValue::Type::TemporalData:
      case PropertyValue::Type::ZonedTemporalData:
      default:
        continue;
    }
  }
  return utils::Join(indexable_properties_as_string, " ");
}

std::vector<mgcxx::text_search::Context *> TextIndex::GetApplicableTextIndices(
    std::span<storage::LabelId const> labels) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  std::vector<mgcxx::text_search::Context *> applicable_text_indices;
  for (const auto &label : labels) {
    if (label_to_index_.contains(label)) {
      applicable_text_indices.push_back(&index_.at(label_to_index_.at(label)).context_);
    }
  }
  return applicable_text_indices;
}

void TextIndex::LoadNodeToTextIndices(const std::int64_t gid, const nlohmann::json &properties,
                                      const std::string &property_values_as_str,
                                      const std::vector<mgcxx::text_search::Context *> &applicable_text_indices) {
  if (applicable_text_indices.empty()) {
    return;
  }

  // NOTE: Text indexes are presently all-property indices. If we allow text indexes restricted to specific properties,
  // an indexable document should be created for each applicable index.
  nlohmann::json document = {};
  document["data"] = properties;
  document["all"] = property_values_as_str;
  document["metadata"] = {};
  document["metadata"]["gid"] = gid;
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
      throw query::TextSearchException("Tantivy error: {}", e.what());
    }
  }
}

void TextIndex::CommitLoadedNodes(mgcxx::text_search::Context &index_context) {
  // As CREATE TEXT INDEX (...) queries don’t accumulate deltas, db_transactional_accessor_->Commit() does not reach
  // the code area where changes to indices are committed. To get around that without needing to commit text indices
  // after every such query, we commit here.
  try {
    mgcxx::text_search::commit(index_context);
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

void TextIndex::AddNode(
    Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
    const std::optional<std::vector<mgcxx::text_search::Context *>> &maybe_applicable_text_indices) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  auto applicable_text_indices =
      maybe_applicable_text_indices.value_or(GetApplicableTextIndices(vertex_after_update->labels));
  if (applicable_text_indices.empty()) {
    return;
  }

  auto vertex_properties = vertex_after_update->properties.Properties();
  LoadNodeToTextIndices(vertex_after_update->gid.AsInt(), SerializeProperties(vertex_properties, name_id_mapper),
                        StringifyProperties(vertex_properties), applicable_text_indices);
}

void TextIndex::UpdateNode(Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
                           const std::vector<LabelId> &removed_labels) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  if (!removed_labels.empty()) {
    auto indexes_to_remove_node_from = GetApplicableTextIndices(removed_labels);
    RemoveNode(vertex_after_update, indexes_to_remove_node_from);
  }

  auto applicable_text_indices = GetApplicableTextIndices(vertex_after_update->labels);
  if (applicable_text_indices.empty()) return;
  RemoveNode(vertex_after_update, applicable_text_indices);
  AddNode(vertex_after_update, name_id_mapper, applicable_text_indices);
}

void TextIndex::RemoveNode(
    Vertex *vertex_after_update,
    const std::optional<std::vector<mgcxx::text_search::Context *>> &maybe_applicable_text_indices) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  auto search_node_to_be_deleted =
      mgcxx::text_search::SearchInput{.search_query = fmt::format("metadata.gid:{}", vertex_after_update->gid.AsInt())};

  for (auto *index_context :
       maybe_applicable_text_indices.value_or(GetApplicableTextIndices(vertex_after_update->labels))) {
    try {
      mgcxx::text_search::delete_document(*index_context, search_node_to_be_deleted, kDoSkipCommit);
    } catch (const std::exception &e) {
      throw query::TextSearchException("Tantivy error: {}", e.what());
    }
  }
}

void TextIndex::CreateIndex(const std::filesystem::path &storage_dir, const std::string &index_name, LabelId label,
                            memgraph::query::DbAccessor *db) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  CreateEmptyIndex(storage_dir, index_name, label);

  for (const auto &v : db->Vertices(View::NEW)) {
    if (!v.HasLabel(View::NEW, label).GetValue()) {
      continue;
    }

    auto vertex_properties = v.Properties(View::NEW).GetValue();
    LoadNodeToTextIndices(v.Gid().AsInt(), SerializeProperties(vertex_properties, db),
                          StringifyProperties(vertex_properties), {&index_.at(index_name).context_});
  }

  CommitLoadedNodes(index_.at(index_name).context_);
}

void TextIndex::RecoverIndex(const std::filesystem::path &storage_dir, const std::string &index_name, LabelId label,
                             memgraph::utils::SkipList<Vertex>::Accessor vertices, NameIdMapper *name_id_mapper) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  // Clear Tantivy-internal files if they exist from previous sessions
  std::filesystem::remove_all(storage_dir / kTextIndicesDirectory / index_name);

  CreateEmptyIndex(storage_dir, index_name, label);

  for (const auto &v : vertices) {
    if (std::find(v.labels.begin(), v.labels.end(), label) == v.labels.end()) {
      continue;
    }

    auto vertex_properties = v.properties.Properties();
    LoadNodeToTextIndices(v.gid.AsInt(), SerializeProperties(vertex_properties, name_id_mapper),
                          StringifyProperties(vertex_properties), {&index_.at(index_name).context_});
  }

  CommitLoadedNodes(index_.at(index_name).context_);
}

LabelId TextIndex::DropIndex(const std::filesystem::path &storage_dir, const std::string &index_name) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  if (!index_.contains(index_name)) {
    throw query::TextSearchException("Text index \"{}\" doesn’t exist.", index_name);
  }

  try {
    mgcxx::text_search::drop_index(MakeIndexPath(storage_dir, index_name));
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
  auto deleted_index_label = index_.at(index_name).scope_;

  index_.erase(index_name);
  std::erase_if(label_to_index_, [index_name](const auto &item) { return item.second == index_name; });

  return deleted_index_label;
}

bool TextIndex::IndexExists(const std::string &index_name) const { return index_.contains(index_name); }

mgcxx::text_search::SearchOutput TextIndex::SearchGivenProperties(const std::string &index_name,
                                                                  const std::string &search_query) {
  try {
    return mgcxx::text_search::search(
        index_.at(index_name).context_,
        mgcxx::text_search::SearchInput{.search_query = search_query, .return_fields = {"metadata"}});
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }

  return mgcxx::text_search::SearchOutput{};
}

mgcxx::text_search::SearchOutput TextIndex::RegexSearch(const std::string &index_name,
                                                        const std::string &search_query) {
  try {
    return mgcxx::text_search::regex_search(
        index_.at(index_name).context_,
        mgcxx::text_search::SearchInput{
            .search_fields = {"all"}, .search_query = search_query, .return_fields = {"metadata"}});
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }

  return mgcxx::text_search::SearchOutput{};
}

mgcxx::text_search::SearchOutput TextIndex::SearchAllProperties(const std::string &index_name,
                                                                const std::string &search_query) {
  try {
    return mgcxx::text_search::search(
        index_.at(index_name).context_,
        mgcxx::text_search::SearchInput{
            .search_fields = {"all"}, .search_query = search_query, .return_fields = {"metadata"}});
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }

  return mgcxx::text_search::SearchOutput{};
}

std::vector<Gid> TextIndex::Search(const std::string &index_name, const std::string &search_query,
                                   text_search_mode search_mode) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  if (!index_.contains(index_name)) {
    throw query::TextSearchException("Text index \"{}\" doesn’t exist.", index_name);
  }

  mgcxx::text_search::SearchOutput search_results;
  switch (search_mode) {
    case text_search_mode::SPECIFIED_PROPERTIES:
      search_results = SearchGivenProperties(index_name, search_query);
      break;
    case text_search_mode::REGEX:
      search_results = RegexSearch(index_name, search_query);
      break;
    case text_search_mode::ALL_PROPERTIES:
      search_results = SearchAllProperties(index_name, search_query);
      break;
    default:
      throw query::TextSearchException(
          "Unsupported search mode: please use one of text_search.search, text_search.search_all, or "
          "text_search.regex_search.");
  }

  std::vector<Gid> found_nodes;
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

std::string TextIndex::Aggregate(const std::string &index_name, const std::string &search_query,
                                 const std::string &aggregation_query) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  if (!index_.contains(index_name)) {
    throw query::TextSearchException("Text index \"{}\" doesn’t exist.", index_name);
  }

  mgcxx::text_search::DocumentOutput aggregation_result;
  try {
    aggregation_result = mgcxx::text_search::aggregate(
        index_.at(index_name).context_,
        mgcxx::text_search::SearchInput{
            .search_fields = {"all"}, .search_query = search_query, .aggregation_query = aggregation_query});

  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
  // The CXX .data() method (https://cxx.rs/binding/string.html) may overestimate string length, causing JSON parsing
  // errors downstream. We prevent this by resizing the converted string with the correctly-working .length() method.
  std::string result_string = aggregation_result.data.data();
  result_string.resize(aggregation_result.data.length());
  return result_string;
}

void TextIndex::Commit() {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  for (auto &[_, index_data] : index_) {
    mgcxx::text_search::commit(index_data.context_);
  }
}

void TextIndex::Rollback() {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

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

}  // namespace memgraph::storage
