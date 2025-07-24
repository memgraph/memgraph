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

#include "storage/v2/indices/text_index.hpp"

#include "mgcxx_text_search.hpp"
#include "query/exceptions.hpp"  // TODO: remove from storage
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"

#include <span>
#include <vector>

namespace r = ranges;
namespace memgraph::storage {

std::string GetPropertyName(PropertyId prop_id, NameIdMapper *name_id_mapper) {
  return name_id_mapper->IdToName(prop_id.AsUint());
}

inline std::string TextIndex::MakeIndexPath(std::string_view index_name) const {
  return (text_index_storage_dir_ / index_name).string();
}

void TextIndex::CreateTantivyIndex(const std::string &index_name, LabelId label, const std::string &index_path) {
  if (index_.contains(index_name)) {
    throw query::TextSearchException("Text index \"{}\" already exists.", index_name);
  }

  try {
    nlohmann::json mappings = {};
    mappings["properties"] = {};
    mappings["properties"]["metadata"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["all"] = {{"type", "text"}, {"fast", true}, {"stored", true}, {"text", true}};

    index_.emplace(index_name,
                   TextIndexData{.context_ = mgcxx::text_search::create_index(
                                     index_path, mgcxx::text_search::IndexConfig{.mappings = mappings.dump()}),
                                 .scope_ = label});
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
  label_to_index_.emplace(label, index_name);
}

nlohmann::json TextIndex::SerializeProperties(const std::map<PropertyId, PropertyValue> &properties,
                                              NameIdMapper *name_id_mapper) {
  // Property types that are indexed in Tantivy are Bool, Int, Double, and String.
  nlohmann::json serialized_properties = nlohmann::json::value_t::object;
  for (const auto &[prop_id, prop_value] : properties) {
    switch (prop_value.type()) {
      case PropertyValue::Type::Bool:
        serialized_properties[GetPropertyName(prop_id, name_id_mapper)] = prop_value.ValueBool();
        break;
      case PropertyValue::Type::Int:
        serialized_properties[GetPropertyName(prop_id, name_id_mapper)] = prop_value.ValueInt();
        break;
      case PropertyValue::Type::Double:
        serialized_properties[GetPropertyName(prop_id, name_id_mapper)] = prop_value.ValueDouble();
        break;
      case PropertyValue::Type::String:
        serialized_properties[GetPropertyName(prop_id, name_id_mapper)] = prop_value.ValueString();
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
  // Property types that are indexed in Tantivy are Bool, Int, Double, and String.
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

std::string TextIndex::ToLowerCasePreservingBooleanOperators(std::string_view input) {
  if (input.empty()) return {};

  std::string result;
  result.reserve(input.length());

  auto it = input.cbegin();
  while (it != input.cend()) {
    if (std::isspace(*it)) {
      result += *it++;
      continue;
    }

    auto word_start = it;
    it = r::find_if(word_start, input.cend(), [](char c) { return std::isspace(c); });

    // Extract the word
    auto word = input.substr(word_start - input.cbegin(), it - word_start);
    auto uppercase_word = utils::ToUpperCase(word);

    // Check if it's a boolean operator (case-insensitive)
    if (uppercase_word == kBooleanAnd || uppercase_word == kBooleanOr || uppercase_word == kBooleanNot) {
      // Preserve the boolean operator in uppercase
      result += uppercase_word;
    } else {
      result += utils::ToLowerCase(word);
    }
  }

  return result;
}

std::vector<mgcxx::text_search::Context *> TextIndex::GetApplicableTextIndices(
    std::span<storage::LabelId const> labels) {
  std::vector<mgcxx::text_search::Context *> applicable_text_indices;
  for (const auto &label : labels) {
    if (label_to_index_.contains(label)) {
      applicable_text_indices.push_back(&index_.at(label_to_index_.at(label)).context_);
    }
  }
  return applicable_text_indices;
}

void TextIndex::AddNodeToTextIndices(std::int64_t gid, const nlohmann::json &properties,
                                     const std::string &property_values_as_str,
                                     const std::vector<mgcxx::text_search::Context *> &applicable_text_indices) {
  if (applicable_text_indices.empty()) {
    return;
  }

  // NOTE: Text indexes are presently all-property indices. If we allow text indexes restricted to specific properties,
  // an indexable document should be created for each applicable index. TODO(@DavIvek): check this
  nlohmann::json document = {};
  document["data"] = properties;
  document["all"] = property_values_as_str;
  document["metadata"] = {};
  document["metadata"]["gid"] = gid;

  std::shared_lock lock(index_writer_mutex_);
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

void TextIndex::AddNode(Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
                        const std::vector<mgcxx::text_search::Context *> &maybe_applicable_text_indices) {
  auto applicable_text_indices = maybe_applicable_text_indices.empty()
                                     ? GetApplicableTextIndices(vertex_after_update->labels)
                                     : maybe_applicable_text_indices;
  if (applicable_text_indices.empty()) return;

  auto vertex_properties = vertex_after_update->properties.Properties();
  AddNodeToTextIndices(vertex_after_update->gid.AsInt(), SerializeProperties(vertex_properties, name_id_mapper),
                       StringifyProperties(vertex_properties), applicable_text_indices);
}

void TextIndex::UpdateNode(Vertex *vertex_after_update, NameIdMapper *name_id_mapper) {
  auto applicable_text_indices = GetApplicableTextIndices(vertex_after_update->labels);
  if (applicable_text_indices.empty()) return;
  RemoveNode(vertex_after_update, applicable_text_indices);  // In order to update the node, we first remove it.
  AddNode(vertex_after_update, name_id_mapper, applicable_text_indices);
}

void TextIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper) {
  auto applicable_text_indices = GetApplicableTextIndices(std::array{label});
  if (applicable_text_indices.empty()) return;

  AddNode(vertex, name_id_mapper, applicable_text_indices);
}

void TextIndex::UpdateOnRemoveLabel(LabelId label, Vertex *vertex) {
  auto applicable_text_indices = GetApplicableTextIndices(std::array{label});
  if (applicable_text_indices.empty()) return;

  RemoveNode(vertex, applicable_text_indices);
}

void TextIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                    NameIdMapper *name_id_mapper) {
  // TODO(@DavIvek): This will get extended to handle specific properties in the future.
  UpdateNode(vertex, name_id_mapper);
}

void TextIndex::RemoveNode(Vertex *vertex_after_update,
                           const std::vector<mgcxx::text_search::Context *> &maybe_applicable_text_indices) {
  auto search_node_to_be_deleted =
      mgcxx::text_search::SearchInput{.search_query = fmt::format("metadata.gid:{}", vertex_after_update->gid.AsInt())};
  auto applicable_text_indices = maybe_applicable_text_indices.empty()
                                     ? GetApplicableTextIndices(vertex_after_update->labels)
                                     : maybe_applicable_text_indices;
  std::shared_lock lock(index_writer_mutex_);
  for (auto *index_context : applicable_text_indices) {
    try {
      mgcxx::text_search::delete_document(*index_context, search_node_to_be_deleted, kDoSkipCommit);
    } catch (const std::exception &e) {
      throw query::TextSearchException("Tantivy error: {}", e.what());
    }
  }
}

void TextIndex::CreateIndex(std::string const &index_name, LabelId label, storage::VerticesIterable vertices,
                            NameIdMapper *name_id_mapper) {
  CreateTantivyIndex(index_name, label, MakeIndexPath(index_name));

  for (const auto &v : vertices) {
    if (!v.HasLabel(label, View::NEW).GetValue()) {
      continue;
    }

    auto vertex_properties = v.Properties(View::NEW).GetValue();
    AddNodeToTextIndices(v.Gid().AsInt(), SerializeProperties(vertex_properties, name_id_mapper),
                         StringifyProperties(vertex_properties), {&index_.at(index_name).context_});
  }
}

void TextIndex::RecoverIndex(const std::string &index_name, LabelId label,
                             memgraph::utils::SkipList<Vertex>::Accessor vertices, NameIdMapper *name_id_mapper,
                             std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto index_path = MakeIndexPath(index_name);
  if (!std::filesystem::exists(index_path)) {
    throw query::TextSearchException("Text index \"{}\" does not exist at path: {}. Recovery failed.", index_name,
                                     index_path);
  }
  CreateTantivyIndex(index_name, label, index_path);
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::TEXT_IDX);
  }
}

LabelId TextIndex::DropIndex(const std::string &index_name) {
  if (!index_.contains(index_name)) {
    throw query::TextSearchException("Text index \"{}\" doesn’t exist.", index_name);
  }

  try {
    std::lock_guard lock(index_writer_mutex_);
    mgcxx::text_search::drop_index(MakeIndexPath(index_name));
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
  if (!index_.contains(index_name)) {
    throw query::TextSearchException("Text index \"{}\" doesn’t exist.", index_name);
  }

  mgcxx::text_search::SearchOutput search_results;
  switch (search_mode) {
    case text_search_mode::SPECIFIED_PROPERTIES:
      search_results = SearchGivenProperties(index_name, ToLowerCasePreservingBooleanOperators(search_query));
      break;
    case text_search_mode::REGEX:
      search_results = RegexSearch(index_name, ToLowerCasePreservingBooleanOperators(search_query));
      break;
    case text_search_mode::ALL_PROPERTIES:
      search_results = SearchAllProperties(index_name, ToLowerCasePreservingBooleanOperators(search_query));
      break;
    default:
      throw query::TextSearchException(
          "Unsupported search mode: please use one of text_search.search, text_search.search_all, or "
          "text_search.regex_search.");
  }

  std::vector<Gid> found_nodes;
  for (const auto &doc : search_results.docs) {
    // Create string using both data pointer and length to avoid buffer overflow
    // The CXX .data() method may not null-terminate the string properly
    std::string doc_string(doc.data.data(), doc.data.length());
    auto doc_json = nlohmann::json::parse(doc_string);
    found_nodes.push_back(storage::Gid::FromString(doc_json["metadata"]["gid"].dump()));
  }
  return found_nodes;
}

std::string TextIndex::Aggregate(const std::string &index_name, const std::string &search_query,
                                 const std::string &aggregation_query) {
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
  std::string result_string(aggregation_result.data.data(), aggregation_result.data.length());
  return result_string;
}

void TextIndex::Commit() {
  std::lock_guard lock(index_writer_mutex_);
  for (auto &[_, index_data] : index_) {
    mgcxx::text_search::commit(index_data.context_);
  }
}

void TextIndex::Rollback() {
  std::lock_guard lock(index_writer_mutex_);
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

void TextIndex::Clear() {
  std::lock_guard lock(index_writer_mutex_);
  for (const auto &[index_name, _] : index_) {
    mgcxx::text_search::drop_index(MakeIndexPath(index_name));
  }
  index_.clear();
  label_to_index_.clear();
}

}  // namespace memgraph::storage
