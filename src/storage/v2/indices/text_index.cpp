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

#include "flags/experimental.hpp"
#include "mgcxx_text_search.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace r = ranges;
namespace rv = r::views;
namespace memgraph::storage {

inline std::string TextIndex::MakeIndexPath(std::string_view index_name) const {
  return (text_index_storage_dir_ / index_name).string();
}

void TextIndex::CreateTantivyIndex(const std::string &index_path, const TextIndexSpec &index_info) {
  try {
    nlohmann::json mappings = {};
    mappings["properties"] = {};
    mappings["properties"]["metadata"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["all"] = {{"type", "text"}, {"fast", true}, {"stored", true}, {"text", true}};

    auto [_, success] = index_.try_emplace(
        index_info.index_name_,
        mgcxx::text_search::create_index(index_path, mgcxx::text_search::IndexConfig{.mappings = mappings.dump()}),
        index_info.label_, index_info.properties_);
    if (!success) {
      spdlog::error("Text index \"{}\" already exists at path: {}.", index_info.index_name_, index_path);
      throw query::TextSearchException("Text index \"{}\" already exists at path: {}.", index_info.index_name_,
                                       index_path);
    }
  } catch (const std::exception &e) {
    spdlog::error("Failed to create text index \"{}\" at path: {}. Error: {}", index_info.index_name_, index_path,
                  e.what());
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

std::vector<TextIndexData *> TextIndex::GetApplicableTextIndices(std::span<storage::LabelId const> labels,
                                                                 std::span<PropertyId const> properties) {
  std::vector<TextIndexData *> applicable_text_indices;
  auto matches_label = [&](const auto &text_index_data) {
    return r::any_of(labels, [&](auto label_id) { return label_id == text_index_data.scope_; });
  };

  auto matches_property = [&](const auto &text_index_data) {
    if (!text_index_data.properties_) {
      return true;
    }
    return r::any_of(properties,
                     [&](auto property_id) { return r::contains(*text_index_data.properties_, property_id); });
  };

  for (auto &[index_name, text_index_data] : index_) {
    if (matches_label(text_index_data) && matches_property(text_index_data)) {
      applicable_text_indices.push_back(&text_index_data);
    }
  }
  return applicable_text_indices;
}

std::map<PropertyId, PropertyValue> TextIndex::ExtractVertexProperties(const PropertyStore &property_store,
                                                                       std::span<PropertyId const> properties) {
  if (properties.empty()) {
    return property_store.Properties();
  }

  auto property_paths = properties |
                        rv::transform([](PropertyId property) { return storage::PropertyPath{property}; }) |
                        r::to<std::vector<storage::PropertyPath>>();
  auto property_values = property_store.ExtractPropertyValuesMissingAsNull(property_paths);

  return rv::zip(properties, property_values) | rv::transform([](const auto &property_id_value_pair) {
           return std::make_pair(std::get<0>(property_id_value_pair), std::get<1>(property_id_value_pair));
         }) |
         r::to<std::map<PropertyId, PropertyValue>>();
}

void TextIndex::AddNodeToTextIndex(std::int64_t gid, const nlohmann::json &properties,
                                   const std::string &property_values_as_str, TextIndexData *applicable_text_index) {
  if (!applicable_text_index) {
    return;
  }

  nlohmann::json document = {};
  document["data"] = properties;
  document["all"] = property_values_as_str;
  document["metadata"] = {};
  document["metadata"]["gid"] = gid;

  auto context_ptr = applicable_text_index->synchronized_context_.MutableSharedLock();
  try {
    mgcxx::text_search::add_document(
        *context_ptr,
        mgcxx::text_search::DocumentInput{.data =
                                              document.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace)},
        kDoSkipCommit);
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

void TextIndex::AddNode(Vertex *vertex_after_update, NameIdMapper *name_id_mapper,
                        std::span<TextIndexData *> applicable_text_indices) {
  for (auto *applicable_text_index : applicable_text_indices) {
    auto vertex_properties =
        applicable_text_index->properties_
            ? ExtractVertexProperties(vertex_after_update->properties, *applicable_text_index->properties_)
            : vertex_after_update->properties.Properties();
    AddNodeToTextIndex(vertex_after_update->gid.AsInt(), SerializeProperties(vertex_properties, name_id_mapper),
                       StringifyProperties(vertex_properties), applicable_text_index);
  }
}

void TextIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper, Transaction &tx) {
  auto applicable_text_indices = GetApplicableTextIndices(std::array{label}, vertex->properties.ExtractPropertyIds());
  if (applicable_text_indices.empty()) return;
  AddNode(vertex, name_id_mapper, applicable_text_indices);
  tx.text_index_operations_performed_ = true;
}

void TextIndex::UpdateOnRemoveLabel(LabelId label, Vertex *vertex, Transaction &tx) {
  auto applicable_text_indices = GetApplicableTextIndices(std::array{label}, vertex->properties.ExtractPropertyIds());
  if (applicable_text_indices.empty()) return;
  RemoveNode(vertex, applicable_text_indices);
  tx.text_index_operations_performed_ = true;
}

void TextIndex::UpdateOnSetProperty(Vertex *vertex, NameIdMapper *name_id_mapper, Transaction &tx) {
  auto applicable_text_indices = GetApplicableTextIndices(vertex->labels, vertex->properties.ExtractPropertyIds());
  if (applicable_text_indices.empty()) return;
  RemoveNode(vertex, applicable_text_indices);
  AddNode(vertex, name_id_mapper, applicable_text_indices);
  tx.text_index_operations_performed_ = true;
}

void TextIndex::RemoveNode(Vertex *vertex, Transaction &tx) {
  auto applicable_text_indices = GetApplicableTextIndices(vertex->labels, vertex->properties.ExtractPropertyIds());
  if (applicable_text_indices.empty()) return;
  RemoveNode(vertex, applicable_text_indices);
  tx.text_index_operations_performed_ = true;
}

void TextIndex::RemoveNode(Vertex *vertex_after_update, std::span<TextIndexData *> applicable_text_indices) {
  auto search_node_to_be_deleted =
      mgcxx::text_search::SearchInput{.search_query = fmt::format("metadata.gid:{}", vertex_after_update->gid.AsInt())};
  for (auto *applicable_text_index : applicable_text_indices) {
    try {
      auto context_ptr = applicable_text_index->synchronized_context_.MutableSharedLock();
      mgcxx::text_search::delete_document(*context_ptr, search_node_to_be_deleted, kDoSkipCommit);
    } catch (const std::exception &e) {
      throw query::TextSearchException("Tantivy error: {}", e.what());
    }
  }
}

void TextIndex::CreateIndex(const TextIndexSpec &index_info, storage::VerticesIterable vertices,
                            NameIdMapper *name_id_mapper) {
  CreateTantivyIndex(MakeIndexPath(index_info.index_name_),
                     {index_info.index_name_, index_info.label_, index_info.properties_});

  for (const auto &v : vertices) {
    if (!v.HasLabel(index_info.label_, View::NEW).GetValue()) {
      continue;
    }
    // If properties are specified, we serialize only those properties; otherwise, all properties of the vertex.
    auto vertex_properties = index_info.properties_.has_value() && !index_info.properties_->empty()
                                 ? v.PropertiesByPropertyIds(*index_info.properties_, View::NEW).GetValue()
                                 : v.Properties(View::NEW).GetValue();
    AddNodeToTextIndex(v.Gid().AsInt(), SerializeProperties(vertex_properties, name_id_mapper),
                       StringifyProperties(vertex_properties), &index_.at(index_info.index_name_));
  }
}

void TextIndex::RecoverIndex(const TextIndexSpec &index_info,
                             std::optional<SnapshotObserverInfo> const &snapshot_info) {
  CreateTantivyIndex(MakeIndexPath(index_info.index_name_), index_info);
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::TEXT_IDX);
  }
}

void TextIndex::DropIndex(const std::string &index_name) {
  if (!index_.contains(index_name)) {
    throw query::TextSearchException("Text index \"{}\" doesn’t exist.", index_name);
  }
  try {
    index_.erase(index_name);
    mgcxx::text_search::drop_index(MakeIndexPath(index_name));
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

bool TextIndex::IndexExists(const std::string &index_name) const { return index_.contains(index_name); }

mgcxx::text_search::SearchOutput TextIndex::SearchGivenProperties(const std::string &index_name,
                                                                  const std::string &search_query) {
  try {
    auto context_ptr = index_.at(index_name).synchronized_context_.MutableSharedLock();
    return mgcxx::text_search::search(
        *context_ptr, mgcxx::text_search::SearchInput{.search_query = search_query, .return_fields = {"metadata"}});
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }

  return mgcxx::text_search::SearchOutput{};
}

mgcxx::text_search::SearchOutput TextIndex::RegexSearch(const std::string &index_name,
                                                        const std::string &search_query) {
  try {
    auto context_ptr = index_.at(index_name).synchronized_context_.MutableSharedLock();
    return mgcxx::text_search::regex_search(
        *context_ptr, mgcxx::text_search::SearchInput{
                          .search_fields = {"all"}, .search_query = search_query, .return_fields = {"metadata"}});
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }

  return mgcxx::text_search::SearchOutput{};
}

mgcxx::text_search::SearchOutput TextIndex::SearchAllProperties(const std::string &index_name,
                                                                const std::string &search_query) {
  try {
    auto context_ptr = index_.at(index_name).synchronized_context_.MutableSharedLock();
    return mgcxx::text_search::search(
        *context_ptr, mgcxx::text_search::SearchInput{
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
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }
  if (!index_.contains(index_name)) {
    throw query::TextSearchException("Text index \"{}\" doesn’t exist.", index_name);
  }

  mgcxx::text_search::DocumentOutput aggregation_result;
  try {
    auto context_ptr = index_.at(index_name).synchronized_context_.MutableSharedLock();
    aggregation_result = mgcxx::text_search::aggregate(
        *context_ptr,
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
  for (auto &[_, index_data] : index_) {
    auto context_ptr = index_data.synchronized_context_.Lock();
    mgcxx::text_search::commit(*context_ptr);
  }
}

void TextIndex::Rollback() {
  for (auto &[_, index_data] : index_) {
    auto context_ptr = index_data.synchronized_context_.Lock();
    mgcxx::text_search::rollback(*context_ptr);
  }
}

std::vector<TextIndexSpec> TextIndex::ListIndices() const {
  std::vector<TextIndexSpec> ret;
  ret.reserve(index_.size());
  for (const auto &[index_name, index_data] : index_) {
    ret.emplace_back(index_name, index_data.scope_, index_data.properties_);
  }
  return ret;
}

void TextIndex::Clear() {
  if (!index_.empty()) {
    std::error_code ec;
    std::filesystem::remove_all(text_index_storage_dir_, ec);
    if (ec) {
      spdlog::error("Error removing text index directory '{}': {}", text_index_storage_dir_, ec.message());
      return;
    }
  }
  index_.clear();
}

}  // namespace memgraph::storage
