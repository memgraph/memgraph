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
#include "query/exceptions.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace r = ranges;
namespace memgraph::storage {

void TextIndex::CreateTantivyIndex(const std::string &index_path, const TextIndexSpec &index_info) {
  try {
    nlohmann::json mappings = {};
    mappings["properties"] = {};
    mappings["properties"]["metadata"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["all"] = {{"type", "text"}, {"fast", true}, {"stored", true}, {"text", true}};

    auto [_, success] = index_.try_emplace(
        index_info.index_name,
        mgcxx::text_search::create_index(index_path, mgcxx::text_search::IndexConfig{.mappings = mappings.dump()}),
        index_info.label, index_info.properties);
    if (!success) {
      throw query::TextSearchException("Text index {} already exists at path: {}.", index_info.index_name, index_path);
    }
  } catch (const std::exception &e) {
    spdlog::error("Failed to create text index {} at path: {}. Error: {}", index_info.index_name, index_path, e.what());
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

std::vector<TextIndexData *> TextIndex::GetApplicableTextIndices(std::span<storage::LabelId const> labels,
                                                                 std::span<PropertyId const> properties) {
  std::vector<TextIndexData *> applicable_text_indices;
  auto matches_label = [&](const auto &text_index_data) {
    return r::any_of(labels, [&](auto labelid) { return labelid == text_index_data.scope; });
  };

  auto matches_property = [&](const auto &text_index_data) {
    if (text_index_data.properties.empty()) {  // If no properties are specified, all properties match
      return true;
    }
    return r::any_of(properties,
                     [&](auto property_id) { return r::contains(text_index_data.properties, property_id); });
  };

  for (auto &[index_name, text_index_data] : index_) {
    if (matches_label(text_index_data) && matches_property(text_index_data)) {
      applicable_text_indices.push_back(&text_index_data);
    }
  }
  return applicable_text_indices;
}

void TextIndex::AddNodeToTextIndex(std::int64_t gid, const nlohmann::json &properties,
                                   const std::string &property_values_as_str, mgcxx::text_search::Context &context) {
  if (property_values_as_str.empty()) return;
  nlohmann::json document = {};
  document["data"] = properties;
  document["all"] = property_values_as_str;
  document["metadata"] = {};
  document["metadata"]["gid"] = gid;

  try {
    mgcxx::text_search::add_document(
        context,
        mgcxx::text_search::DocumentInput{.data =
                                              document.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace)},
        kDoSkipCommit);
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

void TextIndex::UpdateOnAddLabel(LabelId label, const Vertex *vertex, Transaction &tx) {
  auto applicable_text_indices = GetApplicableTextIndices(std::array{label}, vertex->properties.ExtractPropertyIds());
  if (applicable_text_indices.empty()) return;
  TrackTextIndexChange(tx.text_index_change_collector_, applicable_text_indices, vertex, TextIndexOp::ADD);
}

void TextIndex::UpdateOnRemoveLabel(LabelId label, const Vertex *vertex, Transaction &tx) {
  auto applicable_text_indices = GetApplicableTextIndices(std::array{label}, vertex->properties.ExtractPropertyIds());
  if (applicable_text_indices.empty()) return;
  TrackTextIndexChange(tx.text_index_change_collector_, applicable_text_indices, vertex, TextIndexOp::REMOVE);
}

void TextIndex::UpdateOnSetProperty(const Vertex *vertex, Transaction &tx) {
  auto applicable_text_indices = GetApplicableTextIndices(vertex->labels, vertex->properties.ExtractPropertyIds());
  if (applicable_text_indices.empty()) return;
  TrackTextIndexChange(tx.text_index_change_collector_, applicable_text_indices, vertex, TextIndexOp::UPDATE);
}

void TextIndex::RemoveNode(const Vertex *vertex, Transaction &tx) {
  auto applicable_text_indices = GetApplicableTextIndices(vertex->labels, vertex->properties.ExtractPropertyIds());
  if (applicable_text_indices.empty()) return;
  TrackTextIndexChange(tx.text_index_change_collector_, applicable_text_indices, vertex, TextIndexOp::REMOVE);
}

void TextIndex::CreateIndex(const TextIndexSpec &index_info, storage::VerticesIterable vertices,
                            NameIdMapper *name_id_mapper) {
  CreateTantivyIndex(MakeIndexPath(text_index_storage_dir_, index_info.index_name),
                     {index_info.index_name, index_info.label, index_info.properties});

  auto &index_data = index_.at(index_info.index_name);
  for (const auto &v : vertices) {
    if (!v.HasLabel(index_info.label, View::NEW).GetValue()) {
      continue;
    }
    // If properties are specified, we serialize only those properties; otherwise, all properties of the vertex.
    auto vertex_properties = index_info.properties.empty()
                                 ? v.Properties(View::NEW).GetValue()
                                 : v.PropertiesByPropertyIds(index_info.properties, View::NEW).GetValue();
    TextIndex::AddNodeToTextIndex(v.Gid().AsInt(), SerializeProperties(vertex_properties, name_id_mapper),
                                  StringifyProperties(vertex_properties), index_data.context);
  }
  try {
    mgcxx::text_search::commit(index_data.context);
  } catch (const std::exception &e) {
    throw query::TextSearchException("Text index commit error: {}", e.what());
  }
}

void TextIndex::RecoverIndex(const TextIndexSpec &index_info,
                             std::optional<SnapshotObserverInfo> const &snapshot_info) {
  CreateTantivyIndex(MakeIndexPath(text_index_storage_dir_, index_info.index_name), index_info);
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::TEXT_IDX);
  }
}

void TextIndex::DropIndex(const std::string &index_name) {
  auto node = index_.extract(index_name);
  if (node.empty()) {
    throw query::TextSearchException("Text index {} doesn't exist.", index_name);
  }

  auto &entry = node.mapped();
  try {
    mgcxx::text_search::drop_index(std::move(entry.context));
  } catch (const std::exception &e) {
    CreateTantivyIndex(
        MakeIndexPath(text_index_storage_dir_, index_name),
        TextIndexSpec{.index_name = index_name, .label = entry.scope, .properties = std::move(entry.properties)});
    throw query::TextSearchException("Text index error on drop: {}", e.what());
  }
}

bool TextIndex::IndexExists(const std::string &index_name) const { return index_.contains(index_name); }

std::vector<TextSearchResult> TextIndex::Search(const std::string &index_name, const std::string &search_query,
                                                text_search_mode search_mode, std::size_t limit) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }
  auto &context = std::invoke([&]() -> mgcxx::text_search::Context & {
    if (const auto it = index_.find(index_name); it != index_.end()) {
      return it->second.context;
    }
    throw query::TextSearchException("Text index {} doesn't exist.", index_name);
  });

  const auto search_results = PerformTextSearch(context, search_query, search_mode, limit);
  std::vector<TextSearchResult> found_nodes;
  found_nodes.reserve(search_results.docs.size());
  for (const auto &doc : search_results.docs) {
    // Create string using both data pointer and length to avoid buffer overflow
    // The CXX .data() method may not null-terminate the string properly
    const std::string doc_string(doc.data.data(), doc.data.length());
    const auto doc_json = nlohmann::json::parse(doc_string);
    found_nodes.emplace_back(storage::Gid::FromString(doc_json["metadata"]["gid"].dump()), doc.score);
  }
  return found_nodes;
}

std::string TextIndex::Aggregate(const std::string &index_name, const std::string &search_query,
                                 const std::string &aggregation_query) {
  if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    throw query::TextSearchDisabledException();
  }

  auto &context = std::invoke([&]() -> mgcxx::text_search::Context & {
    if (const auto it = index_.find(index_name); it != index_.end()) {
      return it->second.context;
    }
    throw query::TextSearchException("Text index {} doesn't exist.", index_name);
  });
  mgcxx::text_search::DocumentOutput aggregation_result;
  try {
    aggregation_result = mgcxx::text_search::aggregate(
        context, mgcxx::text_search::SearchInput{
                     .search_fields = {"all"}, .search_query = search_query, .aggregation_query = aggregation_query});

  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
  // The CXX .data() method (https://cxx.rs/binding/string.html) may overestimate string length, causing JSON parsing
  // errors downstream. We prevent this by resizing the converted string with the correctly-working .length() method.
  std::string result_string(aggregation_result.data.data(), aggregation_result.data.length());
  return result_string;
}

std::vector<TextIndexSpec> TextIndex::ListIndices() const {
  std::vector<TextIndexSpec> ret;
  ret.reserve(index_.size());
  for (const auto &[index_name, index_data] : index_) {
    ret.emplace_back(index_name, index_data.scope, index_data.properties);
  }
  return ret;
}

std::optional<uint64_t> TextIndex::ApproximateVerticesTextCount(std::string_view index_name) const {
  if (const auto it = index_.find(index_name); it != index_.end()) {
    const auto &index_data = it->second;
    return mgcxx::text_search::get_num_docs(index_data.context);
  }
  return std::nullopt;
}

void TextIndex::Clear() {
  if (!index_.empty()) {
    std::error_code ec;
    std::filesystem::remove_all(text_index_storage_dir_, ec);
    if (ec) {
      spdlog::error("Error removing text index directory '{}': {}", text_index_storage_dir_, ec.message());
      return;
    }
    index_.clear();
  }
}

void TextIndex::ApplyTrackedChanges(Transaction &tx, NameIdMapper *name_id_mapper) {
  for (const auto &[index_data_ptr, pending] : tx.text_index_change_collector_) {
    // Take exclusive lock to properly serialize all updates and hold it for the entire operation
    const std::lock_guard lock(index_data_ptr->write_mutex);
    try {
      for (const auto *vertex : pending.to_remove) {
        auto search_node_to_be_deleted =
            mgcxx::text_search::SearchInput{.search_query = fmt::format("metadata.gid:{}", vertex->gid.AsInt())};
        mgcxx::text_search::delete_document(index_data_ptr->context, search_node_to_be_deleted, kDoSkipCommit);
      }
      for (const auto *vertex : pending.to_add) {
        auto vertex_properties = index_data_ptr->properties.empty()
                                     ? vertex->properties.Properties()
                                     : ExtractProperties(vertex->properties, index_data_ptr->properties);
        TextIndex::AddNodeToTextIndex(vertex->gid.AsInt(), SerializeProperties(vertex_properties, name_id_mapper),
                                      StringifyProperties(vertex_properties), index_data_ptr->context);
      }
      mgcxx::text_search::commit(index_data_ptr->context);
    } catch (const std::exception &e) {
      throw query::TextSearchException("Text search error: {}", e.what());
    }
  }
}

}  // namespace memgraph::storage
