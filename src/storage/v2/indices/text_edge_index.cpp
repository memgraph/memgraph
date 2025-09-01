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

#include "storage/v2/indices/text_edge_index.hpp"

#include "mgcxx_text_search.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/text_index_utils.hpp"

namespace r = ranges;
namespace memgraph::storage {

void TextEdgeIndex::CreateTantivyIndex(const std::string &index_path, const TextEdgeIndexSpec &index_info) {
  try {
    nlohmann::json mappings = {};
    mappings["properties"] = {};
    mappings["properties"]["metadata"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["all"] = {{"type", "text"}, {"fast", true}, {"stored", true}, {"text", true}};

    auto [_, success] = index_.try_emplace(
        index_info.index_name_,
        mgcxx::text_search::create_index(index_path, mgcxx::text_search::IndexConfig{.mappings = mappings.dump()}),
        index_info.edge_type_, index_info.properties_);
    if (!success) {
      throw query::TextSearchException("Text edge index \"{}\" already exists at path: {}.", index_info.index_name_,
                                       index_path);
    }
  } catch (const std::exception &e) {
    spdlog::error("Failed to create text edge index \"{}\" at path: {}. Error: {}", index_info.index_name_, index_path,
                  e.what());
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

std::vector<TextEdgeIndexData *> TextEdgeIndex::GetApplicableTextIndices(EdgeTypeId edge_type,
                                                                         std::span<PropertyId const> properties) {
  std::vector<TextEdgeIndexData *> applicable_text_indices;
  auto matches_edge_type = [&](const auto &text_edge_index_data) { return edge_type == text_edge_index_data.scope_; };

  auto matches_property = [&](const auto &text_edge_index_data) {
    if (text_edge_index_data.properties_.empty()) {  // If no properties are specified, all properties match
      return true;
    }
    return r::any_of(properties,
                     [&](auto property_id) { return r::contains(text_edge_index_data.properties_, property_id); });
  };

  for (auto &[index_name, text_edge_index_data] : index_) {
    if (matches_edge_type(text_edge_index_data) && matches_property(text_edge_index_data)) {
      applicable_text_indices.push_back(&text_edge_index_data);
    }
  }
  return applicable_text_indices;
}

void TextEdgeIndex::AddEdgeToTextIndex(std::int64_t edge_gid, std::int64_t from_vertex_gid, std::int64_t to_vertex_gid,
                                       nlohmann::json properties, std::string property_values_as_str,
                                       mgcxx::text_search::Context &context) {
  nlohmann::json document = {};
  document["data"] = std::move(properties);
  document["all"] = std::move(property_values_as_str);
  document["metadata"] = {};
  document["metadata"]["edge_gid"] = edge_gid;
  document["metadata"]["from_vertex_gid"] = from_vertex_gid;
  document["metadata"]["to_vertex_gid"] = to_vertex_gid;

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

void TextEdgeIndex::UpdateOnEdgeCreation(const Edge *edge, const Vertex *from_vertex, const Vertex *to_vertex,
                                         EdgeTypeId edge_type, Transaction &tx) {
  auto applicable_text_indices = GetApplicableTextIndices(edge_type, edge->properties.ExtractPropertyIds());
  if (applicable_text_indices.empty()) return;
  TrackTextEdgeIndexChange(tx.text_edge_index_change_collector_, applicable_text_indices, edge, from_vertex, to_vertex,
                           TextIndexOp::ADD);
}

void TextEdgeIndex::RemoveEdge(const Edge *edge, EdgeTypeId edge_type, Transaction &tx) {
  auto applicable_text_indices = GetApplicableTextIndices(edge_type, edge->properties.ExtractPropertyIds());
  if (applicable_text_indices.empty()) return;
  TrackTextEdgeIndexChange(tx.text_edge_index_change_collector_, applicable_text_indices, edge, nullptr, nullptr,
                           TextIndexOp::REMOVE);
}

void TextEdgeIndex::UpdateOnSetProperty(const Edge *edge, const Vertex *from_vertex, const Vertex *to_vertex,
                                        EdgeTypeId edge_type, Transaction &tx) {
  auto applicable_text_indices = GetApplicableTextIndices(edge_type, edge->properties.ExtractPropertyIds());
  if (applicable_text_indices.empty()) return;
  TrackTextEdgeIndexChange(tx.text_edge_index_change_collector_, applicable_text_indices, edge, from_vertex, to_vertex,
                           TextIndexOp::UPDATE);
}

void TextEdgeIndex::CreateIndex(const TextEdgeIndexSpec &index_info, VerticesIterable vertices,
                                NameIdMapper *name_id_mapper) {
  CreateTantivyIndex(MakeIndexPath(text_index_storage_dir_, index_info.index_name_), index_info);

  auto &index_data = index_.at(index_info.index_name_);
  for (const auto &vertex : vertices) {
    const auto edges_accessor = vertex.OutEdges(View::NEW, {index_info.edge_type_}).GetValue();
    for (const auto &edge : edges_accessor.edges) {
      // If properties are specified, we serialize only those properties; otherwise, all properties of the edge.
      auto edge_properties = index_info.properties_.empty()
                                 ? edge.Properties(View::NEW).GetValue()
                                 : edge.PropertiesByPropertyIds(index_info.properties_, View::NEW).GetValue();
      TextEdgeIndex::AddEdgeToTextIndex(edge.Gid().AsInt(), edge.FromVertex().Gid().AsInt(),
                                        edge.ToVertex().Gid().AsInt(),
                                        SerializeProperties(edge_properties, name_id_mapper),
                                        StringifyProperties(edge_properties), index_data.context_);
    }
  }
}

void TextEdgeIndex::RecoverIndex(const TextEdgeIndexSpec &index_info,
                                 std::optional<SnapshotObserverInfo> const &snapshot_info) {
  CreateTantivyIndex(MakeIndexPath(text_index_storage_dir_, index_info.index_name_), index_info);
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::TEXT_IDX);
  }
}

void TextEdgeIndex::DropIndex(const std::string &index_name) {
  auto node = index_.extract(index_name);
  if (node.empty()) {
    throw query::TextSearchException("Text index {} doesn't exist.", index_name);
  }

  auto &entry = node.mapped();
  try {
    mgcxx::text_search::drop_index(std::move(entry.context_));
  } catch (const std::exception &e) {
    CreateTantivyIndex(
        MakeIndexPath(text_index_storage_dir_, index_name),
        TextEdgeIndexSpec{
            .index_name_ = index_name, .edge_type_ = entry.scope_, .properties_ = std::move(entry.properties_)});
    throw query::TextSearchException("Text index error on drop: {}", e.what());
  }
}

bool TextEdgeIndex::IndexExists(const std::string &index_name) const { return index_.contains(index_name); }

mgcxx::text_search::SearchOutput TextEdgeIndex::SearchGivenProperties(const std::string &index_name,
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

mgcxx::text_search::SearchOutput TextEdgeIndex::RegexSearch(const std::string &index_name,
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

mgcxx::text_search::SearchOutput TextEdgeIndex::SearchAllProperties(const std::string &index_name,
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

std::vector<EdgeTextSearchResult> TextEdgeIndex::Search(const std::string &index_name, const std::string &search_query,
                                                        text_search_mode search_mode) {
  if (!index_.contains(index_name)) {
    throw query::TextSearchException("Text edge index \"{}\" doesn't exist.", index_name);
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

  std::vector<EdgeTextSearchResult> found_edges;
  for (const auto &doc : search_results.docs) {
    // Create string using both data pointer and length to avoid buffer overflow
    // The CXX .data() method may not null-terminate the string properly
    std::string doc_string(doc.data.data(), doc.data.length());
    auto doc_json = nlohmann::json::parse(doc_string);

    Gid edge_gid = storage::Gid::FromString(doc_json["metadata"]["edge_gid"].dump());
    Gid from_vertex_gid = storage::Gid::FromString(doc_json["metadata"]["from_vertex_gid"].dump());
    Gid to_vertex_gid = storage::Gid::FromString(doc_json["metadata"]["to_vertex_gid"].dump());

    found_edges.emplace_back(edge_gid, from_vertex_gid, to_vertex_gid);
  }
  return found_edges;
}

std::string TextEdgeIndex::Aggregate(const std::string &index_name, const std::string &search_query,
                                     const std::string &aggregation_query) {
  if (!index_.contains(index_name)) {
    throw query::TextSearchException("Text edge index \"{}\" doesn't exist.", index_name);
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

std::vector<TextEdgeIndexSpec> TextEdgeIndex::ListIndices() const {
  std::vector<TextEdgeIndexSpec> ret;
  ret.reserve(index_.size());
  for (const auto &[index_name, index_data] : index_) {
    ret.emplace_back(index_name, index_data.scope_, index_data.properties_);
  }
  return ret;
}

void TextEdgeIndex::Clear() {
  if (!index_.empty()) {
    std::error_code ec;
    std::filesystem::remove_all(text_index_storage_dir_, ec);
    if (ec) {
      spdlog::error("Error removing text edge index directory '{}': {}", text_index_storage_dir_, ec.message());
      return;
    }
  }
  index_.clear();
}

void TextEdgeIndex::ApplyTrackedChanges(Transaction &tx, NameIdMapper *name_id_mapper) {
  for (const auto &[index_data_ptr, pending] : tx.text_edge_index_change_collector_) {
    // Take exclusive lock to properly serialize all updates and hold it for the entire operation
    std::lock_guard lock(index_data_ptr->write_mutex_);
    try {
      for (const auto *edge : pending.to_remove_) {
        auto search_edge_to_be_deleted =
            mgcxx::text_search::SearchInput{.search_query = fmt::format("metadata.edge_gid:{}", edge->gid.AsInt())};
        mgcxx::text_search::delete_document(index_data_ptr->context_, search_edge_to_be_deleted, kDoSkipCommit);
      }
      for (const auto &edge_with_vertices : pending.to_add_) {
        auto edge_properties =
            index_data_ptr->properties_.empty()
                ? edge_with_vertices.edge->properties.Properties()
                : ExtractProperties(edge_with_vertices.edge->properties, index_data_ptr->properties_);
        // Now we have access to the vertex GIDs through the EdgeWithVertices structure
        TextEdgeIndex::AddEdgeToTextIndex(
            edge_with_vertices.edge->gid.AsInt(), edge_with_vertices.from_vertex->gid.AsInt(),
            edge_with_vertices.to_vertex->gid.AsInt(), SerializeProperties(edge_properties, name_id_mapper),
            StringifyProperties(edge_properties), index_data_ptr->context_);
      }
      mgcxx::text_search::commit(index_data_ptr->context_);
    } catch (const std::exception &e) {
      throw query::TextSearchException("Text search error: {}", e.what());
    }
  }
}

}  // namespace memgraph::storage
