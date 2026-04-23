// Copyright 2026 Memgraph Ltd.
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
#include "query/exceptions.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/transaction.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

void TextEdgeIndex::CreateTantivyIndex(const std::string &index_path, const TextEdgeIndexSpec &index_info) {
  try {
    nlohmann::json mappings = {};
    mappings["properties"] = {};
    mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["all"] = {{"type", "text"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["edge_gid"] = {{"type", "u64"}, {"fast", true}, {"stored", true}, {"indexed", true}};
    mappings["properties"]["from_vertex_gid"] = {{"type", "u64"}, {"fast", true}, {"stored", true}, {"indexed", true}};
    mappings["properties"]["to_vertex_gid"] = {{"type", "u64"}, {"fast", true}, {"stored", true}, {"indexed", true}};

    auto [_, success] = index_.try_emplace(
        index_info.index_name,
        // If index already exists, it will be loaded and reused.
        mgcxx::text_search::create_index(index_path, mgcxx::text_search::IndexConfig{.mappings = mappings.dump()}),
        index_info.edge_type,
        index_info.properties);
    if (!success) {
      throw query::TextSearchException(
          "Text edge index {} already exists at path: {}.", index_info.index_name, index_path);
    }
  } catch (const std::exception &e) {
    spdlog::error(
        "Failed to create text edge index {} at path: {}. Error: {}", index_info.index_name, index_path, e.what());
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

std::vector<TextEdgeIndexData *> TextEdgeIndex::EdgeTypeApplicableTextIndices(EdgeTypeId edge_type) {
  std::vector<TextEdgeIndexData *> applicable_text_indices;
  for (auto &[_, index_data] : index_) {
    if (edge_type == index_data.scope) {
      applicable_text_indices.push_back(&index_data);
    }
  }
  return applicable_text_indices;
}

std::vector<TextEdgeIndexData *> TextEdgeIndex::GetIndicesMatchingProperties(
    std::span<TextEdgeIndexData *const> edge_type_indices, std::span<const PropertyId> properties) {
  std::vector<TextEdgeIndexData *> result;
  for (const auto &text_edge_index_data : edge_type_indices) {
    if (IndexPropertiesMatch(text_edge_index_data->properties, properties)) {
      result.push_back(text_edge_index_data);
    }
  }
  return result;
}

void TextEdgeIndex::AddEdgeToTextIndex(std::int64_t edge_gid, std::int64_t from_vertex_gid, std::int64_t to_vertex_gid,
                                       nlohmann::json properties, std::string all_property_values,
                                       mgcxx::text_search::Context &context) {
  if (all_property_values.empty()) return;
  nlohmann::json document = {};
  document["data"] = std::move(properties);
  document["all"] = std::move(all_property_values);
  document["edge_gid"] = static_cast<std::uint64_t>(edge_gid);
  document["from_vertex_gid"] = static_cast<std::uint64_t>(from_vertex_gid);
  document["to_vertex_gid"] = static_cast<std::uint64_t>(to_vertex_gid);

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

void TextEdgeIndex::RemoveEdge(const Edge *edge, const Vertex *from_vertex, const Vertex *to_vertex,
                               EdgeTypeId edge_type, Transaction &tx) {
  if (index_.empty()) return;
  auto edge_type_applicable_text_indices = EdgeTypeApplicableTextIndices(edge_type);
  if (edge_type_applicable_text_indices.empty()) return;
  const auto edge_properties = edge->properties.ExtractPropertyIds();
  auto applicable_text_indices = GetIndicesMatchingProperties(edge_type_applicable_text_indices, edge_properties);
  TrackTextEdgeIndexChange(
      tx.text_edge_index_change_collector_, applicable_text_indices, edge, from_vertex, to_vertex, TextIndexOp::REMOVE);
}

void TextEdgeIndex::UpdateOnSetProperty(const Edge *edge, const Vertex *from_vertex, const Vertex *to_vertex,
                                        EdgeTypeId edge_type, Transaction &tx, PropertyId property) {
  if (index_.empty()) return;
  auto has_edge_type = [&](const auto &text_edge_index_data) { return edge_type == text_edge_index_data.scope; };
  std::vector<TextEdgeIndexData *> applicable_text_indices;
  for (auto &[_, index_data] : index_) {
    if (IndexPropertiesMatch(index_data.properties, std::array{property}) && has_edge_type(index_data)) {
      applicable_text_indices.push_back(&index_data);
    }
  }
  TrackTextEdgeIndexChange(
      tx.text_edge_index_change_collector_, applicable_text_indices, edge, from_vertex, to_vertex, TextIndexOp::UPDATE);
}

void TextEdgeIndex::CreateIndex(const TextEdgeIndexSpec &index_info, VerticesIterable vertices,
                                NameIdMapper *name_id_mapper) {
  CreateTantivyIndex(MakeIndexPath(text_index_storage_dir_, index_info.index_name), index_info);

  auto &index_data = index_.at(index_info.index_name);
  for (const auto &vertex : vertices) {
    const auto edges_accessor = vertex.OutEdges(View::NEW, {index_info.edge_type}).value();
    for (const auto &edge : edges_accessor.edges) {
      // If properties are specified, we serialize only those properties; otherwise, all properties of the edge.
      auto edge_properties = index_info.properties.empty()
                                 ? edge.Properties(View::NEW).value()
                                 : edge.PropertiesByPropertyIds(index_info.properties, View::NEW).value();
      TextEdgeIndex::AddEdgeToTextIndex(edge.Gid().AsInt(),
                                        edge.FromVertex().Gid().AsInt(),
                                        edge.ToVertex().Gid().AsInt(),
                                        SerializeProperties(edge_properties, name_id_mapper),
                                        StringifyProperties(edge_properties),
                                        index_data.context);
    }
  }
  try {
    mgcxx::text_search::commit(index_data.context);
  } catch (const std::exception &e) {
    throw query::TextSearchException("Text index commit error: {}", e.what());
  }
}

void TextEdgeIndex::RecoverIndex(const TextEdgeIndexSpec &index_info, utils::SkipListDb<Vertex>::Accessor vertices,
                                 NameIdMapper *name_id_mapper,
                                 std::optional<SnapshotObserverInfo> const &snapshot_info) {
  const auto index_path = MakeIndexPath(text_index_storage_dir_, index_info.index_name);
  auto needs_rebuild = !std::filesystem::exists(index_path);
  try {
    CreateTantivyIndex(index_path, index_info);
  } catch (const query::TextSearchException &) {
    if (needs_rebuild) throw;
    // It's possible that index on disk has incompatible schema if, for example, new required properties were added to
    // the index spec in new versions
    spdlog::warn("Text edge index {} has incompatible schema on disk, rebuilding.", index_info.index_name);
    std::error_code ec;
    std::filesystem::remove_all(index_path, ec);
    if (ec)
      throw query::TextSearchException(
          "Failed to remove stale text edge index {}: {}", index_info.index_name, ec.message());
    needs_rebuild = true;
    CreateTantivyIndex(index_path, index_info);
  }

  if (needs_rebuild) {
    auto &context = index_.at(index_info.index_name).context;
    for (const auto &vertex : vertices) {
      for (const auto &[edge_type, to_vertex, edge_ref] : vertex.out_edges) {
        if (edge_type != index_info.edge_type) continue;

        auto *edge = edge_ref.ptr;
        auto properties_to_index =
            FilterPropertiesToIndex(index_info.properties, edge->properties.ExtractPropertyIds());
        if (properties_to_index.empty()) continue;

        auto properties_to_index_map = ExtractProperties(edge->properties, properties_to_index);
        TextEdgeIndex::AddEdgeToTextIndex(edge->gid.AsInt(),
                                          vertex.gid.AsInt(),
                                          to_vertex->gid.AsInt(),
                                          SerializeProperties(properties_to_index_map, name_id_mapper),
                                          StringifyProperties(properties_to_index_map),
                                          context);
      }
    }

    try {
      mgcxx::text_search::commit(context);
    } catch (const std::exception &e) {
      throw query::TextSearchException("Text index commit error: {}", e.what());
    }
  }

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
    mgcxx::text_search::drop_index(std::move(entry.context));
  } catch (const std::exception &e) {
    CreateTantivyIndex(
        MakeIndexPath(text_index_storage_dir_, index_name),
        TextEdgeIndexSpec{
            .index_name = index_name, .edge_type = entry.scope, .properties = std::move(entry.properties)});
    throw query::TextSearchException("Text index error on drop: {}", e.what());
  }
}

bool TextEdgeIndex::IndexExists(const std::string &index_name) const { return index_.contains(index_name); }

std::vector<TextEdgeSearchResult> TextEdgeIndex::Search(const std::string &index_name, const std::string &search_query,
                                                        text_search_mode search_mode, std::size_t limit,
                                                        const Transaction &tx) {
  auto it = index_.find(index_name);
  if (it == index_.end()) {
    throw query::TextSearchException("Text index {} doesn't exist.", index_name);
  }
  auto &index_data = it->second;
  auto &context = index_data.context;

  mgcxx::text_search::EdgeGidScoreOutput search_results;
  try {
    if (!tx.text_search_session_) {
      tx.text_search_session_ = std::make_unique<TextSearchSession>();
    }
    auto &searcher = *tx.text_search_session_->GetOrAcquire(&index_data, context);

    const auto lowered_query = ToLowerCasePreservingBooleanOperators(search_query);
    switch (search_mode) {
      case text_search_mode::SPECIFIED_PROPERTIES:
        search_results = mgcxx::text_search::search_edge_gids_pinned(
            context, searcher, mgcxx::text_search::SearchInput{.search_query = lowered_query, .limit = limit});
        break;
      case text_search_mode::REGEX:
        search_results = mgcxx::text_search::regex_search_edge_gids_pinned(
            context,
            searcher,
            mgcxx::text_search::SearchInput{.search_fields = {"all"}, .search_query = lowered_query, .limit = limit});
        break;
      case text_search_mode::ALL_PROPERTIES:
        search_results = mgcxx::text_search::search_edge_gids_pinned(
            context,
            searcher,
            mgcxx::text_search::SearchInput{.search_fields = {"all"}, .search_query = lowered_query, .limit = limit});
        break;
      default:
        throw query::TextSearchException(
            "Unsupported search mode: please use one of text_search.search_edges, text_search.search_all_edges, or "
            "text_search.regex_search_edges.");
    }
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }

  return search_results.docs | rv::transform([](const auto &doc) -> TextEdgeSearchResult {
           return {.edge_gid = storage::Gid::FromUint(doc.edge_gid),
                   .from_vertex_gid = storage::Gid::FromUint(doc.from_gid),
                   .to_vertex_gid = storage::Gid::FromUint(doc.to_gid),
                   .score = doc.score};
         }) |
         r::to<std::vector>();
}

std::string TextEdgeIndex::Aggregate(const std::string &index_name, const std::string &search_query,
                                     const std::string &aggregation_query) {
  auto &context = std::invoke([&]() -> mgcxx::text_search::Context & {
    if (const auto it = index_.find(index_name); it != index_.end()) {
      return it->second.context;
    }
    throw query::TextSearchException("Text index {} doesn't exist.", index_name);
  });
  mgcxx::text_search::DocumentOutput aggregation_result;
  try {
    aggregation_result = mgcxx::text_search::aggregate(
        context,
        mgcxx::text_search::SearchInput{
            .search_fields = {"data"}, .search_query = search_query, .aggregation_query = aggregation_query});

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
    ret.emplace_back(index_name, index_data.scope, index_data.properties);
  }
  return ret;
}

void TextEdgeIndex::Clear() {
  // Collect all index specs before modifying the map -> we will need to recover them if we fail to drop any of them
  std::vector<TextEdgeIndexSpec> all_index_specs;
  all_index_specs.reserve(index_.size());
  for (const auto &[index_name, index_data] : index_) {
    all_index_specs.emplace_back(index_name, index_data.scope, index_data.properties);
  }

  std::vector<TextEdgeIndexSpec> successfully_dropped;
  successfully_dropped.reserve(all_index_specs.size());
  for (const auto &index_spec : all_index_specs) {
    try {
      DropIndex(index_spec.index_name);
      successfully_dropped.push_back(index_spec);
    } catch (const std::exception &e) {
      // Recover indices that were successfully dropped before this failure
      for (const auto &dropped_spec : successfully_dropped) {
        CreateTantivyIndex(MakeIndexPath(text_index_storage_dir_, dropped_spec.index_name), dropped_spec);
      }
      throw;
    }
  }
}

std::optional<uint64_t> TextEdgeIndex::ApproximateEdgesTextCount(std::string_view index_name) const {
  if (const auto it = index_.find(index_name); it != index_.end()) {
    const auto &index_data = it->second;
    return mgcxx::text_search::get_num_docs(index_data.context);
  }
  return std::nullopt;
}

void TextEdgeIndex::ApplyTrackedChanges(Transaction &tx, NameIdMapper *name_id_mapper) {
  for (const auto &[index_data_ptr, pending] : tx.text_edge_index_change_collector_) {
    struct PreparedEdgeDoc {
      std::int64_t edge_gid;
      std::int64_t from_vertex_gid;
      std::int64_t to_vertex_gid;
      nlohmann::json properties;
      std::string all_property_values;
    };

    std::vector<std::int64_t> gids_to_remove;
    gids_to_remove.reserve(pending.to_remove.size());
    for (const auto *edge : pending.to_remove) {
      gids_to_remove.push_back(edge->gid.AsInt());
    }

    std::vector<PreparedEdgeDoc> docs_to_add;
    docs_to_add.reserve(pending.to_add.size());
    for (const auto &edge_with_vertices : pending.to_add) {
      auto edge_properties = index_data_ptr->properties.empty()
                                 ? edge_with_vertices.edge->properties.Properties()
                                 : ExtractProperties(edge_with_vertices.edge->properties, index_data_ptr->properties);
      docs_to_add.push_back({edge_with_vertices.edge->gid.AsInt(),
                             edge_with_vertices.from_vertex->gid.AsInt(),
                             edge_with_vertices.to_vertex->gid.AsInt(),
                             SerializeProperties(edge_properties, name_id_mapper),
                             StringifyProperties(edge_properties)});
    }

    const std::lock_guard lock(index_data_ptr->write_mutex);
    try {
      for (auto gid : gids_to_remove) {
        mgcxx::text_search::delete_document(
            index_data_ptr->context,
            mgcxx::text_search::SearchInput{.search_query = fmt::format("edge_gid:{}", gid)},
            kDoSkipCommit);
      }
      for (auto &doc : docs_to_add) {
        TextEdgeIndex::AddEdgeToTextIndex(doc.edge_gid,
                                          doc.from_vertex_gid,
                                          doc.to_vertex_gid,
                                          std::move(doc.properties),
                                          std::move(doc.all_property_values),
                                          index_data_ptr->context);
      }
      mgcxx::text_search::commit(index_data_ptr->context);
    } catch (const std::exception &e) {
      throw query::TextSearchException("Text search error: {}", e.what());
    }
  }
}

}  // namespace memgraph::storage
