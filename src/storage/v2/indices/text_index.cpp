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

#include "storage/v2/indices/text_index.hpp"
#include <range/v3/all.hpp>
#include "mgcxx_text_search.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

void TextIndex::CreateTantivyIndex(const std::string &index_path, const TextIndexSpec &index_info) {
  try {
    nlohmann::json mappings = {};
    mappings["properties"] = {};
    mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["all"] = {{"type", "text"}, {"fast", true}, {"stored", true}, {"text", true}};
    mappings["properties"]["gid"] = {{"type", "u64"}, {"fast", true}, {"stored", true}, {"indexed", true}};

    auto [_, success] = index_.try_emplace(
        index_info.index_name,
        // If index already exists, it will be loaded and reused.
        mgcxx::text_search::create_index(index_path, mgcxx::text_search::IndexConfig{.mappings = mappings.dump()}),
        index_info.label,
        index_info.properties);
    if (!success) {
      throw query::TextSearchException("Text index {} already exists at path: {}.", index_info.index_name, index_path);
    }
  } catch (const std::exception &e) {
    spdlog::error("Failed to create text index {} at path: {}. Error: {}", index_info.index_name, index_path, e.what());
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

std::vector<TextIndexData *> TextIndex::LabelApplicableTextIndices(std::span<storage::LabelId const> labels) {
  std::vector<TextIndexData *> applicable_text_indices;
  for (auto &[_, index_data] : index_) {
    if (r::any_of(labels, [&](auto label) { return label == index_data.scope; })) {
      applicable_text_indices.push_back(&index_data);
    }
  }
  return applicable_text_indices;
}

std::vector<TextIndexData *> TextIndex::GetIndicesMatchingProperties(std::span<TextIndexData *const> label_indices,
                                                                     std::span<const PropertyId> properties) {
  std::vector<TextIndexData *> result;
  for (const auto &text_index_data : label_indices) {
    if (IndexPropertiesMatch(text_index_data->properties, properties)) {
      result.push_back(text_index_data);
    }
  }
  return result;
}

void TextIndex::AddNodeToTextIndex(std::int64_t gid, nlohmann::json properties, std::string all_property_values,
                                   mgcxx::text_search::Context &context) {
  if (all_property_values.empty()) return;
  nlohmann::json document = {};
  document["data"] = std::move(properties);
  document["all"] = std::move(all_property_values);
  document["gid"] = static_cast<std::uint64_t>(gid);

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
  if (index_.empty()) return;
  auto label_applicable_text_indices = LabelApplicableTextIndices(std::array{label});
  if (label_applicable_text_indices.empty()) return;
  const auto vertex_properties = vertex->properties.ExtractPropertyIds();
  auto applicable_text_indices = GetIndicesMatchingProperties(label_applicable_text_indices, vertex_properties);
  TrackTextIndexChange(tx.text_index_change_collector_, applicable_text_indices, vertex, TextIndexOp::ADD);
}

void TextIndex::UpdateOnRemoveLabel(LabelId label, const Vertex *vertex, Transaction &tx) {
  if (index_.empty()) return;
  auto label_applicable_text_indices = LabelApplicableTextIndices(std::array{label});
  if (label_applicable_text_indices.empty()) return;
  const auto vertex_properties = vertex->properties.ExtractPropertyIds();
  auto applicable_text_indices = GetIndicesMatchingProperties(label_applicable_text_indices, vertex_properties);
  TrackTextIndexChange(tx.text_index_change_collector_, applicable_text_indices, vertex, TextIndexOp::REMOVE);
}

void TextIndex::UpdateOnSetProperty(const Vertex *vertex, Transaction &tx, PropertyId property) {
  if (index_.empty() || vertex->labels.empty()) return;
  auto has_label = [&](const auto &text_index_data) {
    return r::any_of(vertex->labels, [&](auto label) { return label == text_index_data.scope; });
  };
  std::vector<TextIndexData *> applicable_text_indices;
  for (auto &[_, index_data] : index_) {
    if (IndexPropertiesMatch(index_data.properties, std::array{property}) && has_label(index_data)) {
      applicable_text_indices.push_back(&index_data);
    }
  };
  TrackTextIndexChange(tx.text_index_change_collector_, applicable_text_indices, vertex, TextIndexOp::UPDATE);
}

void TextIndex::RemoveNode(const Vertex *vertex, Transaction &tx) {
  if (index_.empty()) return;
  auto label_applicable_text_indices = LabelApplicableTextIndices(vertex->labels);
  if (label_applicable_text_indices.empty()) return;
  const auto vertex_properties = vertex->properties.ExtractPropertyIds();
  auto applicable_text_indices = GetIndicesMatchingProperties(label_applicable_text_indices, vertex_properties);
  TrackTextIndexChange(tx.text_index_change_collector_, applicable_text_indices, vertex, TextIndexOp::REMOVE);
}

void TextIndex::CreateIndex(const TextIndexSpec &index_info, storage::VerticesIterable vertices,
                            NameIdMapper *name_id_mapper) {
  CreateTantivyIndex(
      MakeIndexPath(text_index_storage_dir_, index_info.index_name),
      {.index_name = index_info.index_name, .label = index_info.label, .properties = index_info.properties});

  auto &index_data = index_.at(index_info.index_name);
  for (const auto &v : vertices) {
    if (!v.HasLabel(index_info.label, View::NEW).value()) {
      continue;
    }
    // If properties are specified, we serialize only those properties; otherwise, all properties of the vertex.
    auto vertex_properties = index_info.properties.empty()
                                 ? v.Properties(View::NEW).value()
                                 : v.PropertiesByPropertyIds(index_info.properties, View::NEW).value();
    TextIndex::AddNodeToTextIndex(v.Gid().AsInt(),
                                  SerializeProperties(vertex_properties, name_id_mapper),
                                  StringifyProperties(vertex_properties),
                                  index_data.context);
  }
  try {
    mgcxx::text_search::commit(index_data.context);
  } catch (const std::exception &e) {
    throw query::TextSearchException("Text index commit error: {}", e.what());
  }
}

void TextIndex::RecoverIndex(const TextIndexSpec &index_info, utils::SkipList<Vertex>::Accessor vertices,
                             NameIdMapper *name_id_mapper, std::optional<SnapshotObserverInfo> const &snapshot_info) {
  const auto index_path = MakeIndexPath(text_index_storage_dir_, index_info.index_name);
  auto needs_rebuild = !std::filesystem::exists(index_path);
  try {
    CreateTantivyIndex(index_path, index_info);
  } catch (const query::TextSearchException &) {
    if (needs_rebuild) throw;
    // It's possible that index on disk has incompatible schema if, for example, new required properties were added to
    // the index spec in new versions
    spdlog::warn("Text index {} has incompatible schema on disk, rebuilding.", index_info.index_name);
    std::error_code ec;
    std::filesystem::remove_all(index_path, ec);
    if (ec)
      throw query::TextSearchException("Failed to remove stale text index {}: {}", index_info.index_name, ec.message());
    needs_rebuild = true;
    CreateTantivyIndex(index_path, index_info);
  }

  if (needs_rebuild) {
    auto &context = index_.at(index_info.index_name).context;
    for (const auto &vertex : vertices) {
      if (!std::ranges::contains(vertex.labels, index_info.label)) continue;

      auto properties_to_index = FilterPropertiesToIndex(index_info.properties, vertex.properties.ExtractPropertyIds());
      if (properties_to_index.empty()) continue;

      auto properties_to_index_map = ExtractProperties(vertex.properties, properties_to_index);
      TextIndex::AddNodeToTextIndex(vertex.gid.AsInt(),
                                    SerializeProperties(properties_to_index_map, name_id_mapper),
                                    StringifyProperties(properties_to_index_map),
                                    context);
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
                                                text_search_mode search_mode, std::size_t limit,
                                                const Transaction &tx) {
  auto it = index_.find(index_name);
  if (it == index_.end()) {
    throw query::TextSearchException("Text index {} doesn't exist.", index_name);
  }
  auto &index_data = it->second;
  auto &context = index_data.context;

  mgcxx::text_search::GidScoreOutput search_results;
  try {
    if (!tx.text_search_session_) {
      tx.text_search_session_ = std::make_unique<TextSearchSession>();
    }
    auto &searcher = *tx.text_search_session_->GetOrAcquire(&index_data, context);

    const auto lowered_query = ToLowerCasePreservingBooleanOperators(search_query);
    switch (search_mode) {
      case text_search_mode::SPECIFIED_PROPERTIES:
        search_results = mgcxx::text_search::search_gids_pinned(
            context, searcher, mgcxx::text_search::SearchInput{.search_query = lowered_query, .limit = limit});
        break;
      case text_search_mode::REGEX:
        search_results = mgcxx::text_search::regex_search_gids_pinned(
            context,
            searcher,
            mgcxx::text_search::SearchInput{.search_fields = {"all"}, .search_query = lowered_query, .limit = limit});
        break;
      case text_search_mode::ALL_PROPERTIES:
        search_results = mgcxx::text_search::search_gids_pinned(
            context,
            searcher,
            mgcxx::text_search::SearchInput{.search_fields = {"all"}, .search_query = lowered_query, .limit = limit});
        break;
      default:
        throw query::TextSearchException(
            "Unsupported search mode: please use one of text_search.search, text_search.search_all, or "
            "text_search.regex_search.");
    }
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }

  return search_results.docs | rv::transform([](const auto &doc) -> TextSearchResult {
           return {.vertex_gid = storage::Gid::FromUint(doc.gid), .score = doc.score};
         }) |
         r::to<std::vector>();
}

std::string TextIndex::Aggregate(const std::string &index_name, const std::string &search_query,
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
  // Collect all index specs before modifying the map -> we will need to recover them if we fail to drop any of them
  std::vector<TextIndexSpec> all_index_specs;
  all_index_specs.reserve(index_.size());
  for (const auto &[index_name, index_data] : index_) {
    all_index_specs.emplace_back(index_name, index_data.scope, index_data.properties);
  }

  std::vector<TextIndexSpec> successfully_dropped;
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

void TextIndex::ApplyTrackedChanges(Transaction &tx, NameIdMapper *name_id_mapper) {
  for (const auto &[index_data_ptr, pending] : tx.text_index_change_collector_) {
    // Prepare documents outside the lock to minimize critical section
    struct PreparedDoc {
      std::int64_t gid;
      nlohmann::json properties;
      std::string all_property_values;
    };

    std::vector<std::int64_t> gids_to_remove;
    gids_to_remove.reserve(pending.to_remove.size());
    for (const auto *vertex : pending.to_remove) {
      gids_to_remove.push_back(vertex->gid.AsInt());
    }

    std::vector<PreparedDoc> docs_to_add;
    docs_to_add.reserve(pending.to_add.size());
    for (const auto *vertex : pending.to_add) {
      auto vertex_properties = index_data_ptr->properties.empty()
                                   ? vertex->properties.Properties()
                                   : ExtractProperties(vertex->properties, index_data_ptr->properties);
      docs_to_add.push_back({vertex->gid.AsInt(),
                             SerializeProperties(vertex_properties, name_id_mapper),
                             StringifyProperties(vertex_properties)});
    }

    // Lock only for tantivy writer operations
    const std::lock_guard lock(index_data_ptr->write_mutex);
    try {
      for (auto gid : gids_to_remove) {
        mgcxx::text_search::delete_document(index_data_ptr->context,
                                            mgcxx::text_search::SearchInput{.search_query = fmt::format("gid:{}", gid)},
                                            kDoSkipCommit);
      }
      for (auto &doc : docs_to_add) {
        TextIndex::AddNodeToTextIndex(
            doc.gid, std::move(doc.properties), std::move(doc.all_property_values), index_data_ptr->context);
      }
      mgcxx::text_search::commit(index_data_ptr->context);
    } catch (const std::exception &e) {
      throw query::TextSearchException("Text search error: {}", e.what());
    }
  }
}

}  // namespace memgraph::storage
