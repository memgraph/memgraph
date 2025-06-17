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

#include <algorithm>
#include <cstdint>
#include <ranges>
#include <shared_mutex>
#include <stop_token>
#include <string_view>
#include <variant>

#include "flags/bolt.hpp"
#include "query/exceptions.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index.hpp"

#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index_dense.hpp"
#include "utils/algorithm.hpp"
#include "utils/counter.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

// unum::usearch::index_dense_gt is the index type used for vector indices. It is thread-safe and supports concurrent
// operations.
using mg_vector_index_t = unum::usearch::index_dense_gt<Vertex *, unum::usearch::uint40_t>;
using mg_edge_type_vector_index_t = unum::usearch::index_dense_gt<Edge *, unum::usearch::uint40_t>;

/// Map from usearch metric kind to similarity function
/// TODO(@DavIvek): Check if this functions are correct -> l2sq and cosine are correct and they are most critical ATM
static const std::unordered_map<unum::usearch::metric_kind_t, std::function<double(double)>> similarity_map = {
    {unum::usearch::metric_kind_t::ip_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::cos_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::l2sq_k, [](double distance) { return 1.0 / (1.0 + distance); }},
    {unum::usearch::metric_kind_t::pearson_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::haversine_k, [](double distance) { return 1.0 / (1.0 + distance); }},
    {unum::usearch::metric_kind_t::divergence_k, [](double distance) { return 1.0 / (1.0 + distance); }},
    {unum::usearch::metric_kind_t::hamming_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::tanimoto_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::sorensen_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::jaccard_k, [](double distance) { return 1.0 - distance; }}};

const char *VectorIndex::NameFromMetric(unum::usearch::metric_kind_t metric) {
  switch (metric) {
    case unum::usearch::metric_kind_t::l2sq_k:
      return "l2sq";
    case unum::usearch::metric_kind_t::ip_k:
      return "ip";
    case unum::usearch::metric_kind_t::cos_k:
      return "cos";
    case unum::usearch::metric_kind_t::haversine_k:
      return "haversine";
    case unum::usearch::metric_kind_t::divergence_k:
      return "divergence";
    case unum::usearch::metric_kind_t::pearson_k:
      return "pearson";
    case unum::usearch::metric_kind_t::hamming_k:
      return "hamming";
    case unum::usearch::metric_kind_t::tanimoto_k:
      return "tanimoto";
    case unum::usearch::metric_kind_t::sorensen_k:
      return "sorensen";
    default:
      throw query::VectorSearchException(
          "Unsupported metric kind. Supported metrics are l2sq, ip, cos, haversine, divergence, pearson, hamming, "
          "tanimoto, and sorensen.");
  }
}

unum::usearch::metric_kind_t VectorIndex::MetricFromName(std::string_view name) {
  if (name == "l2sq" || name == "euclidean_sq") {
    return unum::usearch::metric_kind_t::l2sq_k;
  }
  if (name == "ip" || name == "inner" || name == "dot") {
    return unum::usearch::metric_kind_t::ip_k;
  }
  if (name == "cos" || name == "angular") {
    return unum::usearch::metric_kind_t::cos_k;
  }
  if (name == "haversine") {
    return unum::usearch::metric_kind_t::haversine_k;
  }
  if (name == "divergence") {
    return unum::usearch::metric_kind_t::divergence_k;
  }
  if (name == "pearson") {
    return unum::usearch::metric_kind_t::pearson_k;
  }
  if (name == "hamming") {
    return unum::usearch::metric_kind_t::hamming_k;
  }
  if (name == "tanimoto") {
    return unum::usearch::metric_kind_t::tanimoto_k;
  }
  if (name == "sorensen") {
    return unum::usearch::metric_kind_t::sorensen_k;
  }
  throw query::VectorSearchException(
      fmt::format("Unsupported metric name: {}. Supported metrics are l2sq, ip, cos, haversine, divergence, pearson, "
                  "hamming, tanimoto, and sorensen.",
                  name));
}

const char *VectorIndex::NameFromScalar(unum::usearch::scalar_kind_t scalar) {
  switch (scalar) {
    case unum::usearch::scalar_kind_t::b1x8_k:
      return "b1x8";
    case unum::usearch::scalar_kind_t::u40_k:
      return "u40";
    case unum::usearch::scalar_kind_t::uuid_k:
      return "uuid";
    case unum::usearch::scalar_kind_t::bf16_k:
      return "bf16";
    case unum::usearch::scalar_kind_t::f64_k:
      return "f64";
    case unum::usearch::scalar_kind_t::f32_k:
      return "f32";
    case unum::usearch::scalar_kind_t::f16_k:
      return "f16";
    case unum::usearch::scalar_kind_t::f8_k:
      return "f8";
    case unum::usearch::scalar_kind_t::u64_k:
      return "u64";
    case unum::usearch::scalar_kind_t::u32_k:
      return "u32";
    case unum::usearch::scalar_kind_t::u16_k:
      return "u16";
    case unum::usearch::scalar_kind_t::u8_k:
      return "u8";
    case unum::usearch::scalar_kind_t::i64_k:
      return "i64";
    case unum::usearch::scalar_kind_t::i32_k:
      return "i32";
    case unum::usearch::scalar_kind_t::i16_k:
      return "i16";
    case unum::usearch::scalar_kind_t::i8_k:
      return "i8";
    default:
      throw query::VectorSearchException(
          "Unsupported scalar kind. Supported scalars are b1x8, u40, uuid, bf16, f64, f32, f16, f8, "
          "u64, u32, u16, u8, i64, i32, i16, and i8.");
  }
}

unum::usearch::scalar_kind_t VectorIndex::ScalarFromName(std::string_view name) {
  if (name == "b1x8" || name == "binary") {
    return unum::usearch::scalar_kind_t::b1x8_k;
  }
  if (name == "u40") {
    return unum::usearch::scalar_kind_t::u40_k;
  }
  if (name == "uuid") {
    return unum::usearch::scalar_kind_t::uuid_k;
  }
  if (name == "bf16" || name == "bfloat16") {
    return unum::usearch::scalar_kind_t::bf16_k;
  }
  if (name == "f64" || name == "float64" || name == "double") {
    return unum::usearch::scalar_kind_t::f64_k;
  }
  if (name == "f32" || name == "float32" || name == "float") {
    return unum::usearch::scalar_kind_t::f32_k;
  }
  if (name == "f16" || name == "float16") {
    return unum::usearch::scalar_kind_t::f16_k;
  }
  if (name == "f8" || name == "float8") {
    return unum::usearch::scalar_kind_t::f8_k;
  }
  if (name == "u64" || name == "uint64") {
    return unum::usearch::scalar_kind_t::u64_k;
  }
  if (name == "u32" || name == "uint32") {
    return unum::usearch::scalar_kind_t::u32_k;
  }
  if (name == "u16" || name == "uint16") {
    return unum::usearch::scalar_kind_t::u16_k;
  }
  if (name == "u8" || name == "uint8") {
    return unum::usearch::scalar_kind_t::u8_k;
  }
  if (name == "i64" || name == "int64") {
    return unum::usearch::scalar_kind_t::i64_k;
  }
  if (name == "i32" || name == "int32") {
    return unum::usearch::scalar_kind_t::i32_k;
  }
  if (name == "i16" || name == "int16") {
    return unum::usearch::scalar_kind_t::i16_k;
  }
  if (name == "i8" || name == "int8") {
    return unum::usearch::scalar_kind_t::i8_k;
  }

  throw query::VectorSearchException(
      fmt::format("Unsupported scalar name: {}. Supported scalars are b1x8, u40, uuid, bf16, f64, f32, f16, f8, "
                  "u64, u32, u16, u8, i64, i32, i16, and i8.",
                  name));
}

// NOLINTNEXTLINE(bugprone-exception-escape)
struct NodeIndexItem {
  // unum::usearch::index_dense_gt is thread-safe and supports concurrent operations. However, we still need to use
  // locking because resizing the index requires exclusive access. For all other operations, we can use shared lock even
  // though we are modifying index. In the case of removing or adding elements to the index we will use
  // MutableSharedLock to acquire an shared lock.
  std::shared_ptr<utils::Synchronized<mg_vector_index_t, std::shared_mutex>> mg_index;
  VectorIndexSpec spec;
};

struct EdgeTypeIndexItem {
  // Similar to NodeIndexItem, but for edge type indices.
  std::shared_ptr<utils::Synchronized<mg_edge_type_vector_index_t, std::shared_mutex>> mg_index;
  VectorIndexSpec spec;
};

/// @brief Implements the underlying functionality of the `VectorIndex` class.
///
/// The `Impl` structure follows the PIMPL (Pointer to Implementation) idiom to separate
/// the interface of `VectorIndex` from its implementation
struct VectorIndex::Impl {
  /// The `index_` member is a map that associates a `LabelPropKey` (a combination of label and property)
  /// with the pair of a IndexItem.
  // std::map<std::variant<LabelPropKey, EdgeTypePropKey>, IndexItem, std::less<>> index_;
  std::map<LabelPropKey, NodeIndexItem, std::less<>> node_index_;
  std::map<EdgeTypePropKey, EdgeTypeIndexItem, std::less<>> edge_index_;

  /// The `index_name_to_index_impl_` is a map that maps an index name (as a string) to the corresponding
  /// `LabelPropKey`. This allows the system to quickly resolve an index name to the spec
  /// associated with that index, enabling easy lookup and management of indexes by name.
  std::map<std::string, std::variant<LabelPropKey, EdgeTypePropKey>, std::less<>> index_name_to_index_impl_;
};

VectorIndex::VectorIndex() : pimpl(std::make_unique<Impl>()) {}
VectorIndex::~VectorIndex() {}

bool VectorIndex::CreateIndex(const VectorIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                              std::optional<SnapshotObserverInfo> const &snapshot_info) {
  try {
    if (pimpl->index_name_to_index_impl_.contains(spec.index_name)) {
      throw query::VectorSearchException("Vector index with the given name already exists.");
    }
    const unum::usearch::metric_punned_t metric(spec.dimension, spec.metric_kind, spec.scalar_kind);
    const unum::usearch::index_limits_t limits(spec.capacity, FLAGS_bolt_num_workers);

    if (std::holds_alternative<LabelId>(spec.label_or_edge_type)) {
      const auto label_id = std::get<LabelId>(spec.label_or_edge_type);
      const LabelPropKey key{label_id, spec.property};

      if (pimpl->node_index_.contains(key)) {
        throw query::VectorSearchException("Vector index with the given label and property already exists.");
      }

      auto mg_node_index = mg_vector_index_t::make(metric);
      if (!mg_node_index) {
        throw query::VectorSearchException(fmt::format("Failed to create vector index {}, error message: {}",
                                                       spec.index_name, mg_node_index.error.what()));
      }

      if (!mg_node_index.index.try_reserve(limits)) {
        throw query::VectorSearchException(
            fmt::format("Failed to reserve memory for vector index {}", spec.index_name));
      }

      spdlog::info("Created vector index {}", spec.index_name);
      pimpl->index_name_to_index_impl_.emplace(spec.index_name, key);
      pimpl->node_index_.emplace(
          key, NodeIndexItem{std::make_shared<utils::Synchronized<mg_vector_index_t, std::shared_mutex>>(
                                 std::move(mg_node_index)),
                             spec});

      for (auto &vertex : vertices) {
        if (!utils::Contains(vertex.labels, label_id)) continue;
        if (UpdateVectorIndex(VectorIndexOnLabelEntry{key, &vertex}) && snapshot_info) {
          snapshot_info->Update(UpdateType::VECTOR_IDX);
        }
      }

    } else {
      const auto edge_type = std::get<EdgeTypeId>(spec.label_or_edge_type);
      const EdgeTypePropKey key{edge_type, spec.property};

      if (pimpl->edge_index_.contains(key)) {
        throw query::VectorSearchException("Vector index with the given edge type and property already exists.");
      }

      auto mg_edge_index = mg_edge_type_vector_index_t::make(metric);
      if (!mg_edge_index) {
        throw query::VectorSearchException(fmt::format("Failed to create vector index {}, error message: {}",
                                                       spec.index_name, mg_edge_index.error.what()));
      }

      if (!mg_edge_index.index.try_reserve(limits)) {
        throw query::VectorSearchException(
            fmt::format("Failed to reserve memory for vector index {}", spec.index_name));
      }

      spdlog::info("Created vector index {}", spec.index_name);
      pimpl->index_name_to_index_impl_.emplace(spec.index_name, key);
      pimpl->edge_index_.emplace(
          key, EdgeTypeIndexItem{std::make_shared<utils::Synchronized<mg_edge_type_vector_index_t, std::shared_mutex>>(
                                     std::move(mg_edge_index)),
                                 spec});

      for (auto &vertex : vertices) {
        for (auto &edge_variant : vertex.out_edges) {
          if (std::get<EdgeTypeId>(edge_variant) != edge_type) continue;
          auto *edge = std::get<EdgeRef>(edge_variant).ptr;
          if (UpdateVectorIndex(VectorIndexOnEdgeTypeEntry{key, edge}) && snapshot_info) {
            snapshot_info->Update(UpdateType::VECTOR_IDX);
          }
        }
      }
    }
  } catch (const std::exception &e) {
    spdlog::error("Failed to create vector index {}: {}", spec.index_name, e.what());
    return false;
  }
  return true;
}

bool VectorIndex::DropIndex(std::string_view index_name) {
  auto it = pimpl->index_name_to_index_impl_.find(index_name.data());
  if (it == pimpl->index_name_to_index_impl_.end()) {
    return false;
  }

  auto erase_index = [&](auto &index_map, const auto &key_variant) -> bool {
    auto index_it = index_map.find(key_variant);
    if (index_it == index_map.end()) {
      return false;
    }
    index_map.erase(index_it);
    pimpl->index_name_to_index_impl_.erase(it);
    spdlog::info("Dropped vector index {}", index_name);
    return true;
  };

  if (std::holds_alternative<LabelPropKey>(it->second)) {
    return erase_index(pimpl->node_index_, std::get<LabelPropKey>(it->second));
  } else {
    return erase_index(pimpl->edge_index_, std::get<EdgeTypePropKey>(it->second));
  }
}

void VectorIndex::Clear() {
  pimpl->index_name_to_index_impl_.clear();
  pimpl->node_index_.clear();
  pimpl->edge_index_.clear();
}

bool VectorIndex::UpdateVectorIndex(std::variant<VectorIndexOnLabelEntry, VectorIndexOnEdgeTypeEntry> entry,
                                    const PropertyValue *value) {
  // lambda for updating vector index
  auto update_vector_index = [&](auto *entry, const auto &spec, auto &mg_index) -> bool {
    bool is_index_full = false;
    // try to remove entry (if it exists) and then add a new one + check if index is full
    {
      auto locked_index = mg_index->MutableSharedLock();
      locked_index->remove(entry);
      is_index_full = locked_index->size() == locked_index->capacity();
    }
    const auto &property = (value != nullptr ? *value : entry->properties.GetProperty(spec.property));
    if (property.IsNull()) {
      // if property is null, that means that the vertex should not be in the index and
      // we shouldn't do any other updates
      return false;
    }
    if (!property.IsList()) {
      throw query::VectorSearchException("Vector index property must be a list.");
    }
    const auto &vector_property = property.ValueList();
    if (spec.dimension != vector_property.size()) {
      throw query::VectorSearchException("Vector index property must have the same number of dimensions as the index.");
    }
    if (is_index_full) {
      spdlog::warn("Vector index is full, resizing...");

      // we need unique lock when we are resizing the index
      auto exclusively_locked_index = mg_index->Lock();
      const auto new_size = spec.resize_coefficient * exclusively_locked_index->capacity();
      const unum::usearch::index_limits_t new_limits(new_size, FLAGS_bolt_num_workers);
      if (!exclusively_locked_index->try_reserve(new_limits)) {
        throw query::VectorSearchException("Failed to resize vector index.");
      }
    }
    std::vector<float> vector;
    vector.reserve(vector_property.size());
    std::transform(vector_property.begin(), vector_property.end(), std::back_inserter(vector), [](const auto &value) {
      if (value.IsDouble()) {
        return static_cast<float>(value.ValueDouble());
      }
      if (value.IsInt()) {
        return static_cast<float>(value.ValueInt());
      }
      throw query::VectorSearchException("Vector index property must be a list of floats or integers.");
    });
    {
      auto locked_index = mg_index->MutableSharedLock();
      locked_index->add(entry, vector.data(), mg_vector_index_t::any_thread(), false);
    }
    return true;
  };

  if (std::holds_alternative<VectorIndexOnLabelEntry>(entry)) {
    auto &[mg_index, spec] = pimpl->node_index_.at(std::get<VectorIndexOnLabelEntry>(entry).label_prop_key);
    return update_vector_index(std::get<VectorIndexOnLabelEntry>(entry).vertex, spec, mg_index);
  } else {
    auto &[mg_index, spec] = pimpl->edge_index_.at(std::get<VectorIndexOnEdgeTypeEntry>(entry).edge_type_prop_key);
    return update_vector_index(std::get<VectorIndexOnEdgeTypeEntry>(entry).edge, spec, mg_index);
  }
}

void VectorIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update) {
  std::ranges::for_each(pimpl->node_index_ | std::views::keys, [&](const auto &label_prop) {
    if (label_prop.label() == added_label) {
      UpdateVectorIndex(VectorIndexOnLabelEntry{label_prop, vertex_after_update});
    }
  });
}

void VectorIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update) {
  std::ranges::for_each(pimpl->node_index_ | std::views::keys, [&](const auto &label_prop) {
    if (label_prop.label() == removed_label) {
      auto &[mg_index, _] = pimpl->node_index_.at(label_prop);
      auto locked_index = mg_index->MutableSharedLock();
      locked_index->remove(vertex_before_update);
    }
  });
}

void VectorIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex) {
  auto has_property = [&](const auto &label_prop) { return label_prop.property() == property; };
  auto has_label = [&](const auto &label_prop) { return utils::Contains(vertex->labels, label_prop.label()); };

  auto view = pimpl->node_index_ | std::views::keys | std::views::filter(has_property) | std::views::filter(has_label);
  for (const auto &label_prop : view) {
    UpdateVectorIndex(VectorIndexOnLabelEntry{label_prop, vertex}, &value);
  }
}

std::vector<VectorIndexInfo> VectorIndex::ListVectorIndicesInfo() const {
  std::vector<VectorIndexInfo> result;
  result.reserve(pimpl->node_index_.size() + pimpl->edge_index_.size());
  for (const auto &[_, index_item] : pimpl->node_index_) {
    const auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index->ReadLock();
    result.emplace_back(VectorIndexInfo{
        spec.index_name, spec.label_or_edge_type, spec.property, NameFromMetric(locked_index->metric().metric_kind()),
        static_cast<std::uint16_t>(locked_index->dimensions()), locked_index->capacity(), locked_index->size(),
        NameFromScalar(locked_index->metric().scalar_kind())});
  }
  for (const auto &[_, index_item] : pimpl->edge_index_) {
    const auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index->ReadLock();
    result.emplace_back(VectorIndexInfo{
        spec.index_name, spec.label_or_edge_type, spec.property, NameFromMetric(locked_index->metric().metric_kind()),
        static_cast<std::uint16_t>(locked_index->dimensions()), locked_index->capacity(), locked_index->size(),
        NameFromScalar(locked_index->metric().scalar_kind())});
  }
  return result;
}

std::vector<VectorIndexSpec> VectorIndex::ListIndices() const {
  std::vector<VectorIndexSpec> result;
  result.reserve(pimpl->node_index_.size() + pimpl->edge_index_.size());
  std::ranges::transform(pimpl->node_index_, std::back_inserter(result),
                         [](const auto &label_prop_index_item) { return label_prop_index_item.second.spec; });
  std::ranges::transform(pimpl->edge_index_, std::back_inserter(result),
                         [](const auto &edge_type_index_item) { return edge_type_index_item.second.spec; });
  return result;
}

std::optional<uint64_t> VectorIndex::ApproximateVectorCount(LabelId label, PropertyId property) const {
  auto it = pimpl->node_index_.find(LabelPropKey{label, property});
  if (it == pimpl->node_index_.end()) {
    return std::nullopt;
  }
  auto &[mg_index, _] = it->second;
  auto locked_index = mg_index->ReadLock();
  return locked_index->size();
}

VectorSearchNodeResults VectorIndex::SearchNodes(std::string_view index_name, uint64_t result_set_size,
                                                 const std::vector<float> &query_vector) const {
  const auto label_prop = std::get<LabelPropKey>(pimpl->index_name_to_index_impl_.at(index_name.data()));
  const auto &mg_index = pimpl->node_index_.at(label_prop).mg_index;

  VectorSearchNodeResults result;
  result.reserve(result_set_size);

  auto locked_index = mg_index->ReadLock();
  const auto result_keys = locked_index->filtered_search(query_vector.data(), result_set_size, [](Vertex *vertex) {
    auto guard = std::shared_lock{vertex->lock};
    return !vertex->deleted;
  });

  auto metric_kind = locked_index->metric().metric_kind();
  auto similarity_fn = similarity_map.at(metric_kind);

  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    auto *vertex = static_cast<Vertex *>(result_keys[i].member.key);
    result.emplace_back(vertex, result_keys[i].distance, similarity_fn(result_keys[i].distance));
  }
  return result;
}

VectorSearchEdgeResults VectorIndex::SearchEdges(std::string_view index_name, uint64_t result_set_size,
                                                 const std::vector<float> &query_vector) const {
  const auto edge_type_prop = std::get<EdgeTypePropKey>(pimpl->index_name_to_index_impl_.at(index_name.data()));
  const auto &mg_index = pimpl->edge_index_.at(edge_type_prop).mg_index;

  VectorSearchEdgeResults result;
  result.reserve(result_set_size);

  auto locked_index = mg_index->ReadLock();
  const auto result_keys = locked_index->filtered_search(query_vector.data(), result_set_size, [](Edge *edge) {
    auto guard = std::shared_lock{edge->lock};
    return !edge->deleted;
  });

  auto metric_kind = locked_index->metric().metric_kind();
  auto similarity_fn = similarity_map.at(metric_kind);

  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    auto *edge = static_cast<Edge *>(result_keys[i].member.key);
    result.emplace_back(edge, result_keys[i].distance, similarity_fn(result_keys[i].distance));
  }
  return result;
}

void VectorIndex::AbortEntries(const LabelPropKey &label_prop, std::span<Vertex *const> vertices) {
  auto &[mg_index, spec] = pimpl->node_index_.at(label_prop);
  auto locked_index = mg_index->MutableSharedLock();
  for (const auto &vertex : vertices) {
    locked_index->remove(vertex);
  }
}

void VectorIndex::RestoreEntries(const LabelPropKey &label_prop,
                                 std::span<std::pair<PropertyValue, Vertex *> const> prop_vertices) {
  for (const auto &property_value_vertex : prop_vertices) {
    UpdateVectorIndex(VectorIndexOnLabelEntry{label_prop, property_value_vertex.second}, &property_value_vertex.first);
  }
}

void VectorIndex::RemoveObsoleteEntries(std::stop_token token) const {
  auto maybe_stop = utils::ResettableCounter(2048);
  for (auto &[_, index_item] : pimpl->node_index_) {
    if (maybe_stop() && token.stop_requested()) {
      return;
    }
    auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index->MutableSharedLock();
    std::vector<Vertex *> vertices_to_remove(locked_index->size());
    locked_index->export_keys(vertices_to_remove.data(), 0, locked_index->size());

    auto deleted = vertices_to_remove | std::views::filter([](const Vertex *vertex) {
                     auto guard = std::shared_lock{vertex->lock};
                     return vertex->deleted;
                   });
    for (const auto &vertex : deleted) {
      locked_index->remove(vertex);
    }
  }
  for (auto &[_, index_item] : pimpl->edge_index_) {
    if (maybe_stop() && token.stop_requested()) {
      return;
    }
    auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index->MutableSharedLock();
    std::vector<Edge *> edges_to_remove(locked_index->size());
    locked_index->export_keys(edges_to_remove.data(), 0, locked_index->size());

    auto deleted = edges_to_remove | std::views::filter([](const Edge *edge) {
                     auto guard = std::shared_lock{edge->lock};
                     return edge->deleted;
                   });
    for (const auto &edge : deleted) {
      locked_index->remove(edge);
    }
  }
}

VectorIndex::IndexStats VectorIndex::Analysis() const {
  // IndexStats res{};
  // for (const auto &[label_prop, _] : pimpl->index_) {
  //   const auto label = label_prop.label();
  //   const auto property = label_prop.property();
  //   res.l2p[label].emplace_back(property);
  //   res.p2l[property].emplace_back(label);
  // }
  // return res;
  return IndexStats{};  // TODO: Implement the index analysis logic
}

void VectorIndex::TryInsertVertex(Vertex *vertex) {
  auto guard = std::shared_lock{vertex->lock};
  auto has_property = [&](const auto &label_prop) { return vertex->properties.HasProperty(label_prop.property()); };
  auto has_label = [&](const auto &label_prop) { return utils::Contains(vertex->labels, label_prop.label()); };
  for (const auto &[label_prop, _] : pimpl->node_index_) {
    if (has_property(label_prop) && has_label(label_prop)) {
      UpdateVectorIndex(VectorIndexOnLabelEntry{label_prop, vertex});
    }
  }
}

}  // namespace memgraph::storage
