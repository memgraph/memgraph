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

#include "flags/bolt.hpp"
#include "query/exceptions.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index.hpp"

#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index_dense.hpp"
#include "utils/algorithm.hpp"
#include "utils/counter.hpp"
#include "utils/synchronized.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

// unum::usearch::index_dense_gt is the index type used for vector indices. It is thread-safe and supports concurrent
// operations.
using mg_vector_index_t = unum::usearch::index_dense_gt<Vertex *, unum::usearch::uint40_t>;
using synchronized_mg_vector_index_t = utils::Synchronized<mg_vector_index_t, std::shared_mutex>;

// NOLINTNEXTLINE(bugprone-exception-escape)
struct IndexItem {
  // unum::usearch::index_dense_gt is thread-safe and supports concurrent operations. However, we still need to use
  // locking because resizing the index requires exclusive access. For all other operations, we can use shared lock even
  // though we are modifying index. In the case of removing or adding elements to the index we will use
  // MutableSharedLock to acquire an shared lock.
  std::shared_ptr<synchronized_mg_vector_index_t> mg_index;
  VectorIndexSpec spec;
};

/// @brief Implements the underlying functionality of the `VectorIndex` class.
///
/// The `Impl` structure follows the PIMPL (Pointer to Implementation) idiom to separate
/// the interface of `VectorIndex` from its implementation
struct VectorIndex::Impl {
  /// The `index_` member is a map that associates a `LabelPropKey` (a combination of label and property)
  /// with the pair of a IndexItem.
  absl::flat_hash_map<LabelPropKey, IndexItem> index_;

  /// The `index_name_to_label_prop_` is a map that maps an index name (as a string) to the corresponding
  /// `LabelPropKey`. This allows the system to quickly resolve an index name to the spec
  /// associated with that index, enabling easy lookup and management of indexes by name.
  std::map<std::string, LabelPropKey, std::less<>> index_name_to_label_prop_;

  /// The `label_to_index_` is a map that maps a label to a map of property ids to index names. This allows the system
  /// to quickly resolve a label to the indexes associated with that label, enabling easy lookup and management of
  /// indexes by label.
  std::unordered_map<LabelId, std::unordered_map<PropertyId, std::string>> label_to_index_;
};

VectorIndex::VectorIndex() : pimpl(std::make_unique<Impl>()) {}
VectorIndex::~VectorIndex() = default;

bool VectorIndex::CreateIndex(const VectorIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                              std::optional<SnapshotObserverInfo> const &snapshot_info) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  const auto label_prop = LabelPropKey{spec.label_id, spec.property};
  try {
    // Create the index
    const unum::usearch::metric_punned_t metric(spec.dimension, spec.metric_kind, spec.scalar_kind);

    // use the number of workers as the number of possible concurrent index operations
    const unum::usearch::index_limits_t limits(spec.capacity, FLAGS_bolt_num_workers);
    if (pimpl->index_.contains(label_prop) || pimpl->index_name_to_label_prop_.contains(spec.index_name)) {
      throw query::VectorSearchException("Given vector index already exists.");
    }
    auto mg_vector_index = mg_vector_index_t::make(metric);
    if (!mg_vector_index) {
      throw query::VectorSearchException(fmt::format("Failed to create vector index {}, error message: {}",
                                                     spec.index_name, mg_vector_index.error.what()));
    }
    pimpl->index_name_to_label_prop_.try_emplace(spec.index_name, label_prop);
    pimpl->label_to_index_[spec.label_id].emplace(spec.property, spec.index_name);
    if (mg_vector_index.index.try_reserve(limits)) {
      spdlog::info("Created vector index {}", spec.index_name);
    } else {
      throw query::VectorSearchException(
          fmt::format("Failed to create vector index {}", spec.index_name, ". Failed to reserve memory for the index"));
    }

    // Update the index with the vertices
    for (auto &vertex : vertices) {
      if (!utils::Contains(vertex.labels, spec.label_id)) {
        continue;
      }
      auto property = vertex.properties.GetProperty(spec.property);
      if (!property.IsList()) {
        continue;
      }
      std::vector<float> vector;
      vector.reserve(property.ValueList().size());
      std::transform(
          property.ValueList().begin(), property.ValueList().end(), std::back_inserter(vector), [](const auto &value) {
            if (value.IsDouble()) {
              return static_cast<float>(value.ValueDouble());
            }
            if (value.IsInt()) {
              return static_cast<float>(value.ValueInt());
            }
            throw query::VectorSearchException("Vector index property must be a list of floats or integers.");
          });
      if (vector.size() != spec.dimension) {
        throw query::VectorSearchException(
            "Vector index property must have the same number of dimensions as the index.");
      }
      auto is_index_full = mg_vector_index.index.size() == mg_vector_index.index.capacity();
      if (is_index_full) {
        const auto new_size = spec.resize_coefficient * mg_vector_index.index.capacity();
        const unum::usearch::index_limits_t new_limits(new_size, FLAGS_bolt_num_workers);
        if (!mg_vector_index.index.try_reserve(new_limits)) {
          throw query::VectorSearchException("Failed to resize vector index.");
        }
      }
      mg_vector_index.index.add(&vertex, vector.data());
    }
    pimpl->index_.try_emplace(label_prop,
                              IndexItem{std::make_shared<utils::Synchronized<mg_vector_index_t, std::shared_mutex>>(
                                            std::move(mg_vector_index.index)),
                                        spec});
  } catch (const utils::OutOfMemoryException &) {
    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    pimpl->index_name_to_label_prop_.erase(spec.index_name);
    pimpl->index_.erase(label_prop);
    pimpl->label_to_index_.erase(spec.label_id);
    throw;
  }
  return true;
}

void VectorIndex::CreateIndex(const VectorIndexSpec &spec) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  const auto label_prop = LabelPropKey{spec.label_id, spec.property};
  try {
    // Create the index
    const unum::usearch::metric_punned_t metric(spec.dimension, spec.metric_kind, spec.scalar_kind);

    // use the number of workers as the number of possible concurrent index operations
    const unum::usearch::index_limits_t limits(spec.capacity, FLAGS_bolt_num_workers);
    if (pimpl->index_.contains(label_prop) || pimpl->index_name_to_label_prop_.contains(spec.index_name)) {
      throw query::VectorSearchException("Given vector index already exists.");
    }
    auto mg_vector_index = mg_vector_index_t::make(metric);
    if (!mg_vector_index) {
      throw query::VectorSearchException(fmt::format("Failed to create vector index {}, error message: {}",
                                                     spec.index_name, mg_vector_index.error.what()));
    }
    pimpl->index_name_to_label_prop_.try_emplace(spec.index_name, label_prop);
    pimpl->label_to_index_[spec.label_id].emplace(spec.property, spec.index_name);
    if (mg_vector_index.index.try_reserve(limits)) {
      spdlog::info("Created vector index {}", spec.index_name);
    } else {
      throw query::VectorSearchException(
          fmt::format("Failed to create vector index {}", spec.index_name, ". Failed to reserve memory for the index"));
    }
    pimpl->index_.try_emplace(label_prop,
                              IndexItem{std::make_shared<utils::Synchronized<mg_vector_index_t, std::shared_mutex>>(
                                            std::move(mg_vector_index.index)),
                                        spec});

  } catch (const utils::OutOfMemoryException &) {
    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    pimpl->index_name_to_label_prop_.erase(spec.index_name);
    pimpl->index_.erase(label_prop);
    pimpl->label_to_index_.erase(spec.label_id);
    throw;
  }
}

bool VectorIndex::DropIndex(std::string_view index_name) {
  auto it = pimpl->index_name_to_label_prop_.find(index_name);
  if (it == pimpl->index_name_to_label_prop_.end()) {
    return false;
  }
  const auto &label_prop = it->second;
  pimpl->index_.erase(label_prop);
  pimpl->index_name_to_label_prop_.erase(it);
  pimpl->label_to_index_.erase(label_prop.label());
  spdlog::info("Dropped vector index {}", index_name);
  return true;
}

void VectorIndex::Clear() {
  pimpl->index_name_to_label_prop_.clear();
  pimpl->index_.clear();
  pimpl->label_to_index_.clear();
}

std::vector<LabelPropKey> VectorIndex::GetMatchingLabelProps(std::span<LabelId const> labels,
                                                             std::span<PropertyId const> properties) const {
  auto has_property = [&](const auto &label_prop) { return utils::Contains(properties, label_prop.property()); };
  auto has_label = [&](const auto &label_prop) { return utils::Contains(labels, label_prop.label()); };
  return pimpl->index_ | rv::keys | rv::filter(has_label) | rv::filter(has_property) | r::to<std::vector>();
}

void VectorIndex::UpdateOnSetProperty(const PropertyValue &value, Vertex *vertex, NameIdMapper *name_id_mapper) {
  if (!value.IsVectorIndexId()) {
    return;
  }
  auto index_ids = value.ValueVectorIndexIds();
  for (const auto &index_id : index_ids) {
    auto index_name = name_id_mapper->IdToName(index_id);
    auto label_prop = pimpl->index_name_to_label_prop_.at(index_name);
    // This means that label is already on the vertex and we are setting a property in the index
    auto &[mg_index, spec] = pimpl->index_.at(label_prop);
    bool is_index_full = false;
    {
      auto locked_index = mg_index->MutableSharedLock();
      locked_index->remove(vertex);
      is_index_full = locked_index->size() == locked_index->capacity();
    }
    if (is_index_full) {
      spdlog::warn("Vector index is full, resizing...");
      auto exclusively_locked_index = mg_index->Lock();
      const auto new_size = spec.resize_coefficient * exclusively_locked_index->capacity();
      const unum::usearch::index_limits_t new_limits(new_size, FLAGS_bolt_num_workers);
      if (!exclusively_locked_index->try_reserve(new_limits)) {
        throw query::VectorSearchException("Failed to resize vector index.");
      }
    }
    const auto &vector_property = value.ValueVectorIndexList();
    if (vector_property.empty()) {
      continue;
    }
    if (spec.dimension != vector_property.size()) {
      throw query::VectorSearchException("Vector index property must have the same number of dimensions as the index.");
    }
    auto locked_index = mg_index->MutableSharedLock();
    locked_index->add(vertex, vector_property.data());
  }
}

std::vector<float> VectorIndex::UpdateIndex(const PropertyValue &value, Vertex *vertex, std::string_view index_name) {
  auto label_prop = pimpl->index_name_to_label_prop_.at(index_name.data());
  auto &[mg_index, spec] = pimpl->index_.at(label_prop);
  bool is_index_full = false;
  {
    auto locked_index = mg_index->MutableSharedLock();
    locked_index->remove(vertex);
    is_index_full = locked_index->size() == locked_index->capacity();
  }
  if (is_index_full) {
    spdlog::warn("Vector index is full, resizing...");
    auto exclusively_locked_index = mg_index->Lock();
    const auto new_size = spec.resize_coefficient * exclusively_locked_index->capacity();
    const unum::usearch::index_limits_t new_limits(new_size, FLAGS_bolt_num_workers);
    if (!exclusively_locked_index->try_reserve(new_limits)) {
      throw query::VectorSearchException("Failed to resize vector index.");
    }
  }
  if (value.IsNull()) {
    // if property is null, that means that the vertex should not be in the index and we shouldn't do any other
    // updates
    return {};
  }
  auto vector_property = std::invoke([&]() {
    if (value.IsVectorIndexId()) {
      return value.ValueVectorIndexList();
    }
    if (value.IsList()) {
      auto vector_property = value.ValueList();
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
      return vector;
    }
    throw query::VectorSearchException("Vector index property must be a list of floats or integers.");
  });
  if (vector_property.empty()) {
    return {};
  }
  if (spec.dimension != vector_property.size()) {
    throw query::VectorSearchException("Vector index property must have the same number of dimensions as the index.");
  }
  auto locked_index = mg_index->MutableSharedLock();
  locked_index->add(vertex, vector_property.data());
  return vector_property;
}

PropertyValue VectorIndex::GetPropertyValue(Vertex *vertex, std::string_view index_name) const {
  auto it = pimpl->index_name_to_label_prop_.find(index_name);
  if (it == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &[mg_index, _] = pimpl->index_.at(it->second);
  return GetVectorAsPropertyValue(mg_index, vertex);
}

std::vector<float> VectorIndex::GetVectorProperty(Vertex *vertex, std::string_view index_name) const {
  auto it = pimpl->index_name_to_label_prop_.find(index_name);
  if (it == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &[mg_index, _] = pimpl->index_.at(it->second);
  return GetVector(mg_index, vertex);
}

std::vector<VectorIndexInfo> VectorIndex::ListVectorIndicesInfo() const {
  std::vector<VectorIndexInfo> result;
  result.reserve(pimpl->index_.size());
  for (const auto &[_, index_item] : pimpl->index_) {
    const auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index->ReadLock();
    result.emplace_back(spec.index_name, spec.label_id, spec.property,
                        NameFromMetric(locked_index->metric().metric_kind()),
                        static_cast<std::uint16_t>(locked_index->dimensions()), locked_index->capacity(),
                        locked_index->size(), NameFromScalar(locked_index->metric().scalar_kind()));
  }
  return result;
}

std::vector<VectorIndexSpec> VectorIndex::ListIndices() const {
  std::vector<VectorIndexSpec> result;
  result.reserve(pimpl->index_.size());
  r::transform(pimpl->index_, std::back_inserter(result),
               [](const auto &label_prop_index_item) { return label_prop_index_item.second.spec; });
  return result;
}

std::optional<uint64_t> VectorIndex::ApproximateNodesVectorCount(LabelId label, PropertyId property) const {
  auto it = pimpl->index_.find(LabelPropKey{label, property});
  if (it == pimpl->index_.end()) {
    return std::nullopt;
  }
  auto &[mg_index, _] = it->second;
  auto locked_index = mg_index->ReadLock();
  return locked_index->size();
}

VectorIndex::VectorSearchNodeResults VectorIndex::SearchNodes(std::string_view index_name, uint64_t result_set_size,
                                                              const std::vector<float> &query_vector) const {
  const auto label_prop = pimpl->index_name_to_label_prop_.find(index_name);
  if (label_prop == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &[mg_index, _] = pimpl->index_.at(label_prop->second);

  // The result vector will contain pairs of vertices and their score.
  VectorSearchNodeResults result;
  result.reserve(result_set_size);

  auto locked_index = mg_index->ReadLock();
  const auto result_keys =
      locked_index->filtered_search(query_vector.data(), result_set_size, [](const Vertex *vertex) {
        auto guard = std::shared_lock{vertex->lock};
        return !vertex->deleted;
      });
  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    const auto &vertex = static_cast<Vertex *>(result_keys[i].member.key);
    result.emplace_back(
        vertex, static_cast<double>(result_keys[i].distance),
        std::abs(SimilarityFromDistance(locked_index->metric().metric_kind(), result_keys[i].distance)));
  }

  return result;
}

void VectorIndex::AbortEntries(const LabelPropKey &label_prop, std::span<Vertex *const> vertices) {
  auto &[mg_index, _] = pimpl->index_.at(label_prop);
  auto locked_index = mg_index->MutableSharedLock();
  for (const auto &vertex : vertices) {
    locked_index->remove(vertex);
  }
}

void VectorIndex::RestoreEntries(const LabelPropKey &label_prop,
                                 std::span<std::pair<PropertyValue, Vertex *> const> prop_vertices) {
  for (const auto &property_value_vertex : prop_vertices) {
    // TODO(@DavIvek): Implement this
    // UpdateVectorIndex(property_value_vertex.second, label_prop, &property_value_vertex.first);
  }
}

void VectorIndex::RemoveObsoleteEntries(std::stop_token token) const {
  auto maybe_stop = utils::ResettableCounter(2048);
  for (auto &[_, index_item] : pimpl->index_) {
    if (maybe_stop() && token.stop_requested()) {
      return;
    }
    auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index->MutableSharedLock();
    std::vector<Vertex *> vertices_to_remove(locked_index->size());
    locked_index->export_keys(vertices_to_remove.data(), 0, locked_index->size());

    auto deleted = vertices_to_remove | rv::filter([](const Vertex *vertex) {
                     auto guard = std::shared_lock{vertex->lock};
                     return vertex->deleted;
                   });
    for (const auto &vertex : deleted) {
      locked_index->remove(vertex);
    }
  }
}

VectorIndex::IndexStats VectorIndex::Analysis() const {
  IndexStats res{};
  for (const auto &[label_prop, _] : pimpl->index_) {
    const auto label = label_prop.label();
    const auto property = label_prop.property();
    res.l2p[label].emplace_back(property);
    res.p2l[property].emplace_back(label);
  }
  return res;
}

bool VectorIndex::IndexExists(std::string_view index_name) const {
  return pimpl->index_name_to_label_prop_.contains(index_name);
}

std::optional<std::vector<uint64_t>> VectorIndex::IsPropertyInVectorIndex(Vertex *vertex, PropertyId property,
                                                                          NameIdMapper *name_id_mapper) {
  auto matching_label_props = GetMatchingLabelProps(vertex->labels, std::array{property});
  if (matching_label_props.empty()) {
    return std::nullopt;
  }
  std::vector<uint64_t> index_ids;
  index_ids.reserve(matching_label_props.size());
  for (const auto &label_prop : matching_label_props) {
    auto [_, spec] = pimpl->index_.at(label_prop);
    auto index_id = name_id_mapper->NameToId(spec.index_name);
    index_ids.emplace_back(index_id);
  }
  return index_ids;
}

std::unordered_map<PropertyId, std::string> VectorIndex::GetProperties(LabelId label) const {
  if (pimpl->label_to_index_.empty()) {
    return {};
  }
  if (auto it = pimpl->label_to_index_.find(label); it != pimpl->label_to_index_.end()) {
    return it->second;
  }
  return {};
}
}  // namespace memgraph::storage
