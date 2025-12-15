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
#include "utils/counter.hpp"
#include "utils/synchronized.hpp"

namespace r = std::ranges;
namespace rv = r::views;

namespace memgraph::storage {

// unum::usearch::index_dense_gt is the index type used for vector indices. It is thread-safe and supports concurrent
// operations.
using mg_vector_index_t = unum::usearch::index_dense_gt<Vertex *, unum::usearch::uint40_t>;

// NOLINTNEXTLINE(bugprone-exception-escape)
struct IndexItem {
  // unum::usearch::index_dense_gt is thread-safe and supports concurrent operations. However, we still need to use
  // locking because resizing the index requires exclusive access. For all other operations, we can use shared lock even
  // though we are modifying index. In the case of removing or adding elements to the index we will use
  // MutableSharedLock to acquire an shared lock.
  std::shared_ptr<utils::Synchronized<mg_vector_index_t, std::shared_mutex>> mg_index;
  VectorIndexSpec spec;
};

/// @brief Implements the underlying functionality of the `VectorIndex` class.
///
/// The `Impl` structure follows the PIMPL (Pointer to Implementation) idiom to separate
/// the interface of `VectorIndex` from its implementation
struct VectorIndex::Impl {
  /// The `index_` member is a map that associates a `LabelPropKey` (a combination of label and property)
  /// with the pair of a IndexItem.
  std::map<LabelPropKey, IndexItem> index_;

  /// The `index_name_to_label_prop_` is a map that maps an index name (as a string) to the corresponding
  /// `LabelPropKey`. This allows the system to quickly resolve an index name to the spec
  /// associated with that index, enabling easy lookup and management of indexes by name.
  std::map<std::string, LabelPropKey, std::less<>> index_name_to_label_prop_;
};

VectorIndex::VectorIndex() : pimpl(std::make_unique<Impl>()) {}
VectorIndex::~VectorIndex() = default;
VectorIndex::VectorIndex(VectorIndex &&) noexcept = default;
VectorIndex &VectorIndex::operator=(VectorIndex &&) noexcept = default;

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
    if (mg_vector_index.index.try_reserve(limits)) {
      spdlog::info("Created vector index {}", spec.index_name);
    } else {
      throw query::VectorSearchException(
          fmt::format("Failed to create vector index {}", spec.index_name, ". Failed to reserve memory for the index"));
    }
    pimpl->index_.try_emplace(
        label_prop, IndexItem{.mg_index = std::make_shared<utils::Synchronized<mg_vector_index_t, std::shared_mutex>>(
                                  std::move(mg_vector_index.index)),
                              .spec = spec});

    // Update the index with the vertices
    for (auto &vertex : vertices) {
      if (!std::ranges::contains(vertex.labels, spec.label_id)) {
        continue;
      }
      if (UpdateVectorIndex(&vertex, label_prop) && snapshot_info) {
        snapshot_info->Update(UpdateType::VECTOR_IDX);
      }
    }
  } catch (const utils::OutOfMemoryException &) {
    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    pimpl->index_name_to_label_prop_.erase(spec.index_name);
    pimpl->index_.erase(label_prop);
    throw;
  }
  return true;
}

bool VectorIndex::DropIndex(std::string_view index_name) {
  auto it = pimpl->index_name_to_label_prop_.find(index_name.data());
  if (it == pimpl->index_name_to_label_prop_.end()) {
    return false;
  }
  const auto &label_prop = it->second;
  pimpl->index_.erase(label_prop);
  pimpl->index_name_to_label_prop_.erase(it);
  spdlog::info("Dropped vector index {}", index_name);
  return true;
}

void VectorIndex::Clear() {
  pimpl->index_name_to_label_prop_.clear();
  pimpl->index_.clear();
}

bool VectorIndex::UpdateVectorIndex(Vertex *vertex, const LabelPropKey &label_prop, const PropertyValue *value) {
  auto &[mg_index, spec] = pimpl->index_.at(label_prop);
  bool is_index_full = false;
  // try to remove entry (if it exists) and then add a new one + check if index is full
  {
    auto locked_index = mg_index->MutableSharedLock();
    locked_index->remove(vertex);
    is_index_full = locked_index->size() == locked_index->capacity();
  }

  const auto &property = (value != nullptr ? *value : vertex->properties.GetProperty(label_prop.property()));
  if (property.IsNull()) {
    // if property is null, that means that the vertex should not be in the index and we shouldn't do any other updates
    return false;
  }
  if (!property.IsAnyList()) {
    throw query::VectorSearchException("Vector index property must be a list.");
  }
  const auto vector_size = GetListSize(property);
  if (spec.dimension != vector_size) {
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
    spec.capacity = exclusively_locked_index->capacity();  // capacity might be larger than the requested capacity
  }

  std::vector<float> vector;
  vector.reserve(vector_size);
  for (size_t i = 0; i < vector_size; ++i) {
    const auto numeric_value = GetNumericValueAt(property, i);
    if (!numeric_value) {
      throw query::VectorSearchException("Vector index property must be a list of numeric values.");
    }
    const auto float_value =
        std::visit([](const auto &val) -> float { return static_cast<float>(val); }, *numeric_value);
    vector.push_back(float_value);
  }
  {
    auto locked_index = mg_index->MutableSharedLock();
    locked_index->add(vertex, vector.data());
  }
  return true;
}

void VectorIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update) {
  r::for_each(pimpl->index_ | rv::keys, [&](const auto &label_prop) {
    if (label_prop.label() == added_label) {
      UpdateVectorIndex(vertex_after_update, label_prop);
    }
  });
}

void VectorIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update) {
  r::for_each(pimpl->index_ | rv::keys, [&](const auto &label_prop) {
    if (label_prop.label() == removed_label) {
      auto &[mg_index, _] = pimpl->index_.at(label_prop);
      auto locked_index = mg_index->MutableSharedLock();
      locked_index->remove(vertex_before_update);
    }
  });
}

void VectorIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex) {
  if (pimpl->index_.empty()) return;

  auto has_property = [&](const auto &label_prop) { return label_prop.property() == property; };
  auto has_label = [&](const auto &label_prop) { return std::ranges::contains(vertex->labels, label_prop.label()); };

  auto view = pimpl->index_ | rv::keys | rv::filter(has_property) | rv::filter(has_label);
  for (const auto &label_prop : view) {
    UpdateVectorIndex(vertex, label_prop, &value);
  }
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
    UpdateVectorIndex(property_value_vertex.second, label_prop, &property_value_vertex.first);
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

}  // namespace memgraph::storage
