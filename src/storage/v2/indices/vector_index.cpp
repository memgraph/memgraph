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
#include <string_view>

#include "flags/bolt.hpp"
#include "flags/general.hpp"
#include "query/exceptions.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index.hpp"

#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index_dense.hpp"
#include "utils/counter.hpp"
#include "utils/synchronized.hpp"

namespace r = ranges;
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

namespace {

using SyncVectorIndex = utils::Synchronized<mg_vector_index_t, std::shared_mutex>;

/// @brief Attempts to add a vertex to the vector index if it matches the spec criteria.
/// Handles resize if the index is full.
/// @param mg_index The synchronized index wrapper.
/// @param spec The index specification (may be modified if resize occurs).
/// @param vertex The vertex to potentially add.
/// @param snapshot_info Optional snapshot observer for progress tracking.
/// @param thread_id Optional thread ID hint for usearch's internal optimizations.
void TryAddVertexToIndex(SyncVectorIndex &mg_index, VectorIndexSpec &spec, Vertex &vertex,
                         std::optional<SnapshotObserverInfo> const &snapshot_info,
                         std::optional<std::size_t> thread_id = std::nullopt) {
  if (!std::ranges::contains(vertex.labels, spec.label_id)) {
    return;
  }
  auto property = vertex.properties.GetProperty(spec.property);
  if (property.IsNull()) {
    return;
  }
  auto vector = PropertyToFloatVector(property, spec.dimension);
  AddToVectorIndex(mg_index, spec, &vertex, vector.data(), thread_id);
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::VECTOR_IDX);
  }
}

}  // namespace

VectorIndex::VectorIndex() : pimpl(std::make_unique<Impl>()) {}
VectorIndex::~VectorIndex() = default;
VectorIndex::VectorIndex(VectorIndex &&) noexcept = default;
VectorIndex &VectorIndex::operator=(VectorIndex &&) noexcept = default;

bool VectorIndex::CreateIndex(const VectorIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                              std::optional<SnapshotObserverInfo> const &snapshot_info) {
  const utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  try {
    SetupIndex(spec);
    PopulateIndexOnSingleThread(vertices, spec, snapshot_info);
  } catch (const utils::OutOfMemoryException &) {
    const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    CleanupFailedIndex(spec);
    throw;
  }
  return true;
}

bool VectorIndex::RecoverIndex(const VectorIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                               std::optional<SnapshotObserverInfo> const &snapshot_info) {
  const utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  try {
    SetupIndex(spec);
    if (FLAGS_storage_parallel_schema_recovery && FLAGS_storage_recovery_thread_count > 1) {
      PopulateIndexOnMultipleThreads(vertices, spec, snapshot_info);
    } else {
      PopulateIndexOnSingleThread(vertices, spec, snapshot_info);
    }
  } catch (const utils::OutOfMemoryException &) {
    const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    CleanupFailedIndex(spec);
    throw;
  }
  return true;
}

void VectorIndex::SetupIndex(const VectorIndexSpec &spec) {
  const auto label_prop = LabelPropKey{spec.label_id, spec.property};
  if (pimpl->index_.contains(label_prop) || pimpl->index_name_to_label_prop_.contains(spec.index_name)) {
    throw query::VectorSearchException("Given vector index already exists.");
  }

  const unum::usearch::metric_punned_t metric(spec.dimension, spec.metric_kind, spec.scalar_kind);
  auto mg_vector_index = mg_vector_index_t::make(metric);
  if (!mg_vector_index) {
    throw query::VectorSearchException(fmt::format("Failed to create vector index {}, error message: {}",
                                                   spec.index_name, mg_vector_index.error.what()));
  }

  // Use the number of workers as the number of possible concurrent index operations
  const unum::usearch::index_limits_t limits(spec.capacity, GetVectorIndexThreadCount());
  if (!mg_vector_index.index.try_reserve(limits)) {
    throw query::VectorSearchException(
        fmt::format("Failed to create vector index {}. Failed to reserve memory for the index", spec.index_name));
  }

  pimpl->index_name_to_label_prop_.try_emplace(spec.index_name, label_prop);
  pimpl->index_.try_emplace(
      label_prop, IndexItem{.mg_index = std::make_shared<utils::Synchronized<mg_vector_index_t, std::shared_mutex>>(
                                std::move(mg_vector_index.index)),
                            .spec = spec});

  spdlog::info("Created vector index {}", spec.index_name);
}

void VectorIndex::CleanupFailedIndex(const VectorIndexSpec &spec) {
  const auto label_prop = LabelPropKey{spec.label_id, spec.property};
  pimpl->index_name_to_label_prop_.erase(spec.index_name);
  pimpl->index_.erase(label_prop);
}

void VectorIndex::PopulateIndexOnSingleThread(utils::SkipList<Vertex>::Accessor &vertices, const VectorIndexSpec &spec,
                                              std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto &[mg_index, mutable_spec] = pimpl->index_.at({spec.label_id, spec.property});
  PopulateVectorIndexSingleThreaded(*mg_index, mutable_spec, vertices, snapshot_info, TryAddVertexToIndex);
}

void VectorIndex::PopulateIndexOnMultipleThreads(utils::SkipList<Vertex>::Accessor &vertices,
                                                 const VectorIndexSpec &spec,
                                                 std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto &[mg_index, mutable_spec] = pimpl->index_.at({spec.label_id, spec.property});
  PopulateVectorIndexMultiThreaded(*mg_index, mutable_spec, vertices, snapshot_info, TryAddVertexToIndex);
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

  // Try to remove entry (if it exists)
  {
    auto locked_index = mg_index->MutableSharedLock();
    if (locked_index->contains(vertex)) {
      auto result = locked_index->remove(vertex);
      if (result.error) {
        throw query::VectorSearchException(
            fmt::format("Failed to remove existing vertex from vector index: {}", result.error.release()));
      }
    }
  }

  const auto &property = value != nullptr ? *value : vertex->properties.GetProperty(label_prop.property());
  if (property.IsNull()) {
    // Property is null means vertex should not be in the index
    return false;
  }

  auto vector = PropertyToFloatVector(property, spec.dimension);
  AddToVectorIndex(*mg_index, spec, vertex, vector.data());
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

std::vector<float> VectorIndex::GetVectorFromVertex(Vertex *vertex, std::string_view index_name) const {
  const auto label_prop = pimpl->index_name_to_label_prop_.find(index_name);
  if (label_prop == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &[mg_index, _] = pimpl->index_.at(label_prop->second);
  auto locked_index = mg_index->ReadLock();
  std::vector<float> vector(locked_index->dimensions());
  locked_index->get(vertex, vector.data());
  return vector;
}

}  // namespace memgraph::storage
