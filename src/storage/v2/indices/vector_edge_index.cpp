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

#include <ranges>
#include <usearch/index_dense.hpp>

#include "flags/bolt.hpp"
#include "flags/general.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "utils/synchronized.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

// unum::usearch::index_dense_gt is the index type used for vector indices. It is thread-safe and supports concurrent
// operations.
using mg_vector_edge_index_t = unum::usearch::index_dense_gt<VectorEdgeIndex::EdgeIndexEntry, unum::usearch::uint40_t>;

struct EdgeTypeIndexItem {
  // unum::usearch::index_dense_gt is thread-safe and supports concurrent operations. However, we still need to use
  // locking because resizing the index requires exclusive access. For all other operations, we can use shared lock even
  // though we are modifying index. In the case of removing or adding elements to the index we will use
  // MutableSharedLock to acquire an shared lock.
  std::shared_ptr<utils::Synchronized<mg_vector_edge_index_t, std::shared_mutex>> mg_index;
  VectorEdgeIndexSpec spec;
};

/// @brief Implements the underlying functionality of the `VectorIndex` class.
///
/// The `Impl` structure follows the PIMPL (Pointer to Implementation) idiom to separate
/// the interface of `VectorEdgeIndex` from its implementation
struct VectorEdgeIndex::Impl {
  /// The `index_` member is a map that associates a `EdgeTypePropKey` (a combination of edge type and property)
  /// with the pair of a IndexItem.
  /// std::map<EdgeTypePropKey, EdgeTypeIndexItem> edge_index_;
  std::map<EdgeTypePropKey, EdgeTypeIndexItem, std::less<>> edge_index_;

  /// The `index_name_to_edge_type_prop_` is a map that maps an index name (as a string) to the corresponding
  /// `EdgeTypePropKey`. This allows the system to quickly resolve an index name to the spec
  /// associated with that index, enabling easy lookup and management of indexes by name.
  std::map<std::string, EdgeTypePropKey, std::less<>> index_name_to_edge_type_prop_;
};

namespace {

using EdgeIndexEntry = VectorEdgeIndex::EdgeIndexEntry;
using SyncVectorEdgeIndex = utils::Synchronized<mg_vector_edge_index_t, std::shared_mutex>;

/// @brief Attempts to add all matching edges from a vertex to the vector index.
/// Handles resize if the index is full.
/// @param mg_index The synchronized index wrapper.
/// @param spec The index specification (may be modified if resize occurs).
/// @param from_vertex The source vertex whose edges to process.
/// @param snapshot_info Optional snapshot observer for progress tracking.
/// @param thread_id Optional thread ID hint for usearch's internal optimizations.
void TryAddEdgesToIndex(SyncVectorEdgeIndex &mg_index, VectorEdgeIndexSpec &spec, Vertex &from_vertex,
                        std::optional<SnapshotObserverInfo> const &snapshot_info,
                        std::optional<std::size_t> thread_id = std::nullopt) {
  if (from_vertex.deleted) {
    return;
  }
  for (auto &edge_tuple : from_vertex.out_edges) {
    if (std::get<kEdgeTypeIdPos>(edge_tuple) != spec.edge_type_id) {
      continue;
    }
    auto *to_vertex = std::get<kVertexPos>(edge_tuple);
    if (to_vertex->deleted) {
      continue;
    }
    auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;
    if (edge->deleted) {
      continue;
    }
    auto property = edge->properties.GetProperty(spec.property);
    if (property.IsNull()) {
      continue;
    }
    auto vector = PropertyToFloatVector(property, spec.dimension);
    const EdgeIndexEntry entry{&from_vertex, to_vertex, edge};
    AddToVectorIndex(mg_index, spec, entry, vector.data(), thread_id);
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::VECTOR_EDGE_IDX);
    }
  }
}

}  // namespace

VectorEdgeIndex::VectorEdgeIndex() : pimpl(std::make_unique<Impl>()) {}
VectorEdgeIndex::~VectorEdgeIndex() = default;
VectorEdgeIndex::VectorEdgeIndex(VectorEdgeIndex &&) noexcept = default;
VectorEdgeIndex &VectorEdgeIndex::operator=(VectorEdgeIndex &&) noexcept = default;

bool VectorEdgeIndex::CreateIndex(const VectorEdgeIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
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

bool VectorEdgeIndex::RecoverIndex(const VectorEdgeIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
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

void VectorEdgeIndex::SetupIndex(const VectorEdgeIndexSpec &spec) {
  const EdgeTypePropKey edge_type_prop{spec.edge_type_id, spec.property};

  if (pimpl->index_name_to_edge_type_prop_.contains(spec.index_name)) {
    throw query::VectorSearchException("Vector index with the given name already exists.");
  }
  if (pimpl->edge_index_.contains(edge_type_prop)) {
    throw query::VectorSearchException("Vector index with the given edge type and property already exists.");
  }

  const unum::usearch::metric_punned_t metric(spec.dimension, spec.metric_kind, spec.scalar_kind);
  auto mg_edge_index = mg_vector_edge_index_t::make(metric);
  if (!mg_edge_index) {
    throw query::VectorSearchException(fmt::format("Failed to create vector index {}, error message: {}",
                                                   spec.index_name, mg_edge_index.error.what()));
  }

  // Use the number of workers as the number of possible concurrent index operations
  const unum::usearch::index_limits_t limits(spec.capacity, GetVectorIndexThreadCount());
  if (!mg_edge_index.index.try_reserve(limits)) {
    throw query::VectorSearchException(fmt::format("Failed to reserve memory for vector index {}", spec.index_name));
  }

  pimpl->index_name_to_edge_type_prop_.emplace(spec.index_name, edge_type_prop);
  pimpl->edge_index_.emplace(
      edge_type_prop,
      EdgeTypeIndexItem{.mg_index = std::make_shared<utils::Synchronized<mg_vector_edge_index_t, std::shared_mutex>>(
                            std::move(mg_edge_index)),
                        .spec = spec});

  spdlog::info("Created vector index {}", spec.index_name);
}

void VectorEdgeIndex::CleanupFailedIndex(const VectorEdgeIndexSpec &spec) {
  const EdgeTypePropKey edge_type_prop{spec.edge_type_id, spec.property};
  pimpl->index_name_to_edge_type_prop_.erase(spec.index_name);
  pimpl->edge_index_.erase(edge_type_prop);
}

void VectorEdgeIndex::PopulateIndexOnSingleThread(utils::SkipList<Vertex>::Accessor &vertices,
                                                  const VectorEdgeIndexSpec &spec,
                                                  std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto &[mg_index, mutable_spec] = pimpl->edge_index_.at({spec.edge_type_id, spec.property});
  PopulateVectorIndexSingleThreaded(*mg_index, mutable_spec, vertices, snapshot_info, TryAddEdgesToIndex);
}

void VectorEdgeIndex::PopulateIndexOnMultipleThreads(utils::SkipList<Vertex>::Accessor &vertices,
                                                     const VectorEdgeIndexSpec &spec,
                                                     std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto &[mg_index, mutable_spec] = pimpl->edge_index_.at({spec.edge_type_id, spec.property});
  PopulateVectorIndexMultiThreaded(*mg_index, mutable_spec, vertices, snapshot_info, TryAddEdgesToIndex);
}

bool VectorEdgeIndex::DropIndex(std::string_view index_name) {
  auto it = pimpl->index_name_to_edge_type_prop_.find(index_name.data());
  if (it == pimpl->index_name_to_edge_type_prop_.end()) {
    return false;
  }
  const auto &edge_type_prop = it->second;
  pimpl->edge_index_.erase(edge_type_prop);
  pimpl->index_name_to_edge_type_prop_.erase(it);
  spdlog::info("Dropped vector index {}", index_name);
  return true;
}

void VectorEdgeIndex::Clear() {
  pimpl->index_name_to_edge_type_prop_.clear();
  pimpl->edge_index_.clear();
}

bool VectorEdgeIndex::UpdateVectorIndex(EdgeIndexEntry entry, const EdgeTypePropKey &edge_type_prop,
                                        const PropertyValue *value) {
  auto &[mg_index, spec] = pimpl->edge_index_.at(edge_type_prop);

  // Try to remove entry (if it exists)
  {
    auto locked_index = mg_index->MutableSharedLock();
    if (locked_index->contains(entry)) {
      auto result = locked_index->remove(entry);
      if (result.error) {
        throw query::VectorSearchException(
            fmt::format("Failed to remove existing edge from vector index: {}", result.error.release()));
      }
    }
  }

  const auto &property = value != nullptr ? *value : entry.edge->properties.GetProperty(edge_type_prop.property());
  if (property.IsNull()) {
    // Property is null means edge should not be in the index
    return false;
  }

  auto vector = PropertyToFloatVector(property, spec.dimension);
  AddToVectorIndex(*mg_index, spec, entry, vector.data());
  return true;
}

void VectorEdgeIndex::UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                                          PropertyId property, const PropertyValue &value) {
  auto has_property = [&](const auto &edge_type_prop) { return edge_type_prop.property() == property; };
  if (std::ranges::any_of(pimpl->edge_index_ | rv::keys | rv::filter(has_property),
                          [&](const auto &edge_type_prop) { return edge_type_prop.edge_type() == edge_type; })) {
    UpdateVectorIndex({.from_vertex = from_vertex, .to_vertex = to_vertex, .edge = edge},
                      EdgeTypePropKey{edge_type, property}, &value);
  }
}

std::vector<VectorEdgeIndexInfo> VectorEdgeIndex::ListVectorIndicesInfo() const {
  std::vector<VectorEdgeIndexInfo> result;
  result.reserve(pimpl->edge_index_.size());
  for (const auto &[_, index_item] : pimpl->edge_index_) {
    const auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index->ReadLock();
    result.emplace_back(spec.index_name, spec.edge_type_id, spec.property,
                        NameFromMetric(locked_index->metric().metric_kind()),
                        static_cast<std::uint16_t>(locked_index->dimensions()), locked_index->capacity(),
                        locked_index->size(), NameFromScalar(locked_index->metric().scalar_kind()));
  }
  return result;
}

std::vector<VectorEdgeIndexSpec> VectorEdgeIndex::ListIndices() const {
  std::vector<VectorEdgeIndexSpec> result;
  result.reserve(pimpl->edge_index_.size());
  r::transform(pimpl->edge_index_, std::back_inserter(result),
               [](const auto &label_prop_index_item) { return label_prop_index_item.second.spec; });
  return result;
}

std::optional<uint64_t> VectorEdgeIndex::ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const {
  auto it = pimpl->edge_index_.find(EdgeTypePropKey{edge_type, property});
  if (it == pimpl->edge_index_.end()) {
    return std::nullopt;
  }
  auto &[mg_index, _] = it->second;
  auto locked_index = mg_index->ReadLock();
  return locked_index->size();
}

VectorEdgeIndex::VectorSearchEdgeResults VectorEdgeIndex::SearchEdges(std::string_view index_name,
                                                                      uint64_t result_set_size,
                                                                      const std::vector<float> &query_vector) const {
  const auto edge_type_prop = pimpl->index_name_to_edge_type_prop_.find(index_name);
  if (edge_type_prop == pimpl->index_name_to_edge_type_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &[mg_index, _] = pimpl->edge_index_.at(edge_type_prop->second);

  // The result vector will contain pairs of edges and their score.
  VectorSearchEdgeResults result;
  result.reserve(result_set_size);

  auto locked_index = mg_index->ReadLock();
  const auto result_keys =
      locked_index->filtered_search(query_vector.data(), result_set_size, [](const EdgeIndexEntry &entry) {
        auto guard = std::shared_lock{entry.edge->lock};
        return !entry.from_vertex->deleted && !entry.to_vertex->deleted && !entry.edge->deleted;
      });
  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    const auto &entry = static_cast<EdgeIndexEntry>(result_keys[i].member.key);
    result.emplace_back(
        entry, static_cast<double>(result_keys[i].distance),
        std::abs(SimilarityFromDistance(locked_index->metric().metric_kind(), result_keys[i].distance)));
  }

  return result;
}

void VectorEdgeIndex::RestoreEntries(
    const EdgeTypePropKey &edge_type_prop,
    std::span<std::pair<PropertyValue, std::tuple<Vertex *const, Vertex *const, Edge *const>> const> prop_edges) {
  for (const auto &property_value_edge : prop_edges) {
    const auto &[property_value, edge_tuple] = property_value_edge;
    const auto &[from_vertex, to_vertex, edge] = edge_tuple;
    UpdateVectorIndex({.from_vertex = from_vertex, .to_vertex = to_vertex, .edge = edge}, edge_type_prop,
                      &property_value);
  }
}

void VectorEdgeIndex::RemoveObsoleteEntries(std::stop_token token) const {
  auto maybe_stop = utils::ResettableCounter(2048);
  for (auto &[_, index_item] : pimpl->edge_index_) {
    if (maybe_stop() && token.stop_requested()) {
      return;
    }
    auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index->MutableSharedLock();
    std::vector<EdgeIndexEntry> edges_to_remove(locked_index->size());
    locked_index->export_keys(edges_to_remove.data(), 0, locked_index->size());

    auto deleted = edges_to_remove | rv::filter([](const EdgeIndexEntry &entry) {
                     auto guard = std::shared_lock{entry.edge->lock};
                     return entry.edge->deleted;
                   });
    for (const auto &entry : deleted) {
      locked_index->remove(entry);
    }
  }
}

VectorEdgeIndex::IndexStats VectorEdgeIndex::Analysis() const {
  IndexStats res{};
  for (const auto &[edge_type_prop, _] : pimpl->edge_index_) {
    const auto edge_type = edge_type_prop.edge_type();
    const auto property = edge_type_prop.property();
    res.et2p[edge_type].emplace_back(property);
    res.p2et[property].emplace_back(edge_type);
  }
  return res;
}

EdgeTypeId VectorEdgeIndex::GetEdgeTypeId(std::string_view index_name) {
  auto it = pimpl->index_name_to_edge_type_prop_.find(index_name.data());
  if (it == pimpl->index_name_to_edge_type_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  return it->second.edge_type();
}

bool VectorEdgeIndex::IndexExists(std::string_view index_name) const {
  return pimpl->index_name_to_edge_type_prop_.contains(index_name);
}

std::vector<float> VectorEdgeIndex::GetVectorFromEdge(Vertex *from_vertex, Vertex *to_vertex, Edge *edge,
                                                      std::string_view index_name) const {
  const auto edge_type_prop = pimpl->index_name_to_edge_type_prop_.find(index_name);
  if (edge_type_prop == pimpl->index_name_to_edge_type_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &[mg_index, _] = pimpl->edge_index_.at(edge_type_prop->second);
  auto locked_index = mg_index->ReadLock();
  std::vector<float> vector(static_cast<std::size_t>(locked_index->dimensions()));
  const EdgeIndexEntry entry{.from_vertex = from_vertex, .to_vertex = to_vertex, .edge = edge};
  locked_index->get(entry, vector.data());
  return vector;
}

}  // namespace memgraph::storage
