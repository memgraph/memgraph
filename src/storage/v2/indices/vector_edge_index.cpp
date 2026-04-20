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

#include <mutex>
#include <range/v3/all.hpp>
#include <ranges>
#include <shared_mutex>
#include <unordered_set>

#include "flags/general.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/active_indices_updater.hpp"
#include "storage/v2/indices/tracked_vector_allocator.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "usearch/index_dense.hpp"
#include "utils/resource_lock.hpp"

namespace r = ranges;

namespace memgraph::storage {

// Types moved to vector_edge_index.hpp

std::optional<uint64_t> VectorEdgeIndex::SetupIndex(const VectorEdgeIndexSpec &spec, NameIdMapper *name_id_mapper) {
  const auto index_id = name_id_mapper->NameToId(spec.index_name);
  if (index_->contains(index_id)) {
    return std::nullopt;
  }
  if (r::any_of(*index_, [&](const auto &id_index_item) {
        auto &index_spec = id_index_item.second->spec;
        return spec.edge_type_id == index_spec.edge_type_id && spec.property == index_spec.property;
      })) {
    return std::nullopt;
  }

  const unum::usearch::metric_punned_t metric(spec.dimension, spec.metric_kind, spec.scalar_kind);
  const unum::usearch::index_limits_t limits(spec.capacity, GetVectorIndexThreadCount());

  auto mg_edge_index = mg_vector_edge_index_t::make(metric);
  if (!mg_edge_index) {
    throw query::VectorSearchException(fmt::format(
        "Failed to create vector edge index {}, error message: {}", spec.index_name, mg_edge_index.error.what()));
  }

  if (!mg_edge_index.index.try_reserve(limits)) {
    throw query::VectorSearchException(
        fmt::format("Failed to create vector edge index {}. Failed to reserve memory for the index", spec.index_name));
  }

  auto new_map = std::make_shared<VectorEdgeIndexContainer>(*index_);
  const auto [_, inserted] =
      new_map->try_emplace(index_id, std::make_shared<EdgeTypeIndexItem>(std::move(mg_edge_index.index), spec));
  if (inserted) {
    index_ = new_map;
  }
  return inserted ? std::optional<uint64_t>{index_id} : std::nullopt;
}

void VectorEdgeIndex::AddEdgeToIndex(uint64_t index_id, Edge *edge, Vertex *from_vertex, Vertex *to_vertex,
                                     std::optional<std::size_t> thread_id) {
  auto it = index_->find(index_id);
  if (it == index_->end()) {
    throw query::VectorSearchException(fmt::format("Vector edge index {} does not exist.", index_id));
  }
  auto &item_ptr = it->second;
  auto &spec = item_ptr->spec;
  auto property = edge->properties.GetProperty(spec.property);
  if (property.IsNull()) return;

  auto vector = RegisterIndexId(property, index_id);
  edge->properties.SetProperty(spec.property, property);

  // Lock order: uSearch mutex (inside UpdateVectorIndex) → edge_endpoints_mutex_
  UpdateVectorIndex(item_ptr->mg_index, spec, edge, vector, thread_id);
  {
    auto lock = std::unique_lock{edge_endpoints_mutex_};
    edge_endpoints_[edge] = {from_vertex, to_vertex};
  }
}

bool VectorEdgeIndex::CreateIndex(const VectorEdgeIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                                  NameIdMapper *name_id_mapper,
                                  std::optional<SnapshotObserverInfo> const &snapshot_info) {
  try {
    const auto index_id = SetupIndex(spec, name_id_mapper);
    if (!index_id.has_value()) return false;
    PopulateVectorIndexSingleThreaded(vertices, [&](Vertex &vertex, std::optional<std::size_t> thread_id) {
      if (vertex.deleted()) return;
      for (auto &edge_tuple : vertex.out_edges) {
        if (std::get<kEdgeTypeIdPos>(edge_tuple) != spec.edge_type_id) continue;

        auto *to_vertex = std::get<kVertexPos>(edge_tuple);
        auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;
        if (edge->deleted() || to_vertex->deleted()) continue;

        AddEdgeToIndex(*index_id, edge, &vertex, to_vertex, thread_id);
        if (snapshot_info) {
          snapshot_info->Update(UpdateType::VECTOR_EDGE_IDX);
        }
      }
    });
    return true;
  } catch (const std::exception &) {
    DropIndex(spec.index_name, name_id_mapper);
    throw;
  }
}

void VectorEdgeIndex::RecoverIndex(VectorEdgeIndexRecoveryInfo &recovery_info,
                                   utils::SkipList<Vertex>::Accessor &vertices, NameIdMapper *name_id_mapper,
                                   ActiveIndicesUpdater const &updater,
                                   std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto &spec = recovery_info.spec;
  try {
    auto &recovery_entries = recovery_info.index_entries;
    const auto index_id = SetupIndex(spec, name_id_mapper);
    if (!index_id.has_value()) {
      throw query::VectorSearchException(
          "Given vector edge index already exists. Corrupted or invalid index recovery files.");
    }
    auto &item_ptr = index_->at(*index_id);
    auto &mg_index = item_ptr->mg_index;

    auto process_vertex_for_recovery = [&](Vertex &vertex, std::optional<std::size_t> thread_id) {
      for (auto &edge_tuple : vertex.out_edges) {
        if (std::get<kEdgeTypeIdPos>(edge_tuple) != spec.edge_type_id) continue;

        auto *to_vertex = std::get<kVertexPos>(edge_tuple);
        auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;
        if (vertex.deleted() || to_vertex->deleted() || edge->deleted()) continue;

        if (auto it = recovery_entries.find(edge->gid); it != recovery_entries.end()) {
          auto &vector = it->second;
          // Lock order: uSearch mutex (inside UpdateVectorIndex) → edge_endpoints_mutex_
          UpdateVectorIndex(mg_index, spec, edge, vector, thread_id);
          {
            auto lock = std::unique_lock{edge_endpoints_mutex_};
            edge_endpoints_[edge] = {&vertex, to_vertex};
          }
          // release vector resources to prevent memory growth while doing recovery
          vector.clear();
          vector.shrink_to_fit();
        } else {
          AddEdgeToIndex(*index_id, edge, &vertex, to_vertex, thread_id);
        }
      }
      if (snapshot_info) {
        snapshot_info->Update(UpdateType::VECTOR_EDGE_IDX);
      }
    };

    if (FLAGS_storage_parallel_schema_recovery && FLAGS_storage_recovery_thread_count > 1) {
      PopulateVectorIndexMultiThreaded(vertices, process_vertex_for_recovery);
    } else {
      PopulateVectorIndexSingleThreaded(vertices, process_vertex_for_recovery);
    }
  } catch (const std::exception &) {
    DropIndex(spec.index_name, name_id_mapper);
    throw;
  }

  updater(GetActiveIndices());
}

bool VectorEdgeIndex::DropIndex(std::string_view index_name, NameIdMapper *name_id_mapper) {
  auto maybe_id = name_id_mapper->NameToIdIfExists(index_name);
  if (!maybe_id.has_value()) {
    return false;
  }
  const auto index_id = *maybe_id;
  auto it = index_->find(index_id);
  if (it == index_->end()) {
    return false;
  }
  auto &item_ptr = it->second;
  auto &mg_index = item_ptr->mg_index;
  auto &spec = item_ptr->spec;
  std::vector<Edge *> dropped_edges;
  {
    auto guard = std::lock_guard{mg_index.mutex};

    const auto dimension = mg_index.index.dimensions();
    CheckGraphMemoryForIndexDrop(index_name, mg_index.index.size(), dimension);

    auto const index_size = mg_index.index.size();
    dropped_edges.resize(index_size);
    mg_index.index.export_keys(dropped_edges.data(), 0, index_size);

    // Convert indexed vectors back to property values with OOM protection.
    // Track processed vertices so we can rollback on OOM.
    std::size_t processed = 0;
    try {
      const utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_enabler;
      std::vector<double> vector(dimension);
      for (auto *edge : dropped_edges) {
        auto vector_property = edge->properties.GetProperty(spec.property);
        if (UnregisterIndexId(vector_property, index_id)) {
          mg_index.index.get(edge, vector.data());
          edge->properties.SetProperty(spec.property, PropertyValue(vector));
        } else {
          edge->properties.SetProperty(spec.property, vector_property);
        }
        ++processed;
      }
    } catch (const utils::OutOfMemoryException &) {
      const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
      // Rollback: restore already-processed edges to their indexed representation.
      for (std::size_t i = 0; i < processed; ++i) {
        auto *edge = dropped_edges[i];
        auto property_value = edge->properties.GetProperty(spec.property);
        if (property_value.IsVectorIndexId()) {
          auto &ids = property_value.ValueVectorIndexIds();
          ids.push_back(index_id);
        } else {
          property_value = PropertyValue(
              PropertyValue::VectorIndexIdData{.ids = utils::small_vector<uint64_t>{index_id}, .vector = {}});
        }
        edge->properties.SetProperty(spec.property, property_value);
      }
      throw;
    }
  }
  auto new_map = std::make_shared<VectorEdgeIndexContainer>(*index_);
  new_map->erase(index_id);
  index_ = new_map;
  // Clean up endpoints for edges no longer in any index.
  std::unordered_set<Edge *> still_indexed;
  for (const auto &[_, iptr] : *index_) {
    auto guard = utils::SharedResourceLockGuard(iptr->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    for (auto *edge : dropped_edges) {
      if (iptr->mg_index.index.contains(edge)) {
        still_indexed.insert(edge);
      }
    }
  }
  {
    auto lock = std::unique_lock{edge_endpoints_mutex_};
    for (auto *edge : dropped_edges) {
      DMG_ASSERT(edge != nullptr, "Null edge pointer in vector edge index");
      if (!still_indexed.contains(edge)) {
        edge_endpoints_.erase(edge);
      }
    }
  }
  return true;
}

void VectorEdgeIndex::Clear() {
  index_ = std::make_shared<VectorEdgeIndexContainer>();
  auto lock = std::unique_lock{edge_endpoints_mutex_};
  edge_endpoints_.clear();
}

void VectorEdgeIndex::UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                                          PropertyId property, const PropertyValue &value) {
  // Property should already be updated to the vector index id if it has vector index defined on it.
  if (value.IsVectorIndexId()) {
    const auto &vector_property = value.ValueVectorIndexList();
    const auto &index_ids = value.ValueVectorIndexIds();
    // Lock order: uSearch mutex (inside UpdateVectorIndex) → edge_endpoints_mutex_
    for (auto index_id : index_ids) {
      auto &item_ptr = index_->at(index_id);
      UpdateVectorIndex(item_ptr->mg_index, item_ptr->spec, edge, vector_property);
    }
    {
      auto lock = std::unique_lock{edge_endpoints_mutex_};
      edge_endpoints_[edge] = {from_vertex, to_vertex};
    }
  } else if (value.IsNull()) {
    // If value is null, we have to remove the edge from all indices that contain it (by edge type).
    auto indices = GetIndicesByProperty(property);
    for (const auto &[et, idx_id] : indices) {
      if (et != edge_type) continue;
      RemoveEdgeFromIndex(edge, idx_id);
    }
  }
  // Otherwise, we don't update the index.
}

void VectorEdgeIndex::RemoveEdgeFromIndex(Edge *edge, uint64_t index_id) {
  auto it = index_->find(index_id);
  if (it == index_->end()) {
    throw query::VectorSearchException(
        fmt::format("Error in removing edge from index: index id {} does not exist.", index_id));
  }
  auto &item_ptr = it->second;
  UpdateVectorIndex(item_ptr->mg_index, item_ptr->spec, edge, utils::small_vector<float>{});
}

std::vector<VectorEdgeIndexInfo> VectorEdgeIndex::ListVectorIndicesInfo() const {
  std::vector<VectorEdgeIndexInfo> result;
  result.reserve(index_->size());
  for (const auto &[_, item_ptr] : *index_) {
    auto &mg_index = item_ptr->mg_index;
    auto &spec = item_ptr->spec;
    auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    result.emplace_back(spec.index_name,
                        spec.edge_type_id,
                        spec.property,
                        NameFromMetric(mg_index.index.metric().metric_kind()),
                        static_cast<std::uint16_t>(mg_index.index.dimensions()),
                        mg_index.index.capacity(),
                        mg_index.index.size(),
                        NameFromScalar(mg_index.index.metric().scalar_kind()));
  }
  return result;
}

std::vector<VectorEdgeIndexSpec> VectorEdgeIndex::ListIndices() const {
  std::vector<VectorEdgeIndexSpec> result;
  result.reserve(index_->size());
  r::transform(
      *index_, std::back_inserter(result), [](const auto &id_index_item) { return id_index_item.second->spec; });
  return result;
}

std::optional<uint64_t> VectorEdgeIndex::ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const {
  auto it = r::find_if(*index_, [&](const auto &id_index_item) {
    const auto &spec = id_index_item.second->spec;
    return spec.edge_type_id == edge_type && spec.property == property;
  });
  if (it != index_->end()) {
    auto guard = utils::SharedResourceLockGuard(it->second->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    return it->second->mg_index.index.size();
  }
  return std::nullopt;
}

VectorEdgeIndex::VectorSearchEdgeResults VectorEdgeIndex::SearchEdges(std::string_view index_name,
                                                                      uint64_t result_set_size,
                                                                      const std::vector<float> &query_vector) const {
  auto maybe_id = std::invoke([&]() -> std::optional<uint64_t> {
    for (const auto &[id, item_ptr] : *index_) {
      if (item_ptr->spec.index_name == index_name) return id;
    }
    return std::nullopt;
  });
  if (!maybe_id.has_value()) {
    throw query::VectorSearchException("Vector edge index {} does not exist.", index_name);
  }
  auto &item_ptr = index_->at(*maybe_id);
  auto &mg_index = item_ptr->mg_index;

  VectorSearchEdgeResults result;
  result.reserve(result_set_size);

  auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  auto ep_lock = std::shared_lock{edge_endpoints_mutex_};
  const auto result_keys = mg_index.index.filtered_search(
      query_vector.data(), result_set_size, [this](Edge *edge) { return edge_endpoints_.contains(edge); });
  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    auto *edge = static_cast<Edge *>(result_keys[i].member.key);
    auto [from_vertex, to_vertex] = edge_endpoints_.at(edge);
    result.emplace_back(
        VectorEdgeIndex::EdgeIndexEntry{.from_vertex = from_vertex, .to_vertex = to_vertex, .edge = edge},
        static_cast<double>(result_keys[i].distance),
        std::abs(SimilarityFromDistance(mg_index.index.metric().metric_kind(), result_keys[i].distance)));
  }

  return result;
}

void VectorEdgeIndex::AbortEntries(AbortProcessor::AbortableInfo &cleanup_collection) {
  for (auto &[edge, info] : cleanup_collection) {
    for (const auto &[property, old_value] : info.properties) {
      if (old_value.IsVectorIndexId()) {
        const auto &vector_property = old_value.ValueVectorIndexList();
        const auto &index_ids = old_value.ValueVectorIndexIds();
        // Lock order: uSearch mutex (inside UpdateVectorIndex) → edge_endpoints_mutex_
        for (auto index_id : index_ids) {
          auto &item_ptr = index_->at(index_id);
          storage::UpdateVectorIndex(item_ptr->mg_index, item_ptr->spec, edge, vector_property);
        }
        {
          auto lock = std::unique_lock{edge_endpoints_mutex_};
          edge_endpoints_[edge] = {info.from_vertex, info.to_vertex};
        }
      } else {
        DMG_ASSERT(old_value.IsNull(), "Unexpected property value type in abort processor of vector edge index");
        auto indices_by_prop = GetIndicesByProperty(property);
        for (const auto &[et, idx_id] : indices_by_prop) {
          if (et != info.edge_type) continue;
          RemoveEdgeFromIndex(edge, idx_id);
        }
      }
    }
  }
}

bool VectorEdgeIndex::Empty() const { return index_->empty(); }

void VectorEdgeIndex::RemoveEdges(std::vector<Edge *> const &edges_to_remove) {
  if (edges_to_remove.empty()) return;

  // Lock order: uSearch mutex → edge_endpoints_mutex_ (matches SearchEdges)
  for (const auto &[_, item_ptr] : *index_) {
    auto guard = std::lock_guard{item_ptr->mg_index.mutex};
    for (auto *edge : edges_to_remove) {
      if (item_ptr->mg_index.index.contains(edge)) {
        item_ptr->mg_index.index.remove(edge);
      }
    }
  }

  {
    auto lock = std::unique_lock{edge_endpoints_mutex_};
    for (auto *edge : edges_to_remove) {
      edge_endpoints_.erase(edge);
    }
  }
}

VectorEdgeIndex::AbortProcessor VectorEdgeIndex::GetAbortProcessor() const {
  AbortProcessor res{};
  for (const auto &[_, item_ptr] : *index_) {
    const auto edge_type = item_ptr->spec.edge_type_id;
    const auto property = item_ptr->spec.property;
    res.et2p[edge_type].push_back(property);
    res.p2et[property].push_back(edge_type);
  }
  return res;
}

void VectorEdgeIndex::AbortProcessor::CollectOnPropertyChange(EdgeTypeId edge_type, PropertyId property,
                                                              const PropertyValue &old_value, Vertex *from_vertex,
                                                              Vertex *to_vertex, Edge *edge) {
  auto edge_types = p2et.find(property);
  if (edge_types == p2et.end() || !r::contains(edge_types->second, edge_type)) return;

  auto &info = cleanup_collection[edge];
  info.edge_type = edge_type;
  info.from_vertex = from_vertex;
  info.to_vertex = to_vertex;
  info.properties[property] = old_value;
}

EdgeTypeId VectorEdgeIndex::GetEdgeTypeId(std::string_view index_name) {
  for (const auto &[_, item_ptr] : *index_) {
    if (item_ptr->spec.index_name == index_name) {
      return item_ptr->spec.edge_type_id;
    }
  }
  throw query::VectorSearchException("Vector edge index {} does not exist.", index_name);
}

bool VectorEdgeIndex::IndexExists(std::string_view index_name) const {
  return r::any_of(*index_, [&](const auto &id_item) { return id_item.second->spec.index_name == index_name; });
}

utils::small_vector<float> VectorEdgeIndex::GetVectorPropertyFromEdgeIndex(Edge *edge, std::string_view index_name,
                                                                           NameIdMapper *name_id_mapper) const {
  auto maybe_id = name_id_mapper->NameToIdIfExists(index_name);
  if (!maybe_id.has_value()) {
    throw query::VectorSearchException("Vector edge index {} does not exist.", index_name);
  }
  auto it = index_->find(*maybe_id);
  if (it == index_->end()) {
    throw query::VectorSearchException("Vector edge index {} does not exist.", index_name);
  }
  auto &item_ptr = it->second;
  auto guard = utils::SharedResourceLockGuard(item_ptr->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  utils::small_vector<float> vector(item_ptr->mg_index.index.dimensions());
  if (!item_ptr->mg_index.index.get(edge, vector.data())) return {};
  return vector;
}

std::pair<Vertex *, Vertex *> VectorEdgeIndex::GetEdgeEndpoints(Edge *edge) const {
  auto lock = std::shared_lock{edge_endpoints_mutex_};
  auto it = edge_endpoints_.find(edge);
  DMG_ASSERT(it != edge_endpoints_.end(), "Edge not found in endpoints map");
  return it->second;
}

std::optional<uint64_t> VectorEdgeIndex::GetIndexIdForEdgeTypeProperty(EdgeTypeId edge_type,
                                                                       PropertyId property) const {
  for (const auto &[index_id, item_ptr] : *index_) {
    if (item_ptr->spec.edge_type_id == edge_type && item_ptr->spec.property == property) {
      return index_id;
    }
  }
  return std::nullopt;
}

std::unordered_map<EdgeTypeId, uint64_t> VectorEdgeIndex::GetIndicesByProperty(PropertyId property) const {
  std::unordered_map<EdgeTypeId, uint64_t> result;
  for (const auto &[index_id, item_ptr] : *index_) {
    if (item_ptr->spec.property == property) {
      result.emplace(item_ptr->spec.edge_type_id, index_id);
    }
  }
  return result;
}

void VectorEdgeIndex::SerializeAllVectorEdgeIndices(durability::BaseEncoder *encoder,
                                                    std::unordered_set<uint64_t> &mapped_ids) const {
  auto write_mapping = [&](auto mapping) {
    mapped_ids.insert(mapping.AsUint());
    encoder->WriteUint(mapping.AsUint());
  };

  encoder->WriteUint(index_->size());
  for (const auto &[_, item_ptr] : *index_) {
    auto &spec = item_ptr->spec;
    auto &mg_index = item_ptr->mg_index;
    encoder->WriteString(spec.index_name);
    write_mapping(spec.edge_type_id);
    write_mapping(spec.property);
    encoder->WriteString(NameFromMetric(spec.metric_kind));
    encoder->WriteUint(spec.dimension);
    encoder->WriteUint(spec.resize_coefficient);
    encoder->WriteUint(spec.capacity);
    encoder->WriteUint(static_cast<uint64_t>(spec.scalar_kind));

    using Entry = std::pair<uint64_t, std::vector<float>>;
    auto const entries = std::invoke([&mg_index]() -> std::vector<Entry> {
      auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
      auto const size = mg_index.index.size();
      if (size == 0) return {};

      std::vector<Edge *> keys(size);
      mg_index.index.export_keys(keys.data(), 0, size);

      std::vector<Entry> result;
      result.reserve(size);
      std::vector<float> buffer(mg_index.index.dimensions());
      for (auto *edge : keys) {
        if (edge == nullptr || edge->deleted()) continue;
        if (!mg_index.index.get(edge, buffer.data())) continue;
        result.emplace_back(edge->gid.AsUint(), buffer);
      }
      return result;
    });

    encoder->WriteUint(entries.size());
    for (const auto &[gid, vector] : entries) {
      encoder->WriteUint(gid);
      for (auto value : vector) encoder->WriteDouble(value);
    }
  }
}

// VectorEdgeIndexRecovery implementation
void VectorEdgeIndexRecovery::UpdateOnIndexDrop(std::string_view index_name, NameIdMapper *name_id_mapper,
                                                std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec,
                                                utils::SkipList<Vertex>::Accessor &vertices) {
  for (auto &recovery_info : recovery_info_vec) {
    if (recovery_info.spec.index_name == index_name) {
      auto maybe_index_id = name_id_mapper->NameToIdIfExists(index_name);
      DMG_ASSERT(maybe_index_id.has_value(), "Index name not found in name-id mapper during recovery drop");
      auto index_id = *maybe_index_id;
      // Iterate all vertices to find edges and restore properties
      for (auto &vertex : vertices) {
        for (auto &edge_tuple : vertex.out_edges) {
          if (std::get<kEdgeTypeIdPos>(edge_tuple) != recovery_info.spec.edge_type_id) continue;
          auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;

          auto it = recovery_info.index_entries.find(edge->gid);
          if (it == recovery_info.index_entries.end()) continue;

          auto edge_property = edge->properties.GetProperty(recovery_info.spec.property);
          if (UnregisterIndexId(edge_property, index_id)) {
            edge->properties.SetProperty(recovery_info.spec.property,
                                         PropertyValue(std::vector<double>(it->second.begin(), it->second.end())));
          } else {
            edge->properties.SetProperty(recovery_info.spec.property, edge_property);
          }
        }
      }
    }
  }
  std::erase_if(recovery_info_vec, [&](const auto &ri) { return ri.spec.index_name == index_name; });
}

void VectorEdgeIndexRecovery::UpdateOnSetEdgeProperty(PropertyId property, const PropertyValue &value, const Edge *edge,
                                                      std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec) {
  const auto maybe_vector = std::invoke([&]() -> std::optional<utils::small_vector<float>> {
    if (value.IsVectorIndexId()) return value.ValueVectorIndexList();
    return TryListToVector(value);
  });
  if (!maybe_vector) return;

  for (auto &ri : recovery_info_vec) {
    if (ri.spec.property == property) {
      ri.index_entries[edge->gid] = *maybe_vector;
    }
  }
}

// ---- VectorEdgeIndex::ActiveIndices (live shared reference) ----

std::vector<VectorEdgeIndexSpec> VectorEdgeIndex::ActiveIndices::ListIndices() const {
  if (!index_container_) return {};
  std::vector<VectorEdgeIndexSpec> result;
  result.reserve(index_container_->size());
  r::transform(*index_container_, std::back_inserter(result), [](const auto &id_item) { return id_item.second->spec; });
  return result;
}

std::vector<VectorEdgeIndexInfo> VectorEdgeIndex::ActiveIndices::ListVectorIndicesInfo() const {
  if (!index_container_) return {};
  std::vector<VectorEdgeIndexInfo> result;
  result.reserve(index_container_->size());
  for (const auto &[_, item_ptr] : *index_container_) {
    auto &mg_index = item_ptr->mg_index;
    auto &spec = item_ptr->spec;
    auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    result.emplace_back(spec.index_name,
                        spec.edge_type_id,
                        spec.property,
                        NameFromMetric(mg_index.index.metric().metric_kind()),
                        static_cast<std::uint16_t>(mg_index.index.dimensions()),
                        mg_index.index.capacity(),
                        mg_index.index.size(),
                        NameFromScalar(mg_index.index.metric().scalar_kind()));
  }
  return result;
}

std::optional<uint64_t> VectorEdgeIndex::ActiveIndices::ApproximateEdgesVectorCount(EdgeTypeId edge_type,
                                                                                    PropertyId property) const {
  if (!index_container_) return std::nullopt;
  auto it = r::find_if(*index_container_, [&](const auto &id_item) {
    const auto &spec = id_item.second->spec;
    return spec.edge_type_id == edge_type && spec.property == property;
  });
  if (it != index_container_->end()) {
    auto guard = utils::SharedResourceLockGuard(it->second->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    return it->second->mg_index.index.size();
  }
  return std::nullopt;
}

}  // namespace memgraph::storage
