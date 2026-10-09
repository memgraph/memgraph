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
#include <string>
#include <unordered_set>
#include <utility>

#include <spdlog/spdlog.h>

#include "flags/general.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/exceptions.hpp"
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

VectorEdgeIndex::VectorEdgeIndex(utils::MemoryTracker *memory_tracker) : memory_tracker_(memory_tracker) {}

VectorEdgeIndex::~VectorEdgeIndex() = default;

void VectorEdgeIndex::PublishActiveIndices(ActiveIndicesUpdater const &updater) const { updater(GetActiveIndices()); }

std::optional<uint64_t> VectorEdgeIndex::SetupIndex(const VectorEdgeIndexSpec &spec, NameIdMapper *name_id_mapper) {
  const auto index_id = name_id_mapper->NameToId(spec.index_name);
  if (index_->contains(index_id)) {
    return std::nullopt;
  }
  if (r::any_of(*index_, [&](const auto &id_index_item) {
        auto &index_spec = id_index_item.second->spec;
        return spec.edge_type_filter == index_spec.edge_type_filter && spec.property == index_spec.property;
      })) {
    return std::nullopt;
  }

  const unum::usearch::metric_punned_t metric(spec.dimension, spec.metric_kind, spec.scalar_kind);
  const unum::usearch::index_limits_t limits(spec.capacity, GetVectorIndexThreadCount());

  // Create allocators with the database-specific memory tracker
  TrackedVectorAllocator<64> tape_allocator{memory_tracker_};
  TrackedVectorAllocator<8> vectors_tape_allocator{memory_tracker_};

  auto mg_edge_index =
      mg_vector_edge_index_t::make(metric, {}, {}, std::move(tape_allocator), std::move(vectors_tape_allocator));
  if (!mg_edge_index) {
    throw VectorSearchException(fmt::format(
        "Failed to create vector edge index {}, error message: {}", spec.index_name, mg_edge_index.error.what()));
  }

  if (!mg_edge_index.index.try_reserve(limits)) {
    throw VectorSearchException(
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

void VectorEdgeIndex::AddEdgeToIndex(uint64_t index_id, Edge *edge, EdgeTypeId edge_type, Vertex *from_vertex,
                                     Vertex *to_vertex, NameIdMapper *name_id_mapper,
                                     std::optional<std::size_t> thread_id) {
  auto it = index_->find(index_id);
  if (it == index_->end()) {
    throw VectorSearchException(fmt::format("Vector edge index {} does not exist.", index_id));
  }
  auto &item_ptr = it->second;
  auto &spec = item_ptr->spec;
  auto property = edge->properties.GetProperty(spec.property);
  if (property.IsNull()) return;
  // An empty plain list has nothing to index and stays a plain list; never promote it to a tag.
  if (!property.IsVectorIndexId() && property.IsAnyList() && property.ListSize() == 0) return;
  // an edge already indexed by another vector-edge index stores no inline vector; recover it from that
  // index's uSearch so RegisterIndexId re-registers the real vector — else this second index gets nothing
  if (property.IsVectorIndexId()) {
    DMG_ASSERT(!property.ValueVectorIndexIds().empty(), "VectorIndexId property has no index IDs");
    property.ValueVectorIndexList() = GetVectorPropertyFromEdgeIndex(
        edge, name_id_mapper->IdToName(property.ValueVectorIndexIds()[0]), name_id_mapper);
  }

  auto vector = RegisterIndexId(property, index_id);
  edge->properties.SetProperty(spec.property, property);

  // Lock order: uSearch mutex (inside UpdateVectorIndex) → edge_endpoints_mutex_
  UpdateVectorIndex(item_ptr->mg_index, spec, edge, vector, thread_id);
  {
    auto lock = std::unique_lock{edge_endpoints_mutex_};
    edge_endpoints_[edge] = EdgeEndpoints{.from_vertex = from_vertex, .to_vertex = to_vertex, .edge_type = edge_type};
  }
}

bool VectorEdgeIndex::CreateIndex(const VectorEdgeIndexSpec &spec, utils::SkipListDb<Vertex>::Accessor &vertices,
                                  NameIdMapper *name_id_mapper, ProgressCallback const &on_progress) {
  try {
    const auto index_id = SetupIndex(spec, name_id_mapper);
    if (!index_id.has_value()) return false;
    PopulateVectorIndexSingleThreaded(vertices, [&](Vertex &vertex, std::optional<std::size_t> thread_id) {
      if (vertex.deleted()) return;
      for (auto &edge_tuple : vertex.out_edges) {
        const auto edge_type = std::get<kEdgeTypeIdPos>(edge_tuple);
        if (!spec.edge_type_filter.Matches(edge_type)) continue;

        auto *to_vertex = std::get<kVertexPos>(edge_tuple);
        auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;
        if (edge->deleted() || to_vertex->deleted()) continue;

        AddEdgeToIndex(*index_id, edge, edge_type, &vertex, to_vertex, name_id_mapper, thread_id);
        if (on_progress) on_progress();
      }
    });
    return true;
  } catch (const std::exception &) {
    DropIndex(spec.index_name, name_id_mapper);
    throw;
  }
}

void VectorEdgeIndex::RecoverAllVectorEdgeIndices(std::vector<VectorEdgeIndexRecoveryInfo> &recovery_infos,
                                                  VectorEdgeIndexRecovery::EdgeVectors &edge_vectors,
                                                  utils::SkipListDb<Vertex>::Accessor &vertices,
                                                  NameIdMapper *name_id_mapper, ActiveIndicesUpdater const &updater,
                                                  ProgressCallback const &on_progress) {
  if (recovery_infos.empty()) return;

  absl::flat_hash_map<PropertyId, std::vector<std::pair<uint64_t, std::shared_ptr<EdgeTypeIndexItem>>>> prop_to_items;
  try {
    for (auto &ri : recovery_infos) {
      auto index_id = SetupIndex(ri.spec, name_id_mapper);
      if (!index_id.has_value()) {
        throw VectorSearchException(fmt::format(
            "Vector edge index '{}' already exists. Corrupted or invalid recovery files.", ri.spec.index_name));
      }
      prop_to_items[ri.spec.property].emplace_back(*index_id, index_->at(*index_id));
    }

    auto find_captured_vector = [&](PropertyId property, Gid gid) -> utils::small_vector<float> * {
      auto map_it = edge_vectors.find(property);
      if (map_it == edge_vectors.end()) return nullptr;
      auto entry_it = map_it->second.find(gid);
      if (entry_it == map_it->second.end()) return nullptr;
      return &entry_it->second;
    };

    // No structural changes to edge_vectors (no insert/erase on either map level) — only the
    // small_vector value per gid is consumed, so the multi-threaded path needs no lock on the map.
    auto process_edge = [&](Edge *edge, EdgeEndpoints endpoints, std::optional<std::size_t> thread_id) {
      for (auto &[property, item_list] : prop_to_items) {
        auto stored_value = edge->properties.GetProperty(property);
        if (stored_value.IsNull()) continue;

        const bool stored_as_tag = stored_value.IsVectorIndexId();
        utils::small_vector<float> vec;

        if (stored_as_tag) {
          auto *entry_ptr = find_captured_vector(property, edge->gid);
          if (!entry_ptr) {
            spdlog::error(
                "Recovery: edge {} property {} stored as tag but missing vector — "
                "data was lost before this build; storing null.",
                edge->gid.AsUint(),
                name_id_mapper->IdToName(property.AsUint()));
            edge->properties.SetProperty(property, PropertyValue());
            continue;
          }
          vec = std::exchange(*entry_ptr, {});
        } else {
          auto maybe_vec = TryListToVector(stored_value);
          if (!maybe_vec) continue;
          vec = std::move(*maybe_vec);
        }
        // A plain [] is a legitimate value with nothing to index; it stays a plain list and is never
        // promoted to a tag (a tag with no usearch entry would read as lost on the next recovery).
        if (vec.empty()) continue;

        utils::small_vector<uint64_t> member_ids;
        for (auto &[index_id, item_ptr] : item_list) {
          if (!item_ptr->spec.edge_type_filter.Matches(endpoints.edge_type)) continue;
          UpdateVectorIndex(item_ptr->mg_index, item_ptr->spec, edge, vec, thread_id);
          member_ids.push_back(index_id);
        }

        if (!member_ids.empty()) {
          // Every usearch mutex is released by now, so taking edge_endpoints_mutex_ here keeps the lock order.
          {
            auto lock = std::unique_lock{edge_endpoints_mutex_};
            edge_endpoints_[edge] = endpoints;
          }
          const bool already_correct =
              stored_as_tag && std::ranges::is_permutation(stored_value.ValueVectorIndexIds(), member_ids);
          if (!already_correct) {
            // The property store persists only the ids; the vector already lives in usearch.
            edge->properties.SetProperty(
                property, PropertyValue(PropertyValue::VectorIndexIdData{.ids = std::move(member_ids), .vector = {}}));
          }
        } else if (stored_as_tag) {
          edge->properties.SetProperty(property, PropertyValue(std::vector<double>(vec.begin(), vec.end())));
        }
      }
    };

    auto process_vertex = [&](Vertex &vertex, std::optional<std::size_t> thread_id) {
      if (!vertex.deleted()) {
        for (auto &edge_tuple : vertex.out_edges) {
          const auto edge_type = std::get<kEdgeTypeIdPos>(edge_tuple);
          auto *to_vertex = std::get<kVertexPos>(edge_tuple);
          auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;
          if (to_vertex->deleted() || edge->deleted()) continue;
          process_edge(
              edge, EdgeEndpoints{.from_vertex = &vertex, .to_vertex = to_vertex, .edge_type = edge_type}, thread_id);
        }
      }
      if (on_progress) on_progress();
    };

    if (FLAGS_storage_parallel_schema_recovery && FLAGS_storage_recovery_thread_count > 1) {
      PopulateVectorIndexMultiThreaded(vertices, process_vertex);
    } else {
      PopulateVectorIndexSingleThreaded(vertices, process_vertex);
    }

    edge_vectors.clear();
  } catch (const std::exception &) {
    for (auto &ri : recovery_infos) {
      try {
        DropIndex(ri.spec.index_name, name_id_mapper);
      } catch (const std::exception &e) {
        spdlog::warn("Failed to drop vector edge index '{}' after recovery failure: {}", ri.spec.index_name, e.what());
      }
    }
    throw;
  }

  updater(GetActiveIndices());
}

std::optional<VectorEdgeIndex::DroppedIndexCapture> VectorEdgeIndex::DropIndex(std::string_view index_name,
                                                                               NameIdMapper *name_id_mapper,
                                                                               ProgressCallback const &on_progress) {
  auto maybe_id = name_id_mapper->NameToIdIfExists(index_name);
  if (!maybe_id.has_value()) return std::nullopt;
  const auto index_id = *maybe_id;
  auto it = index_->find(index_id);
  if (it == index_->end()) return std::nullopt;
  auto evicted_item = it->second;  // keep usearch state alive
  auto &mg_index = evicted_item->mg_index;
  auto &spec = evicted_item->spec;

  std::vector<Edge *> dropped_edges;
  {
    auto guard = std::lock_guard{mg_index.mutex};

    const auto dimension = mg_index.index.dimensions();
    CheckGraphMemoryForIndexDrop(index_name, mg_index.index.size(), dimension);

    auto const index_size = mg_index.index.size();
    dropped_edges.resize(index_size);
    mg_index.index.export_keys(dropped_edges.data(), 0, index_size);

    // Convert indexed vectors back to property values with OOM protection.
    // Track processed edges so we can roll back on OOM.
    std::size_t processed = 0;
    try {
      const utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_enabler;
      std::vector<double> vector(dimension);
      for (auto *edge : dropped_edges) {
        if (on_progress) on_progress();
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
      for (std::size_t i = 0; i < processed; ++i) ReinstallIndexIdInProperty(dropped_edges[i], spec.property, index_id);
      throw;
    }
  }
  auto new_map = std::make_shared<VectorEdgeIndexContainer>(*index_);
  new_map->erase(index_id);
  index_ = new_map;

  // Remove endpoints for edges no longer indexed elsewhere; capture the evicted
  // entries so RestoreIndex can put them back.
  std::unordered_set<Edge *> still_indexed;
  for (const auto &[_, iptr] : *index_) {
    auto guard = utils::SharedResourceLockGuard(iptr->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    for (auto *edge : dropped_edges) {
      if (iptr->mg_index.index.contains(edge)) still_indexed.insert(edge);
    }
  }
  std::vector<std::pair<Edge *, EdgeEndpoints>> evicted_endpoints;
  {
    auto lock = std::unique_lock{edge_endpoints_mutex_};
    for (auto *edge : dropped_edges) {
      DMG_ASSERT(edge != nullptr, "Null edge pointer in vector edge index");
      if (still_indexed.contains(edge)) continue;
      if (auto ep_it = edge_endpoints_.find(edge); ep_it != edge_endpoints_.end()) {
        evicted_endpoints.emplace_back(edge, ep_it->second);
        edge_endpoints_.erase(ep_it);
      }
    }
  }
  return DroppedIndexCapture{.index_id = index_id,
                             .evicted_item = std::move(evicted_item),
                             .rewritten_edges = std::move(dropped_edges),
                             .evicted_endpoints = std::move(evicted_endpoints)};
}

void VectorEdgeIndex::RestoreIndex(DroppedIndexCapture &&capture) {
  // Abort path: must not propagate OOM (called from a noexcept abort callback).
  const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
  for (auto *edge : capture.rewritten_edges) {
    ReinstallIndexIdInProperty(edge, capture.evicted_item->spec.property, capture.index_id);
  }
  auto new_map = std::make_shared<VectorEdgeIndexContainer>(*index_);
  new_map->try_emplace(capture.index_id, std::move(capture.evicted_item));
  index_ = new_map;
  {
    auto lock = std::unique_lock{edge_endpoints_mutex_};
    for (auto &[edge, endpoints] : capture.evicted_endpoints) {
      edge_endpoints_.try_emplace(edge, endpoints);
    }
  }
}

void VectorEdgeIndex::Clear() {
  index_ = std::make_shared<VectorEdgeIndexContainer>();
  auto lock = std::unique_lock{edge_endpoints_mutex_};
  edge_endpoints_.clear();
}

void VectorEdgeIndex::UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                                          PropertyId property, const PropertyValue &value) {
  // No vector edge indexes: a value can only be a vector-index id when one exists, so there is nothing to do.
  if (index_->empty()) return;
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
      edge_endpoints_[edge] = EdgeEndpoints{.from_vertex = from_vertex, .to_vertex = to_vertex, .edge_type = edge_type};
    }
  } else {
    const auto indices = GetIndicesByProperty(property);
    for (const auto &[idx_id, filter] : indices) {
      if (!filter->Matches(edge_type)) continue;
      RemoveEdgeFromIndex(edge, idx_id);
    }
    EraseEndpointsIfUnreferenced(edge);
  }
}

void VectorEdgeIndex::EraseEndpointsIfUnreferenced(Edge *edge) {
  const auto still_indexed = std::ranges::any_of(*index_, [&](const auto &kv) {
    auto guard = utils::SharedResourceLockGuard(kv.second->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    return kv.second->mg_index.index.contains(edge);
  });
  if (still_indexed) return;
  auto lock = std::unique_lock{edge_endpoints_mutex_};
  edge_endpoints_.erase(edge);
}

void VectorEdgeIndex::RemoveEdgeFromIndex(Edge *edge, uint64_t index_id) {
  auto it = index_->find(index_id);
  if (it == index_->end()) {
    throw VectorSearchException(
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
                        spec.edge_type_filter,
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

std::optional<uint64_t> VectorEdgeIndex::ApproximateEdgesVectorCount(std::string_view index_name) const {
  auto it = r::find_if(*index_, [&](const auto &id_item) { return id_item.second->spec.index_name == index_name; });
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
    throw VectorSearchException("Vector edge index {} does not exist.", index_name);
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
    auto [from_vertex, to_vertex, edge_type] = edge_endpoints_.at(edge);
    result.emplace_back(
        VectorEdgeIndex::EdgeIndexEntry{
            .from_vertex = from_vertex, .to_vertex = to_vertex, .edge = edge, .edge_type = edge_type},
        static_cast<double>(result_keys[i].distance),
        std::abs(SimilarityFromDistance(mg_index.index.metric().metric_kind(), result_keys[i].distance)));
  }

  return result;
}

bool VectorEdgeIndex::HasIndexOnProperty(PropertyId property) const { return AnyIndexOnProperty(*index_, property); }

void VectorEdgeIndex::DropEntries(Edge *edge, PropertyId property) {
  for (const auto &[index_id, _] : GetIndicesByProperty(property)) {
    RemoveEdgeFromIndex(edge, index_id);
  }
  EraseEndpointsIfUnreferenced(edge);
}

void VectorEdgeIndex::RestoreOnSetProperty(Vertex *from_vertex, Edge *edge, PropertyId property,
                                           const PropertyValue &before,
                                           std::optional<std::pair<EdgeTypeId, Vertex *>> link) {
  if (!HasIndexOnProperty(property)) return;
  // A non-tag value is in no index, so no endpoints are needed; every index on the property is cleared, not only
  // those matching the edge type, in case a forward write left an entry the filter no longer admits.
  if (!before.IsVectorIndexId()) {
    DropEntries(edge, property);
    return;
  }
  if (!link) {
    // The link is gone (e.g. the edge was deleted and its deltas hold no type): fall back to the recorded one.
    auto lock = std::shared_lock{edge_endpoints_mutex_};
    const auto it = edge_endpoints_.find(edge);
    // Nothing recorded to restore against: skipped, as the deferred pass did.
    if (it == edge_endpoints_.end()) return;
    link = std::pair{it->second.edge_type, it->second.to_vertex};
  }
  UpdateOnSetProperty(from_vertex, link->second, edge, link->first, property, before);
}

bool VectorEdgeIndex::Empty() const { return index_->empty(); }

void VectorEdgeIndex::RemoveEdges(std::span<Edge *const> edges_to_remove) const {
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

bool VectorEdgeIndex::IndexExists(std::string_view index_name) const {
  return r::any_of(*index_, [&](const auto &id_item) { return id_item.second->spec.index_name == index_name; });
}

utils::small_vector<float> VectorEdgeIndex::GetVectorPropertyFromEdgeIndex(Edge *edge, std::string_view index_name,
                                                                           NameIdMapper *name_id_mapper) const {
  auto maybe_id = name_id_mapper->NameToIdIfExists(index_name);
  if (!maybe_id.has_value()) {
    throw VectorSearchException("Vector edge index {} does not exist.", index_name);
  }
  auto it = index_->find(*maybe_id);
  if (it == index_->end()) {
    throw VectorSearchException("Vector edge index {} does not exist.", index_name);
  }
  auto &item_ptr = it->second;
  auto guard = utils::SharedResourceLockGuard(item_ptr->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  utils::small_vector<float> vector(item_ptr->mg_index.index.dimensions());
  if (!item_ptr->mg_index.index.get(edge, vector.data())) return {};
  return vector;
}

utils::small_vector<uint64_t> VectorEdgeIndex::GetIndexIdsForEdgeTypeProperty(EdgeTypeId edge_type,
                                                                              PropertyId property) const {
  utils::small_vector<uint64_t> result;
  for (const auto &[index_id, item_ptr] : *index_) {
    if (item_ptr->spec.property != property) continue;
    if (item_ptr->spec.edge_type_filter.Matches(edge_type)) {
      result.push_back(index_id);
    }
  }
  return result;
}

std::vector<std::pair<uint64_t, VectorEdgeTypeFilter const *>> VectorEdgeIndex::GetIndicesByProperty(
    PropertyId property) const {
  std::vector<std::pair<uint64_t, VectorEdgeTypeFilter const *>> result;
  result.reserve(index_->size());
  for (const auto &[index_id, item_ptr] : *index_) {
    if (item_ptr->spec.property == property) {
      result.emplace_back(index_id, &item_ptr->spec.edge_type_filter);
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
    encoder->WriteUint(static_cast<uint64_t>(spec.edge_type_filter.mode));
    encoder->WriteUint(spec.edge_type_filter.ids.size());
    for (const auto &edge_type : spec.edge_type_filter.ids) {
      write_mapping(edge_type);
    }
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
void VectorEdgeIndexRecovery::UpdateOnSetEdgeProperty(PropertyId property, PropertyValue &value, const Edge *edge,
                                                      std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec,
                                                      EdgeVectors &edge_vectors) {
  // A tag with no vector is the legacy on-disk form of []; treat it as the plain empty list it stands for.
  if (value.IsVectorIndexId() && value.ValueVectorIndexList().empty()) {
    value = PropertyValue(std::vector<double>{});
  }

  const bool has_spec = r::any_of(recovery_info_vec, [&](const auto &ri) { return ri.spec.property == property; });

  if (has_spec) {
    if (value.IsVectorIndexId()) {
      edge_vectors[property][edge->gid] = value.ValueVectorIndexList();
    } else if (auto it = edge_vectors.find(property); it != edge_vectors.end()) {
      it->second.erase(edge->gid);
    }
  } else {
    // No active spec for this property. A tag value here is a stale artifact (its index was
    // dropped before this WAL record). Convert it to a plain list so the final build sees no tag.
    if (value.IsVectorIndexId()) {
      auto vec = value.ValueVectorIndexList();
      value = PropertyValue(std::vector<double>(vec.begin(), vec.end()));
    }
  }
}

void VectorEdgeIndexRecovery::UpdateOnIndexDrop(std::string_view index_name_view,
                                                std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec,
                                                EdgeVectors &edge_vectors,
                                                utils::SkipListDb<Vertex>::Accessor &vertices,
                                                NameIdMapper *name_id_mapper) {
  // The view may alias the spec erased below; keep an owning copy for everything after the erase.
  const std::string index_name{index_name_view};
  auto it = r::find_if(recovery_info_vec, [&](const auto &ri) { return ri.spec.index_name == index_name; });
  if (it == recovery_info_vec.end()) return;
  const PropertyId property = it->spec.property;
  recovery_info_vec.erase(it);

  // If another spec still covers this property, edge_vectors[property] remains valid for the final
  // build. Only when no spec remains must we restore stored tags to plain lists and drop the map entry.
  const bool other_spec_on_property =
      r::any_of(recovery_info_vec, [&](const auto &ri) { return ri.spec.property == property; });
  if (other_spec_on_property) return;

  auto map_it = edge_vectors.find(property);

  // A tag on this property is an artifact of the dropped index; demote to a plain list
  // (or null if no vector is available).
  for (auto &vertex : vertices) {
    if (vertex.deleted()) continue;
    for (auto &edge_tuple : vertex.out_edges) {
      auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;
      if (edge->deleted()) continue;
      auto stored = edge->properties.GetProperty(property);
      if (!stored.IsVectorIndexId()) continue;

      if (map_it != edge_vectors.end()) {
        auto entry_it = map_it->second.find(edge->gid);
        if (entry_it != map_it->second.end()) {
          edge->properties.SetProperty(
              property, PropertyValue(std::vector<double>(entry_it->second.begin(), entry_it->second.end())));
          continue;
        }
      }
      spdlog::error(
          "Recovery: edge {} property {} carries a VectorIndexId tag for dropped index '{}' "
          "but has no vector entry — data was lost before this drop; setting to null.",
          edge->gid.AsUint(),
          name_id_mapper->IdToName(property.AsUint()),
          index_name);
      edge->properties.SetProperty(property, PropertyValue());
    }
  }

  if (map_it != edge_vectors.end()) {
    edge_vectors.erase(map_it);
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
                        spec.edge_type_filter,
                        spec.property,
                        NameFromMetric(mg_index.index.metric().metric_kind()),
                        static_cast<std::uint16_t>(mg_index.index.dimensions()),
                        mg_index.index.capacity(),
                        mg_index.index.size(),
                        NameFromScalar(mg_index.index.metric().scalar_kind()));
  }
  return result;
}

std::optional<uint64_t> VectorEdgeIndex::ActiveIndices::ApproximateEdgesVectorCount(std::string_view index_name) const {
  if (!index_container_) return std::nullopt;
  auto it =
      r::find_if(*index_container_, [&](const auto &id_item) { return id_item.second->spec.index_name == index_name; });
  if (it == index_container_->end()) return std::nullopt;
  auto guard = utils::SharedResourceLockGuard(it->second->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  return it->second->mg_index.index.size();
}

std::vector<PropertyId> VectorEdgeIndex::ActiveIndices::IndexedProperties(EdgeTypeId edge_type) const {
  if (!index_container_) return {};
  std::vector<PropertyId> result;
  for (const auto &[_, item_ptr] : *index_container_) {
    if (item_ptr->spec.edge_type_filter.Matches(edge_type)) result.push_back(item_ptr->spec.property);
  }
  return result;
}

}  // namespace memgraph::storage
