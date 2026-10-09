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

#include "storage/v2/indices/vector_index.hpp"

#include <range/v3/all.hpp>
#include "spdlog/spdlog.h"
#include "storage/v2/exceptions.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indexed_property_decoder.hpp"
#include "storage/v2/indices/active_indices_updater.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index_dense.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/resource_lock.hpp"
#include "utils/small_vector.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

VectorIndex::VectorIndex(utils::MemoryTracker *memory_tracker) : memory_tracker_(memory_tracker) {}

VectorIndex::~VectorIndex() = default;

void VectorIndex::PublishActiveIndices(ActiveIndicesUpdater const &updater) const { updater(GetActiveIndices()); }

bool VectorIndex::CreateIndex(VectorIndexSpec &spec, utils::SkipListDb<Vertex>::Accessor &vertices, Indices *indices,
                              NameIdMapper *name_id_mapper, ProgressCallback const &on_progress) {
  try {
    const auto index_id = SetupIndex(spec, name_id_mapper);
    if (!index_id.has_value()) return false;
    PopulateVectorIndexSingleThreaded(vertices, [&](Vertex &vertex, std::optional<std::size_t> thread_id) {
      AddVertexToIndex(
          *index_id,
          vertex,
          IndexedPropertyDecoder<Vertex>{.indices = indices, .name_id_mapper = name_id_mapper, .entity = &vertex},
          thread_id);
      if (on_progress) on_progress();
    });
    return true;
  } catch (const std::exception &) {
    DropIndex(spec.index_name, name_id_mapper);
    throw;
  }
}

std::optional<uint64_t> VectorIndex::SetupIndex(const VectorIndexSpec &spec, NameIdMapper *name_id_mapper) {
  const auto index_id = name_id_mapper->NameToId(spec.index_name);
  if (index_->contains(index_id)) {
    return std::nullopt;
  }
  if (r::any_of(*index_, [&](const auto &id_index_item) {
        auto &index_spec = id_index_item.second->spec;
        return spec.label_filter == index_spec.label_filter && spec.property == index_spec.property;
      })) {
    return std::nullopt;
  }

  const unum::usearch::metric_punned_t metric(spec.dimension, spec.metric_kind, spec.scalar_kind);
  const unum::usearch::index_limits_t limits(spec.capacity, GetVectorIndexThreadCount());

  // Create allocators with the database-specific memory tracker
  TrackedVectorAllocator<64> tape_allocator{memory_tracker_};
  TrackedVectorAllocator<8> vectors_tape_allocator{memory_tracker_};

  auto mg_vector_index =
      mg_vector_index_t::make(metric, {}, {}, std::move(tape_allocator), std::move(vectors_tape_allocator));
  if (!mg_vector_index) {
    throw VectorSearchException(fmt::format(
        "Failed to create vector index {}, error message: {}", spec.index_name, mg_vector_index.error.what()));
  }

  if (!mg_vector_index.index.try_reserve(limits)) {
    throw VectorSearchException(
        fmt::format("Failed to create vector index {}. Failed to reserve memory for the index", spec.index_name));
  }

  auto new_map = std::make_shared<VectorIndexContainer>(*index_);
  const auto [_, inserted] =
      new_map->try_emplace(index_id, std::make_shared<IndexItem>(std::move(mg_vector_index.index), spec));
  if (inserted) {
    index_ = new_map;
  }
  return inserted ? std::optional<uint64_t>{index_id} : std::nullopt;
}

void VectorIndex::RecoverAllVectorIndices(std::vector<VectorIndexRecoveryInfo> &recovery_infos,
                                          VectorIndexRecovery::VertexVectors &vertex_vectors,
                                          utils::SkipListDb<Vertex>::Accessor &vertices, NameIdMapper *name_id_mapper,
                                          ActiveIndicesUpdater const &updater, ProgressCallback const &on_progress) {
  if (recovery_infos.empty()) return;

  absl::flat_hash_map<PropertyId, std::vector<std::pair<uint64_t, std::shared_ptr<IndexItem>>>> prop_to_items;
  try {
    for (auto &ri : recovery_infos) {
      auto index_id = SetupIndex(ri.spec, name_id_mapper);
      if (!index_id.has_value()) {
        throw VectorSearchException(
            fmt::format("Vector index '{}' already exists. Corrupted or invalid recovery files.", ri.spec.index_name));
      }
      prop_to_items[ri.spec.property].emplace_back(*index_id, index_->at(*index_id));
    }

    // No structural changes to vertex_vectors (no insert/erase on either map level) — only
    // the small_vector *value* per gid is consumed, so the multi-threaded path needs no lock.
    auto process_vertex = [&](Vertex &vertex, std::optional<std::size_t> thread_id) {
      for (auto &[property, item_list] : prop_to_items) {
        auto stored_value = vertex.properties.GetProperty(property);
        if (stored_value.IsNull()) continue;

        const bool stored_as_tag = stored_value.IsVectorIndexId();
        utils::small_vector<float> vec;

        if (stored_as_tag) {
          utils::small_vector<float> *entry_ptr = nullptr;
          if (auto map_it = vertex_vectors.find(property); map_it != vertex_vectors.end()) {
            if (auto entry_it = map_it->second.find(vertex.gid); entry_it != map_it->second.end()) {
              entry_ptr = &entry_it->second;
            }
          }
          if (!entry_ptr) {
            spdlog::error(
                "Recovery: vertex {} property {} stored as tag but missing vector — "
                "data was lost before this build; storing null.",
                vertex.gid.AsUint(),
                name_id_mapper->IdToName(property.AsUint()));
            vertex.properties.SetProperty(property, PropertyValue());
            continue;
          }
          vec = std::exchange(*entry_ptr, {});
        } else {
          auto maybe_vec = TryListToVector(stored_value);
          if (!maybe_vec) continue;
          vec = std::move(*maybe_vec);
          // A plain [] stays plain and is never promoted to a tag.
          if (vec.empty()) continue;
        }

        utils::small_vector<uint64_t> member_ids;
        for (auto &[index_id, item_ptr] : item_list) {
          if (!item_ptr->spec.label_filter.Matches(vertex.labels)) continue;
          UpdateVectorIndex(item_ptr->mg_index, item_ptr->spec, &vertex, vec, thread_id);
          member_ids.push_back(index_id);
        }

        if (!member_ids.empty()) {
          const bool already_correct =
              stored_as_tag && std::ranges::is_permutation(stored_value.ValueVectorIndexIds(), member_ids);
          if (!already_correct) {
            vertex.properties.SetProperty(
                property, PropertyValue(PropertyValue::VectorIndexIdData{.ids = std::move(member_ids), .vector = {}}));
          }
        } else if (stored_as_tag) {
          vertex.properties.SetProperty(property, PropertyValue(std::vector<double>(vec.begin(), vec.end())));
        }
      }
      if (on_progress) on_progress();
    };

    if (FLAGS_storage_parallel_schema_recovery && FLAGS_storage_recovery_thread_count > 1) {
      PopulateVectorIndexMultiThreaded(vertices, process_vertex);
    } else {
      PopulateVectorIndexSingleThreaded(vertices, process_vertex);
    }

    vertex_vectors.clear();
  } catch (const std::exception &) {
    for (auto &ri : recovery_infos) {
      try {
        DropIndex(ri.spec.index_name, name_id_mapper);
      } catch (const std::exception &e) {
        spdlog::warn("Failed to drop vector index '{}' after recovery failure: {}", ri.spec.index_name, e.what());
      }
    }
    throw;
  }

  updater(GetActiveIndices());
}

void VectorIndex::AddVertexToIndex(uint64_t index_id, Vertex &vertex, const IndexedPropertyDecoder<Vertex> &decoder,
                                   std::optional<std::size_t> thread_id) {
  auto it = index_->find(index_id);
  if (it == index_->end()) {
    throw VectorSearchException(fmt::format("Vector index {} does not exist.", index_id));
  }
  auto &item_ptr = it->second;
  auto &spec = item_ptr->spec;
  if (!spec.label_filter.Matches(vertex.labels)) {
    return;
  }
  auto property = vertex.properties.GetProperty(spec.property, decoder);
  if (property.IsNull()) return;
  // An empty plain list has nothing to index and stays a plain list — never promote it.
  if (!property.IsVectorIndexId() && property.IsAnyList() && property.ListSize() == 0) return;

  auto vector = RegisterIndexId(property, index_id);
  vertex.properties.SetProperty(spec.property, property);
  UpdateVectorIndex(item_ptr->mg_index, spec, &vertex, vector, thread_id);
}

std::optional<VectorIndex::DroppedIndexCapture> VectorIndex::DropIndex(std::string_view index_name,
                                                                       NameIdMapper *name_id_mapper,
                                                                       ProgressCallback const &on_progress) {
  auto maybe_id = name_id_mapper->NameToIdIfExists(index_name);
  if (!maybe_id.has_value()) return std::nullopt;
  const auto index_id = *maybe_id;
  auto it = index_->find(index_id);
  if (it == index_->end()) return std::nullopt;
  auto evicted_item = it->second;  // keep IndexItem (and its usearch state) alive
  auto &mg_index = evicted_item->mg_index;
  auto &spec = evicted_item->spec;

  std::vector<Vertex *> rewritten_vertices;
  {
    auto guard = std::lock_guard{mg_index.mutex};

    const auto dimension = mg_index.index.dimensions();
    CheckGraphMemoryForIndexDrop(index_name, mg_index.index.size(), dimension);

    auto const index_size = mg_index.index.size();
    std::vector<Vertex *> indexed_vertices(index_size);
    mg_index.index.export_keys(indexed_vertices.data(), 0, index_size);

    // Convert indexed vectors back to property values with OOM protection.
    // Track processed vertices so we can rollback on OOM.
    rewritten_vertices.reserve(indexed_vertices.size());
    try {
      const utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_enabler;
      std::vector<double> vector(dimension);
      for (auto *vertex : indexed_vertices) {
        if (on_progress) on_progress();
        auto vector_property = vertex->properties.GetProperty(spec.property);
        if (UnregisterIndexId(vector_property, index_id)) {
          mg_index.index.get(vertex, vector.data());
          vertex->properties.SetProperty(spec.property, PropertyValue(vector));
        } else {
          vertex->properties.SetProperty(spec.property, vector_property);
        }
        rewritten_vertices.push_back(vertex);
      }
    } catch (const utils::OutOfMemoryException &) {
      const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
      for (auto *vertex : rewritten_vertices) ReinstallIndexIdInProperty(vertex, spec.property, index_id);
      throw;
    }
  }
  auto new_map = std::make_shared<VectorIndexContainer>(*index_);
  new_map->erase(index_id);
  index_ = new_map;
  return DroppedIndexCapture{.index_id = index_id,
                             .evicted_item = std::move(evicted_item),
                             .rewritten_vertices = std::move(rewritten_vertices)};
}

void VectorIndex::RestoreIndex(DroppedIndexCapture &&capture) {
  // Abort path: must not propagate OOM (called from a noexcept abort callback).
  const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
  for (auto *vertex : capture.rewritten_vertices) {
    ReinstallIndexIdInProperty(vertex, capture.evicted_item->spec.property, capture.index_id);
  }
  auto new_map = std::make_shared<VectorIndexContainer>(*index_);
  new_map->try_emplace(capture.index_id, std::move(capture.evicted_item));
  index_ = new_map;
}

void VectorIndex::Clear() { index_ = std::make_shared<VectorIndexContainer>(); }

void VectorIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, const IndexedPropertyDecoder<Vertex> &decoder) {
  ApplyAddLabel(label, vertex, decoder, /*restore=*/false);
}

void VectorIndex::ApplyAddLabel(LabelId label, Vertex *vertex, const IndexedPropertyDecoder<Vertex> &decoder,
                                bool restore) {
  auto matching = GetIndicesByLabel(label);
  if (matching.empty()) {
    return;
  }

  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  for (auto property_id : vertex_properties) {
    for (const auto &[idx_property, index_id] : matching) {
      if (idx_property != property_id) continue;
      auto &item_ptr = index_->at(index_id);
      if (!item_ptr->spec.label_filter.Matches(vertex->labels)) continue;

      auto old_property_value = vertex->properties.GetProperty(property_id, decoder);
      if (old_property_value.IsNull()) continue;
      // An empty plain list has nothing to index and stays a plain list — never promote it.
      if (!old_property_value.IsVectorIndexId() && old_property_value.IsAnyList() && old_property_value.ListSize() == 0)
        continue;

      auto ids = old_property_value.IsVectorIndexId() ? old_property_value.ValueVectorIndexIds()
                                                      : utils::small_vector<uint64_t>{};
      if (std::ranges::contains(ids, index_id)) continue;

      utils::small_vector<float> vector_property;
      if (old_property_value.IsVectorIndexId()) {
        vector_property = old_property_value.ValueVectorIndexList();
      } else if (restore) {
        // A value that is not a vector of this index's dimension was never indexed, so there is nothing to restore.
        auto maybe_vector = TryListToVector(old_property_value);
        if (!maybe_vector || maybe_vector->size() != item_ptr->spec.dimension) continue;
        vector_property = *std::move(maybe_vector);
      } else {
        vector_property = ListToVector(old_property_value);
      }
      ids.push_back(index_id);
      const PropertyValue tag(PropertyValue::VectorIndexIdData{.ids = std::move(ids), .vector = {}});
      UpdateVectorIndex(item_ptr->mg_index, item_ptr->spec, vertex, vector_property);

      // The entry is already in usearch, so a memory-limit refusal of this write would leave an entry abort cannot
      // find. The write stays tracked; its size is bounded by this vertex's property buffer.
      {
        const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
        vertex->properties.SetProperty(property_id, tag);
      }
    }
  }
}

void VectorIndex::UpdateOnRemoveLabel(LabelId label, Vertex *vertex, const IndexedPropertyDecoder<Vertex> &decoder) {
  auto matching = GetIndicesByLabel(label);
  if (matching.empty()) {
    return;
  }

  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  for (auto property_id : vertex_properties) {
    for (const auto &[idx_property, index_id] : matching) {
      if (idx_property != property_id) continue;
      auto &item_ptr = index_->at(index_id);
      // After removing this label, the vertex may still match other matching indices — only act if
      // it no longer matches THIS one.
      if (item_ptr->spec.label_filter.Matches(vertex->labels)) continue;

      auto old_vertex_property_value = vertex->properties.GetProperty(property_id, decoder);
      if (!old_vertex_property_value.IsVectorIndexId()) continue;
      auto &ids = old_vertex_property_value.ValueVectorIndexIds();
      if (!std::ranges::contains(ids, index_id)) continue;
      ids.erase(ranges::remove(ids, index_id), ids.end());

      auto guard = std::lock_guard{item_ptr->mg_index.mutex};

      const auto property_value_to_set = std::invoke([&]() {
        if (ids.empty()) {
          std::vector<double> vector(item_ptr->mg_index.index.dimensions());
          if (!item_ptr->mg_index.index.get(vertex, vector.data())) return PropertyValue();
          return PropertyValue(std::move(vector));
        }
        return old_vertex_property_value;
      });
      // Store first: a failure here must not leave a tag whose usearch entry is already gone.
      vertex->properties.SetProperty(property_id, property_value_to_set);
      item_ptr->mg_index.index.remove(vertex);
    }
  }
}

void VectorIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex) {
  // No vector indexes: a value can only be a vector-index id when one exists, so there is nothing to do.
  if (index_->empty()) return;
  // Property should already be updated to the vector index id if it has vector index defined on it.
  if (value.IsVectorIndexId()) {
    const auto &vector_property = value.ValueVectorIndexList();
    const auto &index_ids = value.ValueVectorIndexIds();
    for (auto index_id : index_ids) {
      auto &item_ptr = index_->at(index_id);
      UpdateVectorIndex(item_ptr->mg_index, item_ptr->spec, vertex, vector_property);
    }
  } else {
    auto indices = GetIndicesByProperty(property);
    auto vertex_matches = [&](const auto &id_filter_pair) { return id_filter_pair.second->Matches(vertex->labels); };
    r::for_each(indices | rv::filter(vertex_matches),
                [&](const auto &id_filter_pair) { RemoveVertexFromIndex(vertex, id_filter_pair.first); });
  }
}

void VectorIndex::RemoveVertexFromIndex(Vertex *vertex, uint64_t index_id) {
  auto it = index_->find(index_id);
  if (it == index_->end()) {
    throw VectorSearchException(
        fmt::format("Error in removing vertex from index: index id {} does not exist.", index_id));
  }
  auto &item_ptr = it->second;
  UpdateVectorIndex(item_ptr->mg_index, item_ptr->spec, vertex, utils::small_vector<float>{});
}

utils::small_vector<float> VectorIndex::GetVectorPropertyFromIndex(Vertex *vertex, std::string_view index_name,
                                                                   NameIdMapper *name_id_mapper) const {
  auto maybe_id = name_id_mapper->NameToIdIfExists(index_name);
  if (!maybe_id.has_value()) {
    throw VectorSearchException("Vector index {} does not exist.", index_name);
  }
  auto it = index_->find(*maybe_id);
  if (it == index_->end()) {
    throw VectorSearchException("Vector index {} does not exist.", index_name);
  }
  auto &item_ptr = it->second;
  auto guard = utils::SharedResourceLockGuard(item_ptr->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  utils::small_vector<float> vector(item_ptr->mg_index.index.dimensions());
  if (!item_ptr->mg_index.index.get(vertex, vector.data())) return {};
  return vector;
}

std::vector<VectorIndexInfo> VectorIndex::ListVectorIndicesInfo() const {
  std::vector<VectorIndexInfo> result;
  result.reserve(index_->size());
  for (const auto &[_, item_ptr] : *index_) {
    auto &mg_index = item_ptr->mg_index;
    auto &spec = item_ptr->spec;
    auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);

    result.emplace_back(spec.index_name,
                        spec.label_filter,
                        spec.property,
                        NameFromMetric(mg_index.index.metric().metric_kind()),
                        static_cast<std::uint16_t>(mg_index.index.dimensions()),
                        mg_index.index.capacity(),
                        mg_index.index.size(),
                        NameFromScalar(mg_index.index.metric().scalar_kind()));
  }
  return result;
}

std::vector<VectorIndexSpec> VectorIndex::ListIndices() const {
  std::vector<VectorIndexSpec> result;
  result.reserve(index_->size());
  std::ranges::transform(
      *index_, std::back_inserter(result), [](const auto &id_index_item) { return id_index_item.second->spec; });
  return result;
}

void VectorIndex::SerializeAllVectorIndices(durability::BaseEncoder *encoder,
                                            std::unordered_set<uint64_t> &mapped_ids) const {
  auto write_mapping = [&](auto mapping) {
    mapped_ids.insert(mapping.AsUint());
    encoder->WriteUint(mapping.AsUint());
  };

  encoder->WriteUint(index_->size());
  for (const auto &[_, item_ptr] : *index_) {
    auto &mg_index = item_ptr->mg_index;
    auto &spec = item_ptr->spec;
    encoder->WriteString(spec.index_name);
    encoder->WriteUint(static_cast<uint64_t>(spec.label_filter.mode));
    encoder->WriteUint(spec.label_filter.ids.size());
    for (const auto &label : spec.label_filter.ids) {
      write_mapping(label);
    }
    write_mapping(spec.property);
    encoder->WriteString(NameFromMetric(spec.metric_kind));
    encoder->WriteUint(spec.dimension);
    encoder->WriteUint(spec.resize_coefficient);
    encoder->WriteUint(spec.capacity);
    encoder->WriteUint(static_cast<uint64_t>(spec.scalar_kind));

    using Entry = std::pair<uint64_t, std::vector<float>>;
    auto const entries = std::invoke([&]() -> std::vector<Entry> {
      // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
      auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
      auto const size = mg_index.index.size();
      if (size == 0) return {};

      std::vector<Vertex *> keys(size);
      mg_index.index.export_keys(keys.data(), 0, size);

      std::vector<Entry> result;
      result.reserve(size);
      std::vector<float> buffer(mg_index.index.dimensions());
      for (auto *vertex : keys) {
        if (vertex == nullptr || vertex->deleted()) continue;
        if (!mg_index.index.get(vertex, buffer.data())) continue;
        result.emplace_back(vertex->gid.AsUint(), buffer);
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

std::optional<uint64_t> VectorIndex::ApproximateNodesVectorCount(std::string_view index_name) const {
  auto it = r::find_if(*index_, [&](const auto &id_item) { return id_item.second->spec.index_name == index_name; });
  if (it == index_->end()) return std::nullopt;
  auto guard = utils::SharedResourceLockGuard(it->second->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  return it->second->mg_index.index.size();
}

VectorIndex::VectorSearchNodeResults VectorIndex::SearchNodes(std::string_view index_name, uint64_t result_set_size,
                                                              const std::vector<float> &query_vector,
                                                              NameIdMapper *name_id_mapper) const {
  auto maybe_id = name_id_mapper->NameToIdIfExists(index_name);
  if (!maybe_id.has_value()) {
    throw VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto it = index_->find(*maybe_id);
  if (it == index_->end()) {
    throw VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &item_ptr = it->second;

  VectorSearchNodeResults result;
  result.reserve(result_set_size);

  auto guard = utils::SharedResourceLockGuard(item_ptr->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  const auto result_keys = item_ptr->mg_index.index.search(query_vector.data(), result_set_size);
  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    const auto &vertex = static_cast<Vertex *>(result_keys[i].member.key);
    result.emplace_back(
        vertex,
        static_cast<double>(result_keys[i].distance),
        std::abs(SimilarityFromDistance(item_ptr->mg_index.index.metric().metric_kind(), result_keys[i].distance)));
  }

  return result;
}

void VectorIndex::RemoveVertices(std::vector<Vertex *> const &vertices_to_remove) const {
  for (const auto &[index_id, item_ptr] : *index_) {
    auto &mg_index = item_ptr->mg_index;

    std::vector<Vertex *> loc_vertices_to_remove;
    {
      // read only to check which vertices should be removed in that index
      auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);

      loc_vertices_to_remove =
          vertices_to_remove |
          std::ranges::views::filter([&mg_index](Vertex *vertex) { return mg_index.index.contains(vertex); }) |
          std::ranges::to<std::vector>();
    }

    if (loc_vertices_to_remove.empty()) {
      // Avoid UNIQUE LOCK on indices which won't be used
      continue;
    }

    // take unique lock for removing
    auto guard = std::lock_guard{item_ptr->mg_index.mutex};
    auto &index = item_ptr->mg_index.index;
    index.remove(loc_vertices_to_remove.begin(), loc_vertices_to_remove.end());
  }
}

bool VectorIndex::IndexExists(std::string_view index_name, NameIdMapper *name_id_mapper) const {
  auto maybe_id = name_id_mapper->NameToIdIfExists(index_name);
  return maybe_id.has_value() && index_->contains(*maybe_id);
}

bool VectorIndex::Empty() const { return index_->empty(); }

utils::small_vector<uint64_t> VectorIndex::GetVectorIndexIdsForVertex(Vertex *vertex, PropertyId property) const {
  utils::small_vector<uint64_t> result;
  result.reserve(static_cast<uint32_t>(index_->size()));
  for (const auto &[index_id, item_ptr] : *index_) {
    if (item_ptr->spec.property != property) continue;
    if (!item_ptr->spec.label_filter.Matches(vertex->labels)) continue;
    result.push_back(index_id);
  }
  return result;
}

std::vector<std::pair<PropertyId, uint64_t>> VectorIndex::GetIndicesByLabel(LabelId label) const {
  std::vector<std::pair<PropertyId, uint64_t>> result;
  result.reserve(index_->size());
  for (const auto &[index_id, item_ptr] : *index_) {
    if (item_ptr->spec.label_filter.IsInteresting(label)) {
      result.emplace_back(item_ptr->spec.property, index_id);
    }
  }
  return result;
}

std::vector<std::pair<uint64_t, VectorLabelFilter const *>> VectorIndex::GetIndicesByProperty(
    PropertyId property) const {
  std::vector<std::pair<uint64_t, VectorLabelFilter const *>> result;
  result.reserve(index_->size());
  for (const auto &[index_id, item_ptr] : *index_) {
    if (item_ptr->spec.property == property) {
      result.emplace_back(index_id, &item_ptr->spec.label_filter);
    }
  }
  return result;
}

void LogUndoFailure(const char *what) noexcept {
  try {
    spdlog::error("Vector index undo on abort failed: {}", what);
  } catch (...) {
  }
}

void LogUndoRepairFailure() noexcept {
  try {
    spdlog::error("Vector index repair after a failed undo on abort failed.");
  } catch (...) {
  }
}

bool VectorIndex::HasIndexOnLabel(LabelId label) const {
  return r::any_of(*index_, [&](const auto &kv) { return kv.second->spec.label_filter.IsInteresting(label); });
}

bool VectorIndex::HasIndexOnProperty(PropertyId property) const { return AnyIndexOnProperty(*index_, property); }

void VectorIndex::DropEntries(Vertex *vertex, PropertyId property) {
  for (const auto &[index_id, _] : GetIndicesByProperty(property)) {
    try {
      RemoveVertexFromIndex(vertex, index_id);
    } catch (...) {
      LogUndoRepairFailure();
    }
  }
}

void VectorIndex::ReconcileEntry(Vertex *vertex, PropertyId property, uint64_t index_id) {
  auto &item_ptr = index_->at(index_id);
  auto value = vertex->properties.GetProperty(property);
  if (value.IsVectorIndexId() && std::ranges::contains(value.ValueVectorIndexIds(), index_id)) {
    if (item_ptr->spec.label_filter.Matches(vertex->labels)) return;
    // Tagged but no longer matching: strip the id first so no tag outlives the entry removed below.
    auto &ids = value.ValueVectorIndexIds();
    ids.erase(ranges::remove(ids, index_id), ids.end());
    if (ids.empty()) {
      std::vector<double> vector(item_ptr->mg_index.index.dimensions());
      auto guard = utils::SharedResourceLockGuard(item_ptr->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
      value = item_ptr->mg_index.index.get(vertex, vector.data()) ? PropertyValue(std::move(vector)) : PropertyValue();
    }
    vertex->properties.SetProperty(property, value);
  }
  RemoveVertexFromIndex(vertex, index_id);
}

void VectorIndex::RepairLabelUndo(LabelId label, Vertex *vertex) {
  for (const auto &[property, index_id] : GetIndicesByLabel(label)) {
    try {
      ReconcileEntry(vertex, property, index_id);
    } catch (...) {
      LogUndoRepairFailure();
    }
  }
}

void VectorIndex::RestoreOnAddLabel(LabelId label, Vertex *vertex,
                                    const IndexedPropertyDecoder<Vertex> &decoder) noexcept {
  if (!HasIndexOnLabel(label)) return;
  UndoNoThrow([&] { ApplyAddLabel(label, vertex, decoder, /*restore=*/true); },
              [&] { RepairLabelUndo(label, vertex); });
}

void VectorIndex::RestoreOnRemoveLabel(LabelId label, Vertex *vertex,
                                       const IndexedPropertyDecoder<Vertex> &decoder) noexcept {
  if (!HasIndexOnLabel(label)) return;
  UndoNoThrow([&] { UpdateOnRemoveLabel(label, vertex, decoder); }, [&] { RepairLabelUndo(label, vertex); });
}

void VectorIndex::RestoreOnSetProperty(PropertyId property, const PropertyValue &before, Vertex *vertex) noexcept {
  if (!HasIndexOnProperty(property)) return;
  // A non-tag value is in no index. Every index on the property is cleared, not only those matching the current
  // labels: a forward write that raced a label change can leave an entry the filter no longer admits.
  if (!before.IsVectorIndexId()) {
    UndoNoThrow([&] { DropEntries(vertex, property); }, [] {});
    return;
  }
  UndoNoThrow([&] { UpdateOnSetProperty(property, before, vertex); },
              [&] {
                // Last resort: the vertex keeps its values as a plain, unindexed list.
                const auto &vector = before.ValueVectorIndexList();
                vertex->properties.SetProperty(property,
                                               PropertyValue(std::vector<double>(vector.begin(), vector.end())));
                for (auto index_id : before.ValueVectorIndexIds()) {
                  try {
                    RemoveVertexFromIndex(vertex, index_id);
                  } catch (...) {
                    LogUndoRepairFailure();
                  }
                }
              });
}

// VectorIndexRecovery implementation

void VectorIndexRecovery::UpdateOnSetProperty(PropertyId property, PropertyValue &value, const Vertex *vertex,
                                              std::vector<VectorIndexRecoveryInfo> &recovery_info_vec,
                                              VertexVectors &vertex_vectors) {
  // Older versions stored [] as a tag with no floats; normalise it to plain [].
  if (value.IsVectorIndexId() && value.ValueVectorIndexList().empty()) {
    value = PropertyValue(std::vector<double>{});
  }

  const bool has_spec = r::any_of(recovery_info_vec, [&](const auto &ri) { return ri.spec.property == property; });

  if (has_spec) {
    if (value.IsVectorIndexId()) {
      vertex_vectors[property][vertex->gid] = std::move(value.ValueVectorIndexList());
    } else if (auto it = vertex_vectors.find(property); it != vertex_vectors.end()) {
      it->second.erase(vertex->gid);
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

void VectorIndexRecovery::UpdateOnIndexDrop(std::string_view index_name,
                                            std::vector<VectorIndexRecoveryInfo> &recovery_info_vec,
                                            VertexVectors &vertex_vectors,
                                            utils::SkipListDb<Vertex>::Accessor &vertices) {
  auto it = r::find_if(recovery_info_vec, [&](const auto &ri) { return ri.spec.index_name == index_name; });
  if (it == recovery_info_vec.end()) return;
  const PropertyId property = it->spec.property;
  recovery_info_vec.erase(it);

  // If another spec still covers this property, vertex_vectors[property] remains valid for the final
  // build. Only when no spec remains must we restore stored tags to plain lists and drop the map entry.
  const bool other_spec_on_property =
      r::any_of(recovery_info_vec, [&](const auto &ri) { return ri.spec.property == property; });
  if (other_spec_on_property) return;

  auto map_it = vertex_vectors.find(property);

  // A tag on this property is an artifact of the dropped index; demote to a plain list
  // (or null if no vector is available).
  for (auto &vertex : vertices) {
    auto stored = vertex.properties.GetProperty(property);
    if (!stored.IsVectorIndexId()) continue;

    if (map_it != vertex_vectors.end()) {
      auto entry_it = map_it->second.find(vertex.gid);
      if (entry_it != map_it->second.end()) {
        vertex.properties.SetProperty(
            property, PropertyValue(std::vector<double>(entry_it->second.begin(), entry_it->second.end())));
        continue;
      }
    }
    spdlog::error(
        "Recovery: vertex {} property {} carries a VectorIndexId tag for dropped index '{}' "
        "but has no vector entry — data was lost before this drop; setting to null.",
        vertex.gid.AsUint(),
        property.AsUint(),
        index_name);
    vertex.properties.SetProperty(property, PropertyValue());
  }

  if (map_it != vertex_vectors.end()) {
    vertex_vectors.erase(map_it);
  }
}

// ---- VectorIndex::ActiveIndices (live shared reference) ----

std::vector<VectorIndexSpec> VectorIndex::ActiveIndices::ListIndices() const {
  if (!index_container_) return {};
  std::vector<VectorIndexSpec> result;
  result.reserve(index_container_->size());
  std::ranges::transform(
      *index_container_, std::back_inserter(result), [](const auto &id_item) { return id_item.second->spec; });
  return result;
}

std::vector<VectorIndexInfo> VectorIndex::ActiveIndices::ListVectorIndicesInfo() const {
  if (!index_container_) return {};
  std::vector<VectorIndexInfo> result;
  result.reserve(index_container_->size());
  for (const auto &[_, item_ptr] : *index_container_) {
    auto &mg_index = item_ptr->mg_index;
    auto &spec = item_ptr->spec;
    auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    result.emplace_back(spec.index_name,
                        spec.label_filter,
                        spec.property,
                        NameFromMetric(mg_index.index.metric().metric_kind()),
                        static_cast<std::uint16_t>(mg_index.index.dimensions()),
                        mg_index.index.capacity(),
                        mg_index.index.size(),
                        NameFromScalar(mg_index.index.metric().scalar_kind()));
  }
  return result;
}

std::optional<uint64_t> VectorIndex::ActiveIndices::ApproximateNodesVectorCount(std::string_view index_name) const {
  if (!index_container_) return std::nullopt;
  auto it =
      r::find_if(*index_container_, [&](const auto &id_item) { return id_item.second->spec.index_name == index_name; });
  if (it == index_container_->end()) return std::nullopt;
  auto guard = utils::SharedResourceLockGuard(it->second->mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  return it->second->mg_index.index.size();
}

std::vector<PropertyId> VectorIndex::ActiveIndices::IndexedProperties(std::span<LabelId const> labels) const {
  if (!index_container_) return {};
  std::vector<PropertyId> result;
  for (const auto &[_, item_ptr] : *index_container_) {
    if (item_ptr->spec.label_filter.Matches(labels)) result.push_back(item_ptr->spec.property);
  }
  return result;
}

}  // namespace memgraph::storage
