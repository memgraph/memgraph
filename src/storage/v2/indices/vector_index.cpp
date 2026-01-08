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

#include <shared_mutex>
#include <usearch/index_dense.hpp>
#include <usearch/index_plugins.hpp>

#include "query/exceptions.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"

namespace r = ranges;
namespace memgraph::storage {

using mg_vector_index_t = unum::usearch::index_dense_gt<Vertex *, unum::usearch::uint40_t>;

struct IndexItem {
  mg_vector_index_t index;
  VectorIndexLock lock;
  VectorIndexSpec spec;
};

namespace {

void TryAddVertexToIndex(IndexItem &index_item, Vertex &vertex,
                         std::optional<SnapshotObserverInfo> const &snapshot_info, NameIdMapper *name_id_mapper,
                         std::optional<std::size_t> thread_id = std::nullopt) {
  if (!std::ranges::contains(vertex.labels, index_item.spec.label_id)) return;
  auto property = vertex.properties.GetProperty(index_item.spec.property);
  if (property.IsNull()) return;
  auto vector = PropertyToFloatVector(property, index_item.spec.dimension);
  AddToVectorIndex(index_item.index, index_item.lock, index_item.spec, &vertex, vector.data(), thread_id);
  vertex.properties.SetProperty(
      index_item.spec.property,
      PropertyValue(utils::small_vector<uint64_t>{name_id_mapper->NameToId(index_item.spec.index_name)},
                    std::vector<float>{}));
  if (snapshot_info) snapshot_info->Update(UpdateType::VECTOR_IDX);
}

}  // namespace

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

VectorIndex::VectorIndex(const std::filesystem::path &storage_dir)
    : pimpl(std::make_unique<Impl>()), vector_index_storage_dir_(storage_dir / kVectorIndicesDirectory) {}
VectorIndex::~VectorIndex() = default;

bool VectorIndex::CreateIndex(const VectorIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                              NameIdMapper *name_id_mapper, std::optional<SnapshotObserverInfo> const &snapshot_info) {
  const utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  try {
    SetupIndex(spec);
    PopulateIndexOnSingleThread(vertices, spec, name_id_mapper, snapshot_info);
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
  const unum::usearch::index_limits_t limits(spec.capacity, GetVectorIndexThreadCount());

  auto mg_vector_index = mg_vector_index_t::make(metric);
  if (!mg_vector_index) {
    throw query::VectorSearchException(fmt::format("Failed to create vector index {}, error message: {}",
                                                   spec.index_name, mg_vector_index.error.what()));
  }

  if (!mg_vector_index.index.try_reserve(limits)) {
    throw query::VectorSearchException(
        fmt::format("Failed to create vector index {}. Failed to reserve memory for the index", spec.index_name));
  }

  pimpl->index_name_to_label_prop_.try_emplace(spec.index_name, label_prop);
  pimpl->label_to_index_[spec.label_id].emplace(spec.property, spec.index_name);
  pimpl->index_.emplace(label_prop, IndexItem{.index = std::move(mg_vector_index.index), .lock = {}, .spec = spec});

  spdlog::info("Created vector index {}", spec.index_name);
}

void VectorIndex::RecoverIndex(const VectorIndexRecoveryInfo &recovery_info,
                               utils::SkipList<Vertex>::Accessor &vertices, NameIdMapper *name_id_mapper,
                               std::optional<SnapshotObserverInfo> const &snapshot_info) {
  const auto &spec = recovery_info.spec;
  const auto &recovery_entries = recovery_info.index_entries;
  SetupIndex(spec);
  auto &index_item = pimpl->index_.at({spec.label_id, spec.property});

  auto process_vertex_for_recovery = [&index_item, &recovery_entries, &snapshot_info, name_id_mapper](
                                         Vertex &vertex, std::optional<std::size_t> thread_id) {
    const auto index_id = name_id_mapper->NameToId(index_item.spec.index_name);
    std::vector<float> vector;
    bool should_set_property = false;

    if (auto it = recovery_entries.find(vertex.gid); it != recovery_entries.end()) {
      vector = it->second;
    } else {
      if (!std::ranges::contains(vertex.labels, index_item.spec.label_id)) {
        return;
      }
      auto property = vertex.properties.GetProperty(index_item.spec.property);
      if (property.IsNull()) {
        return;
      }
      vector = ListToVector(property);
      should_set_property = true;
    }

    if (vector.empty()) {
      return;
    }

    ValidateVectorDimension(vector, index_item.spec.dimension);
    AddToVectorIndex(index_item.index, index_item.lock, index_item.spec, &vertex, vector.data(), thread_id);

    if (should_set_property) {
      vertex.properties.SetProperty(index_item.spec.property,
                                    PropertyValue(utils::small_vector<uint64_t>{index_id}, std::vector<float>{}));
    }

    if (snapshot_info) {
      snapshot_info->Update(UpdateType::VECTOR_IDX);
    }
  };

  if (FLAGS_storage_parallel_index_recovery) {
    PopulateVectorIndexMultiThreaded(vertices, process_vertex_for_recovery);
  } else {
    PopulateVectorIndexSingleThreaded(vertices, process_vertex_for_recovery);
  }
}

void VectorIndex::CreateSnapshot() {
  if (!std::filesystem::exists(vector_index_storage_dir_)) {
    std::filesystem::create_directories(vector_index_storage_dir_);
  }
  for (auto &[label_prop, index_item] : pimpl->index_) {
    auto lock = index_item.lock.LockForSave();
    index_item.index.save((vector_index_storage_dir_ / index_item.spec.index_name).c_str());
  }
}

bool VectorIndex::RecoverSnapshot(const std::filesystem::path &path) {
  if (!std::filesystem::exists(path)) {
    return false;
  }
  for (auto &[label_prop, index_item] : pimpl->index_) {
    auto lock = index_item.lock.LockForResize();
    index_item.index.load((path / index_item.spec.index_name).c_str());
  }
  return true;
}

void VectorIndex::CleanupFailedIndex(const VectorIndexSpec &spec) {
  const auto label_prop = LabelPropKey{spec.label_id, spec.property};
  pimpl->index_name_to_label_prop_.erase(spec.index_name);
  pimpl->index_.erase(label_prop);
}

void VectorIndex::PopulateIndexOnSingleThread(utils::SkipList<Vertex>::Accessor &vertices, const VectorIndexSpec &spec,
                                              NameIdMapper *name_id_mapper,
                                              std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto &index_item = pimpl->index_.at({spec.label_id, spec.property});
  PopulateVectorIndexSingleThreaded(vertices, [&](Vertex &vertex, std::optional<std::size_t> thread_id) {
    TryAddVertexToIndex(index_item, vertex, snapshot_info, name_id_mapper, thread_id);
  });
}

void VectorIndex::PopulateIndexOnMultipleThreads(utils::SkipList<Vertex>::Accessor &vertices,
                                                 const VectorIndexSpec &spec, NameIdMapper *name_id_mapper,
                                                 std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto &index_item = pimpl->index_.at({spec.label_id, spec.property});
  PopulateVectorIndexMultiThreaded(vertices, [&](Vertex &vertex, std::size_t thread_id) {
    TryAddVertexToIndex(index_item, vertex, snapshot_info, name_id_mapper, std::optional{thread_id});
  });
}

bool VectorIndex::DropIndex(std::string_view index_name, utils::SkipList<Vertex>::Accessor &vertices,
                            NameIdMapper *name_id_mapper) {
  auto it = pimpl->index_name_to_label_prop_.find(index_name);
  if (it == pimpl->index_name_to_label_prop_.end()) {
    return false;
  }
  auto label_prop = it->second;
  auto &index_item = pimpl->index_.at(label_prop);
  auto _ = index_item.lock.LockForModify();

  auto restore_vector_from_index = [&](auto *vertex) {
    std::vector<double> vector(index_item.index.dimensions());
    index_item.index.get(vertex, vector.data());
    vertex->properties.SetProperty(label_prop.property(), PropertyValue(std::move(vector)));
  };

  auto index_id = name_id_mapper->NameToId(index_name);
  for (auto &vertex : vertices) {
    if (index_item.index.contains(&vertex)) {
      auto vector_property = vertex.properties.GetProperty(label_prop.property());
      if (RemoveIndexIdFromProperty(vector_property, index_id)) {
        restore_vector_from_index(&vertex);
      } else {
        vertex.properties.SetProperty(label_prop.property(), vector_property);
      }
      index_item.index.remove(&vertex);
    }
  }
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

void VectorIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper) {
  auto matching_index_properties = GetProperties(label);
  if (matching_index_properties.empty()) {
    return;
  }

  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  for (auto property_id : vertex_properties) {
    if (auto index_name = matching_index_properties.find(property_id); index_name != matching_index_properties.end()) {
      auto old_property_value = vertex->properties.GetProperty(property_id);
      UpdateIndex(old_property_value, vertex, std::optional{index_name->second}, name_id_mapper);
      auto vec = GetVectorProperty(vertex, index_name->second);
      auto vector_index_id = std::invoke([&]() {
        if (old_property_value.IsVectorIndexId()) {
          auto ids = old_property_value.ValueVectorIndexIds();
          ids.push_back(name_id_mapper->NameToId(index_name->second));
          return PropertyValue(ids, std::move(vec));
        }
        return PropertyValue(utils::small_vector<uint64_t>{name_id_mapper->NameToId(index_name->second)},
                             std::move(vec));
      });
      vertex->properties.SetProperty(property_id, vector_index_id);
    }
  }
}

void VectorIndex::UpdateOnRemoveLabel(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper) {
  auto matching_index_properties = GetProperties(label);
  if (matching_index_properties.empty()) {
    return;
  }

  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  for (auto property_id : vertex_properties) {
    if (auto index_name = matching_index_properties.find(property_id); index_name != matching_index_properties.end()) {
      auto old_vertex_property_value = vertex->properties.GetProperty(property_id);
      auto &ids = old_vertex_property_value.ValueVectorIndexIds();
      auto [erase_begin, erase_end] = std::ranges::remove(ids, name_id_mapper->NameToId(index_name->second));
      ids.erase(erase_begin, erase_end);
      auto old_vector_property_value = GetPropertyValue(vertex, index_name->second);
      UpdateIndex(PropertyValue(), vertex, index_name->second,
                  name_id_mapper);  // we are removing property from index
      vertex->properties.SetProperty(property_id,
                                     ids.empty() ? old_vector_property_value
                                                 : old_vertex_property_value);  // we transfer list to property store
                                                                                // only if it's not in any index anymore
    }
  }
}

void VectorIndex::UpdateIndex(const PropertyValue &value, Vertex *vertex, std::optional<std::string_view> index_name,
                              NameIdMapper *name_id_mapper) {
  if (index_name.has_value()) {
    auto it = pimpl->index_name_to_label_prop_.find(*index_name);
    if (it == pimpl->index_name_to_label_prop_.end()) {
      throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", *index_name));
    }

    auto &index_item = pimpl->index_.at(it->second);
    if (value.IsNull()) {
      auto _ = index_item.lock.LockForModify();
      index_item.index.remove(vertex);
      return;
    }

    auto vector_property = std::invoke([&]() {
      if (value.IsVectorIndexId()) {
        return GetVectorProperty(vertex, name_id_mapper->IdToName(value.ValueVectorIndexIds()[0]));
      }
      if (value.IsAnyList()) {
        return ListToVector(value);
      }
      throw query::VectorSearchException("Vector index property must be a list of floats or integers.");
    });

    UpdateSingleVectorIndex(index_item.index, index_item.lock, index_item.spec, vertex, vector_property, true);
    return;
  }

  if (!value.IsVectorIndexId()) {
    return;
  }

  const auto &vector_property = value.ValueVectorIndexList();
  const auto &index_ids = value.ValueVectorIndexIds();
  for (const auto &index_id : index_ids) {
    const auto &idx_name = name_id_mapper->IdToName(index_id);
    auto label_prop = pimpl->index_name_to_label_prop_.at(idx_name);
    auto &index_item = pimpl->index_.at(label_prop);
    if (vector_property.empty()) {
      auto _ = index_item.lock.LockForModify();
      index_item.index.remove(vertex);
      continue;
    }
    UpdateSingleVectorIndex(index_item.index, index_item.lock, index_item.spec, vertex, vector_property, false);
  }
}

PropertyValue VectorIndex::GetPropertyValue(Vertex *vertex, std::string_view index_name) const {
  auto it = pimpl->index_name_to_label_prop_.find(index_name);
  if (it == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &index_item = pimpl->index_.at(it->second);
  auto _ = index_item.lock.LockForRead();
  return GetVectorAsPropertyValue(index_item.index, vertex);
}

std::vector<float> VectorIndex::GetVectorProperty(Vertex *vertex, std::string_view index_name) const {
  auto it = pimpl->index_name_to_label_prop_.find(index_name);
  if (it == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &index_item = pimpl->index_.at(it->second);
  auto _ = index_item.lock.LockForRead();
  return GetVector(index_item.index, vertex);
}

std::vector<VectorIndexInfo> VectorIndex::ListVectorIndicesInfo() const {
  std::vector<VectorIndexInfo> result;
  result.reserve(pimpl->index_.size());
  for (auto &[label_prop, index_item] : pimpl->index_) {
    auto lock = index_item.lock.LockForRead();
    result.emplace_back(index_item.spec.index_name, index_item.spec.label_id, index_item.spec.property,
                        NameFromMetric(index_item.index.metric().metric_kind()),
                        static_cast<std::uint16_t>(index_item.index.dimensions()), index_item.index.capacity(),
                        index_item.index.size(), NameFromScalar(index_item.index.metric().scalar_kind()));
  }
  return result;
}

std::vector<VectorIndexSpec> VectorIndex::ListIndices() const {
  std::vector<VectorIndexSpec> result;
  result.reserve(pimpl->index_.size());
  std::ranges::transform(pimpl->index_, std::back_inserter(result),
                         [](const auto &label_prop_index_item) { return label_prop_index_item.second.spec; });
  return result;
}

std::optional<uint64_t> VectorIndex::ApproximateNodesVectorCount(LabelId label, PropertyId property) const {
  auto it = pimpl->index_.find(LabelPropKey{label, property});
  if (it == pimpl->index_.end()) {
    return std::nullopt;
  }
  auto &index_item = it->second;
  auto _ = index_item.lock.LockForRead();
  return index_item.index.size();
}

VectorIndex::VectorSearchNodeResults VectorIndex::SearchNodes(std::string_view index_name, uint64_t result_set_size,
                                                              const std::vector<float> &query_vector) const {
  const auto label_prop = pimpl->index_name_to_label_prop_.find(index_name);
  if (label_prop == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &index_item = pimpl->index_.at(label_prop->second);

  VectorSearchNodeResults result;
  result.reserve(result_set_size);

  auto _ = index_item.lock.LockForRead();
  const auto result_keys =
      index_item.index.filtered_search(query_vector.data(), result_set_size, [](const Vertex *vertex) {
        auto guard = std::shared_lock{vertex->lock};
        return !vertex->deleted;
      });
  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    const auto &vertex = static_cast<Vertex *>(result_keys[i].member.key);
    result.emplace_back(
        vertex, static_cast<double>(result_keys[i].distance),
        std::abs(SimilarityFromDistance(index_item.index.metric().metric_kind(), result_keys[i].distance)));
  }

  return result;
}

void VectorIndex::RemoveObsoleteEntries(std::stop_token token) const {
  auto maybe_stop = utils::ResettableCounter(2048);
  for (auto &[label_prop, index_item] : pimpl->index_) {
    if (maybe_stop() && token.stop_requested()) {
      return;
    }
    auto lock = index_item.lock.LockForModify();
    std::vector<Vertex *> vertices_to_remove(index_item.index.size());
    index_item.index.export_keys(vertices_to_remove.data(), 0, index_item.index.size());

    auto deleted = vertices_to_remove | rv::filter([](const Vertex *vertex) { return vertex->deleted; });
    for (const auto &vertex : deleted) {
      index_item.index.remove(vertex);
    }
  }
}

bool VectorIndex::IndexExists(std::string_view index_name) const {
  return pimpl->index_name_to_label_prop_.contains(index_name);
}

utils::small_vector<uint64_t> VectorIndex::GetVectorIndexIdsForVertex(Vertex *vertex, PropertyId property,
                                                                      NameIdMapper *name_id_mapper) {
  auto has_property = [&](const auto &label_prop) { return label_prop.property() == property; };
  auto has_label = [&](const auto &label_prop) { return std::ranges::contains(vertex->labels, label_prop.label()); };
  auto matching_label_props =
      pimpl->index_ | rv::keys | rv::filter(has_label) | rv::filter(has_property) | r::to<std::vector<LabelPropKey>>();
  if (matching_label_props.empty()) {
    return {};
  }
  utils::small_vector<uint64_t> result;
  result.reserve(matching_label_props.size());
  for (const auto &label_prop : matching_label_props) {
    const auto &index_item = pimpl->index_.at(label_prop);
    result.push_back(name_id_mapper->NameToId(index_item.spec.index_name));
  }
  return result;
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

std::unordered_map<LabelId, std::string> VectorIndex::GetLabels(PropertyId property) const {
  std::unordered_map<LabelId, std::string> result;
  for (const auto &[label, properties_map] : pimpl->label_to_index_) {
    auto properties_view = properties_map | rv::keys;
    if (std::ranges::contains(properties_view, property)) {
      result[label] = properties_map.at(property);
    }
  }
  return result;
}

void VectorIndex::RemoveVertexFromIndex(Vertex *vertex, std::string_view index_name) {
  auto it = pimpl->index_name_to_label_prop_.find(index_name);
  if (it == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(
        fmt::format("Error in removing vertex from index: index name {} does not exist.", index_name));
  }
  auto &index_item = pimpl->index_.at(it->second);
  auto _ = index_item.lock.LockForModify();
  if (index_item.index.contains(vertex)) {
    index_item.index.remove(vertex);
  }
}

void VectorIndex::AbortEntries(NameIdMapper *name_id_mapper, AbortableInfo &cleanup_collection) {
  for (auto &[vertex, info] : cleanup_collection) {
    auto &[labels_to_add, labels_to_remove, property_to_abort] = info;
    for (const auto &label : labels_to_remove) {
      UpdateOnRemoveLabel(label, vertex, name_id_mapper);
    }
    for (const auto &label : labels_to_add) {
      UpdateOnAddLabel(label, vertex, name_id_mapper);
    }
    for (const auto &[property, value] : property_to_abort) {
      if (value.IsVectorIndexId()) {
        UpdateIndex(value, vertex, std::nullopt, name_id_mapper);
      } else {
        for (const auto &index_name : GetLabels(property)) {
          RemoveVertexFromIndex(vertex, index_name.second);
        }
      }
    }
  }
}

VectorIndex::AbortProcessor VectorIndex::GetAbortProcessor() const {
  AbortProcessor res{};
  for (const auto &[label_prop, _] : pimpl->index_) {
    const auto label = label_prop.label();
    const auto property = label_prop.property();
    res.l2p[label].emplace_back(property);
    res.p2l[property].emplace_back(label);
  }
  return res;
}

void VectorIndex::AbortProcessor::CollectOnLabelRemoval(LabelId label, Vertex *vertex) {
  const auto &properties = l2p.find(label);
  auto has_any_property = [&](const auto &property) { return vertex->properties.HasProperty(property); };
  if (properties == l2p.end() || !r::any_of(properties->second, has_any_property)) return;
  auto &[label_to_add, label_to_remove, _] = cleanup_collection[vertex];
  label_to_remove.insert(label);
  label_to_add.erase(label);
}

void VectorIndex::AbortProcessor::CollectOnLabelAddition(LabelId label, Vertex *vertex) {
  const auto &properties = l2p.find(label);
  auto has_any_property = [&](const auto &property) { return vertex->properties.HasProperty(property); };
  if (properties == l2p.end() || !r::any_of(properties->second, has_any_property)) return;
  auto &[label_to_add, label_to_remove, _] = cleanup_collection[vertex];
  label_to_add.insert(label);
  label_to_remove.erase(label);
}

void VectorIndex::AbortProcessor::CollectOnPropertyChange(PropertyId propId, const PropertyValue &old_value,
                                                          Vertex *vertex) {
  const auto &labels = p2l.find(propId);
  auto has_any_label = [&](const auto &label) { return std::ranges::contains(vertex->labels, label); };
  if (labels == p2l.end() || !r::any_of(labels->second, has_any_label)) return;
  auto &[_, label_to_remove, property_to_abort] = cleanup_collection[vertex];
  property_to_abort[propId] = old_value;
}

// VectorIndexRecovery implementation

std::vector<VectorIndexRecoveryInfo *> VectorIndexRecovery::FindMatchingIndices(
    LabelId label, std::vector<VectorIndexRecoveryInfo> &recovery_info_vec) {
  std::vector<VectorIndexRecoveryInfo *> indices;
  indices.reserve(recovery_info_vec.size());
  for (auto &recovery_info : recovery_info_vec) {
    if (recovery_info.spec.label_id == label) {
      indices.push_back(&recovery_info);
    }
  }
  return indices;
}

std::vector<float> VectorIndexRecovery::ExtractVectorForRecovery(
    const PropertyValue &value, Vertex *vertex, const std::vector<VectorIndexRecoveryInfo> &recovery_info_vec,
    NameIdMapper *name_id_mapper) {
  if (value.IsVectorIndexId()) {
    const auto &ids = value.ValueVectorIndexIds();
    if (ids.empty()) {
      throw query::VectorSearchException("Vector index ID list is empty.");
    }
    // Find the vector in recovery info
    for (const auto &recovery_info : recovery_info_vec) {
      if (recovery_info.spec.index_name == name_id_mapper->IdToName(ids[0])) {
        if (auto it = recovery_info.index_entries.find(vertex->gid); it != recovery_info.index_entries.end()) {
          return it->second;
        }
        throw query::VectorSearchException(
            fmt::format("Vector index {} not found in recovery info.", name_id_mapper->IdToName(ids[0])));
      }
    }
    throw query::VectorSearchException(
        fmt::format("Vector index {} not found in recovery info.", name_id_mapper->IdToName(ids[0])));
  }
  return ListToVector(value);
}

void VectorIndexRecovery::UpdateOnIndexDrop(std::string_view index_name, NameIdMapper *name_id_mapper,
                                            std::vector<VectorIndexRecoveryInfo> &recovery_info_vec,
                                            utils::SkipList<Vertex>::Accessor &vertices) {
  for (auto &recovery_info : recovery_info_vec) {
    if (recovery_info.spec.index_name == index_name) {
      for (auto &[gid, vector] : recovery_info.index_entries) {
        auto vertex = vertices.find(gid);
        if (vertex == vertices.end()) continue;

        auto vertex_property = vertex->properties.GetProperty(recovery_info.spec.property);
        auto index_id = name_id_mapper->NameToId(index_name);

        if (RemoveIndexIdFromProperty(vertex_property, index_id)) {
          RestoreVectorOnVertex(&*vertex, recovery_info.spec.property, vector);
        } else {
          vertex->properties.SetProperty(recovery_info.spec.property, vertex_property);
        }
      }
    }
  }
}

void VectorIndexRecovery::UpdateOnLabelAddition(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper,
                                                std::vector<VectorIndexRecoveryInfo> &recovery_info_vec) {
  auto matching_indices = FindMatchingIndices(label, recovery_info_vec);
  if (matching_indices.empty()) {
    return;
  }

  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  for (auto *recovery_info : matching_indices) {
    if (r::contains(vertex_properties, recovery_info->spec.property)) {
      auto old_property_value = vertex->properties.GetProperty(recovery_info->spec.property);
      auto vector_to_add = ExtractVectorForRecovery(old_property_value, vertex, recovery_info_vec, name_id_mapper);

      if (old_property_value.IsVectorIndexId()) {
        auto &ids = old_property_value.ValueVectorIndexIds();
        ids.push_back(name_id_mapper->NameToId(recovery_info->spec.index_name));
        vertex->properties.SetProperty(recovery_info->spec.property, old_property_value);
      } else {
        auto index_id = name_id_mapper->NameToId(recovery_info->spec.index_name);
        std::vector<float> empty_vector;
        utils::small_vector<uint64_t> index_ids{index_id};
        vertex->properties.SetProperty(recovery_info->spec.property,
                                       CreateVectorIndexIdProperty(empty_vector, index_ids));
      }

      recovery_info->index_entries.emplace(vertex->gid, std::move(vector_to_add));
    }
  }
}

void VectorIndexRecovery::UpdateOnLabelRemoval(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper,
                                               std::vector<VectorIndexRecoveryInfo> &recovery_info_vec) {
  auto matching_indices = FindMatchingIndices(label, recovery_info_vec);
  if (matching_indices.empty()) {
    return;
  }

  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  for (auto *recovery_info : matching_indices) {
    if (r::contains(vertex_properties, recovery_info->spec.property)) {
      auto old_property_value = vertex->properties.GetProperty(recovery_info->spec.property);
      auto index_id = name_id_mapper->NameToId(recovery_info->spec.index_name);

      if (RemoveIndexIdFromProperty(old_property_value, index_id)) {
        // Restore vector on vertex
        if (auto it = recovery_info->index_entries.find(vertex->gid); it != recovery_info->index_entries.end()) {
          RestoreVectorOnVertex(vertex, recovery_info->spec.property, it->second);
        } else {
          throw query::VectorSearchException(
              fmt::format("Vector index {} not found in recovery info.", recovery_info->spec.index_name));
        }
      } else {
        vertex->properties.SetProperty(recovery_info->spec.property, old_property_value);
      }

      recovery_info->index_entries.erase(vertex->gid);
    }
  }
}

void VectorIndexRecovery::UpdateOnPropertyChange(PropertyId property, PropertyValue &value, Vertex *vertex,
                                                 std::vector<VectorIndexRecoveryInfo> &recovery_info_vec) {
  // Property has to be in the index because it was stored as VectorIndexId
  for (auto &recovery_info : recovery_info_vec) {
    if (recovery_info.spec.property == property && r::contains(vertex->labels, recovery_info.spec.label_id)) {
      DMG_ASSERT(value.IsVectorIndexId(), "Property value must be a vector index id");
      recovery_info.index_entries[vertex->gid] = value.ValueVectorIndexList();
    }
  }
}

std::vector<float> VectorIndex::GetVectorFromVertex(Vertex *vertex, std::string_view index_name) const {
  const auto label_prop = pimpl->index_name_to_label_prop_.find(index_name);
  if (label_prop == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &index_item = pimpl->index_.at(label_prop->second);
  auto _ = index_item.lock.LockForRead();
  std::vector<float> vector(index_item.index.dimensions());
  index_item.index.get(vertex, vector.data());
  return vector;
}

}  // namespace memgraph::storage
