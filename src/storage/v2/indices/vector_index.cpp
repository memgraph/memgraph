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
#include "query/exceptions.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/small_vector.hpp"
#include "utils/synchronized.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {
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
                         std::optional<SnapshotObserverInfo> const &snapshot_info, NameIdMapper *name_id_mapper,
                         std::optional<std::size_t> thread_id = std::nullopt) {
  if (!std::ranges::contains(vertex.labels, spec.label_id)) {
    return;
  }
  auto property = vertex.properties.GetProperty(spec.property);
  if (property.IsNull()) {
    return;
  }
  auto vector = property.IsVectorIndexId() ? GetVector(mg_index, &vertex) : ListToVector(property);
  UpdateVectorIndex(mg_index, spec, &vertex, vector, thread_id);
  vertex.properties.SetProperty(spec.property,
                                PropertyValue(utils::small_vector<uint64_t>{name_id_mapper->NameToId(spec.index_name)},
                                              utils::small_vector<float>{}));
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::VECTOR_IDX);
  }
}

}  // namespace

// unum::usearch::index_dense_gt is the index type used for vector indices. It is thread-safe and supports concurrent
// operations.
using synchronized_mg_vector_index_t = utils::Synchronized<mg_vector_index_t, std::shared_mutex>;

// NOLINTNEXTLINE(bugprone-exception-escape)
struct IndexItem {
  // unum::usearch::index_dense_gt is thread-safe and supports concurrent operations. However, we still need to use
  // locking because resizing the index requires exclusive access.
  synchronized_mg_vector_index_t mg_index;
  VectorIndexSpec spec;

  IndexItem(mg_vector_index_t &&index, VectorIndexSpec spec) : mg_index(std::move(index)), spec(std::move(spec)) {}
};

/// @brief Implements the underlying functionality of the `VectorIndex` class.
///
/// The `Impl` structure follows the PIMPL (Pointer to Implementation) idiom to separate
/// the interface of `VectorIndex` from its implementation
struct VectorIndex::Impl {
  /// The `index_` member is a map that associates a `LabelPropKey` (a combination of label and property)
  /// with the pair of a IndexItem. We use unordered_map because IndexItem contains a non-movable
  /// Synchronized wrapper (mutex can't be moved), and unordered_map stores elements in nodes.
  std::unordered_map<LabelPropKey, IndexItem> index_;

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
    throw query::VectorSearchException(fmt::format(
        "Failed to create vector index {}, error message: {}", spec.index_name, mg_vector_index.error.what()));
  }

  if (!mg_vector_index.index.try_reserve(limits)) {
    throw query::VectorSearchException(
        fmt::format("Failed to create vector index {}. Failed to reserve memory for the index", spec.index_name));
  }

  pimpl->index_name_to_label_prop_.try_emplace(spec.index_name, label_prop);
  pimpl->label_to_index_[spec.label_id].emplace(spec.property, spec.index_name);
  pimpl->index_.try_emplace(label_prop, std::move(mg_vector_index.index), spec);

  spdlog::info("Created vector index {}", spec.index_name);
}

void VectorIndex::RecoverIndex(const VectorIndexRecoveryInfo &recovery_info,
                               utils::SkipList<Vertex>::Accessor &vertices, NameIdMapper *name_id_mapper,
                               std::optional<SnapshotObserverInfo> const &snapshot_info) {
  const auto &spec = recovery_info.spec;
  const auto &recovery_entries = recovery_info.index_entries;
  SetupIndex(spec);
  auto &[mg_index, mutable_spec] = pimpl->index_.at({spec.label_id, spec.property});

  auto process_vertex_for_recovery = [&mg_index, &mutable_spec, &recovery_entries, &snapshot_info, name_id_mapper](
                                         Vertex &vertex, std::optional<std::size_t> thread_id) {
    const auto index_id =
        name_id_mapper->NameToId(mutable_spec.index_name);  // NOLINT(clang-analyzer-core.CallAndMessage)
    utils::small_vector<float> vector;
    auto should_set_property = false;

    // First check if we have a pre-computed vector from recovery entries
    if (auto it = recovery_entries.find(vertex.gid); it != recovery_entries.end()) {
      vector = it->second;
    } else {
      // Otherwise, check if vertex has the required label and property
      if (!std::ranges::contains(vertex.labels, mutable_spec.label_id)) {
        return;
      }
      auto property = vertex.properties.GetProperty(mutable_spec.property);
      if (property.IsNull()) {
        return;
      }
      vector = ListToVector(property);
      should_set_property = true;
    }
    if (vector.empty()) {
      return;
    }
    UpdateVectorIndex(mg_index, mutable_spec, &vertex, vector, thread_id);

    if (should_set_property) {
      vertex.properties.SetProperty(
          mutable_spec.property, PropertyValue(utils::small_vector<uint64_t>{index_id}, utils::small_vector<float>{}));
    }
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::VECTOR_IDX);
    }
  };

  if (FLAGS_storage_parallel_schema_recovery && FLAGS_storage_recovery_thread_count > 1) {
    PopulateVectorIndexMultiThreaded(vertices, process_vertex_for_recovery);
  } else {
    PopulateVectorIndexSingleThreaded(vertices,
                                      [&](Vertex &vertex) { process_vertex_for_recovery(vertex, std::nullopt); });
  }
}

void VectorIndex::CleanupFailedIndex(const VectorIndexSpec &spec) {
  const auto label_prop = LabelPropKey{spec.label_id, spec.property};
  pimpl->index_name_to_label_prop_.erase(spec.index_name);
  pimpl->index_.erase(label_prop);
}

void VectorIndex::PopulateIndexOnSingleThread(utils::SkipList<Vertex>::Accessor &vertices, const VectorIndexSpec &spec,
                                              NameIdMapper *name_id_mapper,
                                              std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto &[mg_index, mutable_spec] = pimpl->index_.at({spec.label_id, spec.property});
  PopulateVectorIndexSingleThreaded(vertices, [&](Vertex &vertex) {
    // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
    TryAddVertexToIndex(mg_index, mutable_spec, vertex, snapshot_info, name_id_mapper, std::nullopt);
  });
}

void VectorIndex::PopulateIndexOnMultipleThreads(utils::SkipList<Vertex>::Accessor &vertices,
                                                 const VectorIndexSpec &spec, NameIdMapper *name_id_mapper,
                                                 std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto &[mg_index, mutable_spec] = pimpl->index_.at({spec.label_id, spec.property});
  PopulateVectorIndexMultiThreaded(vertices, [&](Vertex &vertex, std::size_t thread_id) {
    // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
    TryAddVertexToIndex(mg_index, mutable_spec, vertex, snapshot_info, name_id_mapper, std::optional{thread_id});
  });
}

bool VectorIndex::DropIndex(std::string_view index_name, utils::SkipList<Vertex>::Accessor &vertices,
                            NameIdMapper *name_id_mapper) {
  auto it = pimpl->index_name_to_label_prop_.find(index_name);
  if (it == pimpl->index_name_to_label_prop_.end()) {
    return false;
  }
  const auto label_prop = it->second;
  auto &[mg_index, spec] = pimpl->index_.at(label_prop);
  auto locked_index = mg_index.MutableSharedLock();

  std::vector<double> vector(locked_index->dimensions());
  auto restore_vector_from_index = [&](auto *vertex) {
    locked_index->get(vertex, vector.data());
    vertex->properties.SetProperty(label_prop.property(), PropertyValue(vector));
  };

  auto index_id = name_id_mapper->NameToId(index_name);
  for (auto &vertex : vertices) {
    if (locked_index->contains(&vertex)) {
      auto vector_property = vertex.properties.GetProperty(label_prop.property());
      if (UnregisterFromIndex(vector_property, index_id)) {
        restore_vector_from_index(&vertex);
      } else {
        vertex.properties.SetProperty(label_prop.property(), vector_property);
      }
      locked_index->remove(&vertex);
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
  auto matching_index_properties = GetIndicesByLabel(label);
  if (matching_index_properties.empty()) {
    return;
  }

  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  for (auto property_id : vertex_properties) {
    auto it = matching_index_properties.find(property_id);
    if (it == matching_index_properties.end()) continue;

    const auto &idx_name = it->second;
    auto old_property_value = vertex->properties.GetProperty(property_id);

    // Get vector from existing property
    auto vector_property = [&]() {
      if (old_property_value.IsVectorIndexId()) {
        return GetVectorProperty(vertex, name_id_mapper->IdToName(old_property_value.ValueVectorIndexIds()[0]));
      }
      if (old_property_value.IsAnyList()) {
        return ListToVector(old_property_value);
      }
      throw query::VectorSearchException("Vector index property must be a list of floats or integers.");
    }();
    auto &index_item = pimpl->index_.at({label, property_id});
    UpdateVectorIndex(index_item.mg_index, index_item.spec, vertex, vector_property);

    // Update property to VectorIndexId type
    auto ids = old_property_value.IsVectorIndexId() ? old_property_value.ValueVectorIndexIds()
                                                    : utils::small_vector<uint64_t>{};
    ids.push_back(name_id_mapper->NameToId(idx_name));
    vertex->properties.SetProperty(property_id, PropertyValue(std::move(ids), std::move(vector_property)));
  }
}

void VectorIndex::UpdateOnRemoveLabel(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper) {
  auto matching_index_properties = GetIndicesByLabel(label);
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
      auto &index_item = pimpl->index_.at({label, property_id});

      // Get vector value before removal if this is the last index for this property
      auto vector_property_value =
          ids.empty() ? std::optional{GetVectorAsPropertyValue(index_item.mg_index, vertex)} : std::nullopt;

      // Remove vertex from the index
      auto locked_index = index_item.mg_index.MutableSharedLock();
      locked_index->remove(vertex);

      // Update property value of the vertex
      // We transfer list to property store only if it's not in any index anymore
      vertex->properties.SetProperty(property_id,
                                     ids.empty() ? std::move(*vector_property_value) : old_vertex_property_value);
    }
  }
}

void VectorIndex::UpdateOnSetProperty(const PropertyValue &value, Vertex *vertex, NameIdMapper *name_id_mapper) {
  if (!value.IsVectorIndexId()) return;

  const auto &vector_property = value.ValueVectorIndexList();
  const auto &index_ids = value.ValueVectorIndexIds();

  for (auto index_id : index_ids) {
    const auto &idx_name = name_id_mapper->IdToName(index_id);
    const auto &label_prop = pimpl->index_name_to_label_prop_.at(idx_name);
    auto &index_item = pimpl->index_.at(label_prop);
    UpdateVectorIndex(index_item.mg_index, index_item.spec, vertex, vector_property);
  }
}

void VectorIndex::RemoveVertexFromIndex(Vertex *vertex, std::string_view index_name) {
  auto it = pimpl->index_name_to_label_prop_.find(index_name);
  if (it == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(
        fmt::format("Error in removing vertex from index: index name {} does not exist.", index_name));
  }
  auto &[mg_index, spec] = pimpl->index_.at(it->second);
  UpdateVectorIndex(mg_index, spec, vertex, {});
}

utils::small_vector<float> VectorIndex::GetVectorProperty(Vertex *vertex, std::string_view index_name) const {
  auto it = pimpl->index_name_to_label_prop_.find(index_name);
  if (it == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &[mg_index, spec] = pimpl->index_.at(it->second);
  return GetVector(mg_index, vertex);
}

std::vector<VectorIndexInfo> VectorIndex::ListVectorIndicesInfo() const {
  std::vector<VectorIndexInfo> result;
  result.reserve(pimpl->index_.size());
  for (const auto &[_, index_item] : pimpl->index_) {
    const auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index.ReadLock();
    result.emplace_back(spec.index_name,
                        spec.label_id,
                        spec.property,
                        NameFromMetric(locked_index->metric().metric_kind()),
                        static_cast<std::uint16_t>(locked_index->dimensions()),
                        locked_index->capacity(),
                        locked_index->size(),
                        NameFromScalar(locked_index->metric().scalar_kind()));
  }
  return result;
}

std::vector<VectorIndexSpec> VectorIndex::ListIndices() const {
  std::vector<VectorIndexSpec> result;
  result.reserve(pimpl->index_.size());
  std::ranges::transform(pimpl->index_, std::back_inserter(result), [](const auto &label_prop_index_item) {
    return label_prop_index_item.second.spec;
  });
  return result;
}

void VectorIndex::SerializeVectorIndex(durability::BaseEncoder *encoder, std::string_view index_name) const {
  const auto label_prop = pimpl->index_name_to_label_prop_.find(index_name);
  if (label_prop == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &[mg_index, spec] = pimpl->index_.at(label_prop->second);
  auto locked_index = mg_index.ReadLock();
  const auto index_size = locked_index->size();
  const auto dimension = locked_index->dimensions();

  if (index_size == 0) {
    encoder->WriteUint(0);
    return;
  }

  std::vector<Vertex *> vertices(index_size);
  locked_index->export_keys(vertices.data(), 0, index_size);

  auto valid_count = std::ranges::count_if(vertices, [](const auto *vertex) {
    // This is safe because even if the vertex->deleted gets aborted, recovery process is going through vertices skip
    // list and it will restore the vertex.
    return !vertex->deleted;
  });
  encoder->WriteUint(valid_count);

  std::vector<float> buffer(dimension);
  for (auto *vertex : vertices) {
    encoder->WriteUint(vertex->gid.AsUint());
    locked_index->get(vertex, buffer.data());
    std::ranges::for_each(buffer, [&](auto value) { encoder->WriteDouble(value); });
  }
}

std::optional<uint64_t> VectorIndex::ApproximateNodesVectorCount(LabelId label, PropertyId property) const {
  auto it = pimpl->index_.find(LabelPropKey{label, property});
  if (it == pimpl->index_.end()) {
    return std::nullopt;
  }
  auto &[mg_index, spec] = it->second;
  auto locked_index = mg_index.ReadLock();
  return locked_index->size();
}

VectorIndex::VectorSearchNodeResults VectorIndex::SearchNodes(std::string_view index_name, uint64_t result_set_size,
                                                              const std::vector<float> &query_vector) const {
  const auto label_prop = pimpl->index_name_to_label_prop_.find(index_name);
  if (label_prop == pimpl->index_name_to_label_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &[mg_index, spec] = pimpl->index_.at(label_prop->second);

  // The result vector will contain pairs of vertices and their score.
  VectorSearchNodeResults result;
  result.reserve(result_set_size);

  auto locked_index = mg_index.ReadLock();
  const auto result_keys =
      locked_index->filtered_search(query_vector.data(), result_set_size, [](const Vertex *vertex) {
        auto guard = std::shared_lock{vertex->lock};
        return !vertex->deleted;
      });
  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    const auto &vertex = static_cast<Vertex *>(result_keys[i].member.key);
    result.emplace_back(
        vertex,
        static_cast<double>(result_keys[i].distance),
        std::abs(SimilarityFromDistance(locked_index->metric().metric_kind(), result_keys[i].distance)));
  }

  return result;
}

void VectorIndex::RemoveObsoleteEntries(std::stop_token token) const {
  auto maybe_stop = utils::ResettableCounter(2048);
  for (auto &[_, index_item] : pimpl->index_) {
    if (maybe_stop() && token.stop_requested()) {
      return;
    }
    auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index.MutableSharedLock();
    const auto index_size = locked_index->size();
    std::vector<Vertex *> vertices_to_remove(index_size);
    locked_index->export_keys(vertices_to_remove.data(), 0, index_size);

    auto deleted = vertices_to_remove | rv::filter([](const Vertex *vertex) { return vertex->deleted; });
    for (const auto &vertex : deleted) {
      locked_index->remove(vertex);
    }
  }
}

bool VectorIndex::IndexExists(std::string_view index_name) const {
  return pimpl->index_name_to_label_prop_.contains(index_name);
}

bool VectorIndex::Empty() const { return pimpl->index_.empty(); }

utils::small_vector<uint64_t> VectorIndex::GetVectorIndexIdsForVertex(Vertex *vertex, PropertyId property,
                                                                      NameIdMapper *name_id_mapper) {
  auto has_property = [&](const auto &label_prop) { return label_prop.property() == property; };
  auto has_label = [&](const auto &label_prop) { return std::ranges::contains(vertex->labels, label_prop.label()); };
  auto matching_label_props =
      pimpl->index_ | rv::keys | rv::filter(has_label) | rv::filter(has_property) | r::to<std::vector<LabelPropKey>>();
  if (matching_label_props.empty()) {
    return {};
  }
  return matching_label_props | rv::transform([&](const auto &label_prop) {
           const auto &[_, spec] = pimpl->index_.at(label_prop);
           return name_id_mapper->NameToId(spec.index_name);
         }) |
         r::to<utils::small_vector<uint64_t>>();
}

std::unordered_map<PropertyId, std::string> VectorIndex::GetIndicesByLabel(LabelId label) const {
  if (auto it = pimpl->label_to_index_.find(label); it != pimpl->label_to_index_.end()) {
    return it->second;
  }
  return {};
}

std::unordered_map<LabelId, std::string> VectorIndex::GetIndicesByProperty(PropertyId property) const {
  auto has_property = [&](const auto &entry) { return entry.second.contains(property); };
  auto to_label_name = [&](const auto &entry) { return std::pair{entry.first, entry.second.at(property)}; };
  return pimpl->label_to_index_ | rv::filter(has_property) | rv::transform(to_label_name) |
         r::to<std::unordered_map<LabelId, std::string>>();
}

void VectorIndex::AbortEntries(NameIdMapper *name_id_mapper, AbortableInfo &cleanup_collection) {
  for (auto &[vertex, info] : cleanup_collection) {
    const auto &[labels_to_add, labels_to_remove, property_to_abort] = info;
    for (auto label : labels_to_remove) {
      UpdateOnRemoveLabel(label, vertex, name_id_mapper);
    }
    for (auto label : labels_to_add) {
      UpdateOnAddLabel(label, vertex, name_id_mapper);
    }
    for (const auto &[property, value] : property_to_abort) {
      if (value.IsVectorIndexId()) {
        UpdateOnSetProperty(value, vertex, name_id_mapper);
      } else {
        for (const auto &[_, index_name] : GetIndicesByProperty(property)) {
          RemoveVertexFromIndex(vertex, index_name);
        }
      }
    }
  }
}

// AbortProcessor implementation

VectorIndex::AbortProcessor VectorIndex::GetAbortProcessor() const {
  AbortProcessor res{};
  for (const auto &[label_prop, _] : pimpl->index_) {
    const auto label = label_prop.label();
    const auto property = label_prop.property();
    res.l2p[label].push_back(property);
    res.p2l[property].push_back(label);
  }
  return res;
}

void VectorIndex::AbortProcessor::CollectOnLabelRemoval(LabelId label, Vertex *vertex) {
  if (l2p.empty()) return;
  auto properties = l2p.find(label);
  if (properties == l2p.end()) return;
  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  if (!r::any_of(properties->second, [&](auto p) { return r::contains(vertex_properties, p); })) return;
  auto &[label_to_add, label_to_remove, _] = cleanup_collection[vertex];
  label_to_remove.insert(label);
  label_to_add.erase(label);
}

void VectorIndex::AbortProcessor::CollectOnLabelAddition(LabelId label, Vertex *vertex) {
  if (l2p.empty()) return;
  auto properties = l2p.find(label);
  if (properties == l2p.end()) return;
  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  if (!r::any_of(properties->second, [&](auto p) { return r::contains(vertex_properties, p); })) return;
  auto &[label_to_add, label_to_remove, _] = cleanup_collection[vertex];
  label_to_add.insert(label);
  label_to_remove.erase(label);
}

void VectorIndex::AbortProcessor::CollectOnPropertyChange(PropertyId propId, const PropertyValue &old_value,
                                                          Vertex *vertex) {
  if (p2l.empty()) return;
  auto has_any_label = [&](auto label) { return r::contains(vertex->labels, label); };
  auto labels = p2l.find(propId);
  if (labels == p2l.end() || !r::any_of(labels->second, has_any_label)) return;
  auto &[_, label_to_remove, property_to_abort] = cleanup_collection[vertex];
  property_to_abort[propId] = old_value;
}

// VectorIndexRecovery implementation

std::vector<VectorIndexRecoveryInfo *> VectorIndexRecovery::FindMatchingIndices(
    LabelId label, std::vector<VectorIndexRecoveryInfo> &recovery_info_vec) {
  auto has_label = [&](auto &ri) { return ri.spec.label_id == label; };
  auto to_ptr = [](auto &ri) { return &ri; };
  return recovery_info_vec | rv::filter(has_label) | rv::transform(to_ptr) |
         r::to<std::vector<VectorIndexRecoveryInfo *>>();
}

utils::small_vector<float> VectorIndexRecovery::ExtractVectorForRecovery(
    const PropertyValue &value, Vertex *vertex, const std::vector<VectorIndexRecoveryInfo> &recovery_info_vec,
    NameIdMapper *name_id_mapper) {
  // If property is a vector index id, vector is stored in recovery info (index doesn't exist yet).
  // Otherwise, it's a list stored in the property store.
  if (!value.IsVectorIndexId()) {
    return ListToVector(value);
  }

  const auto &ids = value.ValueVectorIndexIds();
  if (ids.empty()) {
    throw query::VectorSearchException("Vector index ID list is empty.");
  }

  const auto &index_name = name_id_mapper->IdToName(ids[0]);
  for (const auto &recovery_info : recovery_info_vec) {
    if (recovery_info.spec.index_name == index_name) {
      if (auto it = recovery_info.index_entries.find(vertex->gid); it != recovery_info.index_entries.end()) {
        return it->second;
      }
      break;
    }
  }
  throw query::VectorSearchException(fmt::format("Vector index {} not found in recovery info.", index_name));
}

void VectorIndexRecovery::UpdateOnIndexDrop(std::string_view index_name, NameIdMapper *name_id_mapper,
                                            std::vector<VectorIndexRecoveryInfo> &recovery_info_vec,
                                            utils::SkipList<Vertex>::Accessor &vertices) {
  for (auto &recovery_info : recovery_info_vec) {
    if (recovery_info.spec.index_name == index_name) {
      for (auto &[gid, vector] : recovery_info.index_entries) {
        auto vertex = vertices.find(gid);
        if (vertex == vertices.end()) {
          recovery_info.index_entries.erase(gid);
          continue;
        }

        auto vertex_property = vertex->properties.GetProperty(recovery_info.spec.property);
        auto index_id = name_id_mapper->NameToId(index_name);

        if (UnregisterFromIndex(vertex_property, index_id)) {
          RestoreVectorOnVertex(&*vertex, recovery_info.spec.property, vector);
        } else {
          vertex->properties.SetProperty(recovery_info.spec.property, vertex_property);
        }
      }
    }
  }
  recovery_info_vec.erase(
      r::remove_if(recovery_info_vec,
                   [&](const auto &recovery_info) { return recovery_info.spec.index_name == index_name; }),
      recovery_info_vec.end());
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
        vertex->properties.SetProperty(
            recovery_info->spec.property,
            PropertyValue(utils::small_vector<uint64_t>{index_id}, utils::small_vector<float>{}));
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

      if (UnregisterFromIndex(old_property_value, index_id)) {
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

void VectorIndexRecovery::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, const Vertex *vertex,
                                              std::vector<VectorIndexRecoveryInfo> &recovery_info_vec) {
  auto type = value.type();
  if (type != PropertyValue::Type::VectorIndexId && type != PropertyValue::Type::List &&
      type != PropertyValue::Type::Null) {
    return;
  }
  const auto vector = [&]() -> utils::small_vector<float> {
    switch (type) {
      case PropertyValue::Type::VectorIndexId:
        return value.ValueVectorIndexList();
      case PropertyValue::Type::List:
        return ListToVector(value);
      default:
        return {};
    }
  }();

  auto matches = [&](const VectorIndexRecoveryInfo &ri) {
    return ri.spec.property == property && r::contains(vertex->labels, ri.spec.label_id);
  };
  for (auto &ri : recovery_info_vec | rv::filter(matches)) {
    ri.index_entries[vertex->gid] = vector;
  }
}

}  // namespace memgraph::storage
