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
#include <range/v3/algorithm/find_if.hpp>
#include <ranges>
#include <shared_mutex>
#include <stop_token>
#include <string_view>
#include <usearch/index_plugins.hpp>

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
                              NameIdMapper *name_id_mapper,
                              std::optional<SnapshotObserverInfo> const & /*snapshot_info*/) {
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

    // Update the index with the vertices in parallel using skip list chunks
    const auto num_workers = static_cast<std::size_t>(FLAGS_bolt_num_workers);
    auto chunks = vertices.create_chunks(num_workers);

    // Resizing the underlying index is not thread-safe, so protect it with a mutex.
    std::exception_ptr first_exception;
    std::mutex exception_mutex;
    std::vector<std::jthread> workers;
    workers.reserve(chunks.size());

    for (auto &chunk_idx : chunks) {
      workers.emplace_back([&] {
        try {
          auto &chunk = chunk_idx;
          for (auto &vertex : chunk) {
            if (!utils::Contains(vertex.labels, spec.label_id)) {
              continue;
            }
            auto property = vertex.properties.GetProperty(spec.property);
            if (property.IsNull()) {
              continue;
            }
            if (!property.IsAnyList()) {
              throw query::VectorSearchException("Vector index property must be a list.");
            }
            const auto vector_size = property.ListSize();
            std::vector<float> vector;
            vector.reserve(vector_size);
            for (auto i = 0; i < vector_size; i++) {
              const auto numeric_value = GetNumericValueAt(property, i);
              if (!numeric_value) {
                throw query::VectorSearchException("Vector index property must be a list of floats or integers.");
              }
              const auto float_value =
                  std::visit([](const auto &val) -> float { return static_cast<float>(val); }, *numeric_value);
              vector.push_back(float_value);
            }
            if (vector_size != spec.dimension) {
              throw query::VectorSearchException(
                  "Vector index property must have the same number of dimensions as the index.");
            }
            const auto is_index_full =
                mg_vector_index.index.size() == mg_vector_index.index.capacity();  // TODO: do we want to resize here?
            if (is_index_full) {
              throw query::VectorSearchException("Vector index is full. Try creating index with larger capacity.");
            }

            mg_vector_index.index.add(&vertex, vector.data());
            auto index_id = name_id_mapper->NameToId(spec.index_name);
            vertex.properties.SetProperty(spec.property,
                                          PropertyValue(utils::small_vector<uint64_t>{index_id}, std::vector<float>{}));
          }
        } catch (...) {
          std::lock_guard guard(exception_mutex);
          if (!first_exception) {
            first_exception = std::current_exception();
          }
        }
      });
    }

    for (auto &worker : workers) {
      worker.join();
    }
    if (first_exception) {
      std::rethrow_exception(first_exception);
    }
    pimpl->index_.try_emplace(
        label_prop, IndexItem{.mg_index = std::make_shared<utils::Synchronized<mg_vector_index_t, std::shared_mutex>>(
                                  std::move(mg_vector_index.index)),
                              .spec = spec});
  } catch (const utils::OutOfMemoryException &) {
    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    pimpl->index_name_to_label_prop_.erase(spec.index_name);
    pimpl->index_.erase(label_prop);
    pimpl->label_to_index_.erase(spec.label_id);
    throw;
  }
  return true;
}

void VectorIndex::RecoverIndexEntries(const VectorIndexRecoveryInfo &recovery_info,
                                      utils::SkipList<Vertex>::Accessor &vertices, NameIdMapper *name_id_mapper) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  // Firstly, create the index
  const auto &spec = recovery_info.spec;
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
  pimpl->index_name_to_label_prop_.try_emplace(spec.index_name, label_prop);
  pimpl->label_to_index_[spec.label_id].emplace(spec.property, spec.index_name);
  if (mg_vector_index.index.try_reserve(unum::usearch::index_limits_t(spec.capacity, FLAGS_bolt_num_workers))) {
    spdlog::info("Created vector index {}", spec.index_name);
  } else {
    throw query::VectorSearchException(
        fmt::format("Failed to create vector index {}", spec.index_name, ". Failed to reserve memory for the index"));
  }
  // Secondly, recover index entries in parallel using skip list chunks
  const auto num_workers = static_cast<std::size_t>(FLAGS_bolt_num_workers);
  auto chunks = vertices.create_chunks(num_workers);
  std::vector<std::jthread> workers;
  workers.reserve(chunks.size());
  std::exception_ptr first_exception;
  std::mutex exception_mutex;
  for (const auto &[idx, chunk] : rv::enumerate(chunks)) {
    workers.emplace_back([&, idx] {
      try {
        for (auto &vertex : chunk) {
          if (auto it = recovery_info.index_entries.find(vertex.gid); it != recovery_info.index_entries.end()) {
            const auto &vector = it->second;
            if (!vector.empty()) {
              mg_vector_index.index.add(&vertex, vector.data(), idx, false);
            }
          } else {
            // Maybe the index didn't existed yet, so we need to check labels and properties in order to add the vertex
            // to the index
            if (utils::Contains(vertex.labels, spec.label_id) &&
                r::contains(vertex.properties.ExtractPropertyIds(), spec.property)) {
              auto index_id = name_id_mapper->NameToId(spec.index_name);
              auto vector_property = vertex.properties.GetProperty(spec.property);
              auto vec = ListToVector(vector_property);
              mg_vector_index.index.add(&vertex, vec.data(), idx, false);
              vertex.properties.SetProperty(
                  spec.property, PropertyValue(utils::small_vector<uint64_t>{index_id}, std::vector<float>{}));
            }
          }
        }
      } catch (...) {
        std::lock_guard guard(exception_mutex);
        if (!first_exception) {
          first_exception = std::current_exception();
        }
      }
    });
  }
  for (auto &worker : workers) {
    worker.join();
  }
  if (first_exception) {
    std::rethrow_exception(first_exception);
  }
  pimpl->index_.try_emplace(
      label_prop, IndexItem{.mg_index = std::make_shared<utils::Synchronized<mg_vector_index_t, std::shared_mutex>>(
                                std::move(mg_vector_index.index)),
                            .spec = spec});
}

bool VectorIndex::DropIndex(std::string_view index_name, utils::SkipList<Vertex>::Accessor &vertices) {
  auto it = pimpl->index_name_to_label_prop_.find(index_name);
  if (it == pimpl->index_name_to_label_prop_.end()) {
    return false;
  }
  const auto &label_prop = it->second;
  for (auto &vertex : vertices) {
    if (!utils::Contains(vertex.labels, label_prop.label())) {
      continue;
    }
    vertex.properties.SetProperty(label_prop.property(),
                                  PropertyValue());  // TODO: consider adding vector back to property store
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

std::vector<LabelPropKey> VectorIndex::GetMatchingLabelProps(std::span<LabelId const> labels,
                                                             std::span<PropertyId const> properties) const {
  auto has_property = [&](const auto &label_prop) { return utils::Contains(properties, label_prop.property()); };
  auto has_label = [&](const auto &label_prop) { return utils::Contains(labels, label_prop.label()); };
  return pimpl->index_ | rv::keys | rv::filter(has_label) | rv::filter(has_property) | r::to<std::vector>();
}

void VectorIndex::UpdateRecoveryInfoOnLabelAddition(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper,
                                                    std::vector<VectorIndexRecoveryInfo> &recovery_info_vec) {
  auto matching_indices = std::invoke([&]() {
    std::vector<VectorIndexRecoveryInfo *> indices;
    indices.reserve(recovery_info_vec.size());
    for (auto &recovery_info : recovery_info_vec) {
      if (recovery_info.spec.label_id == label) {
        indices.push_back(&recovery_info);
      }
    }
    return indices;
  });
  if (matching_indices.empty()) {
    return;
  }
  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  for (auto *recovery_info : matching_indices) {
    if (r::contains(vertex_properties, recovery_info->spec.property)) {
      auto vector_to_add = std::invoke([&]() {
        auto old_property_value = vertex->properties.GetProperty(recovery_info->spec.property);
        if (old_property_value.IsVectorIndexId()) {
          auto &ids = old_property_value.ValueVectorIndexIds();
          ids.push_back(name_id_mapper->NameToId(recovery_info->spec.index_name));
          vertex->properties.SetProperty(recovery_info->spec.property, old_property_value);
          // vector is already in the recovery info vector and not on the node
          auto vector = std::invoke([&]() {
            for (auto &recovery_info : recovery_info_vec) {
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
          });
          return vector;
        }

        auto vec = ListToVector(old_property_value);
        vertex->properties.SetProperty(
            recovery_info->spec.property,
            PropertyValue(utils::small_vector<uint64_t>{name_id_mapper->NameToId(recovery_info->spec.index_name)},
                          std::vector<float>{}));
        return vec;
      });
      recovery_info->index_entries.emplace(vertex->gid, std::move(vector_to_add));
    }
  }
}

void VectorIndex::UpdateRecoveryInfoOnLabelRemoval(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper,
                                                   std::vector<VectorIndexRecoveryInfo> &recovery_info_vec) {
  auto matching_indices = std::invoke([&]() {
    std::vector<VectorIndexRecoveryInfo *> indices;
    indices.reserve(recovery_info_vec.size());
    for (auto &recovery_info : recovery_info_vec) {
      if (recovery_info.spec.label_id == label) {
        indices.push_back(&recovery_info);
      }
    }
    return indices;
  });
  if (matching_indices.empty()) {
    return;
  }
  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  for (auto *recovery_info : matching_indices) {
    if (r::contains(vertex_properties, recovery_info->spec.property)) {
      auto old_property_value = vertex->properties.GetProperty(recovery_info->spec.property);
      auto &ids = old_property_value.ValueVectorIndexIds();
      ids.erase(r::remove(ids, name_id_mapper->NameToId(recovery_info->spec.index_name)), ids.end());
      if (ids.empty()) {
        // put it back on the node
        if (auto it = recovery_info->index_entries.find(vertex->gid); it != recovery_info->index_entries.end()) {
          auto &vector_property = it->second;
          auto double_vector = vector_property | rv::transform([](float value) { return static_cast<double>(value); }) |
                               r::to<std::vector<double>>();
          vertex->properties.SetProperty(recovery_info->spec.property, PropertyValue(std::move(double_vector)));
        } else {
          throw query::VectorSearchException(
              fmt::format("Vector index {} not found in recovery info.", name_id_mapper->IdToName(ids[0])));
        }
      } else {
        vertex->properties.SetProperty(recovery_info->spec.property, old_property_value);
      }
      recovery_info->index_entries.erase(vertex->gid);
    }
  }
}

void VectorIndex::UpdateRecoveryInfoOnPropertyChange(PropertyId property, PropertyValue &value, Vertex *vertex,
                                                     std::vector<VectorIndexRecoveryInfo> &recovery_info_vec) {
  // Property has to be in the index because it was stored as VectorIndexId
  for (auto &recovery_info : recovery_info_vec) {
    if (recovery_info.spec.property == property && r::contains(vertex->labels, recovery_info.spec.label_id)) {
      DMG_ASSERT(value.IsVectorIndexId(), "Property value must be a vector index id");
      recovery_info.index_entries[vertex->gid] = value.ValueVectorIndexList();
    }
  }
}

void VectorIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper) {
  auto matching_index_properties = GetProperties(label);
  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  for (const auto &property : vertex_properties) {
    if (auto it = matching_index_properties.find(property); it != matching_index_properties.end()) {
      // update index
      auto old_property_value = vertex->properties.GetProperty(property);
      auto &[mg_index, _] = pimpl->index_.at(LabelPropKey{label, property});
      auto locked_index = mg_index->MutableSharedLock();
      auto vector_index_id = std::invoke([&]() {
        if (old_property_value.IsVectorIndexId()) {
          auto &ids = old_property_value.ValueVectorIndexIds();
          ids.push_back(name_id_mapper->NameToId(it->second));
          auto vec = GetVector(mg_index, vertex);
          return PropertyValue(ids, std::move(vec));
        }
        auto vec = ListToVector(old_property_value);
        return PropertyValue(utils::small_vector<uint64_t>{name_id_mapper->NameToId(it->second)}, std::move(vec));
      });
      locked_index->add(vertex, vector_index_id.ValueVectorIndexList().data());

      // update vertex property store
      vertex->properties.SetProperty(property, vector_index_id);
    }
  }
}

void VectorIndex::UpdateOnRemoveLabel(LabelId label, Vertex *vertex, NameIdMapper *name_id_mapper) {
  auto matching_index_properties = GetProperties(label);
  auto vertex_properties = vertex->properties.ExtractPropertyIds();
  for (const auto &property : vertex_properties) {
    if (auto it = matching_index_properties.find(property); it != matching_index_properties.end()) {
      // update vertex property store
      auto old_vertex_property_value = vertex->properties.GetProperty(property);
      auto &ids = old_vertex_property_value.ValueVectorIndexIds();
      ids.erase(r::remove(ids, name_id_mapper->NameToId(it->second)), ids.end());
      if (ids.empty()) {
        const auto vector_property = GetPropertyValue(vertex, it->second);
        vertex->properties.SetProperty(property, vector_property);
      } else {
        vertex->properties.SetProperty(property, old_vertex_property_value);
      }

      // update index
      auto &[mg_index, _] = pimpl->index_.at(LabelPropKey{label, property});
      auto locked_index = mg_index->MutableSharedLock();
      locked_index->remove(vertex);
    }
  }
}

// TODO: unify this with the other UpdateIndex method
void VectorIndex::UpdateOnSetProperty(const PropertyValue &new_value, Vertex *vertex, NameIdMapper *name_id_mapper) {
  const auto &index_ids =
      new_value.IsVectorIndexId() ? new_value.ValueVectorIndexIds() : utils::small_vector<uint64_t>{};
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
    const auto &vector_property = new_value.ValueVectorIndexList();
    if (vector_property.empty()) {
      continue;
    }
    if (spec.dimension != vector_property.size()) {
      // TODO: is this safe if we have one index that fits and other doesn't?
      throw query::VectorSearchException("Vector index property must have the same number of dimensions as the index.");
    }
    auto locked_index = mg_index->MutableSharedLock();
    locked_index->add(vertex, vector_property.data());
  }
}

std::vector<float> VectorIndex::UpdateIndex(const PropertyValue &value, Vertex *vertex, std::string_view index_name,
                                            NameIdMapper *name_id_mapper) {
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
    // we need unique lock when we are resizing the index
    auto exclusively_locked_index = mg_index->Lock();
    const auto new_size = spec.resize_coefficient * exclusively_locked_index->capacity();
    const unum::usearch::index_limits_t new_limits(new_size, FLAGS_bolt_num_workers);
    if (!exclusively_locked_index->try_reserve(new_limits)) {
      throw query::VectorSearchException("Failed to resize vector index.");
    }
    spec.capacity = exclusively_locked_index->capacity();  // capacity might be larger than the requested capacity
  }
  if (value.IsNull()) {
    // if property is null, that means that the vertex should not be in the index and we shouldn't do any other
    // updates
    return {};
  }
  auto vector_property = std::invoke([&]() {
    if (value.IsVectorIndexId()) {
      // property is in vector index, so we need to return the list from the vector index
      return GetVectorProperty(vertex, name_id_mapper->IdToName(value.ValueVectorIndexIds()[0]));
    }
    if (value.IsAnyList()) {
      const auto list_size = value.ListSize();
      std::vector<float> vector;
      vector.reserve(list_size);
      for (auto i = 0; i < list_size; i++) {
        const auto numeric_value = GetNumericValueAt(value, i);
        if (!numeric_value) {
          throw query::VectorSearchException("Vector index property must be a list of floats or integers.");
        }
        const auto float_value =
            std::visit([](const auto &val) -> float { return static_cast<float>(val); }, *numeric_value);
        vector.push_back(float_value);
      }
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

void VectorIndex::RemoveObsoleteEntries(std::stop_token token) const {
  auto maybe_stop = utils::ResettableCounter(2048);
  for (auto &[_, index_item] : pimpl->index_) {
    if (maybe_stop() && token.stop_requested()) {
      return;
    }
    auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index->MutableSharedLock();
    std::vector<Vertex *> vertices_to_remove(locked_index->size());
    locked_index->export_keys(vertices_to_remove.data(), 0, locked_index->size());  // TODO(@DavIvek): not safe

    auto deleted = vertices_to_remove | rv::filter([](const Vertex *vertex) {
                     auto guard = std::shared_lock{vertex->lock};
                     return vertex->deleted;
                   });
    for (const auto &vertex : deleted) {
      locked_index->remove(vertex);
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
        UpdateOnSetProperty(value, vertex, name_id_mapper);
      } else {
        for (const auto &index_name : GetLabels(property)) {
          RemoveVertexFromIndex(vertex, index_name.second);
        }
      }
    }
  }
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
  auto has_any_label = [&](const auto &label) { return utils::Contains(vertex->labels, label); };
  if (labels == p2l.end() || !r::any_of(labels->second, has_any_label)) return;
  auto &[_, label_to_remove, property_to_abort] = cleanup_collection[vertex];
  property_to_abort[propId] = old_value;
}

bool VectorIndex::IndexExists(std::string_view index_name) const {
  return pimpl->index_name_to_label_prop_.contains(index_name);
}

bool VectorIndex::IsPropertyInVectorIndex(PropertyId property) const {
  return r::any_of(pimpl->index_, [&](const auto &label_prop_index_item) {
    return label_prop_index_item.second.spec.property == property;
  });
}

bool VectorIndex::IsLabelInVectorIndex(LabelId label) const {
  return r::any_of(pimpl->index_, [&](const auto &label_prop_index_item) {
    return label_prop_index_item.second.spec.label_id == label;
  });
}

utils::small_vector<uint64_t> VectorIndex::IsVertexInVectorIndex(Vertex *vertex, PropertyId property,
                                                                 NameIdMapper *name_id_mapper) {
  auto matching_label_props = GetMatchingLabelProps(vertex->labels, std::array{property});
  if (matching_label_props.empty()) {
    return {};
  }
  utils::small_vector<uint64_t> index_ids;
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

std::unordered_map<LabelId, std::string> VectorIndex::GetLabels(PropertyId property) const {
  std::unordered_map<LabelId, std::string> result;
  for (const auto &[label, properties] : pimpl->label_to_index_) {
    if (utils::Contains(properties, property)) {
      result[label] = properties.at(property);
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
  auto &[mg_index, _] = pimpl->index_.at(it->second);
  auto locked_index = mg_index->MutableSharedLock();
  locked_index->remove(vertex);
}

}  // namespace memgraph::storage
