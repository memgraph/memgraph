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

#pragma once

#include <string_view>
#include <vector>
#include "flags/bolt.hpp"
#include "flags/general.hpp" #include < shared_mutex>

#include "flags/bolt.hpp"
#include "query/exceptions.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index_plugins.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

/// @enum VectorIndexType
/// @brief Represents the type of vector index.
enum class VectorIndexType : uint8_t {
  ON_NODES,
  ON_EDGES,
};

/// @brief Converts a VectorIndexType to a string representation.
/// @param type The VectorIndexType to convert.
/// @return A string representation of the VectorIndexType.
/// @throws query::VectorSearchException if the type is unsupported.
constexpr const char *VectorIndexTypeToString(VectorIndexType type) {
  switch (type) {
    case VectorIndexType::ON_NODES:
      return "label+property_vector";
    case VectorIndexType::ON_EDGES:
      return "edge-type+property_vector";
    default:
      return "unsupported vector index type";
  }
}

/// @struct VectorIndexConfigMap
/// @brief Represents the configuration options for a vector index.
///
/// This structure includes the metric name, the dimension of the vectors in the index,
/// the capacity of the index, and the resize coefficient for the index.
struct VectorIndexConfigMap {
  unum::usearch::metric_kind_t metric;
  std::uint16_t dimension;
  std::size_t capacity;
  std::uint16_t resize_coefficient;
  unum::usearch::scalar_kind_t scalar_kind;
};

/// @brief Converts a metric kind to its string representation.
/// @param metric The metric kind to convert.
/// @return A string representation of the metric kind.
/// @throws query::VectorSearchException if the metric kind is unsupported.
const char *NameFromMetric(unum::usearch::metric_kind_t metric);

/// @brief Converts a metric name to its corresponding metric kind.
/// @param name The name of the metric.
/// @return The corresponding metric kind.
/// @throws query::VectorSearchException if the metric name is unsupported.
unum::usearch::metric_kind_t MetricFromName(std::string_view name);

/// @brief Converts a scalar kind to its string representation.
/// @param scalar The scalar kind to convert.
/// @return A string representation of the scalar kind.
/// @throws query::VectorSearchException if the scalar kind is unsupported.
const char *NameFromScalar(unum::usearch::scalar_kind_t scalar);

/// @brief Converts a scalar name to its corresponding scalar kind.
/// @param name The name of the scalar.
/// @return The corresponding scalar kind.
/// @throws query::VectorSearchException if the scalar name is unsupported.
unum::usearch::scalar_kind_t ScalarFromName(std::string_view name);

/// @brief Converts a distance to a similarity score based on the metric kind.
/// @param metric The metric kind used for the distance.
/// @param distance The distance value to convert.
/// @return The similarity score corresponding to the distance.
/// @throws query::VectorSearchException if the metric kind is unsupported.
double SimilarityFromDistance(unum::usearch::metric_kind_t metric, double distance);

/// @brief Retrieves a vector from a USearch index using the get method and returns it as a PropertyValue of type
/// double.
/// @tparam IndexType The type of the USearch index (e.g., mg_vector_index_t or mg_vector_edge_index_t).
/// @tparam KeyType The type of the key used in the index (e.g., Vertex* or EdgeIndexEntry).
/// @param index The USearch index to retrieve the vector from.
/// @param key The key to look up in the index.
/// @return A PropertyValue containing the vector as a list of double values.
/// @throws query::VectorSearchException if the key is not found in the index or if retrieval fails.
template <typename IndexType, typename KeyType>
PropertyValue GetVectorAsPropertyValue(const std::shared_ptr<utils::Synchronized<IndexType, std::shared_mutex>> &index,
                                       KeyType key) {
  auto locked_index = index->ReadLock();
  const auto dimension = locked_index->dimensions();
  std::vector<double> vector(dimension);
  const auto retrieved_count = locked_index->get(key, vector.data());
  if (retrieved_count == 0) {
    return {};
  }
  std::vector<PropertyValue> double_values;
  double_values.reserve(dimension);
  for (const auto &value : vector) {
    double_values.emplace_back(static_cast<double>(value));
  }
  return PropertyValue(std::move(double_values));
}

/// @brief Retrieves a vector from a USearch index using the get method and returns it as a list of float values.
/// @tparam IndexType The type of the USearch index (e.g., mg_vector_index_t or mg_vector_edge_index_t).
/// @tparam KeyType The type of the key used in the index (e.g., Vertex* or EdgeIndexEntry).
/// @param index The USearch index to retrieve the vector from.
/// @param key The key to look up in the index.
/// @return A list of float values representing the vector.
/// @throws query::VectorSearchException if the key is not found in the index or if retrieval fails.
template <typename IndexType, typename KeyType>
std::vector<float> GetVector(const std::shared_ptr<utils::Synchronized<IndexType, std::shared_mutex>> &index,
                             KeyType key) {
  auto locked_index = index->ReadLock();
  const auto dimension = locked_index->dimensions();
  std::vector<unum::usearch::f32_t> vector(dimension);
  const auto retrieved_count = locked_index->get(key, vector.data(), 1);
  if (retrieved_count == 0) {
    return {};
  }
  return vector;
}

/// @brief Converts a PropertyValue list to a vector of floats.
/// @param value The PropertyValue to convert. Must be a list of numeric values (floats or integers).
/// @return A vector of float values.
/// @throws query::VectorSearchException if the value is not a list or contains non-numeric values.
std::vector<float> ListToVector(const PropertyValue &value);

/// @brief Converts a vector of floats to a vector of doubles (for PropertyValue storage).
/// @param vector The vector of floats to convert.
/// @return A vector of double values.
std::vector<double> FloatVectorToDoubleVector(const std::vector<float> &vector);

/// @brief Restores a vector property on a vertex by setting it as a PropertyValue with double values.
/// @param vertex The vertex to restore the property on.
/// @param property_id The property ID to restore.
/// @param vector The vector of float values to restore.
void RestoreVectorOnVertex(Vertex *vertex, PropertyId property_id, const std::vector<float> &vector);

/// @brief Creates a PropertyValue from a vector and index IDs for vector index storage.
/// @param vector The vector of float values.
/// @param index_ids The index IDs associated with this vector.
/// @return A PropertyValue containing the vector index ID and vector data.
PropertyValue CreateVectorIndexIdProperty(const std::vector<float> &vector,
                                          const utils::small_vector<uint64_t> &index_ids);

/// @brief Removes an index ID from a property's vector index ID list.
/// @param property_value The property value to modify (must be a VectorIndexId).
/// @param index_id The index ID to remove.
/// @return true if the property should be restored (no more index IDs), false otherwise.
bool RemoveIndexIdFromProperty(PropertyValue &property_value, uint64_t index_id);

// Helper function to validate vector dimension
void ValidateVectorDimension(const std::vector<float> &vector, std::uint16_t expected_dimension);

/// @brief Updates a single vector index with a vector (common logic for vertex and edge indices).
/// @tparam IndexItemType Type of the index item (must have mg_index and spec members).
/// @tparam KeyType Type of the key used in the index (e.g., Vertex* or EdgeIndexEntry).
/// @param index_item The index item containing the synchronized index and spec.
/// @param key The key to add/update in the index.
/// @param vector The vector data to add.
/// @param update_capacity Whether to update the spec.capacity after resizing (default: true).
/// @throws query::VectorSearchException if dimension mismatch or resize fails.
template <typename IndexItemType, typename KeyType>
void UpdateSingleVectorIndex(IndexItemType &index_item, KeyType key, const std::vector<float> &vector,
                             bool update_capacity = true) {
  auto &[mg_index, spec] = index_item;
  bool is_index_full = false;
  {
    auto locked_index = mg_index->MutableSharedLock();
    if (locked_index->contains(key)) {
      locked_index->remove(key);
    }
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
    if (update_capacity) {
      spec.capacity = exclusively_locked_index->capacity();  // capacity might be larger than the requested capacity
    }
  }

  if (vector.empty()) {
    return;
  }

  if (spec.dimension != vector.size()) {
    throw query::VectorSearchException("Vector index property must have the same number of dimensions as the index.");
  }

  auto locked_index = mg_index->MutableSharedLock();
  locked_index->add(key, vector.data());
}

/// @brief Converts a property value to a float vector for vector index operations.
/// @param property The property value to convert (must be a list of numeric values).
/// @param expected_dimension The expected dimension of the vector.
/// @return A vector of floats representing the property value.
/// @throws query::VectorSearchException if the property is not a valid vector.
[[nodiscard]] inline std::vector<float> PropertyToFloatVector(const PropertyValue &property,
                                                              std::uint16_t expected_dimension) {
  if (!property.IsAnyList()) {
    throw query::VectorSearchException("Vector index property must be a list.");
  }

  const auto vector_size = GetListSize(property);
  if (expected_dimension != vector_size) {
    throw query::VectorSearchException("Vector index property must have the same number of dimensions as the index.");
  }

  std::vector<float> vector;
  vector.reserve(vector_size);
  for (size_t i = 0; i < vector_size; ++i) {
    const auto numeric_value = GetNumericValueAt(property, i);
    if (!numeric_value) {
      throw query::VectorSearchException("Vector index property must be a list of numeric values.");
    }
    vector.push_back(std::visit([](const auto &val) -> float { return static_cast<float>(val); }, *numeric_value));
  }
  return vector;
}

/// @brief Returns the maximum number of concurrent threads for vector index operations.
inline std::size_t GetVectorIndexThreadCount() {
  return std::max(static_cast<std::size_t>(FLAGS_bolt_num_workers),
                  static_cast<std::size_t>(FLAGS_storage_recovery_thread_count));
}

/// @brief Adds an entry to the vector index with automatic resize if the index is full.
/// No need to throw if the error occurred because it will be raised on result destruction.
/// @tparam Index The usearch index type (e.g., index_dense_gt<Key, ...>).
/// @tparam Key The key type used in the index (e.g., Vertex*, EdgeIndexEntry).
/// @tparam Spec The index specification type.
/// @param mg_index The synchronized index wrapper.
/// @param spec The index specification (will be updated if resize occurs).
/// @param key The key to add to the index.
/// @param vector_data Pointer to the float vector data.
/// @param thread_id Optional thread ID hint for usearch's internal thread-local optimizations.
/// @throws query::VectorSearchException if add fails for reasons other than capacity.
template <typename Index, typename Key, typename Spec>
void AddToVectorIndex(utils::Synchronized<Index, std::shared_mutex> &mg_index, Spec &spec, const Key &key,
                      const float *vector_data, std::optional<std::size_t> thread_id = std::nullopt) {
  const auto thread_id_for_adding = thread_id ? *thread_id : Index::any_thread();
  {
    auto locked_index = mg_index.MutableSharedLock();
    auto result = locked_index->add(key, vector_data, thread_id_for_adding);
    if (!result.error) return;
    if (locked_index->size() >= locked_index->capacity()) {
      // Error is due to capacity, release the error because we will resize the index.
      result.error.release();
    }
  }
  {
    // In order to resize the index, we need to acquire an exclusive lock.
    auto exclusively_locked_index = mg_index.Lock();
    if (exclusively_locked_index->size() >= exclusively_locked_index->capacity()) {
      const auto new_size = static_cast<std::size_t>(spec.resize_coefficient * exclusively_locked_index->capacity());
      const unum::usearch::index_limits_t new_limits(new_size, GetVectorIndexThreadCount());
      if (!exclusively_locked_index->try_reserve(new_limits)) {
        throw query::VectorSearchException("Failed to resize vector index.");
      }
      spec.capacity = exclusively_locked_index->capacity();
    }
    auto result = exclusively_locked_index->add(key, vector_data, thread_id_for_adding);
  }
}

/// @brief Populates a vector index by iterating over vertices on a single thread.
/// @tparam SyncIndex The synchronized index wrapper type.
/// @tparam Spec The index specification type.
/// @tparam ProcessFunc Callable with signature void(SyncIndex&, Spec&, Vertex&, const
/// std::optional<SnapshotObserverInfo>&, std::optional<std::size_t> thread_id).
/// @param mg_index The synchronized index wrapper.
/// @param spec The index specification (may be modified if resize occurs).
/// @param vertices The vertices accessor to iterate over.
/// @param snapshot_info Optional snapshot observer info.
/// @param process The function to call for each vertex.
template <typename SyncIndex, typename Spec, typename ProcessFunc>
void PopulateVectorIndexSingleThreaded(SyncIndex &mg_index, Spec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                                       std::optional<SnapshotObserverInfo> const &snapshot_info,
                                       const ProcessFunc &process) {
  for (auto &vertex : vertices) {
    process(mg_index, spec, vertex, snapshot_info, std::nullopt);
  }
}

/// @brief Populates a vector index by iterating over vertices using multiple threads.
/// @tparam SyncIndex The synchronized index wrapper type.
/// @tparam Spec The index specification type (must have resize_coefficient and capacity).
/// @tparam ProcessFunc Callable with signature void(SyncIndex&, Spec&, Vertex&, const
/// std::optional<SnapshotObserverInfo>&, std::optional<std::size_t> thread_id).
/// @param mg_index The synchronized index wrapper.
/// @param spec The index specification (may be modified if resize occurs).
/// @param vertices The vertices accessor to iterate over.
/// @param snapshot_info Optional snapshot observer info.
/// @param process The function to call for each vertex.
template <typename SyncIndex, typename Spec, typename ProcessFunc>
void PopulateVectorIndexMultiThreaded(SyncIndex &mg_index, Spec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                                      std::optional<SnapshotObserverInfo> const &snapshot_info,
                                      const ProcessFunc &process) {
  const auto thread_count = FLAGS_storage_recovery_thread_count;
  auto vertices_chunks = vertices.create_chunks(thread_count);
  std::vector<std::jthread> threads;
  threads.reserve(thread_count);

  for (std::size_t i = 0; i < thread_count; ++i) {
    threads.emplace_back([&, i]() {
      auto &chunk = vertices_chunks[i];
      for (auto &vertex : chunk) {
        process(mg_index, spec, vertex, snapshot_info, i);
      }
    });
  }

  auto locked_index = mg_index->MutableSharedLock();
  locked_index->add(key, vector.data());
}

/// @brief Converts a property value to a float vector for vector index operations.
/// @param property The property value to convert (must be a list of numeric values).
/// @param expected_dimension The expected dimension of the vector.
/// @return A vector of floats representing the property value.
/// @throws query::VectorSearchException if the property is not a valid vector.
[[nodiscard]] inline std::vector<float> PropertyToFloatVector(const PropertyValue &property,
                                                              std::uint16_t expected_dimension) {
  if (!property.IsAnyList()) {
    throw query::VectorSearchException("Vector index property must be a list.");
  }

  const auto vector_size = GetListSize(property);
  if (expected_dimension != vector_size) {
    throw query::VectorSearchException("Vector index property must have the same number of dimensions as the index.");
  }

  std::vector<float> vector;
  vector.reserve(vector_size);
  for (size_t i = 0; i < vector_size; ++i) {
    const auto numeric_value = GetNumericValueAt(property, i);
    if (!numeric_value) {
      throw query::VectorSearchException("Vector index property must be a list of numeric values.");
    }
    vector.push_back(std::visit([](const auto &val) -> float { return static_cast<float>(val); }, *numeric_value));
  }
  return vector;
}

/// @brief Returns the maximum number of concurrent threads for vector index operations.
inline std::size_t GetVectorIndexThreadCount() {
  return std::max(static_cast<std::size_t>(FLAGS_bolt_num_workers),
                  static_cast<std::size_t>(FLAGS_storage_recovery_thread_count));
}

/// @brief Adds an entry to the vector index with automatic resize if the index is full.
/// No need to throw if the error occurred because it will be raised on result destruction.
/// @tparam Index The usearch index type (e.g., index_dense_gt<Key, ...>).
/// @tparam Key The key type used in the index (e.g., Vertex*, EdgeIndexEntry).
/// @tparam Spec The index specification type.
/// @param mg_index The synchronized index wrapper.
/// @param spec The index specification (will be updated if resize occurs).
/// @param key The key to add to the index.
/// @param vector_data Pointer to the float vector data.
/// @param thread_id Optional thread ID hint for usearch's internal thread-local optimizations.
/// @throws query::VectorSearchException if add fails for reasons other than capacity.
template <typename Index, typename Key, typename Spec>
void AddToVectorIndex(utils::Synchronized<Index, std::shared_mutex> &mg_index, Spec &spec, const Key &key,
                      const float *vector_data, std::optional<std::size_t> thread_id = std::nullopt) {
  const auto thread_id_for_adding = thread_id ? *thread_id : Index::any_thread();
  {
    auto locked_index = mg_index.MutableSharedLock();
    auto result = locked_index->add(key, vector_data, thread_id_for_adding);
    if (!result.error) return;
    if (locked_index->size() >= locked_index->capacity()) {
      // Error is due to capacity, release the error because we will resize the index.
      result.error.release();
    }
  }
  {
    // In order to resize the index, we need to acquire an exclusive lock.
    auto exclusively_locked_index = mg_index.Lock();
    if (exclusively_locked_index->size() >= exclusively_locked_index->capacity()) {
      const auto new_size = static_cast<std::size_t>(spec.resize_coefficient * exclusively_locked_index->capacity());
      const unum::usearch::index_limits_t new_limits(new_size, GetVectorIndexThreadCount());
      if (!exclusively_locked_index->try_reserve(new_limits)) {
        throw query::VectorSearchException("Failed to resize vector index.");
      }
      spec.capacity = exclusively_locked_index->capacity();
    }
    auto result = exclusively_locked_index->add(key, vector_data, thread_id_for_adding);
  }
}

/// @brief Populates a vector index by iterating over vertices on a single thread.
/// @tparam SyncIndex The synchronized index wrapper type.
/// @tparam Spec The index specification type.
/// @tparam ProcessFunc Callable with signature void(SyncIndex&, Spec&, Vertex&, const
/// std::optional<SnapshotObserverInfo>&, std::optional<std::size_t> thread_id).
/// @param mg_index The synchronized index wrapper.
/// @param spec The index specification (may be modified if resize occurs).
/// @param vertices The vertices accessor to iterate over.
/// @param snapshot_info Optional snapshot observer info.
/// @param process The function to call for each vertex.
template <typename SyncIndex, typename Spec, typename ProcessFunc>
void PopulateVectorIndexSingleThreaded(SyncIndex &mg_index, Spec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                                       std::optional<SnapshotObserverInfo> const &snapshot_info,
                                       const ProcessFunc &process) {
  for (auto &vertex : vertices) {
    process(mg_index, spec, vertex, snapshot_info, std::nullopt);
  }
}

/// @brief Populates a vector index by iterating over vertices using multiple threads.
/// @tparam SyncIndex The synchronized index wrapper type.
/// @tparam Spec The index specification type (must have resize_coefficient and capacity).
/// @tparam ProcessFunc Callable with signature void(SyncIndex&, Spec&, Vertex&, const
/// std::optional<SnapshotObserverInfo>&, std::optional<std::size_t> thread_id).
/// @param mg_index The synchronized index wrapper.
/// @param spec The index specification (may be modified if resize occurs).
/// @param vertices The vertices accessor to iterate over.
/// @param snapshot_info Optional snapshot observer info.
/// @param process The function to call for each vertex.
template <typename SyncIndex, typename Spec, typename ProcessFunc>
void PopulateVectorIndexMultiThreaded(SyncIndex &mg_index, Spec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                                      std::optional<SnapshotObserverInfo> const &snapshot_info,
                                      const ProcessFunc &process) {
  const auto thread_count = FLAGS_storage_recovery_thread_count;
  auto vertices_chunks = vertices.create_chunks(thread_count);
  std::vector<std::jthread> threads;
  threads.reserve(thread_count);

  for (std::size_t i = 0; i < thread_count; ++i) {
    threads.emplace_back([&, i]() {
      auto &chunk = vertices_chunks[i];
      for (auto &vertex : chunk) {
        process(mg_index, spec, vertex, snapshot_info, i);
      }
    });
  }
}

}  // namespace memgraph::storage
