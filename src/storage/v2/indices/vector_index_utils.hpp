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

#include <cstddef>
#include <cstdint>
#include <shared_mutex>
#include <string_view>
#include <vector>

#include "flags/bolt.hpp"
#include "query/exceptions.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
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

}  // namespace memgraph::storage
