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

#include "storage/v2/property_value.hpp"
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
                                       KeyType key);

/// @brief Retrieves a vector from a USearch index using the get method and returns it as a list of float values.
/// @tparam IndexType The type of the USearch index (e.g., mg_vector_index_t or mg_vector_edge_index_t).
/// @tparam KeyType The type of the key used in the index (e.g., Vertex* or EdgeIndexEntry).
/// @param index The USearch index to retrieve the vector from.
/// @param key The key to look up in the index.
/// @return A list of float values representing the vector.
/// @throws query::VectorSearchException if the key is not found in the index or if retrieval fails.
template <typename IndexType, typename KeyType>
std::vector<float> GetVector(const std::shared_ptr<utils::Synchronized<IndexType, std::shared_mutex>> &index,
                             KeyType key);

/// @brief Converts a PropertyValue list to a vector of floats.
/// @param value The PropertyValue to convert. Must be a list of numeric values (floats or integers).
/// @return A vector of float values.
/// @throws query::VectorSearchException if the value is not a list or contains non-numeric values.
std::vector<float> ListToVector(const PropertyValue &value);
}  // namespace memgraph::storage
