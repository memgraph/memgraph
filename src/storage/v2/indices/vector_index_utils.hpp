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

#include "flags/bolt.hpp"
#include "flags/general.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index_plugins.hpp"
#include "utils/skip_list.hpp"
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
inline const char *NameFromMetric(unum::usearch::metric_kind_t metric) {
  switch (metric) {
    case unum::usearch::metric_kind_t::l2sq_k:
      return "l2sq";
    case unum::usearch::metric_kind_t::ip_k:
      return "ip";
    case unum::usearch::metric_kind_t::cos_k:
      return "cos";
    case unum::usearch::metric_kind_t::haversine_k:
      return "haversine";
    case unum::usearch::metric_kind_t::divergence_k:
      return "divergence";
    case unum::usearch::metric_kind_t::pearson_k:
      return "pearson";
    case unum::usearch::metric_kind_t::hamming_k:
      return "hamming";
    case unum::usearch::metric_kind_t::tanimoto_k:
      return "tanimoto";
    case unum::usearch::metric_kind_t::sorensen_k:
      return "sorensen";
    default:
      throw query::VectorSearchException(
          "Unsupported metric kind. Supported metrics are l2sq, ip, cos, haversine, divergence, pearson, hamming, "
          "tanimoto, and sorensen.");
  }
}

/// @brief Converts a metric name to its corresponding metric kind.
/// @param name The name of the metric.
/// @return The corresponding metric kind.
/// @throws query::VectorSearchException if the metric name is unsupported.
inline unum::usearch::metric_kind_t MetricFromName(std::string_view name) {
  if (name == "l2sq" || name == "euclidean_sq") {
    return unum::usearch::metric_kind_t::l2sq_k;
  }
  if (name == "ip" || name == "inner" || name == "dot") {
    return unum::usearch::metric_kind_t::ip_k;
  }
  if (name == "cos" || name == "angular") {
    return unum::usearch::metric_kind_t::cos_k;
  }
  if (name == "haversine") {
    return unum::usearch::metric_kind_t::haversine_k;
  }
  if (name == "divergence") {
    return unum::usearch::metric_kind_t::divergence_k;
  }
  if (name == "pearson") {
    return unum::usearch::metric_kind_t::pearson_k;
  }
  if (name == "hamming") {
    return unum::usearch::metric_kind_t::hamming_k;
  }
  if (name == "tanimoto") {
    return unum::usearch::metric_kind_t::tanimoto_k;
  }
  if (name == "sorensen") {
    return unum::usearch::metric_kind_t::sorensen_k;
  }
  throw query::VectorSearchException(
      fmt::format("Unsupported metric name: {}. Supported metrics are l2sq, ip, cos, haversine, divergence, pearson, "
                  "hamming, tanimoto, and sorensen.",
                  name));
}

/// @brief Converts a scalar kind to its string representation.
/// @param scalar The scalar kind to convert.
/// @return A string representation of the scalar kind.
/// @throws query::VectorSearchException if the scalar kind is unsupported.
inline const char *NameFromScalar(unum::usearch::scalar_kind_t scalar) {
  switch (scalar) {
    case unum::usearch::scalar_kind_t::b1x8_k:
      return "b1x8";
    case unum::usearch::scalar_kind_t::u40_k:
      return "u40";
    case unum::usearch::scalar_kind_t::uuid_k:
      return "uuid";
    case unum::usearch::scalar_kind_t::bf16_k:
      return "bf16";
    case unum::usearch::scalar_kind_t::f64_k:
      return "f64";
    case unum::usearch::scalar_kind_t::f32_k:
      return "f32";
    case unum::usearch::scalar_kind_t::f16_k:
      return "f16";
    case unum::usearch::scalar_kind_t::f8_k:
      return "f8";
    case unum::usearch::scalar_kind_t::u64_k:
      return "u64";
    case unum::usearch::scalar_kind_t::u32_k:
      return "u32";
    case unum::usearch::scalar_kind_t::u16_k:
      return "u16";
    case unum::usearch::scalar_kind_t::u8_k:
      return "u8";
    case unum::usearch::scalar_kind_t::i64_k:
      return "i64";
    case unum::usearch::scalar_kind_t::i32_k:
      return "i32";
    case unum::usearch::scalar_kind_t::i16_k:
      return "i16";
    case unum::usearch::scalar_kind_t::i8_k:
      return "i8";
    default:
      throw query::VectorSearchException(
          "Unsupported scalar kind. Supported scalars are b1x8, u40, uuid, bf16, f64, f32, f16, f8, "
          "u64, u32, u16, u8, i64, i32, i16, and i8.");
  }
}

/// @brief Converts a scalar name to its corresponding scalar kind.
/// @param name The name of the scalar.
/// @return The corresponding scalar kind.
/// @throws query::VectorSearchException if the scalar name is unsupported.
inline unum::usearch::scalar_kind_t ScalarFromName(std::string_view name) {
  if (name == "b1x8" || name == "binary") {
    return unum::usearch::scalar_kind_t::b1x8_k;
  }
  if (name == "u40") {
    return unum::usearch::scalar_kind_t::u40_k;
  }
  if (name == "uuid") {
    return unum::usearch::scalar_kind_t::uuid_k;
  }
  if (name == "bf16" || name == "bfloat16") {
    return unum::usearch::scalar_kind_t::bf16_k;
  }
  if (name == "f64" || name == "float64" || name == "double") {
    return unum::usearch::scalar_kind_t::f64_k;
  }
  if (name == "f32" || name == "float32" || name == "float") {
    return unum::usearch::scalar_kind_t::f32_k;
  }
  if (name == "f16" || name == "float16") {
    return unum::usearch::scalar_kind_t::f16_k;
  }
  if (name == "f8" || name == "float8") {
    return unum::usearch::scalar_kind_t::f8_k;
  }
  if (name == "u64" || name == "uint64") {
    return unum::usearch::scalar_kind_t::u64_k;
  }
  if (name == "u32" || name == "uint32") {
    return unum::usearch::scalar_kind_t::u32_k;
  }
  if (name == "u16" || name == "uint16") {
    return unum::usearch::scalar_kind_t::u16_k;
  }
  if (name == "u8" || name == "uint8") {
    return unum::usearch::scalar_kind_t::u8_k;
  }
  if (name == "i64" || name == "int64") {
    return unum::usearch::scalar_kind_t::i64_k;
  }
  if (name == "i32" || name == "int32") {
    return unum::usearch::scalar_kind_t::i32_k;
  }
  if (name == "i16" || name == "int16") {
    return unum::usearch::scalar_kind_t::i16_k;
  }
  if (name == "i8" || name == "int8") {
    return unum::usearch::scalar_kind_t::i8_k;
  }

  throw query::VectorSearchException(
      fmt::format("Unsupported scalar name: {}. Supported scalars are b1x8, u40, uuid, bf16, f64, f32, f16, f8, "
                  "u64, u32, u16, u8, i64, i32, i16, and i8.",
                  name));
}

/// @brief Converts a distance to a similarity score based on the metric kind.
/// @param metric The metric kind used for the distance.
/// @param distance The distance value to convert.
/// @return The similarity score corresponding to the distance.
/// @throws query::VectorSearchException if the metric kind is unsupported.
inline double SimilarityFromDistance(unum::usearch::metric_kind_t metric, double distance) {
  switch (metric) {
    case unum::usearch::metric_kind_t::ip_k:
    case unum::usearch::metric_kind_t::cos_k:
    case unum::usearch::metric_kind_t::pearson_k:
    case unum::usearch::metric_kind_t::hamming_k:
    case unum::usearch::metric_kind_t::tanimoto_k:
    case unum::usearch::metric_kind_t::sorensen_k:
    case unum::usearch::metric_kind_t::jaccard_k:
      return 1.0 - distance;

    case unum::usearch::metric_kind_t::l2sq_k:
    case unum::usearch::metric_kind_t::haversine_k:
    case unum::usearch::metric_kind_t::divergence_k:
      return 1.0 / (1.0 + distance);

    default:
      throw query::VectorSearchException(
          fmt::format("Unsupported metric kind for similarity calculation: {}", NameFromMetric(metric)));
  }
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
