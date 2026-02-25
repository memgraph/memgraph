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

#pragma once

#include <concepts>
#include <optional>
#include "flags/bolt.hpp"
#include "flags/general.hpp"
#include "query/exceptions.hpp"
#include "range/v3/algorithm/remove.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/embeddings_memory_counter.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

// Suppress usearch library warnings
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-W#warnings"
#endif

#include "usearch/index_plugins.hpp"

#if defined(__clang__)
#pragma clang diagnostic pop
#endif

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

/// @brief Converts a PropertyValue list to a vector of floats.
/// @param value The PropertyValue to convert. Must be a list of numeric values (floats or integers).
/// @return A vector of float values.
/// @throws query::VectorSearchException if the value is not a list or contains non-numeric values.
inline utils::small_vector<float> ListToVector(const PropertyValue &value) {
  if (value.IsNull()) return {};
  if (!value.IsAnyList())
    throw query::VectorSearchException("Vector index property must be a list of floats or integers.");

  const auto list_size = value.ListSize();
  utils::small_vector<float> vector;
  vector.reserve(list_size);
  for (std::size_t i = 0; i < list_size; i++) {
    auto numeric_value = GetNumericValueAt(value, i);
    if (!numeric_value) {
      throw query::VectorSearchException(
          "Vector index property must be a list of floats or integers; found non-numeric value at index.");
    }
    auto float_value = std::visit([](auto val) { return static_cast<float>(val); }, *numeric_value);
    vector.push_back(float_value);
  }
  return vector;
}

/// @brief Removes an index ID from a property's vector index ID list.
/// @param property_value The property value to modify (must be a VectorIndexId).
/// @param index_id The index ID to remove.
/// @return true if the property should be restored (no more index IDs), false otherwise.
inline bool ShouldUnregisterFromIndex(PropertyValue &property_value, uint64_t index_id) {
  if (!property_value.IsVectorIndexId()) {
    return true;  // Not a vector index ID, should restore
  }
  auto &ids = property_value.ValueVectorIndexIds();
  ids.erase(ranges::remove(ids, index_id), ids.end());
  return ids.empty();  // Return true if should restore (no more IDs)
}

/// @brief Returns the maximum number of concurrent threads for vector index operations.
inline std::size_t GetVectorIndexThreadCount() {
  return std::max(static_cast<std::size_t>(FLAGS_bolt_num_workers),
                  static_cast<std::size_t>(FLAGS_storage_recovery_thread_count));
}

/// Per-vector mmap memory estimate split by allocator.
/// Needed because each usearch index has two independent arena allocator chains
/// (HNSW tape and vector tape) with different doubling progressions.
struct VectorMmapEstimate {
  int64_t hnsw_bytes;  // HNSW graph tape (alignment=64)
  int64_t vec_bytes;   // raw vector data tape (alignment=8)

  int64_t total() const { return hnsw_bytes + vec_bytes; }
};

/// @brief Estimates the mmap-allocated memory cost per vector in a usearch index,
///        split into HNSW tape and vector tape components.
///
/// Usearch uses two separate mmap-backed arena allocators (memory_mapping_allocator_gt):
///   1. tape_allocator     (alignment=64) — stores HNSW graph nodes (key + level + neighbor lists)
///   2. vectors_tape_alloc (alignment=8)  — stores raw vector data (e.g. 128 floats)
///
/// Both bypass jemalloc (they call mmap directly), so total_memory_tracker doesn't see them.
/// This function estimates the per-vector cost in those two allocators.
///
/// Usearch also uses a third allocator (aligned_allocator_gt) for the nodes pointer array
/// and thread contexts. That one goes through aligned_alloc -> jemalloc, so it IS already
/// tracked by total_memory_tracker. We intentionally exclude it to avoid double-counting.
///
/// The HNSW node size depends on the node's random level:
///   node_bytes(level) = node_head + neighbors_base_bytes + neighbors_bytes * level
/// Most nodes are level 0 (~94% for connectivity=16). Rather than tracking the actual level
/// per node (which we can't observe at insert time), we use the expected average:
///   E[level] = 1 / (connectivity - 1)   (geometric distribution)
///
/// Arena alignment waste is included by rounding up to the allocator's alignment boundary,
/// matching what memory_mapping_allocator_gt::allocate() does internally.
///
template <typename Index>
VectorMmapEstimate EstimatePerVectorMmapBytes(const Index &index) {
  // All three come from private usearch types/typedefs that aren't exposed by index_dense_gt.
  // See: index_dense_gt::tape_allocator_t          = memory_mapping_allocator_gt<64>
  //      index_dense_gt::vectors_tape_allocator_t   = memory_mapping_allocator_gt<8>
  //      index_gt::level_t                          = int16_t
  static constexpr std::size_t kHnswAlignment = 64;
  static constexpr std::size_t kVecAlignment = 8;
  static constexpr std::size_t kLevelTSize = sizeof(int16_t);

  // --- Vector data (stored in vectors_tape_allocator, mmap, alignment=8) ---
  const auto raw_vec_bytes = index.bytes_per_vector();
  const auto aligned_vec_bytes = ((raw_vec_bytes + kVecAlignment - 1) / kVecAlignment) * kVecAlignment;

  // --- HNSW graph node (stored in tape_allocator, mmap, alignment=64) ---
  const auto node_head = sizeof(typename Index::vector_key_t) + kLevelTSize;
  const auto raw_node_bytes_l0 = node_head + index.neighbors_base_bytes();
  const auto connectivity = index.connectivity();
  const auto avg_higher_level_bytes = connectivity > 1 ? index.neighbors_bytes() / (connectivity - 1) : 0;
  const auto raw_node_bytes_avg = raw_node_bytes_l0 + avg_higher_level_bytes;
  const auto aligned_node_bytes = ((raw_node_bytes_avg + kHnswAlignment - 1) / kHnswAlignment) * kHnswAlignment;

  return {.hnsw_bytes = static_cast<int64_t>(aligned_node_bytes), .vec_bytes = static_cast<int64_t>(aligned_vec_bytes)};
}

/// @brief Updates an entry in the vector index: removes existing entry if present, then adds new vector.
/// If vector is empty, only removes the entry (if it exists) and returns.
/// Automatically resizes the index if full during add.
/// @tparam Index The usearch index type (e.g., index_dense_gt<Key, ...>).
/// @tparam Key The key type used in the index (e.g., Vertex*, EdgeIndexEntry).
/// @tparam Spec The index specification type.
/// @param mg_index The synchronized index wrapper.
/// @param spec The index specification (will be updated if resize occurs).
/// @param key The key to add/update in the index.
/// @param vector The vector to insert into the index.
/// @param thread_id Optional thread ID hint for usearch's internal thread-local optimizations.
/// @throws query::VectorSearchException if dimension mismatch or add fails for reasons other than capacity.
template <typename Index, typename Key, typename Spec>
void UpdateVectorIndex(utils::Synchronized<Index, std::shared_mutex> &mg_index, Spec &spec, const Key &key,
                       const utils::small_vector<float> &vector, std::optional<std::size_t> thread_id = std::nullopt) {
  if (!vector.empty() && vector.size() != spec.dimension) {
    throw query::VectorSearchException(
        "Vector index property must have the same number of dimensions as specified in the index.");
  }

  auto thread_id_for_adding = thread_id.value_or(Index::any_thread());
  // When usearch reuses a freed slot (update of existing key), no new arena
  // memory is allocated — skip TryAdd/Sub for that case.
  bool is_fresh_insert = true;
  {
    auto locked_index = mg_index.MutableSharedLock();
    const auto est = EstimatePerVectorMmapBytes(*locked_index);

    if (locked_index->contains(key)) {
      locked_index->remove(key);
      is_fresh_insert = false;
    }
    if (vector.empty()) return;

    if (is_fresh_insert && !utils::embeddings_memory_counter.TryAdd(est.hnsw_bytes, est.vec_bytes)) {
      throw query::VectorSearchException("Embeddings memory limit exceeded.");
    }

    auto result = locked_index->add(key, vector.data(), thread_id_for_adding);
    if (!result.error) {
      return;
    }
    if (is_fresh_insert) {
      utils::embeddings_memory_counter.Sub(est.hnsw_bytes, est.vec_bytes);
    }
    if (locked_index->size() >= locked_index->capacity()) {
      result.error.release();
    }
  }
  {
    auto exclusively_locked_index = mg_index.Lock();
    if (exclusively_locked_index->size() >= exclusively_locked_index->capacity()) {
      const auto new_size = static_cast<std::size_t>(spec.resize_coefficient * exclusively_locked_index->capacity());
      const unum::usearch::index_limits_t new_limits(new_size, GetVectorIndexThreadCount());
      if (!exclusively_locked_index->try_reserve(new_limits)) {
        throw query::VectorSearchException("Failed to resize vector index.");
      }
      spec.capacity = exclusively_locked_index->capacity();
    }
    const auto est_retry = EstimatePerVectorMmapBytes(*exclusively_locked_index);
    if (is_fresh_insert && !utils::embeddings_memory_counter.TryAdd(est_retry.hnsw_bytes, est_retry.vec_bytes)) {
      throw query::VectorSearchException("Embeddings memory limit exceeded.");
    }
    auto result = exclusively_locked_index->add(key, vector.data(), thread_id_for_adding);
    if (result.error && is_fresh_insert) {
      utils::embeddings_memory_counter.Sub(est_retry.hnsw_bytes, est_retry.vec_bytes);
    }
  }
}

/// @brief Populates a vector index by iterating over vertices on a single thread.
/// @tparam ProcessFunc Callable with signature void(Vertex&, std::optional<std::size_t> thread_id).
/// @param vertices The vertices accessor to iterate over.
/// @param process The function to call for each vertex (thread_id is std::nullopt).
template <typename ProcessFunc>
  requires std::invocable<ProcessFunc, Vertex &, std::optional<std::size_t>>
void PopulateVectorIndexSingleThreaded(utils::SkipList<Vertex>::Accessor &vertices, ProcessFunc &&process) {
  const utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  for (auto &vertex : vertices) {
    std::forward<ProcessFunc>(process)(vertex, std::nullopt);
  }
}

/// @brief Populates a vector index by iterating over vertices using multiple threads.
/// @tparam ProcessFunc Callable with signature void(Vertex&, std::optional<std::size_t> thread_id).
/// @param vertices The vertices accessor to iterate over.
/// @param process The function to call for each vertex (thread_id is the chunk index).
template <typename ProcessFunc>
  requires std::invocable<ProcessFunc, Vertex &, std::optional<std::size_t>>
void PopulateVectorIndexMultiThreaded(utils::SkipList<Vertex>::Accessor &vertices, ProcessFunc &&process) {
  const utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  auto vertices_chunks = vertices.create_chunks(FLAGS_storage_recovery_thread_count);
  const auto actual_chunk_count = vertices_chunks.size();
  utils::Synchronized<std::exception_ptr, utils::SpinLock> first_exception{};
  {
    std::vector<std::jthread> threads;
    threads.reserve(actual_chunk_count);
    for (std::size_t i = 0; i < actual_chunk_count; ++i) {
      threads.emplace_back([&vertices_chunks, &process, &first_exception, i]() {
        try {
          auto &chunk = vertices_chunks[i];
          for (auto &vertex : chunk) {
            std::forward<ProcessFunc>(process)(vertex, std::optional<std::size_t>(i));
          }
        } catch (...) {
          first_exception.WithLock([captured = std::current_exception()](auto &ex) {
            if (!ex) ex = captured;
          });
        }
      });
    }
  }
  first_exception.WithLock([](auto &ex) {
    if (ex) std::rethrow_exception(ex);
  });
}

}  // namespace memgraph::storage
