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

#include "storage/v2/indices/vector_index_utils.hpp"

#include <string_view>
#include <vector>

#include <fmt/core.h>
#include "query/exceptions.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index_dense.hpp"

namespace memgraph::storage {

const char *NameFromMetric(unum::usearch::metric_kind_t metric) {
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

unum::usearch::metric_kind_t MetricFromName(std::string_view name) {
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

const char *NameFromScalar(unum::usearch::scalar_kind_t scalar) {
  switch (scalar) {
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
          "Unsupported scalar kind. Supported scalars are bf16, f64, f32, f16, f8, "
          "u64, u32, u16, u8, i64, i32, i16, and i8.");
  }
}

unum::usearch::scalar_kind_t ScalarFromName(std::string_view name) {
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
      fmt::format("Unsupported scalar name: {}. Supported scalars are bf16, f64, f32, f16, f8, "
                  "u64, u32, u16, u8, i64, i32, i16, and i8.",
                  name));
}

double SimilarityFromDistance(unum::usearch::metric_kind_t metric, double distance) {
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

template <typename IndexType, typename KeyType>
PropertyValue GetVectorAsPropertyValue(const std::shared_ptr<utils::Synchronized<IndexType, std::shared_mutex>> &index,
                                       KeyType key) {
  auto locked_index = index->ReadLock();
  const auto dimension = locked_index->dimensions();
  std::vector<unum::usearch::f64_t> vector(dimension);
  const auto retrieved_count = locked_index->get(key, vector.data(), 1);
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

using mg_vector_index_t = unum::usearch::index_dense_gt<Vertex *, unum::usearch::uint40_t>;
// using mg_vector_edge_index_t = unum::usearch::index_dense_gt<VectorEdgeIndex::EdgeIndexEntry,
// unum::usearch::uint40_t>;

template PropertyValue GetVectorAsPropertyValue<mg_vector_index_t, Vertex *>(
    const std::shared_ptr<utils::Synchronized<mg_vector_index_t, std::shared_mutex>> &index, Vertex *key);

template std::vector<float> GetVector<mg_vector_index_t, Vertex *>(
    const std::shared_ptr<utils::Synchronized<mg_vector_index_t, std::shared_mutex>> &index, Vertex *key);
// template PropertyValue GetVectorAsPropertyValue<mg_vector_edge_index_t, VectorEdgeIndex::EdgeIndexEntry>(const
// std::shared_ptr<utils::Synchronized<mg_vector_edge_index_t, std::shared_mutex>> &index,
// VectorEdgeIndex::EdgeIndexEntry &key);

}  // namespace memgraph::storage
