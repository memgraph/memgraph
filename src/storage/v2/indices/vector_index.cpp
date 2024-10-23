// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstdint>
#include <thread>

#include "absl/container/flat_hash_map.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index.hpp"
#include "usearch/index_dense.hpp"
#include "utils/algorithm.hpp"
#include "utils/logging.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(experimental_vector_indexes, "",
              "Enables vector search indexes on nodes with Label and property specified in the "
              "IndexName__Label1__property1__{JSON_config};IndexName__Label2__property2 format.");

namespace unum {
namespace usearch {

template <>
struct unum::usearch::hash_gt<memgraph::storage::VectorIndexKey> {
  std::size_t operator()(memgraph::storage::VectorIndexKey const &element) const noexcept {
    return std::hash<uint64_t>{}(element.commit_timestamp) ^ std::hash<memgraph::storage::Gid>{}(element.vertex->gid);
  }
};

template <>
struct unum::usearch::hash_gt<std::pair<uint64_t, uint64_t>> {
  std::size_t operator()(std::pair<uint64_t, uint64_t> const &element) const noexcept {
    return std::hash<uint64_t>{}(element.first) ^ std::hash<uint64_t>{}(element.second);
  }
};

}  // namespace usearch
}  // namespace unum

namespace memgraph::storage {

using mg_vector_index_t = unum::usearch::index_dense_gt<VectorIndexKey, unum::usearch::uint40_t>;

/// Helper function that converts a string representation of a metric kind to the corresponding
/// `unum::usearch::metric_kind_t` value.
unum::usearch::metric_kind_t GetMetricKindFromConfig(const std::string &metric_str) {
  static const std::unordered_map<std::string, unum::usearch::metric_kind_t> metric_map = {
      {"ip", unum::usearch::metric_kind_t::ip_k},
      {"cos", unum::usearch::metric_kind_t::cos_k},
      {"l2sq", unum::usearch::metric_kind_t::l2sq_k},
      {"pearson", unum::usearch::metric_kind_t::pearson_k},
      {"haversine", unum::usearch::metric_kind_t::haversine_k},
      {"divergence", unum::usearch::metric_kind_t::divergence_k},
      {"hamming", unum::usearch::metric_kind_t::hamming_k},
      {"tanimoto", unum::usearch::metric_kind_t::tanimoto_k},
      {"sorensen", unum::usearch::metric_kind_t::sorensen_k},
      {"jaccard", unum::usearch::metric_kind_t::jaccard_k}};

  auto it = metric_map.find(metric_str);
  if (it != metric_map.end()) {
    return it->second;
  }
  throw std::invalid_argument("Unknown metric kind: " + metric_str);
}

/// Helper function that converts a string representation of a scalar kind to the corresponding
/// `unum::usearch::scalar_kind_t` value.
unum::usearch::scalar_kind_t GetScalarKindFromConfig(const std::string &scalar_str) {
  static const std::unordered_map<std::string, unum::usearch::scalar_kind_t> scalar_map = {
      {"b1x8", unum::usearch::scalar_kind_t::b1x8_k}, {"u40", unum::usearch::scalar_kind_t::u40_k},
      {"uuid", unum::usearch::scalar_kind_t::uuid_k}, {"bf16", unum::usearch::scalar_kind_t::bf16_k},
      {"f64", unum::usearch::scalar_kind_t::f64_k},   {"f32", unum::usearch::scalar_kind_t::f32_k},
      {"f16", unum::usearch::scalar_kind_t::f16_k},   {"f8", unum::usearch::scalar_kind_t::f8_k},
      {"u64", unum::usearch::scalar_kind_t::u64_k},   {"u32", unum::usearch::scalar_kind_t::u32_k},
      {"u16", unum::usearch::scalar_kind_t::u16_k},   {"u8", unum::usearch::scalar_kind_t::u8_k},
      {"i64", unum::usearch::scalar_kind_t::i64_k},   {"i32", unum::usearch::scalar_kind_t::i32_k},
      {"i16", unum::usearch::scalar_kind_t::i16_k},   {"i8", unum::usearch::scalar_kind_t::i8_k}};

  auto it = scalar_map.find(scalar_str);
  if (it != scalar_map.end()) {
    return it->second;
  }
  throw std::invalid_argument("Unknown scalar kind: " + scalar_str);
}

// The `Impl` structure implements the underlying functionality of the `VectorIndex` class.
// It uses the PIMPL (Pointer to Implementation) idiom to separate the interface of `VectorIndex`
// from its implementation, making it easier to maintain, extend, and hide implementation details.
struct VectorIndex::Impl {
  Impl() = default;
  ~Impl() = default;

  // The `index_` member is a map that associates a `LabelPropKey` (a combination of label and property)
  // with the actual vector index (`mg_vector_index_t`). This allows us to store multiple vector indexes
  // based on different labels and properties.
  std::map<LabelPropKey, mg_vector_index_t> index_;

  // The `index_name_to_label_prop_` is a hash map that maps an index name (as a string) to the corresponding
  // `LabelPropKey`. This allows the system to quickly resolve an index name to the specific label and property
  // associated with that index, enabling easy lookup and management of indexes by name.
  absl::flat_hash_map<std::string, LabelPropKey> index_name_to_label_prop_;
};

VectorIndex::VectorIndex() : pimpl(std::make_unique<Impl>()) {}
VectorIndex::~VectorIndex() {}

void VectorIndex::CreateIndex(const VectorIndexSpec &spec) {
  // check mandatory fields
  MG_ASSERT(spec.config.contains("dimension"), "Vector index must have a 'dimension' field in the config.");
  MG_ASSERT(spec.config.contains("limit"), "Vector index must have a 'size' field in the config.");

  uint64_t vector_dimension = spec.config["dimension"];
  uint64_t index_size = spec.config["limit"];

  // Read metric kind from config, with a fallback to default 'l2sq_k' if not provided.
  std::string metric_kind_str = spec.config.contains("metric") ? spec.config["metric"] : "l2sq";
  unum::usearch::metric_kind_t metric_kind = GetMetricKindFromConfig(metric_kind_str);

  // Read scalar kind from config, with a fallback to default 'f32_k' if not provided.
  std::string scalar_kind_str = spec.config.contains("scalar") ? spec.config["scalar"] : "f32";
  unum::usearch::scalar_kind_t scalar_kind = GetScalarKindFromConfig(scalar_kind_str);

  unum::usearch::metric_punned_t metric(vector_dimension, metric_kind, scalar_kind);

  const auto label_prop = LabelPropKey{spec.label, spec.property};
  pimpl->index_name_to_label_prop_.emplace(spec.index_name, label_prop);
  pimpl->index_.emplace(label_prop, mg_vector_index_t::make(metric));
  pimpl->index_[label_prop].reserve(index_size);

  spdlog::trace("Created vector index " + spec.index_name);
}

void VectorIndex::AddNodeToNewIndexEntries(Vertex *vertex, std::vector<VectorIndexTuple> &keys) {
  for (const auto &[label_prop, _] : pimpl->index_) {
    if (utils::Contains(vertex->labels, label_prop.label()) && vertex->properties.HasProperty(label_prop.property())) {
      keys.emplace_back(vertex, label_prop);
    }
  }
}

void VectorIndex::AddNodeToIndex(Vertex *vertex, const LabelPropKey &label_prop, uint64_t commit_timestamp) {
  auto &index = pimpl->index_.at(label_prop);
  const auto &vector_property = vertex->properties.GetProperty(label_prop.property()).ValueList();
  std::vector<float> vector;
  vector.reserve(vector_property.size());
  std::transform(vector_property.begin(), vector_property.end(), std::back_inserter(vector), [](const auto &value) {
    if (value.IsDouble()) {
      return static_cast<float>(value.ValueDouble());
    }
    if (value.IsInt()) {
      return static_cast<float>(value.ValueInt());
    }
    throw std::invalid_argument("Vector index property must be a list of floats or integers.");
  });
  const auto key = VectorIndexKey{vertex, commit_timestamp};
  index.add(key, vector.data());
}

std::size_t VectorIndex::Size(std::string_view index_name) {
  const auto label_prop = pimpl->index_name_to_label_prop_.at(index_name);
  return pimpl->index_.at(label_prop).size();
}

std::vector<std::string> VectorIndex::ListAllIndices() {
  std::vector<std::string> indices;
  for (const auto &[index_name, _] : pimpl->index_name_to_label_prop_) {
    indices.push_back(index_name);
  }
  return indices;
}

std::vector<std::pair<Gid, double>> VectorIndex::Search(std::string_view index_name, uint64_t start_timestamp,
                                                        uint64_t result_set_size,
                                                        const std::vector<float> &query_vector) {
  const auto &label_prop = pimpl->index_name_to_label_prop_.at(index_name);
  const auto &index = pimpl->index_.at(label_prop);

  // The result vector will contain pairs of vertices and their score.
  std::vector<std::pair<Gid, double>> result;
  result.reserve(result_set_size);

  auto filtering_function = [start_timestamp](const VectorIndexKey &key) {
    // This transcation can see only nodes that were committed before the start_timestamp.
    // TODO(@DavIvek): Implement MVCC logic. -> This works only when there is no update on the node.
    return key.commit_timestamp < start_timestamp;
  };

  const auto &result_keys = index.filtered_search(query_vector.data(), result_set_size, filtering_function);
  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    const auto &key = static_cast<VectorIndexKey>(result_keys[i].member.key);
    result.emplace_back(key.vertex->gid, result_keys[i].distance);
  }

  return result;
}

}  // namespace memgraph::storage
