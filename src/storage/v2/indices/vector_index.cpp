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

#include <algorithm>
#include <cstdint>
#include <ranges>
#include <shared_mutex>
#include <stop_token>
#include <string_view>

#include "absl/container/flat_hash_map.h"
#include "flags/bolt.hpp"
#include "flags/experimental.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index.hpp"
#include "usearch/index_dense.hpp"
#include "utils/algorithm.hpp"
#include "utils/counter.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

static constexpr std::string_view kLabel = "label";
static constexpr std::string_view kProperty = "property";
static constexpr std::string_view kMetric = "metric";
static constexpr std::string_view kScalar = "scalar";
static constexpr std::string_view kDimension = "dimension";
static constexpr std::string_view kLimit = "limit";
static constexpr std::string_view kDefaultMetric = "l2sq";
static constexpr std::string_view kDefaultScalar = "f32";

using mg_vector_index_t = unum::usearch::index_dense_gt<Vertex *, unum::usearch::uint40_t>;

/// @brief Converts a string representation of a metric kind to the corresponding
/// `unum::usearch::metric_kind_t` value.
///
/// @param metric_str A string representing the metric kind (e.g., "ip", "cos").
/// @return The corresponding `unum::usearch::metric_kind_t` value.
/// @throws std::invalid_argument if the metric kind is unknown.
unum::usearch::metric_kind_t GetMetricKindFromConfig(const std::string_view metric_str) {
  static const std::unordered_map<std::string_view, unum::usearch::metric_kind_t> metric_map = {
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
  throw std::invalid_argument("Unknown metric kind: " + std::string(metric_str));
}

/// @brief Converts a string representation of a scalar kind to the corresponding
/// `unum::usearch::scalar_kind_t` value.
///
/// @param scalar_str A string representing the scalar kind (e.g., "f32", "i64").
/// @return The corresponding `unum::usearch::scalar_kind_t` value.
/// @throws std::invalid_argument if the scalar kind is unknown.
unum::usearch::scalar_kind_t GetScalarKindFromConfig(const std::string_view scalar_str) {
  static const std::unordered_map<std::string_view, unum::usearch::scalar_kind_t> scalar_map = {
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
  throw std::invalid_argument("Unknown scalar kind: " + std::string(scalar_str));
}

/// Map from usearch metric kind to similarity function
/// TODO(@DavIvek): Check if this functions are correct
static const std::unordered_map<unum::usearch::metric_kind_t, std::function<double(double)>> similarity_map = {
    {unum::usearch::metric_kind_t::ip_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::cos_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::l2sq_k, [](double distance) { return 1.0 / (1.0 + distance); }},
    {unum::usearch::metric_kind_t::pearson_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::haversine_k, [](double distance) { return 1.0 / (1.0 + distance); }},
    {unum::usearch::metric_kind_t::divergence_k, [](double distance) { return 1.0 / (1.0 + distance); }},
    {unum::usearch::metric_kind_t::hamming_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::tanimoto_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::sorensen_k, [](double distance) { return 1.0 - distance; }},
    {unum::usearch::metric_kind_t::jaccard_k, [](double distance) { return 1.0 - distance; }}};

/// @brief Implements the underlying functionality of the `VectorIndex` class.
///
/// The `Impl` structure follows the PIMPL (Pointer to Implementation) idiom to separate
/// the interface of `VectorIndex` from its implementation
struct VectorIndex::Impl {
  /// The `index_` member is a map that associates a `LabelPropKey` (a combination of label and property)
  /// with the actual vector index (`mg_vector_index_t`). This allows us to store multiple vector indexes
  /// based on different labels and properties.
  absl::flat_hash_map<LabelPropKey, mg_vector_index_t> index_;

  /// The `index_name_to_label_prop_` is a hash map that maps an index name (as a string) to the corresponding
  /// `LabelPropKey`. This allows the system to quickly resolve an index name to the specific label and property
  /// associated with that index, enabling easy lookup and management of indexes by name.
  absl::flat_hash_map<std::string, LabelPropKey> index_name_to_label_prop_;
};

VectorIndex::VectorIndex() : pimpl(std::make_unique<Impl>()) {}
VectorIndex::~VectorIndex() {}

std::vector<VectorIndexSpec> VectorIndex::ParseIndexSpec(const nlohmann::json &index_spec,
                                                         NameIdMapper *name_id_mapper) {
  if (index_spec.empty()) {
    throw std::invalid_argument("Vector index spec cannot be empty.");
  }

  std::vector<VectorIndexSpec> result;
  result.reserve(index_spec.size());

  try {
    for (const auto &[index_name, index_spec] : index_spec.items()) {
      // Check mandatory fields
      MG_ASSERT(index_spec.contains(kLabel.data()), "Vector index spec must have a 'label' field.");
      MG_ASSERT(index_spec.contains(kProperty.data()), "Vector index spec must have a 'property' field.");
      MG_ASSERT(index_spec.contains(kDimension.data()), "Vector index spec must have a 'dimension' field.");
      MG_ASSERT(index_spec.contains(kLimit.data()), "Vector index spec must have a 'size' field.");

      const auto label_name = index_spec[kLabel.data()].get<std::string>();
      const auto property_name = index_spec[kProperty.data()].get<std::string>();
      const std::string metric = index_spec.contains(kMetric.data()) ? index_spec[kMetric.data()].get<std::string>()
                                                                     : std::string(kDefaultMetric);
      const std::string scalar = index_spec.contains(kScalar.data()) ? index_spec[kScalar.data()].get<std::string>()
                                                                     : std::string(kDefaultScalar);
      const auto dimension = index_spec[kDimension.data()].get<std::uint64_t>();
      const auto size_limit = index_spec[kLimit.data()].get<std::uint64_t>();

      const auto label = LabelId::FromUint(name_id_mapper->NameToId(label_name));
      const auto property = PropertyId::FromUint(name_id_mapper->NameToId(property_name));

      result.emplace_back(VectorIndexSpec{index_name, label, property, metric, scalar, dimension, size_limit});
    }
  } catch (const std::exception &e) {
    throw std::invalid_argument("Error parsing vector index spec: " + std::string(e.what()));
  }

  return result;
}

void VectorIndex::CreateIndex(const VectorIndexSpec &spec) {
  unum::usearch::metric_kind_t metric_kind = GetMetricKindFromConfig(spec.metric);
  unum::usearch::scalar_kind_t scalar_kind = GetScalarKindFromConfig(spec.scalar);

  unum::usearch::metric_punned_t metric(spec.dimension, metric_kind, scalar_kind);

  // use the number of workers as the number of possible concurrent index operations
  unum::usearch::index_limits_t limits(spec.size_limit, FLAGS_bolt_num_workers);

  const auto label_prop = LabelPropKey{spec.label, spec.property};
  pimpl->index_name_to_label_prop_.emplace(spec.index_name, label_prop);
  pimpl->index_.emplace(label_prop, mg_vector_index_t::make(metric));
  pimpl->index_[label_prop].reserve(limits);

  spdlog::trace("Created vector index " + spec.index_name);
}

void VectorIndex::UpdateVectorIndex(Vertex *vertex, const LabelPropKey &label_prop, const PropertyValue *value) const {
  auto &index = pimpl->index_.at(label_prop);

  // first, try to remove entry (if it exists) and then add new one
  index.remove(vertex);
  const auto &property = (value != nullptr ? *value : vertex->properties.GetProperty(label_prop.property()));
  if (property.IsNull()) {
    return;
  }
  if (!property.IsList()) {
    throw std::invalid_argument("Vector index property must be a list.");
  }
  if (index.capacity() == index.size()) {
    throw std::runtime_error("Vector index is full.");
  }
  if (index.dimensions() != property.ValueList().size()) {
    throw std::invalid_argument("Vector index property must have the same number of dimensions as the index.");
  }
  const auto &vector_property = property.ValueList();
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
  index.add(vertex, vector.data(), mg_vector_index_t::any_thread(), false);
}

void VectorIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update) const {
  std::ranges::for_each(pimpl->index_ | std::views::keys, [&](const auto &label_prop) {
    if (label_prop.label() == added_label) {
      UpdateVectorIndex(vertex_after_update, label_prop);
    }
  });
}

void VectorIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update) const {
  std::ranges::for_each(pimpl->index_ | std::views::keys, [&](const auto &label_prop) {
    if (label_prop.label() == removed_label) {
      pimpl->index_.at(label_prop).remove(vertex_before_update);
    }
  });
}

void VectorIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex) const {
  auto has_property = [&](const auto &label_prop) { return label_prop.property() == property; };
  auto has_label = [&](const auto &label_prop) { return utils::Contains(vertex->labels, label_prop.label()); };

  auto view = pimpl->index_ | std::views::keys | std::views::filter(has_property) | std::views::filter(has_label);
  for (const auto &label_prop : view) {
    UpdateVectorIndex(vertex, label_prop, &value);
  }
}

std::vector<VectorIndexInfo> VectorIndex::ListAllIndices() const {
  if (!flags::AreExperimentsEnabled(flags::Experiments::VECTOR_SEARCH)) {
    throw query::VectorSearchDisabledException();
  }
  std::vector<VectorIndexInfo> result;
  result.reserve(pimpl->index_name_to_label_prop_.size());
  std::ranges::transform(pimpl->index_name_to_label_prop_, std::back_inserter(result), [this](const auto &pair) {
    const auto &index_name = pair.first;
    const auto &label_prop = pair.second;
    const auto &index = pimpl->index_.at(label_prop);
    return VectorIndexInfo{index_name, label_prop.label(), label_prop.property(), index.metric().dimensions(),
                           index.size()};
  });
  return result;
}

std::vector<std::tuple<Vertex *, double, double>> VectorIndex::Search(std::string_view index_name,
                                                                      uint64_t result_set_size,
                                                                      const std::vector<float> &query_vector) const {
  if (!flags::AreExperimentsEnabled(flags::Experiments::VECTOR_SEARCH)) {
    throw query::VectorSearchDisabledException();
  }
  const auto &label_prop = pimpl->index_name_to_label_prop_.at(index_name);
  const auto &index = pimpl->index_.at(label_prop);

  // The result vector will contain pairs of vertices and their score.
  std::vector<std::tuple<Vertex *, double, double>> result;
  result.reserve(result_set_size);

  const auto result_keys = index.filtered_search(query_vector.data(), result_set_size, [](const Vertex *vertex) {
    auto guard = std::shared_lock{vertex->lock};
    return !vertex->deleted;
  });
  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    const auto &vertex = static_cast<Vertex *>(result_keys[i].member.key);
    result.emplace_back(vertex, static_cast<double>(result_keys[i].distance),
                        std::abs(similarity_map.at(index.metric().metric_kind())(result_keys[i].distance)));
  }

  return result;
}

void VectorIndex::AbortEntries(const LabelPropKey &label_prop, std::span<Vertex *const> vertices) const {
  auto &index = pimpl->index_.at(label_prop);
  std::ranges::for_each(vertices, [&](Vertex *vertex) { index.remove(vertex); });
}

void VectorIndex::RestoreEntries(const LabelPropKey &label_prop,
                                 std::span<std::pair<PropertyValue, Vertex *> const> prop_vertices) const {
  std::ranges::for_each(prop_vertices, [&](const auto &property_value_vertex) {
    UpdateVectorIndex(property_value_vertex.second, label_prop, &property_value_vertex.first);
  });
}

void VectorIndex::RemoveObsoleteEntries(std::stop_token token) const {
  auto maybe_stop = utils::ResettableCounter<2048>();
  for (auto &[_, index] : pimpl->index_) {
    if (token.stop_requested() || maybe_stop()) {
      return;
    }
    std::vector<Vertex *> vertices_to_remove(index.size());
    index.export_keys(vertices_to_remove.data(), 0, index.size());

    // TODO: Expand to check if the vertex still has the vector in question
    auto deleted = vertices_to_remove | std::views::filter([](const Vertex *vertex) {
                     auto guard = std::shared_lock{vertex->lock};
                     return !vertex->deleted;
                   });
    std::ranges::for_each(deleted, [&](Vertex *vertex) { index.remove(vertex); });
  }
}

VectorIndex::IndexStats VectorIndex::Analysis() const {
  IndexStats res{};
  for (const auto &[label_prop, _] : pimpl->index_) {
    const auto label = label_prop.label();
    const auto property = label_prop.property();
    res.l2p[label].emplace_back(property);
    res.p2l[property].emplace_back(label);
  }
  return res;
}

void VectorIndex::TryInsertVertex(Vertex *vertex) const {
  auto guard = std::shared_lock{vertex->lock};
  auto has_property = [&](const auto &label_prop) { return vertex->properties.HasProperty(label_prop.property()); };
  auto has_label = [&](const auto &label_prop) { return utils::Contains(vertex->labels, label_prop.label()); };
  for (const auto &[label_prop, _] : pimpl->index_) {
    if (has_property(label_prop) && has_label(label_prop)) {
      UpdateVectorIndex(vertex, label_prop);
    }
  }
}

}  // namespace memgraph::storage
