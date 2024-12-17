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
#include <stdexcept>
#include <stop_token>
#include <string_view>

#include "flags/bolt.hpp"
#include "flags/experimental.hpp"
#include "query/exceptions.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index_dense.hpp"
#include "utils/algorithm.hpp"
#include "utils/counter.hpp"
#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::storage {

static constexpr std::string_view kMetric = "metric";
static constexpr std::string_view kDimension = "dimension";
static constexpr std::string_view kCapacity = "capacity";
static constexpr std::string_view kResizeCoefficient = "resize_coefficient";

static constexpr std::uint16_t kDefaultResizeCoefficient = 2;
static constexpr std::string_view kDefaultMetric = "l2sq";

using mg_vector_index_t = unum::usearch::index_dense_gt<Vertex *, unum::usearch::uint40_t>;

// NOLINTNEXTLINE(bugprone-exception-escape)
struct IndexItem {
  mg_vector_index_t mg_index;
  std::shared_ptr<VectorIndexSpec> spec;
  mutable utils::RWSpinLock lock;
};

/// Map from usearch metric kind to similarity function
/// TODO(@DavIvek): Check if this functions are correct -> l2sq and cosine are correct and they are most critical atm
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
  /// with the pair of a `mg_vector_index_t` and a `utils::RWSpinLock`.
  /// Lock is needed because when resize is performed, no other operation should be performed on the index.
  std::map<LabelPropKey, IndexItem> index_;

  /// The `index_name_to_label_prop_` is a hash map that maps an index name (as a string) to the corresponding
  /// `LabelPropKey`. This allows the system to quickly resolve an index name to the spec
  /// associated with that index, enabling easy lookup and management of indexes by name.
  std::map<std::string, LabelPropKey> index_name_to_label_prop_;
};

VectorIndex::VectorIndex() : pimpl(std::make_unique<Impl>()) {}
VectorIndex::~VectorIndex() {}

VectorIndexConfigMap VectorIndex::ParseIndexSpec(
    std::unordered_map<query::Expression *, query::Expression *> const &config_map,
    query::ExpressionVisitor<query::TypedValue> &evaluator) {
  if (config_map.empty()) {
    throw std::invalid_argument("Vector index config map is empty.");
  }

  auto transformed_map = std::ranges::views::all(config_map) | std::views::transform([&evaluator](const auto &pair) {
                           auto key_expr = pair.first->Accept(evaluator);
                           auto value_expr = pair.second->Accept(evaluator);
                           return std::pair{key_expr.ValueString(), value_expr};
                         }) |
                         ranges::to<std::map<std::string, query::TypedValue, std::less<>>>;

  auto metric_str = transformed_map.contains(kMetric.data())
                        ? std::string(transformed_map.at(kMetric.data()).ValueString())
                        : std::string(kDefaultMetric);
  auto metric_kind = unum::usearch::metric_from_name(metric_str.c_str(), metric_str.size());
  if (metric_kind.error) {
    throw std::invalid_argument("Invalid metric kind: " + metric_str);
  }

  auto dimension = transformed_map.find(kDimension.data());
  if (dimension == transformed_map.end()) {
    throw std::invalid_argument("Vector index spec must have a 'dimension' field.");
  }
  auto dimension_value = static_cast<std::uint16_t>(dimension->second.ValueInt());

  auto capacity = transformed_map.find(kCapacity.data());
  if (capacity == transformed_map.end()) {
    throw std::invalid_argument("Vector index spec must have a 'capacity' field.");
  }
  auto capacity_value = static_cast<std::size_t>(capacity->second.ValueInt());

  auto resize_coefficient = transformed_map.contains(kResizeCoefficient.data())
                                ? static_cast<std::uint16_t>(transformed_map.at(kResizeCoefficient.data()).ValueInt())
                                : kDefaultResizeCoefficient;
  return VectorIndexConfigMap{metric_kind.result, dimension_value, capacity_value, resize_coefficient};
}

void VectorIndex::CreateIndex(const std::shared_ptr<VectorIndexSpec> &spec) {
  const unum::usearch::metric_punned_t metric(
      spec->dimension, spec->metric_kind,
      unum::usearch::scalar_kind_t::f32_k);  // TODO(@DavIvek): scalar kind is hardcoded to f32 ATM

  // use the number of workers as the number of possible concurrent index operations
  const unum::usearch::index_limits_t limits(spec->capacity, FLAGS_bolt_num_workers);

  const auto label_prop = LabelPropKey{spec->label, spec->property};
  if (pimpl->index_.contains(label_prop) || pimpl->index_name_to_label_prop_.contains(spec->index_name)) {
    throw std::invalid_argument("Given vector index already exists.");
  }
  pimpl->index_name_to_label_prop_.emplace(spec->index_name, label_prop);
  pimpl->index_.emplace(label_prop, IndexItem{mg_vector_index_t::make(metric), spec, utils::RWSpinLock()});
  if (pimpl->index_[label_prop].mg_index.try_reserve(limits)) {
    spdlog::info("Created vector index " + spec->index_name);
  } else {
    throw std::invalid_argument("Failed to create vector index " + spec->index_name +
                                " due to failed memory allocation. Try again with a smaller size limit.");
  }
}

bool VectorIndex::CreateIndex(const std::shared_ptr<VectorIndexSpec> &spec,
                              utils::SkipList<Vertex>::Accessor vertices) {
  try {
    CreateIndex(spec);
    auto &[index, _, lock] = pimpl->index_.at(LabelPropKey{spec->label, spec->property});
    auto guard = std::unique_lock{lock};
    for (auto &vertex : vertices) {
      UpdateVectorIndex(&vertex, LabelPropKey{spec->label, spec->property});
    }
  } catch (const std::exception &e) {
    spdlog::error("Failed to create vector index {}: {}", spec->index_name, e.what());
    return false;
  }
  return true;
}

bool VectorIndex::DropIndex(std::string_view index_name) {
  auto it = pimpl->index_name_to_label_prop_.find(index_name.data());
  if (it == pimpl->index_name_to_label_prop_.end()) {
    return false;
  }
  const auto &label_prop = it->second;
  auto &[index, _, lock] = pimpl->index_.at(label_prop);
  index.reset();
  pimpl->index_.erase(label_prop);
  pimpl->index_name_to_label_prop_.erase(it);
  spdlog::info("Dropped vector index " + std::string(index_name));
  return true;
}

void VectorIndex::Clear() {
  pimpl->index_name_to_label_prop_.clear();
  for (auto &[_, index_item] : pimpl->index_) {
    index_item.mg_index.reset();
  }
  pimpl->index_.clear();
}

void VectorIndex::UpdateVectorIndex(Vertex *vertex, const LabelPropKey &label_prop, const PropertyValue *value) const {
  auto &[index, spec, lock] = pimpl->index_.at(label_prop);

  // first, try to remove entry (if it exists) and then add a new one
  {
    auto guard = std::shared_lock{lock};
    index.remove(vertex);
  }
  const auto &property = (value != nullptr ? *value : vertex->properties.GetProperty(label_prop.property()));
  if (property.IsNull()) {
    return;
  }
  if (!property.IsList()) {
    throw std::invalid_argument("Vector index property must be a list.");
  }
  const auto &vector_property = property.ValueList();
  if (index.dimensions() != vector_property.size()) {
    throw std::invalid_argument("Vector index property must have the same number of dimensions as the index.");
  }

  if (index.capacity() == index.size()) {
    spdlog::warn("Vector index is full, resizing...");
    auto guard = std::unique_lock{lock};
    const auto new_size = spec->resize_coefficient * index.capacity();
    const unum::usearch::index_limits_t new_limits(new_size, FLAGS_bolt_num_workers);
    if (!index.try_reserve(new_limits)) {
      throw std::invalid_argument("Vector index is full and can't be resized");
    }
  }

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
  {
    auto guard = std::shared_lock{lock};
    index.add(vertex, vector.data(), mg_vector_index_t::any_thread(), false);
  }
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
      auto &[index, _, lock] = pimpl->index_.at(label_prop);
      auto guard = std::shared_lock{lock};
      index.remove(vertex_before_update);
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

std::vector<VectorIndexInfo> VectorIndex::ListVectorIndicesInfo() const {
  if (!flags::AreExperimentsEnabled(flags::Experiments::VECTOR_SEARCH)) {
    throw query::VectorSearchDisabledException();
  }
  std::vector<VectorIndexInfo> result;
  for (const auto &[_, index_item] : pimpl->index_) {
    const auto &[index, spec, lock] = index_item;
    auto guard = std::shared_lock{lock};
    result.emplace_back(VectorIndexInfo{spec->index_name, spec->label, spec->property,
                                        static_cast<std::uint16_t>(index.dimensions()), index.capacity(),
                                        index.size()});
  }
  return result;
}

std::vector<std::shared_ptr<VectorIndexSpec>> VectorIndex::ListIndices() const {
  std::vector<std::shared_ptr<VectorIndexSpec>> result;
  result.reserve(pimpl->index_.size());
  std::ranges::transform(pimpl->index_, std::back_inserter(result),
                         [](const auto &label_prop_index_item) { return label_prop_index_item.second.spec; });
  return result;
}

std::vector<VectorIndexSpec> VectorIndex::ListIndexSpecs() const {
  if (!flags::AreExperimentsEnabled(flags::Experiments::VECTOR_SEARCH)) {
    throw query::VectorSearchDisabledException();
  }
  std::vector<VectorIndexSpec> result;
  result.reserve(pimpl->index_.size());
  std::ranges::transform(pimpl->index_ | std::views::values, std::back_inserter(result),
                         [](const auto &index_item) { return *index_item.spec; });
  return result;
}

std::optional<uint64_t> VectorIndex::ApproximateVectorCount(LabelId label, PropertyId property) const {
  if (!flags::AreExperimentsEnabled(flags::Experiments::VECTOR_SEARCH)) {
    throw query::VectorSearchDisabledException();
  }
  auto it = pimpl->index_.find(LabelPropKey{label, property});
  if (it == pimpl->index_.end()) {
    return std::nullopt;
  }
  auto &[index, _, lock] = it->second;
  auto guard = std::shared_lock{lock};
  return index.size();
}

std::vector<std::tuple<Vertex *, double, double>> VectorIndex::Search(std::string_view index_name,
                                                                      uint64_t result_set_size,
                                                                      const std::vector<float> &query_vector) const {
  if (!flags::AreExperimentsEnabled(flags::Experiments::VECTOR_SEARCH)) {
    throw query::VectorSearchDisabledException();
  }

  const auto label_prop = pimpl->index_name_to_label_prop_.find(std::string(index_name));
  if (label_prop == pimpl->index_name_to_label_prop_.end()) {
    throw std::invalid_argument("Vector index " + std::string(index_name) + " does not exist.");
  }
  auto &[index, _, lock] = pimpl->index_.at(label_prop->second);

  // The result vector will contain pairs of vertices and their score.
  std::vector<std::tuple<Vertex *, double, double>> result;
  result.reserve(result_set_size);

  auto guard = std::shared_lock{lock};
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
  auto &[index, _, lock] = pimpl->index_.at(label_prop);
  auto guard = std::shared_lock{lock};
  for (const auto &vertex : vertices) {
    index.remove(vertex);
  }
}

void VectorIndex::RestoreEntries(const LabelPropKey &label_prop,
                                 std::span<std::pair<PropertyValue, Vertex *> const> prop_vertices) const {
  for (const auto &property_value_vertex : prop_vertices) {
    UpdateVectorIndex(property_value_vertex.second, label_prop, &property_value_vertex.first);
  }
}

void VectorIndex::RemoveObsoleteEntries(std::stop_token token) const {
  auto maybe_stop = utils::ResettableCounter<2048>();
  for (auto &[_, index_item] : pimpl->index_) {
    if (maybe_stop() && token.stop_requested()) {
      return;
    }
    auto &[index, spec, lock] = index_item;
    auto guard = std::shared_lock{lock};
    std::vector<Vertex *> vertices_to_remove(index.size());
    index.export_keys(vertices_to_remove.data(), 0, index.size());

    auto deleted = vertices_to_remove | std::views::filter([](const Vertex *vertex) {
                     auto guard = std::shared_lock{vertex->lock};
                     return vertex->deleted;
                   });
    for (const auto &vertex : deleted) {
      index.remove(vertex);
    }
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
