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

#include "storage/v2/indices/vector_edge_index.hpp"
#include <ranges>
#include <usearch/index_dense.hpp>
#include "flags/bolt.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "utils/synchronized.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

// unum::usearch::index_dense_gt is the index type used for vector indices. It is thread-safe and supports concurrent
// operations.
using mg_vector_edge_index_t = unum::usearch::index_dense_gt<VectorEdgeIndex::EdgeIndexEntry, unum::usearch::uint40_t>;

struct EdgeTypeIndexItem {
  // Similar to NodeIndexItem, but for edge type indices.
  std::shared_ptr<utils::Synchronized<mg_vector_edge_index_t, std::shared_mutex>> mg_index;
  VectorIndexSpec spec;
};

/// @brief Implements the underlying functionality of the `VectorIndex` class.
///
/// The `Impl` structure follows the PIMPL (Pointer to Implementation) idiom to separate
/// the interface of `VectorIndex` from its implementation
struct VectorEdgeIndex::Impl {
  /// The `index_` member is a map that associates a `LabelPropKey` (a combination of label and property)
  /// with the pair of a IndexItem.
  // std::map<std::variant<LabelPropKey, EdgeTypePropKey>, IndexItem, std::less<>> index_;
  std::map<EdgeTypePropKey, EdgeTypeIndexItem, std::less<>> edge_index_;

  /// The `index_name_to_index_impl_` is a map that maps an index name (as a string) to the corresponding
  /// `LabelPropKey`. This allows the system to quickly resolve an index name to the spec
  /// associated with that index, enabling easy lookup and management of indexes by name.
  std::map<std::string, EdgeTypePropKey, std::less<>> index_name_to_edge_type_prop_;
};

VectorEdgeIndex::VectorEdgeIndex() : pimpl(std::make_unique<Impl>()) {}
VectorEdgeIndex::~VectorEdgeIndex() {}

bool VectorEdgeIndex::CreateIndex(const VectorIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                                  std::optional<SnapshotObserverInfo> const &snapshot_info) {
  try {
    if (pimpl->index_name_to_edge_type_prop_.contains(spec.index_name)) {
      throw query::VectorSearchException("Vector index with the given name already exists.");
    }
    const unum::usearch::metric_punned_t metric(spec.dimension, spec.metric_kind, spec.scalar_kind);
    const unum::usearch::index_limits_t limits(spec.capacity, FLAGS_bolt_num_workers);

    const auto edge_type = std::get<EdgeTypeId>(spec.label_or_edge_type);
    const EdgeTypePropKey edge_type_prop{edge_type, spec.property};

    if (pimpl->edge_index_.contains(edge_type_prop)) {
      throw query::VectorSearchException("Vector index with the given edge type and property already exists.");
    }

    auto mg_edge_index = mg_vector_edge_index_t::make(metric);
    if (!mg_edge_index) {
      throw query::VectorSearchException(fmt::format("Failed to create vector index {}, error message: {}",
                                                     spec.index_name, mg_edge_index.error.what()));
    }

    if (!mg_edge_index.index.try_reserve(limits)) {
      throw query::VectorSearchException(fmt::format("Failed to reserve memory for vector index {}", spec.index_name));
    }

    spdlog::info("Created vector index {}", spec.index_name);
    pimpl->index_name_to_edge_type_prop_.emplace(spec.index_name, edge_type_prop);
    pimpl->edge_index_.emplace(
        edge_type_prop,
        EdgeTypeIndexItem{
            std::make_shared<utils::Synchronized<mg_vector_edge_index_t, std::shared_mutex>>(std::move(mg_edge_index)),
            spec});

    for (auto &from_vertex : vertices) {
      if (from_vertex.deleted) {
        continue;
      }

      for (auto &edge : from_vertex.out_edges) {
        const auto type = std::get<kEdgeTypeIdPos>(edge);
        if (type == edge_type) {
          auto *to_vertex = std::get<kVertexPos>(edge);
          if (to_vertex->deleted) {
            continue;
          }
          // update the index with the edge
          if (snapshot_info) {
            snapshot_info->Update(UpdateType::EDGES);
          }
        }
      }
    }

  } catch (const std::exception &e) {
    spdlog::error("Failed to create vector index {}: {}", spec.index_name, e.what());
    return false;
  }
  return true;
}

bool VectorEdgeIndex::DropIndex(std::string_view index_name) {
  auto it = pimpl->index_name_to_edge_type_prop_.find(index_name.data());
  if (it == pimpl->index_name_to_edge_type_prop_.end()) {
    return false;
  }
  const auto &edge_type_prop = it->second;
  pimpl->edge_index_.erase(edge_type_prop);
  pimpl->index_name_to_edge_type_prop_.erase(it);
  spdlog::info("Dropped vector index {}", index_name);
  return true;
}

void VectorEdgeIndex::Clear() {
  pimpl->index_name_to_edge_type_prop_.clear();
  pimpl->edge_index_.clear();
}

bool VectorEdgeIndex::UpdateVectorIndex(EdgeIndexEntry entry, const EdgeTypePropKey &edge_type_prop,
                                        const PropertyValue *value) {
  auto &[mg_index, spec] = pimpl->edge_index_.at(edge_type_prop);
  bool is_index_full = false;
  // try to remove entry (if it exists) and then add a new one + check if index is full
  {
    auto locked_index = mg_index->MutableSharedLock();
    locked_index->remove(entry);
    is_index_full = locked_index->size() == locked_index->capacity();
  }

  const auto &property = (value != nullptr ? *value : entry.edge->properties.GetProperty(edge_type_prop.property()));
  if (property.IsNull()) {
    // if property is null, that means that the vertex should not be in the index and we shouldn't do any other updates
    return false;
  }
  if (!property.IsList()) {
    throw query::VectorSearchException("Vector index property must be a list.");
  }
  const auto &vector_property = property.ValueList();
  if (spec.dimension != vector_property.size()) {
    throw query::VectorSearchException("Vector index property must have the same number of dimensions as the index.");
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
    throw query::VectorSearchException("Vector index property must be a list of floats or integers.");
  });
  {
    auto locked_index = mg_index->MutableSharedLock();
    locked_index->add(entry, vector.data());
  }
  return true;
}

void VectorEdgeIndex::UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                                          PropertyId property, const PropertyValue &value) {
  auto has_property = [&](const auto &edge_type_prop) { return edge_type_prop.property() == property; };
  if (std::ranges::any_of(pimpl->edge_index_ | rv::keys | rv::filter(has_property),
                          [&](const auto &edge_type_prop) { return edge_type_prop.edge_type() == edge_type; })) {
    UpdateVectorIndex({from_vertex, to_vertex, edge}, EdgeTypePropKey{edge_type, property}, &value);
  }
}

std::vector<VectorIndexInfo> VectorEdgeIndex::ListVectorIndicesInfo() const {
  std::vector<VectorIndexInfo> result;
  result.reserve(pimpl->edge_index_.size());
  for (const auto &[_, index_item] : pimpl->edge_index_) {
    const auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index->ReadLock();
    result.emplace_back(VectorIndexInfo{
        spec.index_name, std::get<EdgeTypeId>(spec.label_or_edge_type), spec.property,
        NameFromMetric(locked_index->metric().metric_kind()), static_cast<std::uint16_t>(locked_index->dimensions()),
        locked_index->capacity(), locked_index->size(), NameFromScalar(locked_index->metric().scalar_kind())});
  }
  return result;
}

std::vector<VectorIndexSpec> VectorEdgeIndex::ListIndices() const {
  std::vector<VectorIndexSpec> result;
  result.reserve(pimpl->edge_index_.size());
  r::transform(pimpl->edge_index_, std::back_inserter(result),
               [](const auto &label_prop_index_item) { return label_prop_index_item.second.spec; });
  return result;
}

std::optional<uint64_t> VectorEdgeIndex::ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const {
  auto it = pimpl->edge_index_.find(EdgeTypePropKey{edge_type, property});
  if (it == pimpl->edge_index_.end()) {
    return std::nullopt;
  }
  auto &[mg_index, _] = it->second;
  auto locked_index = mg_index->ReadLock();
  return locked_index->size();
}

VectorEdgeIndex::VectorSearchEdgeResults VectorEdgeIndex::SearchEdges(std::string_view index_name,
                                                                      uint64_t result_set_size,
                                                                      const std::vector<float> &query_vector) const {
  const auto edge_type_prop = pimpl->index_name_to_edge_type_prop_.find(index_name);
  if (edge_type_prop == pimpl->index_name_to_edge_type_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  auto &[mg_index, _] = pimpl->edge_index_.at(edge_type_prop->second);

  // The result vector will contain pairs of vertices and their score.
  VectorSearchEdgeResults result;
  result.reserve(result_set_size);

  auto locked_index = mg_index->ReadLock();
  const auto result_keys =
      locked_index->filtered_search(query_vector.data(), result_set_size, [](const EdgeIndexEntry &entry) {
        auto guard = std::shared_lock{entry.edge->lock};
        return !entry.from_vertex->deleted && !entry.to_vertex->deleted && !entry.edge->deleted;
      });
  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    const auto &entry = static_cast<EdgeIndexEntry>(result_keys[i].member.key);
    result.emplace_back(
        entry, static_cast<double>(result_keys[i].distance),
        std::abs(SimilarityFromDistance(locked_index->metric().metric_kind(), result_keys[i].distance)));
  }

  return result;
}

void VectorEdgeIndex::RestoreEntries(
    const EdgeTypePropKey &edge_type_prop,
    std::span<std::pair<PropertyValue, std::tuple<Vertex *const, Vertex *const, Edge *const>> const> prop_edges) {
  for (const auto &property_value_edge : prop_edges) {
    const auto &[property_value, edge_tuple] = property_value_edge;
    const auto &[from_vertex, to_vertex, edge] = edge_tuple;
    UpdateVectorIndex({from_vertex, to_vertex, edge}, edge_type_prop, &property_value);
  }
}

void VectorEdgeIndex::RemoveObsoleteEntries(std::stop_token token) const {
  auto maybe_stop = utils::ResettableCounter(2048);
  for (auto &[_, index_item] : pimpl->edge_index_) {
    if (maybe_stop() && token.stop_requested()) {
      return;
    }
    auto &[mg_index, spec] = index_item;
    auto locked_index = mg_index->MutableSharedLock();
    std::vector<EdgeIndexEntry> edges_to_remove(locked_index->size());
    locked_index->export_keys(edges_to_remove.data(), 0, locked_index->size());

    auto deleted = edges_to_remove | rv::filter([](const EdgeIndexEntry &entry) {
                     auto guard = std::shared_lock{entry.edge->lock};
                     return entry.from_vertex->deleted || entry.to_vertex->deleted || entry.edge->deleted;
                   });
    for (const auto &entry : deleted) {
      locked_index->remove(entry);
    }
  }
}

VectorEdgeIndex::IndexStats VectorEdgeIndex::Analysis() const {
  IndexStats res{};
  for (const auto &[edge_type_prop, _] : pimpl->edge_index_) {
    const auto edge_type = edge_type_prop.edge_type();
    const auto property = edge_type_prop.property();
    res.et2p[edge_type].emplace_back(property);
    res.p2et[property].emplace_back(edge_type);
  }
  return res;
}

EdgeTypeId VectorEdgeIndex::GetEdgeTypeId(std::string_view index_name) {
  auto it = pimpl->index_name_to_edge_type_prop_.find(index_name.data());
  if (it == pimpl->index_name_to_edge_type_prop_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_name));
  }
  return it->second.edge_type();
}

}  // namespace memgraph::storage
