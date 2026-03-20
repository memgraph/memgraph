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

#include <mutex>
#include <ranges>
#include <unordered_set>

#include "flags/general.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/tracked_vector_allocator.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "usearch/index_dense.hpp"
#include "utils/resource_lock.hpp"

namespace r = ranges;

namespace memgraph::storage {

// unum::usearch::index_dense_gt is the index type used for vector indices. It is thread-safe and supports concurrent
// operations.
using mg_vector_edge_index_t = unum::usearch::index_dense_gt<Edge *, unum::usearch::uint40_t,
                                                             TrackedVectorAllocator<64>, TrackedVectorAllocator<8>>;

struct synchronized_mg_vector_edge_index_t {
  mg_vector_edge_index_t index;
  mutable utils::ResourceLock mutex{};

  explicit synchronized_mg_vector_edge_index_t(mg_vector_edge_index_t &&idx) : index(std::move(idx)) {}
};

struct EdgeTypeIndexItem {
  synchronized_mg_vector_edge_index_t mg_index;
  VectorEdgeIndexSpec spec;

  EdgeTypeIndexItem(mg_vector_edge_index_t index, VectorEdgeIndexSpec spec)
      : mg_index(std::move(index)), spec(std::move(spec)) {}
};

struct VectorEdgeIndex::Impl {
  std::unordered_map<uint64_t, EdgeTypeIndexItem> index_by_id_;
  std::unordered_map<Edge *, std::pair<Vertex *, Vertex *>> edge_endpoints_;  // edge -> (from_vertex, to_vertex)
};

VectorEdgeIndex::VectorEdgeIndex() : pimpl(std::make_unique<Impl>()) {}

VectorEdgeIndex::~VectorEdgeIndex() = default;
VectorEdgeIndex::VectorEdgeIndex(VectorEdgeIndex &&) noexcept = default;
VectorEdgeIndex &VectorEdgeIndex::operator=(VectorEdgeIndex &&) noexcept = default;

std::optional<uint64_t> VectorEdgeIndex::SetupIndex(const VectorEdgeIndexSpec &spec, NameIdMapper *name_id_mapper) {
  const auto index_id = name_id_mapper->NameToId(spec.index_name);
  if (pimpl->index_by_id_.contains(index_id)) {
    return std::nullopt;
  }
  if (r::any_of(pimpl->index_by_id_, [&](const auto &id_index_item) {
        auto &index_spec = id_index_item.second.spec;
        return spec.edge_type_id == index_spec.edge_type_id && spec.property == index_spec.property;
      })) {
    return std::nullopt;
  }

  const unum::usearch::metric_punned_t metric(spec.dimension, spec.metric_kind, spec.scalar_kind);
  const unum::usearch::index_limits_t limits(spec.capacity, GetVectorIndexThreadCount());

  auto mg_edge_index = mg_vector_edge_index_t::make(metric);
  if (!mg_edge_index) {
    throw query::VectorSearchException(fmt::format(
        "Failed to create vector index {}, error message: {}", spec.index_name, mg_edge_index.error.what()));
  }

  if (!mg_edge_index.index.try_reserve(limits)) {
    throw query::VectorSearchException(
        fmt::format("Failed to create vector index {}. Failed to reserve memory for the index", spec.index_name));
  }

  const auto [_, inserted] = pimpl->index_by_id_.try_emplace(index_id, std::move(mg_edge_index.index), spec);
  return inserted ? std::optional<uint64_t>{index_id} : std::nullopt;
}

void VectorEdgeIndex::AddEdgeToIndex(uint64_t index_id, Edge *edge, Vertex *from_vertex, Vertex *to_vertex,
                                     std::optional<std::size_t> thread_id) {
  auto it = pimpl->index_by_id_.find(index_id);
  if (it == pimpl->index_by_id_.end()) {
    throw query::VectorSearchException(fmt::format("Vector index {} does not exist.", index_id));
  }
  auto &index_item = it->second;
  auto &spec = index_item.spec;
  auto property = edge->properties.GetProperty(spec.property);
  if (property.IsNull()) return;

  auto vector = RegisterIndexId(property, index_id);
  edge->properties.SetProperty(spec.property, property);

  pimpl->edge_endpoints_[edge] = {from_vertex, to_vertex};
  UpdateVectorIndex(index_item.mg_index, spec, edge, vector, thread_id);
}

bool VectorEdgeIndex::CreateIndex(const VectorEdgeIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                                  Indices * /*indices*/, NameIdMapper *name_id_mapper,
                                  std::optional<SnapshotObserverInfo> const &snapshot_info) {
  try {
    const auto index_id = SetupIndex(spec, name_id_mapper);
    if (!index_id.has_value()) return false;
    PopulateVectorIndexSingleThreaded(vertices, [&](Vertex &vertex, std::optional<std::size_t> thread_id) {
      if (vertex.deleted()) return;
      for (auto &edge_tuple : vertex.out_edges) {
        if (std::get<kEdgeTypeIdPos>(edge_tuple) != spec.edge_type_id) continue;

        auto *to_vertex = std::get<kVertexPos>(edge_tuple);
        auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;
        if (edge->deleted() || to_vertex->deleted()) continue;

        AddEdgeToIndex(*index_id, edge, &vertex, to_vertex, thread_id);
        if (snapshot_info) {
          snapshot_info->Update(UpdateType::VECTOR_EDGE_IDX);
        }
      }
    });
    return true;
  } catch (const std::exception &) {
    DropIndex(spec.index_name, vertices, name_id_mapper);
    throw;
  }
}

void VectorEdgeIndex::RecoverIndex(VectorEdgeIndexRecoveryInfo &recovery_info,
                                   utils::SkipList<Vertex>::Accessor &vertices, Indices * /*indices*/,
                                   NameIdMapper *name_id_mapper,
                                   std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto &spec = recovery_info.spec;
  try {
    auto &recovery_entries = recovery_info.index_entries;
    const auto index_id = SetupIndex(spec, name_id_mapper);
    if (!index_id.has_value()) {
      throw query::VectorSearchException(
          "Given vector index already exists. Corrupted or invalid index recovery files.");
    }
    auto &index_item = pimpl->index_by_id_.at(*index_id);
    auto &mg_index = index_item.mg_index;

    auto process_vertex_for_recovery = [&](Vertex &vertex, std::optional<std::size_t> thread_id) {
      for (auto &edge_tuple : vertex.out_edges) {
        if (std::get<kEdgeTypeIdPos>(edge_tuple) != spec.edge_type_id) continue;

        auto *to_vertex = std::get<kVertexPos>(edge_tuple);
        auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;
        if (vertex.deleted() || to_vertex->deleted() || edge->deleted()) continue;

        if (auto it = recovery_entries.find(edge->gid); it != recovery_entries.end()) {
          auto &vector = it->second;
          pimpl->edge_endpoints_[edge] = {&vertex, to_vertex};
          UpdateVectorIndex(mg_index, spec, edge, vector, thread_id);

          auto property = edge->properties.GetProperty(spec.property);
          RegisterIndexId(property, *index_id);
          edge->properties.SetProperty(spec.property, property);

          vector.clear();
          vector.shrink_to_fit();
        } else {
          AddEdgeToIndex(*index_id, edge, &vertex, to_vertex, thread_id);
        }
      }
      if (snapshot_info) {
        snapshot_info->Update(UpdateType::VECTOR_EDGE_IDX);
      }
    };

    if (FLAGS_storage_parallel_schema_recovery && FLAGS_storage_recovery_thread_count > 1) {
      PopulateVectorIndexMultiThreaded(vertices, process_vertex_for_recovery);
    } else {
      PopulateVectorIndexSingleThreaded(vertices, process_vertex_for_recovery);
    }
  } catch (const std::exception &) {
    DropIndex(spec.index_name, vertices, name_id_mapper);
    throw;
  }
}

bool VectorEdgeIndex::DropIndex(std::string_view index_name, utils::SkipList<Vertex>::Accessor &vertices,
                                NameIdMapper *name_id_mapper) {
  auto maybe_id = name_id_mapper->NameToIdIfExists(index_name);
  if (!maybe_id.has_value()) {
    return false;
  }
  const auto index_id = *maybe_id;
  auto it = pimpl->index_by_id_.find(index_id);
  if (it == pimpl->index_by_id_.end()) {
    return false;
  }
  auto &index_item = it->second;
  auto &mg_index = index_item.mg_index;
  auto &spec = index_item.spec;
  {
    auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);

    const auto dimension = mg_index.index.dimensions();
    std::vector<double> vector(dimension);
    for (auto &vertex : vertices) {
      for (auto &edge_tuple : vertex.out_edges) {
        if (std::get<kEdgeTypeIdPos>(edge_tuple) != spec.edge_type_id) continue;

        auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;
        if (!mg_index.index.contains(edge)) continue;

        auto vector_property = edge->properties.GetProperty(spec.property);
        if (ShouldUnregisterFromIndex(vector_property, index_id)) {
          mg_index.index.get(edge, vector.data());
          edge->properties.SetProperty(spec.property, PropertyValue(vector));
        } else {
          edge->properties.SetProperty(spec.property, vector_property);
        }
      }
    }
  }
  // Collect edges from the dropped index before erasing
  std::vector<Edge *> dropped_edges;
  {
    auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    auto const size = mg_index.index.size();
    if (size > 0) {
      dropped_edges.resize(size);
      mg_index.index.export_keys(dropped_edges.data(), 0, size);
    }
  }
  pimpl->index_by_id_.erase(it);
  // Clean up endpoints for edges no longer in any index.
  // Iterate indices in outer loop to acquire each lock only once.
  std::unordered_set<Edge *> still_indexed;
  for (const auto &[_, index_item] : pimpl->index_by_id_) {
    auto guard = utils::SharedResourceLockGuard(index_item.mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    for (auto *edge : dropped_edges) {
      if (index_item.mg_index.index.contains(edge)) {
        still_indexed.insert(edge);
      }
    }
  }
  for (auto *edge : dropped_edges) {
    DMG_ASSERT(edge != nullptr, "Null edge pointer in vector edge index");
    if (!still_indexed.contains(edge)) {
      pimpl->edge_endpoints_.erase(edge);
    }
  }
  return true;
}

void VectorEdgeIndex::Clear() {
  pimpl->index_by_id_.clear();
  pimpl->edge_endpoints_.clear();
}

void VectorEdgeIndex::UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                                          PropertyId property, const PropertyValue &value) {
  // Property should already be updated to the vector index id if it has vector index defined on it.
  if (value.IsVectorIndexId()) {
    const auto &vector_property = value.ValueVectorIndexList();
    const auto &index_ids = value.ValueVectorIndexIds();
    pimpl->edge_endpoints_[edge] = {from_vertex, to_vertex};
    for (auto index_id : index_ids) {
      auto &index_item = pimpl->index_by_id_.at(index_id);
      UpdateVectorIndex(index_item.mg_index, index_item.spec, edge, vector_property);
    }
  } else if (value.IsNull()) {
    // If value is null, we have to remove the edge from all indices that contain it (by edge type).
    auto indices = GetIndicesByProperty(property);
    for (const auto &[et, idx_id] : indices) {
      if (et != edge_type) continue;
      RemoveEdgeFromIndex(edge, idx_id);
    }
  }
  // Otherwise, we don't update the index.
}

void VectorEdgeIndex::RemoveEdgeFromIndex(Edge *edge, uint64_t index_id) {
  auto it = pimpl->index_by_id_.find(index_id);
  if (it == pimpl->index_by_id_.end()) {
    throw query::VectorSearchException(
        fmt::format("Error in removing edge from index: index id {} does not exist.", index_id));
  }
  auto &index_item = it->second;
  UpdateVectorIndex(index_item.mg_index, index_item.spec, edge, utils::small_vector<float>{});
}

std::vector<VectorEdgeIndexInfo> VectorEdgeIndex::ListVectorIndicesInfo() const {
  std::vector<VectorEdgeIndexInfo> result;
  result.reserve(pimpl->index_by_id_.size());
  for (const auto &[_, index_item] : pimpl->index_by_id_) {
    const auto &[mg_index, spec] = index_item;
    auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    result.emplace_back(spec.index_name,
                        spec.edge_type_id,
                        spec.property,
                        NameFromMetric(mg_index.index.metric().metric_kind()),
                        static_cast<std::uint16_t>(mg_index.index.dimensions()),
                        mg_index.index.capacity(),
                        mg_index.index.size(),
                        NameFromScalar(mg_index.index.metric().scalar_kind()));
  }
  return result;
}

std::vector<VectorEdgeIndexSpec> VectorEdgeIndex::ListIndices() const {
  std::vector<VectorEdgeIndexSpec> result;
  result.reserve(pimpl->index_by_id_.size());
  r::transform(pimpl->index_by_id_, std::back_inserter(result), [](const auto &id_index_item) {
    return id_index_item.second.spec;
  });
  return result;
}

std::optional<uint64_t> VectorEdgeIndex::ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const {
  auto it = r::find_if(pimpl->index_by_id_, [&](const auto &id_index_item) {
    const auto &spec = id_index_item.second.spec;
    return spec.edge_type_id == edge_type && spec.property == property;
  });
  if (it != pimpl->index_by_id_.end()) {
    auto guard = utils::SharedResourceLockGuard(it->second.mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    return it->second.mg_index.index.size();
  }
  return std::nullopt;
}

VectorEdgeIndex::VectorSearchEdgeResults VectorEdgeIndex::SearchEdges(std::string_view index_name,
                                                                      uint64_t result_set_size,
                                                                      const std::vector<float> &query_vector) const {
  auto maybe_id = std::invoke([&]() -> std::optional<uint64_t> {
    for (const auto &[id, item] : pimpl->index_by_id_) {
      if (item.spec.index_name == index_name) return id;
    }
    return std::nullopt;
  });
  if (!maybe_id.has_value()) {
    throw query::VectorSearchException("Vector index {} does not exist.", index_name);
  }
  auto &index_item = pimpl->index_by_id_.at(*maybe_id);
  auto &mg_index = index_item.mg_index;

  VectorSearchEdgeResults result;
  result.reserve(result_set_size);

  auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  const auto result_keys = mg_index.index.filtered_search(query_vector.data(), result_set_size, [this](Edge *edge) {
    auto guard = std::shared_lock{edge->lock};
    if (edge->deleted()) return false;
    auto ep_it = pimpl->edge_endpoints_.find(edge);
    if (ep_it == pimpl->edge_endpoints_.end()) return false;
    return !ep_it->second.first->deleted() && !ep_it->second.second->deleted();
  });
  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    auto *edge = static_cast<Edge *>(result_keys[i].member.key);
    auto [from_vertex, to_vertex] = pimpl->edge_endpoints_.at(edge);
    result.emplace_back(
        VectorEdgeIndex::EdgeIndexEntry{.from_vertex = from_vertex, .to_vertex = to_vertex, .edge = edge},
        static_cast<double>(result_keys[i].distance),
        std::abs(SimilarityFromDistance(mg_index.index.metric().metric_kind(), result_keys[i].distance)));
  }

  return result;
}

void VectorEdgeIndex::AbortEntries(AbortProcessor::AbortableInfo &cleanup_collection) {
  for (auto &[edge, info] : cleanup_collection) {
    for (const auto &[property, old_value] : info.properties) {
      if (old_value.IsVectorIndexId()) {
        const auto &vector_property = old_value.ValueVectorIndexList();
        const auto &index_ids = old_value.ValueVectorIndexIds();
        pimpl->edge_endpoints_[edge] = {info.from_vertex, info.to_vertex};
        for (auto index_id : index_ids) {
          auto &index_item = pimpl->index_by_id_.at(index_id);
          storage::UpdateVectorIndex(index_item.mg_index, index_item.spec, edge, vector_property);
        }
      } else {
        DMG_ASSERT(old_value.IsNull(), "Unexpected property value type in abort processor of vector edge index");
        auto indices_by_prop = GetIndicesByProperty(property);
        for (const auto &[et, idx_id] : indices_by_prop) {
          if (et != info.edge_type) continue;
          RemoveEdgeFromIndex(edge, idx_id);
        }
      }
    }
  }
}

bool VectorEdgeIndex::Empty() const { return pimpl->index_by_id_.empty(); }

void VectorEdgeIndex::RemoveEdges(std::vector<Edge *> const &edges_to_remove) {
  if (edges_to_remove.empty()) return;

  for (auto *edge : edges_to_remove) {
    pimpl->edge_endpoints_.erase(edge);
  }

  for (auto &[_, index_item] : pimpl->index_by_id_) {
    auto guard = std::lock_guard{index_item.mg_index.mutex};
    for (auto *edge : edges_to_remove) {
      if (index_item.mg_index.index.contains(edge)) {
        index_item.mg_index.index.remove(edge);
      }
    }
  }
}

VectorEdgeIndex::AbortProcessor VectorEdgeIndex::GetAbortProcessor() const {
  AbortProcessor res{};
  for (const auto &[_, index_item] : pimpl->index_by_id_) {
    const auto edge_type = index_item.spec.edge_type_id;
    const auto property = index_item.spec.property;
    res.et2p[edge_type].push_back(property);
    res.p2et[property].push_back(edge_type);
  }
  return res;
}

void VectorEdgeIndex::AbortProcessor::CollectOnPropertyChange(EdgeTypeId edge_type, PropertyId property,
                                                              const PropertyValue &old_value, Vertex *from_vertex,
                                                              Vertex *to_vertex, Edge *edge) {
  auto edge_types = p2et.find(property);
  if (edge_types == p2et.end() || !r::contains(edge_types->second, edge_type)) return;

  auto &info = cleanup_collection[edge];
  info.edge_type = edge_type;
  info.from_vertex = from_vertex;
  info.to_vertex = to_vertex;
  info.properties[property] = old_value;
}

EdgeTypeId VectorEdgeIndex::GetEdgeTypeId(std::string_view index_name) {
  for (const auto &[_, index_item] : pimpl->index_by_id_) {
    if (index_item.spec.index_name == index_name) {
      return index_item.spec.edge_type_id;
    }
  }
  throw query::VectorSearchException("Vector index {} does not exist.", index_name);
}

bool VectorEdgeIndex::IndexExists(std::string_view index_name) const {
  return r::any_of(pimpl->index_by_id_,
                   [&](const auto &id_item) { return id_item.second.spec.index_name == index_name; });
}

utils::small_vector<float> VectorEdgeIndex::GetVectorPropertyFromEdgeIndex(Edge *edge, std::string_view index_name,
                                                                           NameIdMapper *name_id_mapper) const {
  auto maybe_id = name_id_mapper->NameToIdIfExists(index_name);
  if (!maybe_id.has_value()) {
    throw query::VectorSearchException("Vector index {} does not exist.", index_name);
  }
  auto it = pimpl->index_by_id_.find(*maybe_id);
  if (it == pimpl->index_by_id_.end()) {
    throw query::VectorSearchException("Vector index {} does not exist.", index_name);
  }
  auto &index_item = it->second;
  auto guard = utils::SharedResourceLockGuard(index_item.mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  utils::small_vector<float> vector(index_item.mg_index.index.dimensions());
  if (!index_item.mg_index.index.get(edge, vector.data())) return {};
  return vector;
}

std::pair<Vertex *, Vertex *> VectorEdgeIndex::GetEdgeEndpoints(Edge *edge) const {
  auto it = pimpl->edge_endpoints_.find(edge);
  DMG_ASSERT(it != pimpl->edge_endpoints_.end(), "Edge not found in endpoints map");
  return it->second;
}

std::unordered_map<PropertyId, uint64_t> VectorEdgeIndex::GetIndicesByEdgeType(EdgeTypeId edge_type) const {
  std::unordered_map<PropertyId, uint64_t> result;
  for (const auto &[index_id, index_item] : pimpl->index_by_id_) {
    if (index_item.spec.edge_type_id == edge_type) {
      result.emplace(index_item.spec.property, index_id);
    }
  }
  return result;
}

std::unordered_map<EdgeTypeId, uint64_t> VectorEdgeIndex::GetIndicesByProperty(PropertyId property) const {
  std::unordered_map<EdgeTypeId, uint64_t> result;
  for (const auto &[index_id, index_item] : pimpl->index_by_id_) {
    if (index_item.spec.property == property) {
      result.emplace(index_item.spec.edge_type_id, index_id);
    }
  }
  return result;
}

void VectorEdgeIndex::SerializeAllVectorEdgeIndices(durability::BaseEncoder *encoder,
                                                    std::unordered_set<uint64_t> &mapped_ids) const {
  auto write_mapping = [&](auto mapping) {
    mapped_ids.insert(mapping.AsUint());
    encoder->WriteUint(mapping.AsUint());
  };

  encoder->WriteUint(pimpl->index_by_id_.size());
  for (auto &[_, index_item] : pimpl->index_by_id_) {
    auto &spec = index_item.spec;
    auto &mg_index = index_item.mg_index;
    encoder->WriteString(spec.index_name);
    write_mapping(spec.edge_type_id);
    write_mapping(spec.property);
    encoder->WriteString(NameFromMetric(spec.metric_kind));
    encoder->WriteUint(spec.dimension);
    encoder->WriteUint(spec.resize_coefficient);
    encoder->WriteUint(spec.capacity);
    encoder->WriteUint(static_cast<uint64_t>(spec.scalar_kind));

    using Entry = std::pair<uint64_t, std::vector<float>>;
    auto const entries = std::invoke([&mg_index]() -> std::vector<Entry> {
      auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
      auto const size = mg_index.index.size();
      if (size == 0) return {};

      std::vector<Edge *> keys(size);
      mg_index.index.export_keys(keys.data(), 0, size);

      std::vector<Entry> result;
      result.reserve(size);
      std::vector<float> buffer(mg_index.index.dimensions());
      for (auto *edge : keys) {
        if (edge == nullptr || edge->deleted()) continue;
        if (!mg_index.index.get(edge, buffer.data())) continue;
        result.emplace_back(edge->gid.AsUint(), buffer);
      }
      return result;
    });

    encoder->WriteUint(entries.size());
    for (const auto &[gid, vector] : entries) {
      encoder->WriteUint(gid);
      for (auto value : vector) encoder->WriteDouble(value);
    }
  }
}

// VectorEdgeIndexRecovery implementation
void VectorEdgeIndexRecovery::UpdateOnIndexDrop(std::string_view index_name, NameIdMapper *name_id_mapper,
                                                std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec,
                                                utils::SkipList<Vertex>::Accessor &vertices) {
  for (auto &recovery_info : recovery_info_vec) {
    if (recovery_info.spec.index_name == index_name) {
      auto index_id = name_id_mapper->NameToId(index_name);
      // Iterate all vertices to find edges and restore properties
      for (auto &vertex : vertices) {
        for (auto &edge_tuple : vertex.out_edges) {
          if (std::get<kEdgeTypeIdPos>(edge_tuple) != recovery_info.spec.edge_type_id) continue;
          auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;

          auto it = recovery_info.index_entries.find(edge->gid);
          if (it == recovery_info.index_entries.end()) continue;

          auto edge_property = edge->properties.GetProperty(recovery_info.spec.property);
          if (ShouldUnregisterFromIndex(edge_property, index_id)) {
            edge->properties.SetProperty(recovery_info.spec.property,
                                         PropertyValue(std::vector<double>(it->second.begin(), it->second.end())));
          } else {
            edge->properties.SetProperty(recovery_info.spec.property, edge_property);
          }
        }
      }
    }
  }
  std::erase_if(recovery_info_vec, [&](const auto &ri) { return ri.spec.index_name == index_name; });
}

void VectorEdgeIndexRecovery::UpdateOnSetEdgeProperty(PropertyId property, const PropertyValue &value, const Edge *edge,
                                                      std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec) {
  const auto maybe_vector = std::invoke([&]() -> std::optional<utils::small_vector<float>> {
    switch (value.type()) {
      case PropertyValue::Type::VectorIndexId:
        return value.ValueVectorIndexList();
      case PropertyValue::Type::List:
      case PropertyValue::Type::IntList:
      case PropertyValue::Type::DoubleList:
      case PropertyValue::Type::NumericList:
        return ListToVector(value);
      case PropertyValue::Type::Null:
        return utils::small_vector<float>{};
      default:
        return std::nullopt;
    }
  });
  if (!maybe_vector) return;

  for (auto &ri : recovery_info_vec) {
    if (ri.spec.property == property) {
      ri.index_entries[edge->gid] = *maybe_vector;
    }
  }
}

}  // namespace memgraph::storage
