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

#include <ranges>
#include <unordered_set>

#include "flags/general.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/tracked_vector_allocator.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "usearch/index_dense.hpp"
#include "utils/resource_lock.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

// unum::usearch::index_dense_gt is the index type used for vector indices. It is thread-safe and supports concurrent
// operations.
using mg_vector_edge_index_t = unum::usearch::index_dense_gt<VectorEdgeIndex::EdgeIndexEntry, unum::usearch::uint40_t,
                                                             TrackedVectorAllocator<64>, TrackedVectorAllocator<8>>;

struct synchronized_mg_vector_edge_index_t {
  mg_vector_edge_index_t index;
  mutable utils::ResourceLock mutex{};

  explicit synchronized_mg_vector_edge_index_t(mg_vector_edge_index_t &&idx) : index(std::move(idx)) {}
};

struct EdgeTypeIndexItem {
  // unum::usearch::index_dense_gt is thread-safe and supports concurrent operations. However, we still need to use
  // locking because resizing the index requires exclusive access.
  synchronized_mg_vector_edge_index_t mg_index;
  VectorEdgeIndexSpec spec;

  EdgeTypeIndexItem(mg_vector_edge_index_t index, VectorEdgeIndexSpec spec)
      : mg_index(std::move(index)), spec(std::move(spec)) {}
};

/// @brief Implements the underlying functionality of the `VectorIndex` class.
///
/// The `Impl` structure follows the PIMPL (Pointer to Implementation) idiom to separate
/// the interface of `VectorEdgeIndex` from its implementation
struct VectorEdgeIndex::Impl {
  std::map<std::string, EdgeTypeIndexItem, std::less<>> index_by_name_;
};

namespace {

using EdgeIndexEntry = VectorEdgeIndex::EdgeIndexEntry;

/// @brief Attempts to add all matching edges from a vertex to the vector index.
/// Handles resize if the index is full.
/// @param mg_index The synchronized index wrapper.
/// @param spec The index specification (may be modified if resize occurs).
/// @param from_vertex The source vertex whose edges to process.
/// @param snapshot_info Optional snapshot observer for progress tracking.
/// @param thread_id Optional thread ID hint for usearch's internal optimizations.
void TryAddEdgesToIndex(synchronized_mg_vector_edge_index_t &mg_index, VectorEdgeIndexSpec &spec, Vertex &from_vertex,
                        std::optional<SnapshotObserverInfo> const &snapshot_info,
                        std::optional<std::size_t> thread_id = std::nullopt) {
  if (from_vertex.deleted()) {
    return;
  }
  for (auto &edge_tuple : from_vertex.out_edges) {
    if (!spec.edge_type_filter.Matches(std::get<kEdgeTypeIdPos>(edge_tuple))) {
      continue;
    }
    auto *to_vertex = std::get<kVertexPos>(edge_tuple);
    if (to_vertex->deleted()) {
      continue;
    }
    auto *edge = std::get<kEdgeRefPos>(edge_tuple).ptr;
    if (edge->deleted()) {
      continue;
    }
    auto property = edge->properties.GetProperty(spec.property);
    if (property.IsNull()) {
      continue;
    }
    auto vector = ListToVector(property);
    const EdgeIndexEntry entry{&from_vertex, to_vertex, edge};
    UpdateVectorIndex(mg_index, spec, entry, vector, thread_id);
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::VECTOR_EDGE_IDX);
    }
  }
}

}  // namespace

VectorEdgeIndex::VectorEdgeIndex() : pimpl(std::make_unique<Impl>()) {}

VectorEdgeIndex::~VectorEdgeIndex() = default;
VectorEdgeIndex::VectorEdgeIndex(VectorEdgeIndex &&) noexcept = default;
VectorEdgeIndex &VectorEdgeIndex::operator=(VectorEdgeIndex &&) noexcept = default;

bool VectorEdgeIndex::CreateIndex(const VectorEdgeIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                                  std::optional<SnapshotObserverInfo> const &snapshot_info) {
  try {
    if (!SetupIndex(spec)) return false;
    auto &[mg_index, mutable_spec] = pimpl->index_by_name_.at(spec.index_name);
    PopulateVectorIndexSingleThreaded(vertices, [&](Vertex &vertex, std::optional<std::size_t> /* thread_id */) {
      TryAddEdgesToIndex(mg_index, mutable_spec, vertex, snapshot_info);  // NOLINT(clang-analyzer-core.CallAndMessage)
    });
    return true;
  } catch (std::exception &e) {
    DropIndex(spec.index_name);
    throw e;
  }
}

void VectorEdgeIndex::RecoverIndex(const VectorEdgeIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                                   std::optional<SnapshotObserverInfo> const &snapshot_info) {
  try {
    if (!SetupIndex(spec))
      throw query::VectorSearchException(
          "Given vector index already exists. Corrupted or invalid index recovery files.");

    auto &[mg_index, mutable_spec] = pimpl->index_by_name_.at(spec.index_name);
    if (FLAGS_storage_parallel_schema_recovery && FLAGS_storage_recovery_thread_count > 1) {
      // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
      PopulateVectorIndexMultiThreaded(vertices, [&](Vertex &vertex, std::optional<std::size_t> thread_id) {
        TryAddEdgesToIndex(mg_index, mutable_spec, vertex, snapshot_info, thread_id);
      });
    } else {
      PopulateVectorIndexSingleThreaded(vertices, [&](Vertex &vertex, std::optional<std::size_t> /* thread_id */) {
        // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
        TryAddEdgesToIndex(mg_index, mutable_spec, vertex, snapshot_info);
      });
    }
  } catch (std::exception &e) {
    DropIndex(spec.index_name);
    throw e;
  }
}

bool VectorEdgeIndex::SetupIndex(const VectorEdgeIndexSpec &spec) {
  if (pimpl->index_by_name_.contains(spec.index_name)) return false;

  const unum::usearch::metric_punned_t metric(spec.dimension, spec.metric_kind, spec.scalar_kind);
  auto mg_edge_index = mg_vector_edge_index_t::make(metric);
  if (!mg_edge_index) {
    throw query::VectorSearchException(
        "Failed to create vector index {}, error message: {}", spec.index_name, mg_edge_index.error.what());
  }

  const unum::usearch::index_limits_t limits(spec.capacity, GetVectorIndexThreadCount());
  if (!mg_edge_index.index.try_reserve(limits)) {
    throw query::VectorSearchException("Failed to reserve memory for vector index {}", spec.index_name);
  }

  auto [it, inserted] = pimpl->index_by_name_.try_emplace(spec.index_name, std::move(mg_edge_index), spec);
  return inserted;
}

bool VectorEdgeIndex::DropIndex(std::string_view index_name) {
  auto it = pimpl->index_by_name_.find(index_name);
  if (it == pimpl->index_by_name_.end()) {
    return false;
  }
  pimpl->index_by_name_.erase(it);
  return true;
}

void VectorEdgeIndex::Clear() { pimpl->index_by_name_.clear(); }

bool VectorEdgeIndex::UpdateVectorIndex(EdgeIndexEntry entry, const EdgeTypePropKey &edge_type_prop,
                                        const PropertyValue *value) {
  for (auto &[name, index_item] : pimpl->index_by_name_) {
    if (index_item.spec.property != edge_type_prop.property()) continue;
    if (!index_item.spec.edge_type_filter.Matches(edge_type_prop.edge_type())) continue;
    const auto &property = value != nullptr ? *value : entry.edge->properties.GetProperty(edge_type_prop.property());
    auto vector = property.IsNull() ? utils::small_vector<float>{} : ListToVector(property);
    storage::UpdateVectorIndex(index_item.mg_index, index_item.spec, entry, vector);
    return !vector.empty();
  }
  return false;
}

void VectorEdgeIndex::UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                                          PropertyId property, const PropertyValue &value) {
  for (auto &[name, index_item] : pimpl->index_by_name_) {
    if (index_item.spec.property != property) continue;
    if (!index_item.spec.edge_type_filter.Matches(edge_type)) continue;
    const auto entry = EdgeIndexEntry{.from_vertex = from_vertex, .to_vertex = to_vertex, .edge = edge};
    auto vector = value.IsNull() ? utils::small_vector<float>{} : ListToVector(value);
    storage::UpdateVectorIndex(index_item.mg_index, index_item.spec, entry, vector);
    break;
  }
}

std::vector<VectorEdgeIndexInfo> VectorEdgeIndex::ListVectorIndicesInfo() const {
  std::vector<VectorEdgeIndexInfo> result;
  result.reserve(pimpl->index_by_name_.size());
  for (const auto &[_, index_item] : pimpl->index_by_name_) {
    const auto &[mg_index, spec] = index_item;
    auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    result.emplace_back(spec.index_name,
                        spec.edge_type_filter,
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
  result.reserve(pimpl->index_by_name_.size());
  r::transform(pimpl->index_by_name_, std::back_inserter(result), [](const auto &name_index_item) {
    return name_index_item.second.spec;
  });
  return result;
}

std::optional<uint64_t> VectorEdgeIndex::ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const {
  for (const auto &[_, index_item] : pimpl->index_by_name_) {
    if (index_item.spec.property != property) continue;
    if (!index_item.spec.edge_type_filter.Matches(edge_type)) continue;
    auto guard = utils::SharedResourceLockGuard(index_item.mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
    return index_item.mg_index.index.size();
  }
  return std::nullopt;
}

VectorEdgeIndex::VectorSearchEdgeResults VectorEdgeIndex::SearchEdges(std::string_view index_name,
                                                                      uint64_t result_set_size,
                                                                      const std::vector<float> &query_vector) const {
  auto it = pimpl->index_by_name_.find(index_name);
  if (it == pimpl->index_by_name_.end()) {
    throw query::VectorSearchException("Vector index {} does not exist.", index_name);
  }
  auto &[mg_index, _] = it->second;

  // The result vector will contain pairs of edges and their score.
  VectorSearchEdgeResults result;
  result.reserve(result_set_size);

  auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  const auto result_keys =
      mg_index.index.filtered_search(query_vector.data(), result_set_size, [](const EdgeIndexEntry &entry) {
        auto guard = std::shared_lock{entry.edge->lock};
        return !entry.from_vertex->deleted() && !entry.to_vertex->deleted() && !entry.edge->deleted();
      });
  for (std::size_t i = 0; i < result_keys.size(); ++i) {
    const auto &entry = static_cast<EdgeIndexEntry>(result_keys[i].member.key);
    result.emplace_back(
        entry,
        static_cast<double>(result_keys[i].distance),
        std::abs(SimilarityFromDistance(mg_index.index.metric().metric_kind(), result_keys[i].distance)));
  }

  return result;
}

void VectorEdgeIndex::RestoreEntries(
    const EdgeTypePropKey &edge_type_prop,
    std::span<std::pair<PropertyValue, std::tuple<Vertex *const, Vertex *const, Edge *const>> const> prop_edges) {
  for (const auto &property_value_edge : prop_edges) {
    const auto &[property_value, edge_tuple] = property_value_edge;
    const auto &[from_vertex, to_vertex, edge] = edge_tuple;
    UpdateVectorIndex(
        {.from_vertex = from_vertex, .to_vertex = to_vertex, .edge = edge}, edge_type_prop, &property_value);
  }
}

bool VectorEdgeIndex::Empty() const { return pimpl->index_by_name_.empty(); }

void VectorEdgeIndex::RemoveEdges(std::list<Gid> const &deleted_edge_gids) const {
  auto as_uint = deleted_edge_gids | std::views::transform([](auto const &g) { return g.AsUint(); });
  std::unordered_set<uint64_t> const gids_to_remove(as_uint.begin(), as_uint.end());

  for (auto &[_, index_item] : pimpl->index_by_name_) {
    auto &[mg_index, spec] = index_item;

    // Phase 1: READ_ONLY — export keys and find entries matching deleted GIDs
    std::vector<EdgeIndexEntry> entries_to_remove;
    {
      auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
      auto const index_size = mg_index.index.size();
      if (index_size == 0) continue;
      std::vector<EdgeIndexEntry> all_entries(index_size);
      mg_index.index.export_keys(all_entries.data(), 0, index_size);
      for (auto const &entry : all_entries) {
        if (entry.edge != nullptr && gids_to_remove.contains(entry.edge->gid.AsUint())) {
          entries_to_remove.push_back(entry);
        }
      }
    }

    if (entries_to_remove.empty()) continue;

    // Phase 2: UNIQUE — remove matched entries
    auto guard = std::lock_guard{mg_index.mutex};
    mg_index.index.remove(entries_to_remove.begin(), entries_to_remove.end());
  }
}

VectorEdgeIndex::IndexStats VectorEdgeIndex::Analysis() const {
  IndexStats res{};
  for (const auto &[_, index_item] : pimpl->index_by_name_) {
    const auto &filter = index_item.spec.edge_type_filter;
    const auto property = index_item.spec.property;
    for (const auto &edge_type : filter.edge_types) {
      res.et2p[edge_type].emplace_back(property);
      res.p2et[property].emplace_back(edge_type);
    }
  }
  return res;
}

EdgeTypeId VectorEdgeIndex::GetEdgeTypeId(std::string_view index_name) {
  auto it = pimpl->index_by_name_.find(index_name);
  if (it == pimpl->index_by_name_.end()) {
    throw query::VectorSearchException("Vector index {} does not exist.", index_name);
  }
  const auto &filter = it->second.spec.edge_type_filter;
  if (filter.edge_types.empty()) {
    throw query::VectorSearchException("Vector index {} has no edge type (wildcard).", index_name);
  }
  return filter.edge_types[0];
}

bool VectorEdgeIndex::IndexExists(std::string_view index_name) const {
  return pimpl->index_by_name_.contains(index_name);
}

std::vector<float> VectorEdgeIndex::GetVectorFromEdge(Vertex *from_vertex, Vertex *to_vertex, Edge *edge,
                                                      std::string_view index_name) const {
  auto it = pimpl->index_by_name_.find(index_name);
  if (it == pimpl->index_by_name_.end()) {
    throw query::VectorSearchException("Vector index {} does not exist.", index_name);
  }
  auto &[mg_index, _] = it->second;
  auto guard = utils::SharedResourceLockGuard(mg_index.mutex, utils::SharedResourceLockGuard::READ_ONLY);
  std::vector<float> vector(mg_index.index.dimensions());
  const EdgeIndexEntry entry{.from_vertex = from_vertex, .to_vertex = to_vertex, .edge = edge};
  if (!mg_index.index.get(entry, vector.data())) return {};
  return vector;
}

}  // namespace memgraph::storage
