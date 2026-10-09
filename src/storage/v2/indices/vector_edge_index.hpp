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

#include <algorithm>
#include <list>
#include <map>
#include <shared_mutex>
#include <span>

#include "absl/container/flat_hash_map.h"
#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::storage {

struct ActiveIndicesUpdater;
struct Indices;
class NameIdMapper;

using VectorEdgeTypeFilter = VectorMembershipFilter<EdgeTypeId>;

/// @struct VectorEdgeIndexSpec
/// @brief Represents a specification for creating a vector index in the system.
struct VectorEdgeIndexSpec {
  std::string index_name;
  VectorEdgeTypeFilter edge_type_filter;
  PropertyId property;
  unum::usearch::metric_kind_t metric_kind;
  std::uint16_t dimension;
  std::uint16_t resize_coefficient;
  std::size_t capacity;
  unum::usearch::scalar_kind_t scalar_kind;

  friend bool operator==(const VectorEdgeIndexSpec &, const VectorEdgeIndexSpec &) = default;
};

/// @struct VectorEdgeIndexInfo
struct VectorEdgeIndexInfo {
  std::string index_name;
  VectorEdgeTypeFilter edge_type_filter;
  PropertyId property;
  std::string metric;
  std::uint16_t dimension;
  std::size_t capacity;
  std::size_t size;
  std::string scalar_kind;
};

/// @struct VectorEdgeIndexRecoveryInfo
/// @brief Recovery information for a vector edge index.
struct VectorEdgeIndexRecoveryInfo {
  VectorEdgeIndexSpec spec;
};

/// For every edge e and every property p with at least one vector edge spec, if e's stored p is a tag
/// (VectorIndexId), then edge_vectors[p][e.gid] holds its float vector. A plain list in the property store
/// is its own vector and wins over any map entry. Tag IDs are not trusted during recovery.
struct VectorEdgeIndexRecovery {
  using EdgeVectors = absl::flat_hash_map<PropertyId, absl::flat_hash_map<Gid, utils::small_vector<float>>>;

  /// Called on WAL EdgeSetProperty: captures tag→edge_vectors[p][gid] or erases (list/null) when p has a
  /// spec; demotes stale tags to plain lists (mutates value) when p has no spec.
  static void UpdateOnSetEdgeProperty(PropertyId property, PropertyValue &value, const Edge *edge,
                                      std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec,
                                      EdgeVectors &edge_vectors);

  /// Called on WAL VectorIndexDrop: removes the spec. If no other spec covers the same property, iterates
  /// edges to restore stored tags to plain lists, then drops the map entry.
  static void UpdateOnIndexDrop(std::string_view index_name,
                                std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec, EdgeVectors &edge_vectors,
                                utils::SkipListDb<Vertex>::Accessor &vertices, NameIdMapper *name_id_mapper);
};

/// Abstract interface for vector edge index metadata queries accessed through ActiveIndices snapshots.
struct VectorEdgeIndexActiveIndices {
  virtual ~VectorEdgeIndexActiveIndices() = default;
  virtual std::vector<VectorEdgeIndexSpec> ListIndices() const = 0;
  virtual std::vector<VectorEdgeIndexInfo> ListVectorIndicesInfo() const = 0;
  virtual std::optional<uint64_t> ApproximateEdgesVectorCount(std::string_view index_name) const = 0;
  /// Properties of an edge of `edge_type` that are covered by a vector edge index.
  virtual std::vector<PropertyId> IndexedProperties(EdgeTypeId edge_type) const = 0;
};

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

/// Container mapping index IDs to shared edge index items.
using VectorEdgeIndexContainer = std::unordered_map<uint64_t, std::shared_ptr<EdgeTypeIndexItem>>;

/// @class VectorEdgeIndex
/// @brief High-level interface for managing vector edge indexes.
///
/// The VectorEdgeIndex class supports creating new indexes, adding edges to an index,
/// listing all indexes, and searching for edges using a query vector.
/// Currently, vector edge index operates in READ_UNCOMMITTED isolation level. Database can
/// still operate in any other isolation level.
/// The index container is held via a copy-on-write shared_ptr<VectorEdgeIndexContainer>,
/// so ActiveIndices snapshots remain stable while Create/Drop swap in a new version.
class VectorEdgeIndex {
 public:
  struct EdgeIndexEntry {
    Vertex *from_vertex;
    Vertex *to_vertex;
    Edge *edge;
    EdgeTypeId edge_type;
  };

  struct EdgeEndpoints {
    Vertex *from_vertex;
    Vertex *to_vertex;
    EdgeTypeId edge_type;
  };

  using VectorSearchEdgeResults = std::vector<std::tuple<EdgeIndexEntry, double, double>>;

  explicit VectorEdgeIndex(utils::MemoryTracker *memory_tracker = nullptr);
  ~VectorEdgeIndex();

  /// Concrete ActiveIndices implementation holding a shared reference to the live edge index container.
  struct ActiveIndices : VectorEdgeIndexActiveIndices {
    ActiveIndices() = default;

    explicit ActiveIndices(std::shared_ptr<VectorEdgeIndexContainer const> container)
        : index_container_(std::move(container)) {}

    std::vector<VectorEdgeIndexSpec> ListIndices() const override;
    std::vector<VectorEdgeIndexInfo> ListVectorIndicesInfo() const override;
    std::optional<uint64_t> ApproximateEdgesVectorCount(std::string_view index_name) const override;
    std::vector<PropertyId> IndexedProperties(EdgeTypeId edge_type) const override;

   private:
    std::shared_ptr<VectorEdgeIndexContainer const> index_container_;
  };

  VectorEdgeIndex(VectorEdgeIndex &&other) noexcept
      : memory_tracker_(other.memory_tracker_),
        index_(std::move(other.index_)),
        edge_endpoints_(std::move(other.edge_endpoints_)) {}

  VectorEdgeIndex &operator=(VectorEdgeIndex &&other) noexcept {
    if (this != &other) {
      memory_tracker_ = other.memory_tracker_;
      index_ = std::move(other.index_);
      edge_endpoints_ = std::move(other.edge_endpoints_);
    }
    return *this;
  }

  /// Returns the current active indices snapshot for use in transactions.
  auto GetActiveIndices() const -> std::shared_ptr<VectorEdgeIndexActiveIndices> {
    return std::make_shared<ActiveIndices>(index_);
  }

  /// Publishes the current index container as the new ActiveIndices snapshot.
  /// Mirrors the API on TextEdgeIndex / PointIndexStorage.
  void PublishActiveIndices(ActiveIndicesUpdater const &updater) const;

  /// @brief Creates a new index based on the provided specification.
  bool CreateIndex(const VectorEdgeIndexSpec &spec, utils::SkipListDb<Vertex>::Accessor &vertices,
                   NameIdMapper *name_id_mapper, ProgressCallback const &on_progress = {});

  /// Recovers all vector edge indices in one pass. On failure, drops every index set up and rethrows.
  /// edge_vectors is cleared on success; on_progress fires once per vertex, not per insertion.
  void RecoverAllVectorEdgeIndices(std::vector<VectorEdgeIndexRecoveryInfo> &recovery_infos,
                                   VectorEdgeIndexRecovery::EdgeVectors &edge_vectors,
                                   utils::SkipListDb<Vertex>::Accessor &vertices, NameIdMapper *name_id_mapper,
                                   ActiveIndicesUpdater const &updater, ProgressCallback const &on_progress = {});

  /// Mirror of VectorIndex::DroppedIndexCapture, plus evicted_endpoints — endpoint
  /// records erased from edge_endpoints_ that RestoreIndex must put back.
  struct DroppedIndexCapture {
    uint64_t index_id;
    std::shared_ptr<EdgeTypeIndexItem> evicted_item;
    std::vector<Edge *> rewritten_edges;
    std::vector<std::pair<Edge *, EdgeEndpoints>> evicted_endpoints;
  };

  /// @brief Drops an existing index. Returns enough state to undo the drop on
  /// transaction abort, or std::nullopt if the index doesn't exist. Callers that
  /// only need a fire-and-forget drop (e.g. CreateIndex's exception rollback)
  /// can discard the return value.
  /// `on_progress` is invoked once per indexed edge while their properties are rewritten back from index ids to
  /// vectors. See VectorIndex::DropIndex for why the caller needs it.
  std::optional<DroppedIndexCapture> DropIndex(std::string_view index_name, NameIdMapper *name_id_mapper,
                                               ProgressCallback const &on_progress = {});

  /// @brief Reinstalls an edge index previously evicted by DropIndex.
  void RestoreIndex(DroppedIndexCapture &&capture);

  /// @brief Drops all existing indexes.
  void Clear();

  void UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                           PropertyId property, const PropertyValue &value);

  /// @brief Lists the info of all existing indexes.
  std::vector<VectorEdgeIndexInfo> ListVectorIndicesInfo() const;

  /// @brief Lists the labels and properties that have vector indices.
  std::vector<VectorEdgeIndexSpec> ListIndices() const;

  /// @brief Returns number of edges in the named index, or nullopt if no such index.
  std::optional<uint64_t> ApproximateEdgesVectorCount(std::string_view index_name) const;

  /// @brief Searches for edges in the specified index using a query vector.
  VectorSearchEdgeResults SearchEdges(std::string_view index_name, uint64_t result_set_size,
                                      const std::vector<float> &query_vector) const;

  /// @brief Allocation-free: whether any index covers `property`.
  bool HasIndexOnProperty(PropertyId property) const;

  /// @brief Abort-path inverse of UpdateOnSetProperty: called once per undone SET_PROPERTY delta under the edge lock,
  /// after the property store took `before` back. `link` is the edge's type and target as found by the caller; when
  /// absent the recorded endpoints are used. Never throws and leaves no tag without a usearch entry.
  void RestoreOnSetProperty(Vertex *from_vertex, Edge *edge, PropertyId property, const PropertyValue &before,
                            std::optional<std::pair<EdgeTypeId, Vertex *>> link) noexcept;

  /// @brief Removes edges from the index by GID.
  /// Must be called before the edge is removed from the skip list (while the pointer is still valid).
  void RemoveEdges(std::span<Edge *const> edges_to_remove) const;

  /// @brief Checks if any vector index exists.
  bool Empty() const;

  /// @brief Checks if a vector index exists for the given name.
  bool IndexExists(std::string_view index_name) const;

  /// @brief Retrieves the vector from an edge index entry.
  utils::small_vector<float> GetVectorPropertyFromEdgeIndex(Edge *edge, std::string_view index_name,
                                                            NameIdMapper *name_id_mapper) const;

  /// @brief Returns all index ids whose filter matches `edge_type` and which index `property`.
  /// Multiple indices may overlap (e.g. wildcard '(p)' plus specific ':REL(p)'); all are returned.
  utils::small_vector<uint64_t> GetIndexIdsForEdgeTypeProperty(EdgeTypeId edge_type, PropertyId property) const;

  /// @brief Gets all edge types that have vector indices for the given property.
  std::vector<std::pair<uint64_t, VectorEdgeTypeFilter const *>> GetIndicesByProperty(PropertyId property) const;

  /// @brief Serializes all vector edge indices to a durability encoder in one pass.
  void SerializeAllVectorEdgeIndices(durability::BaseEncoder *encoder, std::unordered_set<uint64_t> &mapped_ids) const;

 private:
  /// @brief Sets up a new vector edge index structure without populating it.
  std::optional<uint64_t> SetupIndex(const VectorEdgeIndexSpec &spec, NameIdMapper *name_id_mapper);

  /// @brief Adds a single edge to the index, converting its property to VectorIndexId.
  void AddEdgeToIndex(uint64_t index_id, Edge *edge, EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex,
                      NameIdMapper *name_id_mapper, std::optional<std::size_t> thread_id = std::nullopt);

  /// @brief Removes an edge from a vector index.
  void RemoveEdgeFromIndex(Edge *edge, uint64_t index_id);

  void EraseEndpointsIfUnreferenced(Edge *edge);

  /// Abort path: removes the edge from every index on `property`, each removal guarded on its own.
  void DropEntries(Edge *edge, PropertyId property);

  utils::MemoryTracker *memory_tracker_{nullptr};
  // Invariant: `index_` is only mutated under UNIQUE storage access (see the MG_ASSERTs in
  // InMemoryAccessor::CreateVectorEdgeIndex / DropVectorIndex and in DropGraphClearIndices). Reads
  // from other contexts (regular READ/WRITE accessors, DatabaseInfoQuery) MUST go through the
  // published snapshot in `ActiveIndicesStore` -- UNIQUE excludes READ/WRITE, which is what makes
  // direct access to `index_` from commit-time hot paths race-free.
  std::shared_ptr<VectorEdgeIndexContainer> index_ = std::make_shared<VectorEdgeIndexContainer>();

  mutable std::unordered_map<Edge *, EdgeEndpoints> edge_endpoints_;
  // Lock order: mg_index.mutex → edge_endpoints_mutex_ (never acquire mg_index.mutex while holding
  // edge_endpoints_mutex_)
  mutable std::shared_mutex edge_endpoints_mutex_;
};

}  // namespace memgraph::storage
