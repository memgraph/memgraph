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

#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

struct ActiveIndicesUpdater;
struct Indices;
class NameIdMapper;

enum class VectorEdgeTypeMode : uint8_t {
  SINGLE = 0,
  WILDCARD = 1,
  ANY_OF = 2,
  ALL_OF = 3,
};

struct VectorEdgeTypeFilter {
  VectorEdgeTypeMode mode{VectorEdgeTypeMode::SINGLE};
  std::vector<EdgeTypeId> edge_types;

  bool Matches(EdgeTypeId edge_type) const {
    if (edge_types.empty() && mode != VectorEdgeTypeMode::WILDCARD) return false;
    switch (mode) {
      case VectorEdgeTypeMode::WILDCARD:
        return true;
      case VectorEdgeTypeMode::SINGLE:
        return edge_type == edge_types[0];
      case VectorEdgeTypeMode::ANY_OF:
      case VectorEdgeTypeMode::ALL_OF:
        return std::ranges::contains(edge_types, edge_type);
    }
    return false;
  }

  bool IsAffectedByEdgeType(EdgeTypeId edge_type) const {
    if (mode == VectorEdgeTypeMode::WILDCARD) return false;
    return std::ranges::contains(edge_types, edge_type);
  }

  friend bool operator==(const VectorEdgeTypeFilter &, const VectorEdgeTypeFilter &) = default;

  template <typename NameResolver>
  std::string Format(NameResolver &&resolver) const {
    if (edge_types.empty() && mode != VectorEdgeTypeMode::WILDCARD) return "";
    switch (mode) {
      case VectorEdgeTypeMode::WILDCARD:
        return ":*";
      case VectorEdgeTypeMode::SINGLE:
        return ":" + resolver(edge_types[0]);
      case VectorEdgeTypeMode::ANY_OF: {
        std::string result = ":" + resolver(edge_types[0]);
        for (std::size_t i = 1; i < edge_types.size(); ++i) result += "|" + resolver(edge_types[i]);
        return result;
      }
      case VectorEdgeTypeMode::ALL_OF: {
        std::string result = ":" + resolver(edge_types[0]);
        for (std::size_t i = 1; i < edge_types.size(); ++i) result += "&" + resolver(edge_types[i]);
        return result;
      }
    }
    return "";
  }
};

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
/// @brief Recovery information for a vector edge index, including entry data.
struct VectorEdgeIndexRecoveryInfo {
  VectorEdgeIndexSpec spec;
  absl::flat_hash_map<Gid, utils::small_vector<float>> index_entries;
};

/// @struct VectorEdgeIndexRecovery
/// @brief Handles recovery operations for vector edge indices during WAL replay and snapshot recovery.
struct VectorEdgeIndexRecovery {
  static void UpdateOnIndexDrop(std::string_view index_name, NameIdMapper *name_id_mapper,
                                std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec,
                                utils::SkipList<Vertex>::Accessor &vertices);

  static void UpdateOnSetEdgeProperty(PropertyId property, const PropertyValue &value, const Edge *edge,
                                      std::vector<VectorEdgeIndexRecoveryInfo> &recovery_info_vec);
};

/// Abstract interface for vector edge index metadata queries accessed through ActiveIndices snapshots.
struct VectorEdgeIndexActiveIndices {
  virtual ~VectorEdgeIndexActiveIndices() = default;
  virtual std::vector<VectorEdgeIndexSpec> ListIndices() const = 0;
  virtual std::vector<VectorEdgeIndexInfo> ListVectorIndicesInfo() const = 0;
  virtual std::optional<uint64_t> ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const = 0;
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
  struct AbortProcessor {
    std::map<EdgeTypeId, std::vector<PropertyId>> et2p;
    std::map<PropertyId, std::vector<EdgeTypeId>> p2et;

    struct EdgeAbortInfo {
      EdgeTypeId edge_type;
      Vertex *from_vertex;
      Vertex *to_vertex;
      std::map<PropertyId, PropertyValue> properties;
    };

    using AbortableInfo = std::map<Edge *, EdgeAbortInfo>;
    AbortableInfo cleanup_collection;

    bool IsInteresting(PropertyId property) const { return p2et.contains(property); }

    void CollectOnPropertyChange(EdgeTypeId edge_type, PropertyId property, const PropertyValue &old_value,
                                 Vertex *from_vertex, Vertex *to_vertex, Edge *edge);
  };

  struct EdgeIndexEntry {
    Vertex *from_vertex;
    Vertex *to_vertex;
    Edge *edge;
  };

  using VectorSearchEdgeResults = std::vector<std::tuple<EdgeIndexEntry, double, double>>;

  /// Concrete ActiveIndices implementation holding a shared reference to the live edge index container.
  struct ActiveIndices : VectorEdgeIndexActiveIndices {
    ActiveIndices() = default;

    explicit ActiveIndices(std::shared_ptr<VectorEdgeIndexContainer const> container)
        : index_container_(std::move(container)) {}

    std::vector<VectorEdgeIndexSpec> ListIndices() const override;
    std::vector<VectorEdgeIndexInfo> ListVectorIndicesInfo() const override;
    std::optional<uint64_t> ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const override;

   private:
    std::shared_ptr<VectorEdgeIndexContainer const> index_container_;
  };

  VectorEdgeIndex() = default;
  ~VectorEdgeIndex() = default;

  VectorEdgeIndex(VectorEdgeIndex &&other) noexcept
      : index_(std::move(other.index_)), edge_endpoints_(std::move(other.edge_endpoints_)) {}

  VectorEdgeIndex &operator=(VectorEdgeIndex &&other) noexcept {
    if (this != &other) {
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
  bool CreateIndex(const VectorEdgeIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                   NameIdMapper *name_id_mapper,
                   std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// @brief Recovers a vector edge index based on recovery info.
  void RecoverIndex(VectorEdgeIndexRecoveryInfo &recovery_info, utils::SkipList<Vertex>::Accessor &vertices,
                    NameIdMapper *name_id_mapper, ActiveIndicesUpdater const &updater,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// @brief Drops an existing index.
  bool DropIndex(std::string_view index_name, NameIdMapper *name_id_mapper);

  /// @brief Drops all existing indexes.
  void Clear();

  void UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                           PropertyId property, const PropertyValue &value);

  /// @brief Lists the info of all existing indexes.
  std::vector<VectorEdgeIndexInfo> ListVectorIndicesInfo() const;

  /// @brief Lists the labels and properties that have vector indices.
  std::vector<VectorEdgeIndexSpec> ListIndices() const;

  /// @brief Returns number of edges in the index.
  std::optional<uint64_t> ApproximateEdgesVectorCount(EdgeTypeId edge_type, PropertyId property) const;

  /// @brief Searches for edges in the specified index using a query vector.
  VectorSearchEdgeResults SearchEdges(std::string_view index_name, uint64_t result_set_size,
                                      const std::vector<float> &query_vector) const;

  /// @brief Aborts the entries in the vector edge index.
  void AbortEntries(AbortProcessor::AbortableInfo &cleanup_collection);

  /// @brief Removes edges from the index.
  void RemoveEdges(std::vector<Edge *> const &edges_to_remove);

  /// @brief Returns an abort processor snapshot used during transaction abort.
  AbortProcessor GetAbortProcessor() const;

  /// @brief Checks if any vector index exists.
  bool Empty() const;

  /// @brief Returns the EdgeTypeId for the given index name.
  EdgeTypeId GetEdgeTypeId(std::string_view index_name);

  /// @brief Checks if a vector index exists for the given name.
  bool IndexExists(std::string_view index_name) const;

  /// @brief Retrieves the vector from an edge index entry.
  utils::small_vector<float> GetVectorPropertyFromEdgeIndex(Edge *edge, std::string_view index_name,
                                                            NameIdMapper *name_id_mapper) const;

  /// @brief Looks up the endpoint vertices for an edge in any vector index.
  std::pair<Vertex *, Vertex *> GetEdgeEndpoints(Edge *edge) const;

  /// @brief Finds the index ID for the given (edge_type, property) pair.
  std::optional<uint64_t> GetIndexIdForEdgeTypeProperty(EdgeTypeId edge_type, PropertyId property) const;

  /// @brief Gets all edge types that have vector indices for the given property.
  std::vector<std::pair<uint64_t, VectorEdgeTypeFilter const *>> GetIndicesByProperty(PropertyId property) const;

  /// @brief Serializes all vector edge indices to a durability encoder in one pass.
  void SerializeAllVectorEdgeIndices(durability::BaseEncoder *encoder, std::unordered_set<uint64_t> &mapped_ids) const;

 private:
  /// @brief Sets up a new vector edge index structure without populating it.
  std::optional<uint64_t> SetupIndex(const VectorEdgeIndexSpec &spec, NameIdMapper *name_id_mapper);

  /// @brief Adds a single edge to the index, converting its property to VectorIndexId.
  void AddEdgeToIndex(uint64_t index_id, Edge *edge, Vertex *from_vertex, Vertex *to_vertex,
                      std::optional<std::size_t> thread_id = std::nullopt);

  /// @brief Removes an edge from a vector index.
  void RemoveEdgeFromIndex(Edge *edge, uint64_t index_id);

  // Invariant: `index_` is only mutated under UNIQUE storage access (see the MG_ASSERTs in
  // InMemoryAccessor::CreateVectorEdgeIndex / DropVectorIndex and in DropGraphClearIndices). Reads
  // from other contexts (regular READ/WRITE accessors, DatabaseInfoQuery) MUST go through the
  // published snapshot in `ActiveIndicesStore` -- UNIQUE excludes READ/WRITE, which is what makes
  // direct access to `index_` from commit-time hot paths race-free.
  std::shared_ptr<VectorEdgeIndexContainer> index_ = std::make_shared<VectorEdgeIndexContainer>();
  std::unordered_map<Edge *, std::pair<Vertex *, Vertex *>> edge_endpoints_;
  // Lock order: mg_index.mutex → edge_endpoints_mutex_ (never acquire mg_index.mutex while holding
  // edge_endpoints_mutex_)
  mutable std::shared_mutex edge_endpoints_mutex_;
};

}  // namespace memgraph::storage
