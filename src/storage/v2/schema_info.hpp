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

#pragma once

#include "storage/v2/schema_info_types.hpp"

#include <boost/container_hash/hash_fwd.hpp>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "storage/v2/edge_ref.hpp"
#include "storage/v2/enum_store.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/conccurent_unordered_map.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/small_vector.hpp"

#include <nlohmann/json_fwd.hpp>

namespace memgraph::storage {

struct SchemaTrackingInterface {
  SchemaTrackingInterface() = default;
  virtual ~SchemaTrackingInterface() = default;
  SchemaTrackingInterface(const SchemaTrackingInterface &) = default;
  SchemaTrackingInterface &operator=(const SchemaTrackingInterface &) = default;
  SchemaTrackingInterface(SchemaTrackingInterface &&) = default;
  SchemaTrackingInterface &operator=(SchemaTrackingInterface &&) = default;

  virtual void Clear() = 0;
  virtual nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) const = 0;
  virtual nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store,
                                const std::function<bool(LabelId)> &node_predicate,
                                const std::function<bool(EdgeTypeId)> &edge_predicate) const = 0;
  virtual void RecoverVertex(Vertex *vertex) = 0;
  virtual void RecoverEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges) = 0;
  virtual void AddVertex(Vertex *vertex) = 0;
  virtual void DeleteVertex(Vertex *vertex) = 0;
  virtual void UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                            const utils::small_vector<LabelId> &new_labels) = 0;
  virtual void UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                            const utils::small_vector<LabelId> &new_labels, bool prop_on_edges) = 0;
  virtual void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type) = 0;
  virtual void DeleteEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges) = 0;
  virtual void SetProperty(Vertex *vertex, PropertyId property, const ExtendedPropertyType &now,
                           const ExtendedPropertyType &before) = 0;
  virtual void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property,
                           const ExtendedPropertyType &now, const ExtendedPropertyType &before, bool prop_on_edges) = 0;
  virtual void SetProperty(EdgeTypeId type, const utils::small_vector<LabelId> &from,
                           const utils::small_vector<LabelId> &to, PropertyId property, const ExtendedPropertyType &now,
                           const ExtendedPropertyType &before, bool prop_on_edges) = 0;
};

template <template <class...> class TContainer = utils::ConcurrentUnorderedMap>
struct SchemaTracking;

using LocalSchemaTracking = SchemaTracking<std::unordered_map>;
using SharedSchemaTracking = SchemaTracking<utils::ConcurrentUnorderedMap>;

template <template <class...> class TContainer>
struct SchemaTracking final : public SchemaTrackingInterface {
  template <template <class...> class TOtherContainer>
  void ProcessTransaction(const SchemaTracking<TOtherContainer> &diff, SchemaInfoPostProcess &post_process,
                          uint64_t start_ts, uint64_t commit_ts, bool property_on_edges);

  void Clear() override;

  size_t NumberOfVertices() const { return vertex_state_.size(); }

  size_t NumberOfEdges() const { return edge_state_.size(); }

  nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) const override;

  nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store,
                        const std::function<bool(LabelId)> &node_predicate,
                        const std::function<bool(EdgeTypeId)> &edge_predicate) const override;

  void RecoverVertex(Vertex *vertex) override;

  void RecoverEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges) override;

  void AddVertex(Vertex *vertex) override { ++vertex_state_[vertex->labels].n; }

  void DeleteVertex(Vertex *vertex) override;

  void UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                    const utils::small_vector<LabelId> &new_labels) override;

  void UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                    const utils::small_vector<LabelId> &new_labels, bool prop_on_edges) override;

  void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type) override;

  void DeleteEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges) override;

  void SetProperty(Vertex *vertex, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before) override;

  void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before, bool prop_on_edges) override;

  void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before, bool prop_on_edges, auto &&guard, auto &&other_guard);

  void SetProperty(EdgeTypeId type, const utils::small_vector<LabelId> &from, const utils::small_vector<LabelId> &to,
                   PropertyId property, const ExtendedPropertyType &now, const ExtendedPropertyType &before,
                   bool prop_on_edges) override;

  void UpdateEdgeStats(EdgeRef edge_ref, EdgeTypeId edge_type, const VertexKey &new_from_labels,
                       const VertexKey &new_to_labels, const VertexKey &old_from_labels, const VertexKey &old_to_labels,
                       bool prop_on_edges);

  void UpdateEdgeStats(EdgeRef edge_ref, EdgeTypeId edge_type, const VertexKey &new_from_labels,
                       const VertexKey &new_to_labels, const VertexKey &old_from_labels, const VertexKey &old_to_labels,
                       const std::map<PropertyId, ExtendedPropertyType> &edge_props);

 private:
  friend LocalSchemaTracking;
  friend SharedSchemaTracking;

  TrackingInfo<TContainer> &edge_lookup(const EdgeKeyRef &key);

  void SetProperty(auto &tracking_info, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before);

  void UpdateEdgeStats(auto &new_tracking, auto &old_tracking, EdgeRef edge_ref, bool prop_on_edges);

  void UpdateEdgeStats(auto &new_tracking, auto &old_tracking, EdgeRef edge_ref,
                       const std::map<PropertyId, ExtendedPropertyType> &edge_props);

  TContainer<VertexKey, TrackingInfo<TContainer>> vertex_state_;  //!< vertex statistics
  TContainer<EdgeKey, TrackingInfo<TContainer>> edge_state_;      //!< edge statistics
};

struct SchemaInfo {
  // Snapshot guaranteeing functions
  nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) const {
    auto lock = std::unique_lock{operation_ordering_mutex_};  // No snapshot guarantees for ANALYTICAL
    return tracking_.ToJson(name_id_mapper, enum_store);
  }

  nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store,
                        const std::function<bool(LabelId)> &node_predicate,
                        const std::function<bool(EdgeTypeId)> &edge_predicate) const {
    auto lock = std::unique_lock{operation_ordering_mutex_};  // No snapshot guarantees for ANALYTICAL
    return tracking_.ToJson(name_id_mapper, enum_store, node_predicate, edge_predicate);
  }

  void ProcessTransaction(LocalSchemaTracking &tracking, SchemaInfoPostProcess &post_process, uint64_t start_ts,
                          uint64_t commit_ts, bool property_on_edges) {
    auto lock = std::unique_lock{operation_ordering_mutex_};
    tracking_.ProcessTransaction(tracking, post_process, start_ts, commit_ts, property_on_edges);
  }

  void Clear() {
    auto lock = std::unique_lock{operation_ordering_mutex_};
    tracking_.Clear();
  }

  struct SchemaSize {
    size_t n_vertices;
    size_t n_edges;
  };

  SchemaSize Size() const {
    auto lock = std::shared_lock{operation_ordering_mutex_};  // No snapshot guarantees for ANALYTICAL
    return {tracking_.NumberOfVertices(), tracking_.NumberOfEdges()};
  }

  // Raw reference
  auto &Get() { return tracking_; }

  // Advanced modification accessors
  class TransactionalEdgeModifyingAccessor {
   public:
    TransactionalEdgeModifyingAccessor(LocalSchemaTracking &tracking, SchemaInfoPostProcess *post_process,
                                       uint64_t start_ts, uint64_t commit_ts, bool prop_on_edges)
        : tracking_{&tracking},
          properties_on_edges_{prop_on_edges},
          post_process_{post_process},
          start_ts_{start_ts},
          commit_ts_{commit_ts} {}

    void AddLabel(Vertex *vertex, LabelId label, std::unique_lock<utils::RWSpinLock> v_lock);

    void RemoveLabel(Vertex *vertex, LabelId label, std::unique_lock<utils::RWSpinLock> v_lock);

   private:
    void UpdateTransactionalEdges(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                                  std::unique_lock<utils::RWSpinLock> v_lock);

    LocalSchemaTracking *tracking_{};
    bool properties_on_edges_{};  //!< As defined by the storage configuration
    SchemaInfoPostProcess *post_process_{};
    uint64_t start_ts_{};
    uint64_t commit_ts_{};
  };

  /**
   * @brief We need to force ordering for analytical. This is because an edge is defined via 3 independent objects
   * (from/to vertex and edge object). We need to process each edge or vertex label change in order. Otherwise there is
   * no way to keep track of how the schema is being modified.
   *
   * Use unique accessor when edge modification is needed; shared otherwise.
   */
  class AnalyticalEdgeModifyingAccessor {
   public:
    AnalyticalEdgeModifyingAccessor(SchemaInfo &si, bool prop_on_edges)
        : tracking_{&si.tracking_}, ordering_lock_{si.operation_ordering_mutex_}, properties_on_edges_{prop_on_edges} {}

    void AddLabel(Vertex *vertex, LabelId label);

    void RemoveLabel(Vertex *vertex, LabelId label);

   private:
    void UpdateAnalyticalEdges(Vertex *vertex, const utils::small_vector<LabelId> &old_labels);

    SharedSchemaTracking *tracking_{};
    std::unique_lock<std::shared_mutex> ordering_lock_;  //!< Order guaranteeing lock
    bool properties_on_edges_{};                         //!< As defined by the storage configuration
  };

  /**
   * @brief We need to force ordering for analytical. This is because an edge is defined via 3 independent objects
   * (from/to vertex and edge object). We need to process each edge or vertex label change in order. Otherwise there
   * is no way to keep track of how the schema is being modified.
   *
   * Use unique accessor when edge modification is needed; shared otherwise. (when not all objects are locked)
   */
  class VertexModifyingAccessor {
   public:
    // ANALYTICAL
    explicit VertexModifyingAccessor(SchemaInfo &si, bool prop_on_edges)
        : tracking_{&si.tracking_}, ordering_lock_{si.operation_ordering_mutex_}, properties_on_edges_(prop_on_edges) {}

    // TRANSACTIONAL
    explicit VertexModifyingAccessor(LocalSchemaTracking &tracking, SchemaInfoPostProcess *post_process,
                                     uint64_t start_ts, uint64_t commit_ts, bool prop_on_edges)
        : tracking_{&tracking},
          properties_on_edges_{prop_on_edges},
          post_process_{post_process},
          start_ts_{start_ts},
          commit_ts_{commit_ts} {}

    void CreateVertex(Vertex *vertex);

    void DeleteVertex(Vertex *vertex);

    void AddLabel(Vertex *vertex, LabelId label);

    void RemoveLabel(Vertex *vertex, LabelId label);

    void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type);

    // Multi-threaded deletion in ANALYTICAL is UB, so we can leave it with shared access
    void DeleteEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type, EdgeRef edge);

    void SetProperty(Vertex *vertex, PropertyId property, ExtendedPropertyType now, ExtendedPropertyType before);

    void SetProperty(EdgeRef edge, EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property,
                     ExtendedPropertyType now, ExtendedPropertyType before);

   private:
    SchemaTrackingInterface *tracking_{};
    std::shared_lock<std::shared_mutex> ordering_lock_;  //!< Order guaranteeing lock
    bool properties_on_edges_{};                         //!< As defined by the storage configuration
    SchemaInfoPostProcess *post_process_{};
    uint64_t start_ts_{};
    uint64_t commit_ts_{};
  };

  using ModifyingAccessor =
      std::variant<VertexModifyingAccessor, TransactionalEdgeModifyingAccessor, AnalyticalEdgeModifyingAccessor>;

  ModifyingAccessor CreateVertexModifyingAccessor(bool prop_on_edges) {
    return VertexModifyingAccessor{*this, prop_on_edges};
  }

  ModifyingAccessor CreateEdgeModifyingAccessor(bool prop_on_edges) {
    return AnalyticalEdgeModifyingAccessor{*this, prop_on_edges};
  }

  static ModifyingAccessor CreateVertexModifyingAccessor(auto &tracking, auto &post_process, uint64_t start_ts,
                                                         uint64_t commit_ts, bool prop_on_edges) {
    return VertexModifyingAccessor{tracking, &post_process, start_ts, commit_ts, prop_on_edges};
  }
  static ModifyingAccessor CreateEdgeModifyingAccessor(auto &tracking, SchemaInfoPostProcess *post_process,
                                                       uint64_t start_ts, uint64_t commit_ts, bool prop_on_edges) {
    return TransactionalEdgeModifyingAccessor{tracking, post_process, start_ts, commit_ts, prop_on_edges};
  }

  static std::optional<std::pair<std::shared_lock<utils::RWSpinLock>, std::shared_lock<utils::RWSpinLock>>>
  ReadLockFromTo(auto &schema_acc, StorageMode mode, Vertex *from, Vertex *to) {
    if (!schema_acc) return {};
    return ReadLockFromTo(from, to);
  }

  static std::pair<std::shared_lock<utils::RWSpinLock>, std::shared_lock<utils::RWSpinLock>> ReadLockFromTo(
      Vertex *from, Vertex *to) {
    // Direct modification in both analytical and transactional
    auto from_lock = std::shared_lock{from->lock, std::defer_lock};
    auto to_lock = std::shared_lock{to->lock, std::defer_lock};

    if (from == to) {
      from_lock.lock();
    } else if (from->gid < to->gid) {
      from_lock.lock();
      to_lock.lock();
    } else {
      to_lock.lock();
      from_lock.lock();
    }

    return std::pair{std::move(from_lock), std::move(to_lock)};
  }

 private:
  friend VertexModifyingAccessor;
  friend TransactionalEdgeModifyingAccessor;
  friend AnalyticalEdgeModifyingAccessor;

  SharedSchemaTracking tracking_;                       //!< Tracking schema stats
  mutable std::shared_mutex operation_ordering_mutex_;  //!< Analytical operations ordering | Transactional RW control
};

}  // namespace memgraph::storage
