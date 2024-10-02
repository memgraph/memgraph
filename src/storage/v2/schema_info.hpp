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

#pragma once

#include "storage/v2/schema_info_types.hpp"

#include <boost/container_hash/hash_fwd.hpp>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/enum_store.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/conccurent_unordered_map.hpp"
#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/small_vector.hpp"

#include <json/json.hpp>

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
};

template <template <class...> class TContainer = utils::ConcurrentUnorderedMap>
struct SchemaTracking;

using LocalSchemaTracking = SchemaTracking<std::unordered_map>;
using SharedSchemaTracking = SchemaTracking<utils::ConcurrentUnorderedMap>;

template <template <class...> class TContainer>
struct SchemaTracking final : public SchemaTrackingInterface {
  /**
   * @brief Process transaction (deltas) in order to update the schema.
   *
   * @param transaction
   * @param properties_on_edges
   */
  // void ProcessTransaction(Transaction &transaction, bool properties_on_edges);

  template <template <class...> class TOtherContainer>
  void ProcessTransaction(const SchemaTracking<TOtherContainer> &diff, std::unordered_set<PostProcessPOC> &post_process,
                          uint64_t commit_ts, bool property_on_edges);

  /**
   * @brief Clear all schema statistics.
   */
  void Clear() override {
    vertex_state_.clear();
    edge_state_.clear();
  }

  /**
   * @brief Number of uniquely identified vertices defined in the schema.
   *
   * @return size_t
   */
  size_t NumberOfVertices() const { return vertex_state_.size(); }

  /**
   * @brief Number of uniquely identified edges defined in the schema.
   *
   * @return size_t
   */
  size_t NumberOfEdges() const { return edge_state_.size(); }

  nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) const override;

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

  void SetProperty(auto &tracking_info, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before);

  void SetProperty(Vertex *vertex, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before) override {
    auto &tracking_info = vertex_state_[vertex->labels];
    SetProperty(tracking_info, property, now, before);
  }

  void SetProperty(Vertex *vertex, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before, auto &&guard) {
    auto &tracking_info = vertex_state_[vertex->labels];
    if (guard.owns_lock()) guard.unlock();
    SetProperty(tracking_info, property, now, before);
  }

  void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before, bool prop_on_edges) override {
    if (prop_on_edges) {
      auto &tracking_info = edge_lookup(EdgeKeyRef{type, from->labels, to->labels});
      SetProperty(tracking_info, property, now, before);
    }
  }

  void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before, bool prop_on_edges, auto &&guard, auto &&other_guard) {
    if (prop_on_edges) {
      auto &tracking_info = edge_lookup(EdgeKeyRef{type, from->labels, to->labels});
      if (guard.owns_lock()) guard.unlock();
      if (other_guard.owns_lock()) other_guard.unlock();
      SetProperty(tracking_info, property, now, before);
    }
  }

  void UpdateEdgeStats(EdgeRef edge_ref, EdgeTypeId edge_type, const VertexKey &new_from_labels,
                       const VertexKey &new_to_labels, const VertexKey &old_from_labels, const VertexKey &old_to_labels,
                       bool prop_on_edges) {
    DMG_ASSERT(std::is_same_v<decltype(*this), LocalSchemaTracking>, "Using a local-only function on a shared object");
    UpdateEdgeStats(edge_lookup({edge_type, new_from_labels, new_to_labels}),
                    edge_lookup({edge_type, old_from_labels, old_to_labels}), edge_ref, prop_on_edges);
  }

  void UpdateEdgeStats(EdgeRef edge_ref, EdgeTypeId edge_type, const VertexKey &new_from_labels,
                       const VertexKey &new_to_labels, const VertexKey &old_from_labels, const VertexKey &old_to_labels,
                       auto &&from_lock, auto &&to_lock, bool prop_on_edges) {
    DMG_ASSERT(std::is_same_v<decltype(*this), SharedSchemaTracking>, "Using a shared-only function on a local object");
    // Lookup needs to happen while holding the locks, but the update itself does not
    auto &new_tracking = edge_lookup({edge_type, new_from_labels, new_to_labels});
    auto &old_tracking = edge_lookup({edge_type, old_from_labels, old_to_labels});
    if (from_lock.owns_lock()) from_lock.unlock();
    if (to_lock.owns_lock()) to_lock.unlock();
    UpdateEdgeStats(new_tracking, old_tracking, edge_ref, prop_on_edges);
  }

  void UpdateEdgeStats(auto &new_tracking, auto &old_tracking, EdgeRef edge_ref, bool prop_on_edges) {
    --old_tracking.n;
    ++new_tracking.n;

    if (prop_on_edges) {
      // No need for edge lock since all edge property operations are unique access
      for (const auto &[property, type] : edge_ref.ptr->properties.ExtendedPropertyTypes()) {
        auto &old_info = old_tracking.properties[property];
        --old_info.n;
        --old_info.types[type];
        auto &new_info = new_tracking.properties[property];
        ++new_info.n;
        ++new_info.types[type];
      }
    }
  }

 private:
  friend LocalSchemaTracking;
  friend SharedSchemaTracking;

  TrackingInfo<TContainer> &edge_lookup(const EdgeKeyRef &key);

  TContainer<VertexKey, TrackingInfo<TContainer>> vertex_state_;  //!< vertex statistics
  TContainer<EdgeKey, TrackingInfo<TContainer>> edge_state_;      //!< edge statistics
};

struct SchemaInfo {
  // Snapshot guaranteeing functions
  nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) const {
    auto lock = std::shared_lock{operation_ordering_mutex_};  // No snapshot guarantees for ANALYTICAL
    return tracking_.ToJson(name_id_mapper, enum_store);
  }

  void ProcessTransaction(LocalSchemaTracking &tracking, std::unordered_set<PostProcessPOC> &post_process,
                          uint64_t commit_ts, bool property_on_edges) {
    auto lock = std::unique_lock{operation_ordering_mutex_};
    tracking_.ProcessTransaction(tracking, post_process, commit_ts, property_on_edges);
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
    TransactionalEdgeModifyingAccessor(LocalSchemaTracking &tracking, std::unordered_set<PostProcessPOC> *post_process,
                                       bool prop_on_edges, uint64_t commit_ts)
        : tracking_{&tracking},
          properties_on_edges_{prop_on_edges},
          commit_ts_{commit_ts},
          post_process_{post_process} {}

    void AddLabel(Vertex *vertex, LabelId label);

    void RemoveLabel(Vertex *vertex, LabelId label);

    void SetProperty(EdgeRef edge, EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property,
                     ExtendedPropertyType now, ExtendedPropertyType before);

   private:
    bool EdgeCreatedDuringThisTx(Edge *edge) const;

    bool EdgeCreatedDuringThisTx(Gid edge, Vertex *vertex) const;

    void UpdateTransactionalEdges(Vertex *vertex, const utils::small_vector<LabelId> &old_labels);

    LocalSchemaTracking *tracking_{};
    std::unique_lock<std::shared_mutex> ordering_lock_;  //!< Order guaranteeing lock
    bool properties_on_edges_{};                         //!< As defined by the storage configuration
    uint64_t commit_ts_{};
    std::unordered_set<PostProcessPOC> *post_process_{};
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

    void AddLabel(Vertex *vertex, LabelId label, std::unique_lock<utils::RWSpinLock> vertex_guard);

    void RemoveLabel(Vertex *vertex, LabelId label, std::unique_lock<utils::RWSpinLock> vertex_guard);

    void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property, ExtendedPropertyType now,
                     ExtendedPropertyType before);

   private:
    void UpdateAnalyticalEdges(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                               std::unique_lock<utils::RWSpinLock> &&vertex_lock);

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
    explicit VertexModifyingAccessor(LocalSchemaTracking &tracking, bool prop_on_edges)
        : tracking_{&tracking}, properties_on_edges_{prop_on_edges} {}

    void CreateVertex(Vertex *vertex);

    void DeleteVertex(Vertex *vertex);

    void AddLabel(Vertex *vertex, LabelId label);

    void RemoveLabel(Vertex *vertex, LabelId label);

    void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type);

    void DeleteEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type, EdgeRef edge);

    void SetProperty(Vertex *vertex, PropertyId property, ExtendedPropertyType now, ExtendedPropertyType before);

   private:
    SchemaTrackingInterface *tracking_{};
    std::shared_lock<std::shared_mutex> ordering_lock_;  //!< Order guaranteeing lock
    bool properties_on_edges_{};                         //!< As defined by the storage configuration
  };

  using ModifyingAccessor =
      std::variant<VertexModifyingAccessor, TransactionalEdgeModifyingAccessor, AnalyticalEdgeModifyingAccessor>;

  ModifyingAccessor CreateVertexModifyingAccessor(bool prop_on_edges) {
    return VertexModifyingAccessor{*this, prop_on_edges};
  }

  ModifyingAccessor CreateEdgeModifyingAccessor(bool prop_on_edges) {
    return AnalyticalEdgeModifyingAccessor{*this, prop_on_edges};
  }

  static ModifyingAccessor CreateVertexModifyingAccessor(auto &tracking, bool prop_on_edges) {
    return VertexModifyingAccessor{tracking, prop_on_edges};
  }
  static ModifyingAccessor CreateEdgeModifyingAccessor(auto &tracking, std::unordered_set<PostProcessPOC> *post_process,
                                                       bool prop_on_edges, uint64_t commit_ts) {
    return TransactionalEdgeModifyingAccessor{tracking, post_process, prop_on_edges, commit_ts};
  }

 private:
  friend VertexModifyingAccessor;
  friend TransactionalEdgeModifyingAccessor;
  friend AnalyticalEdgeModifyingAccessor;

  SharedSchemaTracking tracking_;                       //!< Tracking schema stats
  mutable std::shared_mutex operation_ordering_mutex_;  //!< Analytical operations ordering | Transactional RW control
};

}  // namespace memgraph::storage
