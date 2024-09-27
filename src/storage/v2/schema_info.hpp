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

#include <algorithm>
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
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/small_vector.hpp"

#include <json/json.hpp>

namespace memgraph::storage {

template <template <class...> class TContainer = utils::ConcurrentUnorderedMap>
struct SchemaTracking {
  TrackingInfo<TContainer> &edge_lookup(const EdgeKeyRef &key);

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
  void Clear() {
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

  nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store);

  void RecoverVertex(Vertex *vertex);

  void RecoverEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges);

  void AddVertex(Vertex *vertex) { ++vertex_state_[vertex->labels].n; }

  void DeleteVertex(Vertex *vertex);

  void UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                    const utils::small_vector<LabelId> &new_labels);

  void UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                    const utils::small_vector<LabelId> &new_labels, bool prop_on_edges);

  void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type);

  void DeleteEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges);

  void SetProperty(auto &tracking_info, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before);

  void SetProperty(Vertex *vertex, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before) {
    auto &tracking_info = vertex_state_[vertex->labels];
    SetProperty(tracking_info, property, now, before);
  }

  void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before, bool prop_on_edges) {
    if (prop_on_edges) {
      auto &tracking_info = edge_lookup(EdgeKeyRef{type, from->labels, to->labels});
      SetProperty(tracking_info, property, now, before);
    }
  }

 private:
  friend struct SchemaTracking<std::unordered_map>;
  friend struct SchemaTracking<utils::ConcurrentUnorderedMap>;

  TContainer<VertexKey, TrackingInfo<TContainer>> vertex_state_;  //!< vertex statistics
  TContainer<EdgeKey, TrackingInfo<TContainer>> edge_state_;      //!< edge statistics
};

struct SchemaInfo {
  /**
   * @brief Generic tracking info read accessor
   */
  class ReadAccessor {
   public:
    explicit ReadAccessor(SchemaInfo &si, StorageMode mode)
        : schema_info_{&si},
          transactional_lock_{schema_info_->operation_ordering_mutex_, std::defer_lock},
          analytical_lock_{schema_info_->operation_ordering_mutex_, std::defer_lock} {
      if (mode == StorageMode::IN_MEMORY_ANALYTICAL) {
        analytical_lock_.lock();
      } else {
        transactional_lock_.lock();
      }
    }

    const auto &Get() const { return schema_info_->tracking_; }
    nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) {
      return schema_info_->tracking_.ToJson(name_id_mapper, enum_store);
    }

    size_t NumberOfVertices() const { return schema_info_->tracking_.NumberOfVertices(); }
    size_t NumberOfEdges() const { return schema_info_->tracking_.NumberOfEdges(); }

   private:
    SchemaInfo *schema_info_;
    std::shared_lock<std::shared_mutex> transactional_lock_;
    std::unique_lock<std::shared_mutex> analytical_lock_;
  };

  /**
   * @brief Generic tracking info write accessor
   */
  class WriteAccessor {
   public:
    explicit WriteAccessor(SchemaInfo &si) : schema_info_{&si}, lock_{schema_info_->operation_ordering_mutex_} {}

    auto &Get() { return schema_info_->tracking_; }
    void Clear() { schema_info_->tracking_.Clear(); }

    template <template <class...> class TContainer>
    void ProcessTransaction(SchemaTracking<TContainer> &tracking, std::unordered_set<PostProcessPOC> &post_process,
                            uint64_t commit_ts, bool property_on_edges) {
      schema_info_->tracking_.ProcessTransaction(tracking, post_process, commit_ts, property_on_edges);
    }

   private:
    SchemaInfo *schema_info_;
    std::unique_lock<std::shared_mutex> lock_;
  };

  class TransactionalEdgeModifyingAccessor {
   public:
    TransactionalEdgeModifyingAccessor(SchemaTracking<std::unordered_map> &tracking,
                                       std::unordered_set<PostProcessPOC> *post_process, bool prop_on_edges,
                                       uint64_t commit_ts)
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

    SchemaTracking<std::unordered_map> *tracking_{};
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

    SchemaTracking<> *tracking_{};
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
  template <template <class...> class TContainer>
  class VertexModifyingAccessor {
   public:
    // ANALYTICAL
    explicit VertexModifyingAccessor(SchemaInfo &si, bool prop_on_edges)
        : tracking_{&si.tracking_}, ordering_lock_{si.operation_ordering_mutex_}, properties_on_edges_(prop_on_edges) {}

    // TRANSACTIONAL
    explicit VertexModifyingAccessor(SchemaTracking<TContainer> &tracking, bool prop_on_edges)
        : tracking_{&tracking}, properties_on_edges_{prop_on_edges} {}

    // Vertex
    void CreateVertex(Vertex *vertex) { tracking_->AddVertex(vertex); }

    void DeleteVertex(Vertex *vertex) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      tracking_->DeleteVertex(vertex);
    }

    // Special case for vertex without any edges
    void AddLabel(Vertex *vertex, LabelId label) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // For vertex with edges, this needs to be a unique access....
      DMG_ASSERT(vertex->in_edges.empty() && vertex->out_edges.empty(),
                 "Trying to remove label from vertex with edges; LINE {}", __LINE__);
      // Move all stats and edges to new label
      auto old_labels = vertex->labels;
      auto itr = std::find(old_labels.begin(), old_labels.end(), label);
      DMG_ASSERT(itr != old_labels.end(), "Trying to recreate labels pre commit, but label not found!");
      *itr = old_labels.back();
      old_labels.pop_back();
      tracking_->UpdateLabels(vertex, old_labels, vertex->labels);
    }

    // Special case for vertex without any edges
    void RemoveLabel(Vertex *vertex, LabelId label) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // For vertex with edges, this needs to be a unique access....
      DMG_ASSERT(vertex->in_edges.empty() && vertex->out_edges.empty(),
                 "Trying to remove label from vertex with edges; LINE {}", __LINE__);
      // Move all stats and edges to new label
      auto old_labels = vertex->labels;
      old_labels.push_back(label);
      tracking_->UpdateLabels(vertex, old_labels, vertex->labels);
    }

    void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type) {
      DMG_ASSERT(from->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      DMG_ASSERT(to->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // Empty edge; just update the top level stats
      tracking_->CreateEdge(from, to, edge_type);
    }

    void DeleteEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type, EdgeRef edge) {
      // Vertices changed by the tx ( no need to lock )
      tracking_->DeleteEdge(edge_type, edge, from, to, properties_on_edges_);
    }

    void SetProperty(Vertex *vertex, PropertyId property, ExtendedPropertyType now, ExtendedPropertyType before) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      tracking_->SetProperty(vertex, property, now, before);
    }

   private:
    SchemaTracking<TContainer> *tracking_{};
    std::shared_lock<std::shared_mutex> ordering_lock_;  //!< Order guaranteeing lock
    bool properties_on_edges_{};                         //!< As defined by the storage configuration
  };

  ReadAccessor CreateReadAccessor(StorageMode mode) { return ReadAccessor{*this, mode}; }
  WriteAccessor CreateWriteAccessor() { return WriteAccessor{*this}; }

  using AnyAccessor =
      std::variant<VertexModifyingAccessor<utils::ConcurrentUnorderedMap>, VertexModifyingAccessor<std::unordered_map>,
                   TransactionalEdgeModifyingAccessor, AnalyticalEdgeModifyingAccessor>;

  AnyAccessor CreateVertexModifyingAccessor(bool prop_on_edges) {
    return VertexModifyingAccessor<utils::ConcurrentUnorderedMap>{*this, prop_on_edges};
  }

  AnyAccessor CreateEdgeModifyingAccessor(bool prop_on_edges) {
    return AnalyticalEdgeModifyingAccessor{*this, prop_on_edges};
  }

  static AnyAccessor CreateVertexModifyingAccessor(auto &tracking, bool prop_on_edges) {
    return VertexModifyingAccessor<std::unordered_map>{tracking, prop_on_edges};
  }
  static AnyAccessor CreateEdgeModifyingAccessor(auto &tracking, std::unordered_set<PostProcessPOC> *post_process,
                                                 bool prop_on_edges, uint64_t commit_ts) {
    return TransactionalEdgeModifyingAccessor{tracking, post_process, prop_on_edges, commit_ts};
  }

 private:
  friend ReadAccessor;
  friend WriteAccessor;
  friend VertexModifyingAccessor<std::unordered_map>;
  friend VertexModifyingAccessor<utils::ConcurrentUnorderedMap>;
  friend TransactionalEdgeModifyingAccessor;
  friend AnalyticalEdgeModifyingAccessor;

  SchemaTracking<> tracking_;                           //!< Tracking schema stats
  mutable std::shared_mutex operation_ordering_mutex_;  //!< Analytical operations ordering | Transactional RW control
};

}  // namespace memgraph::storage
