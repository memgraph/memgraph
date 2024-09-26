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

#include <algorithm>
#include <atomic>
#include <boost/container_hash/hash_fwd.hpp>
#include <cstdint>
#include <functional>
#include <json/json.hpp>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>

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

#include "json/json.hpp"

// TODO Add namespace schema_info
namespace memgraph::storage {

struct Transaction;

/**
 * @brief Get the last commited labels (the last ones processed by the SchemaInfo)
 *
 * @param vertex
 * @return utils::small_vector<LabelId>
 */
utils::small_vector<LabelId> GetCommittedLabels(const Vertex &vertex);

/**
 * @brief Get labels that need to be processed by this transaction (labels changed by the transaction or last commited).
 *
 * @param vertex
 * @param commit_timestamp
 * @return utils::small_vector<LabelId>
 */
utils::small_vector<LabelId> GetLabels(const Vertex &vertex, uint64_t commit_timestamp,
                                       uint64_t start_ts = (1ULL << 63U));

std::unordered_map<PropertyId, std::pair<ExtendedPropertyType, ExtendedPropertyType>> GetPropertyDiff(
    Edge *edge, uint64_t commit_ts);

std::map<PropertyId, ExtendedPropertyType> GetCommittedProperty(const Edge &edge);

struct PropertyTypeHash {
  size_t operator()(const ExtendedPropertyType &type) const {
    size_t seed = 0;
    boost::hash_combine(seed, type.type);
    boost::hash_combine(seed, type.temporal_type);
    boost::hash_combine(seed, type.enum_type.value_of());
    return seed;
  }
};

struct PropertyInfo {
  int n{0};  //!< Number of objects with this property
  std::unordered_map<ExtendedPropertyType, int, PropertyTypeHash>
      types;  //!< Numer of property instances with a specific type
  nlohmann::json ToJson(const EnumStore &enum_store, std::string_view key, uint32_t max_count) const;
};
struct TrackingInfo {
  int n{0};                                                 //!< Number of tracked objects
  std::unordered_map<PropertyId, PropertyInfo> properties;  //!< Property statistics defined by the tracked object

  nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) const;

  TrackingInfo &operator+=(const TrackingInfo &rhs) {
    n += rhs.n;
    for (const auto &[id, val] : rhs.properties) {
      auto &prop = properties[id];
      prop.n += val.n;
      for (const auto &[type, n] : val.types) {
        prop.types[type] += n;
      }
    }
    return *this;
  }
};

using VertexKey = utils::small_vector<LabelId>;

/**
 * @brief Hash the VertexKey (vector of labels) in an order independent way.
 */
struct VertexKeyHash {
  size_t operator()(const utils::small_vector<LabelId> &x) const {
    uint64_t hash = 0;
    for (const auto &element : x) {
      uint32_t val = element.AsUint();
      val = ((val >> 16) ^ val) * 0x45d9f3b;
      val = ((val >> 16) ^ val) * 0x45d9f3b;
      val = (val >> 16) ^ val;
      hash ^= val;
    }
    return hash;
  }
};

/**
 * @brief Equate VertexKeys (vector of labels) in an order independent way.
 */
struct VertexKeyEqualTo {
  size_t operator()(const utils::small_vector<LabelId> &lhs, const utils::small_vector<LabelId> &rhs) const {
    return lhs.size() == rhs.size() && std::is_permutation(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
  }
};

/**
 * @brief Allow lookup without vector copying.
 */
struct EdgeKeyRef {
  EdgeTypeId type;
  const VertexKey &from;
  const VertexKey &to;

  EdgeKeyRef(EdgeTypeId id, const VertexKey &from, const VertexKey &to) : type{id}, from{from}, to(to) {}
};

/**
 * @brief Uniquely identifies an edge via its type, from and to labels.
 */
struct EdgeKey {
  EdgeTypeId type;
  VertexKey from;
  VertexKey to;

  EdgeKey() = default;
  // Do not pass by value, since move is expensive on vectors with only a few elements
  EdgeKey(EdgeTypeId id, const VertexKey &from, const VertexKey &to) : type{id}, from{from}, to(to) {}

  /**
   * @brief Allow for heterogeneous lookup via EdgeKey and EdgeKeyRef
   */
  struct hash {
    using is_transparent = void;

    static inline size_t ElementHash(const auto &elem, size_t combined_hash, const auto &hash) {
      combined_hash ^= hash(elem) + 0x9e3779b9 + (combined_hash << 6U) + (combined_hash >> 2U);
      return combined_hash;
    }

    size_t operator()(const EdgeKey &et) const {
      size_t combined_hash = 0;
      combined_hash = ElementHash(et.type, combined_hash, std::hash<EdgeTypeId>{});
      combined_hash = ElementHash(et.from, combined_hash, VertexKeyHash{});
      combined_hash = ElementHash(et.to, combined_hash, VertexKeyHash{});
      return combined_hash;
    }

    size_t operator()(const EdgeKeyRef &et) const {
      size_t combined_hash = 0;
      combined_hash = ElementHash(et.type, combined_hash, std::hash<EdgeTypeId>{});
      combined_hash = ElementHash(et.from, combined_hash, VertexKeyHash{});
      combined_hash = ElementHash(et.to, combined_hash, VertexKeyHash{});
      return combined_hash;
    }
  };

  struct equal_to {
    using is_transparent = void;
    size_t operator()(const memgraph::storage::EdgeKey &lhs, const memgraph::storage::EdgeKey &rhs) const;
    size_t operator()(const memgraph::storage::EdgeKeyRef &lhs, const memgraph::storage::EdgeKey &rhs) const;
    size_t operator()(const memgraph::storage::EdgeKey &lhs, const memgraph::storage::EdgeKeyRef &rhs) const;
  };

  bool operator==(const EdgeKey &other) const {
    // Fast check
    if (type != other.type || from.size() != other.from.size() || to.size() != other.to.size()) return false;
    if (from.empty() && to.empty()) return true;
    // Slow check
    return std::is_permutation(from.begin(), from.end(), other.from.begin(), other.from.end()) &&
           std::is_permutation(to.begin(), to.end(), other.to.begin(), other.to.end());
  }

  bool operator==(const EdgeKeyRef &other) const {
    // Fast check
    if (type != other.type || from.size() != other.from.size() || to.size() != other.to.size()) return false;
    if (from.empty() && to.empty()) return true;
    // Slow check
    return std::is_permutation(from.begin(), from.end(), other.from.begin(), other.from.end()) &&
           std::is_permutation(to.begin(), to.end(), other.to.begin(), other.to.end());
  }
};

/**
 * @brief Union of EdgeKey and EdgeKeyRef, allows us to easily use only the best one.
 */
union EdgeKeyWrapper {
  struct RefTag {};
  struct CopyTag {};

  EdgeKeyWrapper(RefTag /* tag */, EdgeTypeId id, const VertexKey &from, const VertexKey &to)
      : ref_{.key{id, from, to}} {}
  EdgeKeyWrapper(CopyTag /* tag */, EdgeTypeId id, const VertexKey &from, const VertexKey &to)
      : copy_{.key{id, from, to}} {}

  bool is_ref = false;
  struct {
    bool is_ref = true;
    EdgeKeyRef key;
  } ref_;
  struct {
    bool is_ref = false;
    EdgeKey key;
  } copy_;

  EdgeKeyWrapper() = delete;
  EdgeKeyWrapper(const EdgeKeyWrapper &) = delete;
  EdgeKeyWrapper(EdgeKeyWrapper &&) = delete;
  EdgeKeyWrapper &operator=(const EdgeKeyWrapper &) = delete;
  EdgeKeyWrapper &operator=(EdgeKeyWrapper &&) = delete;

  ~EdgeKeyWrapper() {
    if (is_ref) {
      std::destroy_at(&ref_.key);
    } else {
      std::destroy_at(&copy_.key);
    }
  }
};

struct Tracking {
  /**
   * @brief Process transaction (deltas) in order to update the schema.
   *
   * @param transaction
   * @param properties_on_edges
   */
  // void ProcessTransaction(Transaction &transaction, bool properties_on_edges);

  void ProcessTransaction(const Tracking &diff, std::unordered_set<PostProcessPOC> &post_process, uint64_t commit_ts,
                          bool property_on_edges);

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

  void AddVertex(Vertex *vertex) { ++(*this)[vertex->labels].n; }

  void DeleteVertex(Vertex *vertex);

  void UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                    const utils::small_vector<LabelId> &new_labels, bool prop_on_edges);

  void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type);

  void DeleteEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges);

  void SetProperty(auto &tracking_info, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before);

  void SetProperty(Vertex *vertex, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before) {
    auto &tracking_info = (*this)[vertex->labels];
    SetProperty(tracking_info, property, now, before);
  }

  void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before, bool prop_on_edges) {
    if (prop_on_edges) {
      auto &tracking_info = (*this)[EdgeKeyRef{type, from->labels, to->labels}];
      SetProperty(tracking_info, property, now, before);
    }
  }

  const auto &vertex_state() const { return vertex_state_; }
  const auto &edge_state() const { return edge_state_; }

  TrackingInfo &operator[](const VertexKey &key) { return vertex_state_[key]; }

  TrackingInfo &operator[](const EdgeKey &key) { return edge_state_[key]; }

  TrackingInfo &operator[](const EdgeKeyRef &key) {
    auto itr = edge_state_.find(key);
    if (itr == edge_state_.end()) {
      auto [new_itr, _] =
          edge_state_.emplace(std::piecewise_construct, std::make_tuple(key.type, key.from, key.to), std::make_tuple());
      return new_itr->second;
    }
    return itr->second;
  }

  TrackingInfo &operator[](const EdgeKeyWrapper &key) {
    if (key.is_ref) {
      return (*this)[key.ref_.key];
    }
    return (*this)[key.copy_.key];
  }

 private:
  /**
   * @brief Clean up the statistics. We try to not change the underlying maps and leave that for the CleanUp.
   */
  void CleanUp();

  std::unordered_map<VertexKey, TrackingInfo, VertexKeyHash, VertexKeyEqualTo> vertex_state_;  //!< vertex statistics
  std::unordered_map<EdgeKey, TrackingInfo, EdgeKey::hash, EdgeKey::equal_to> edge_state_;     //!< edge statistics
};

struct SchemaInfo {
  void RecoverVertex(Vertex *vertex);

  void RecoverEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges);

  //
  //
  // Direct access
  //
  //
  void AddVertex(Vertex *vertex) { tracking_.AddVertex(vertex); }

  void DeleteVertex(Vertex *vertex) { tracking_.DeleteVertex(vertex); }

  void UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                    const utils::small_vector<LabelId> &new_labels, bool prop_on_edges) {
    tracking_.UpdateLabels(vertex, old_labels, new_labels, prop_on_edges);
  }

  void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type) { tracking_.CreateEdge(from, to, edge_type); }

  void DeleteEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges) {
    tracking_.DeleteEdge(edge_type, edge, from, to, prop_on_edges);
  }

  void SetProperty(auto &tracking_info, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before) {
    tracking_.SetProperty(tracking_info, property, now, before);
  }

  void SetProperty(Vertex *vertex, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before) {
    tracking_.SetProperty(vertex, property, now, before);
  }

  void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before, bool prop_on_edges) {
    tracking_.SetProperty(type, from, to, property, now, before, prop_on_edges);
  }

  //
  //
  // TRANSACTIONAL IMPLEMENTATION
  //
  //
  class ReadAccessor {
   public:
    explicit ReadAccessor(SchemaInfo &si) : schema_info_{&si}, lock_{schema_info_->mtx_} {}

    const Tracking &Get() const { return schema_info_->tracking_; }
    nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) {
      return schema_info_->tracking_.ToJson(name_id_mapper, enum_store);
    }

    size_t NumberOfVertices() const { return schema_info_->tracking_.NumberOfVertices(); }
    size_t NumberOfEdges() const { return schema_info_->tracking_.NumberOfEdges(); }

   private:
    SchemaInfo *schema_info_;
    std::shared_lock<utils::RWSpinLock> lock_;
  };

  class WriteAccessor {
   public:
    explicit WriteAccessor(SchemaInfo &si) : schema_info_{&si}, lock_{schema_info_->mtx_} {}

    void Clear() { schema_info_->tracking_.Clear(); }
    Tracking Move() { return std::move(schema_info_->tracking_); }
    void Set(Tracking tracking) { schema_info_->tracking_ = std::move(tracking); }

    void ProcessTransaction(Tracking &tracking, std::unordered_set<PostProcessPOC> &post_process, uint64_t commit_ts,
                            bool property_on_edges) {
      schema_info_->tracking_.ProcessTransaction(tracking, post_process, commit_ts, property_on_edges);
    }

   private:
    SchemaInfo *schema_info_;
    std::unique_lock<utils::RWSpinLock> lock_;
  };

  class TransactionalEdgeModifyingAccessor {
   public:
    TransactionalEdgeModifyingAccessor(Tracking &tracking, std::unordered_set<PostProcessPOC> *post_process,
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
    bool EdgeCreatedDuringThisTx(EdgeRef edge) const {
      bool created_this_tx = false;
      auto *delta = edge.ptr->delta;
      while (delta) {
        const auto ts = delta->timestamp->load(std::memory_order_acquire);
        if (ts != commit_ts_) break;
        if (delta->action == Delta::Action::DELETE_OBJECT ||
            delta->action == Delta::Action::DELETE_DESERIALIZED_OBJECT) {
          created_this_tx = true;
          break;
        }
        delta = delta->next.load(std::memory_order_acquire);
      }
      return created_this_tx;
    }

    bool BothModifiedDuringThisTx(Vertex *from, Vertex *to) const {
      auto modified_during_this_tx = [commit_ts = commit_ts_](Vertex *v) {
        return v->delta && v->delta->timestamp->load(std::memory_order::acquire) == commit_ts;
      };
      return modified_during_this_tx(from) && modified_during_this_tx(to);
    }

    template <bool InEdges>
    void UpdateEdges(Vertex *vertex, const utils::small_vector<LabelId> &old_labels) {
      // Update edge stats
      // Need to loop though the IN/OUT edges and lock both vertices
      // Locking is done in order of GID
      // Optimization: one vertex will already be locked; first loop through all edges that can lock; then unlock the
      // vertex and loop through the rest
      for (const auto &edge : (InEdges ? vertex->in_edges : vertex->out_edges)) {
        const auto [edge_type, other_vertex, edge_ref] = edge;

        bool process_in_place = false;
        if (properties_on_edges_)
          process_in_place = EdgeCreatedDuringThisTx(edge_ref);
        else
          process_in_place = BothModifiedDuringThisTx(vertex, other_vertex);

        if (process_in_place) {
          auto old_tracking = (*tracking_)[EdgeKeyRef(edge_type, InEdges ? other_vertex->labels : old_labels,
                                                      InEdges ? old_labels : other_vertex->labels)];
          auto new_tracking = (*tracking_)[EdgeKeyRef(edge_type, InEdges ? other_vertex->labels : vertex->labels,
                                                      InEdges ? vertex->labels : other_vertex->labels)];

          --old_tracking.n;
          ++new_tracking.n;

          if (properties_on_edges_) {
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
        } else {  // Post process
          post_process_->emplace(edge_ref, edge_type, InEdges ? other_vertex : vertex, InEdges ? vertex : other_vertex);
        }
      }
    }

    Tracking *tracking_{};
    std::unique_lock<std::shared_mutex> ordering_lock_;  //!< Order guaranteeing lock
    bool properties_on_edges_{};                         //!< As defined by the storage configuration
    uint64_t commit_ts_{};
    std::unordered_set<PostProcessPOC> *post_process_{};
  };

  //
  //
  // ANALYTICAL IMPLEMENTATION
  //
  //
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
        : tracking_{&si.tracking_},
          tracking_mtx_{&si.mtx_},
          ordering_lock_{si.operation_ordering_mutex_},
          properties_on_edges_{prop_on_edges} {}

    void AddLabel(Vertex *vertex, LabelId label, std::unique_lock<utils::RWSpinLock> vertex_guard);

    void RemoveLabel(Vertex *vertex, LabelId label, std::unique_lock<utils::RWSpinLock> vertex_guard);

    void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property, ExtendedPropertyType now,
                     ExtendedPropertyType before);

   private:
    template <bool InEdges, bool VertexLocked>
    void UpdateEdges(Vertex *vertex, const utils::small_vector<LabelId> &old_labels) {
      // Update edge stats
      // Need to loop though the IN/OUT edges and lock both vertices
      // Locking is done in order of GID
      // Optimization: one vertex will already be locked; first loop through all edges that can lock; then unlock the
      // vertex and loop through the rest
      for (const auto &edge : (InEdges ? vertex->in_edges : vertex->out_edges)) {
        const auto [edge_type, other_vertex, edge_ref] = edge;

        auto guard = std::unique_lock{vertex->lock, std::defer_lock};
        auto other_guard = std::unique_lock{other_vertex->lock, std::defer_lock};

        if constexpr (VertexLocked) {
          if (vertex == other_vertex) {
            // Nothing to do, both ends are already locked
          } else if (vertex->gid < other_vertex->gid) {
            other_guard.lock();
          } else {
            // Since the vertex is already locked, nothing we can do now; recheck when unlocked
            continue;
          }
        } else {
          if (vertex->gid > other_vertex->gid) {
            other_guard.lock();
            guard.lock();
          } else {
            // Already handled
            continue;
          }
        }

        auto lock = std::invoke([&]() -> std::optional<std::unique_lock<utils::RWSpinLock>> {
          if (tracking_mtx_) return std::unique_lock{*tracking_mtx_};
          return {};
        });
        auto &old_tracking = (*tracking_)[EdgeKeyRef(edge_type, InEdges ? other_vertex->labels : old_labels,
                                                     InEdges ? old_labels : other_vertex->labels)];
        auto &new_tracking = (*tracking_)[EdgeKeyRef(edge_type, InEdges ? other_vertex->labels : vertex->labels,
                                                     InEdges ? vertex->labels : other_vertex->labels)];

        if (guard.owns_lock()) guard.unlock();
        if (other_guard.owns_lock()) other_guard.unlock();

        --old_tracking.n;
        ++new_tracking.n;

        if (properties_on_edges_) {
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
    }

    Tracking *tracking_{};
    mutable utils::RWSpinLock *tracking_mtx_{};          // TODO Remove
    std::unique_lock<std::shared_mutex> ordering_lock_;  //!< Order guaranteeing lock
    bool properties_on_edges_{};                         //!< As defined by the storage configuration
  };

  using EdgeModifyingAccessor = std::variant<TransactionalEdgeModifyingAccessor, AnalyticalEdgeModifyingAccessor>;

  /**
   * @brief We need to force ordering for analytical. This is because an edge is defined via 3 independent objects
   * (from/to vertex and edge object). We need to process each edge or vertex label change in order. Otherwise there
   * is no way to keep track of how the schema is being modified.
   *
   * Use unique accessor when edge modification is needed; shared otherwise. (when not all objects are locked)
   */
  class VertexModifyingAccessor {
   public:
    explicit VertexModifyingAccessor(SchemaInfo &si, StorageMode mode, bool prop_on_edges)
        : tracking_{&si.tracking_},
          tracking_mtx_{&si.mtx_},
          ordering_lock_{si.operation_ordering_mutex_, std::defer_lock},
          properties_on_edges_(prop_on_edges) {
      if (mode == StorageMode::IN_MEMORY_ANALYTICAL) ordering_lock_.lock();
    }

    explicit VertexModifyingAccessor(Tracking &tracking, bool prop_on_edges)
        : tracking_{&tracking}, properties_on_edges_{prop_on_edges} {}

    // Vertex
    void CreateVertex(Vertex *vertex) {
      auto lock = std::invoke([&]() -> std::optional<std::unique_lock<utils::RWSpinLock>> {
        if (tracking_mtx_) return std::unique_lock{*tracking_mtx_};
        return {};
      });
      tracking_->AddVertex(vertex);
    }

    void DeleteVertex(Vertex *vertex) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      auto lock = std::invoke([&]() -> std::optional<std::unique_lock<utils::RWSpinLock>> {
        if (tracking_mtx_) return std::unique_lock{*tracking_mtx_};
        return {};
      });
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
      auto lock = std::invoke([&]() -> std::optional<std::unique_lock<utils::RWSpinLock>> {
        if (tracking_mtx_) return std::unique_lock{*tracking_mtx_};
        return {};
      });
      SchemaInfo::UpdateLabel(*tracking_, vertex, old_labels, vertex->labels);
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
      auto lock = std::invoke([&]() -> std::optional<std::unique_lock<utils::RWSpinLock>> {
        if (tracking_mtx_) return std::unique_lock{*tracking_mtx_};
        return {};
      });
      SchemaInfo::UpdateLabel(*tracking_, vertex, old_labels, vertex->labels);
    }

    void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type) {
      DMG_ASSERT(from->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      DMG_ASSERT(to->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // Empty edge; just update the top level stats
      auto lock = std::invoke([&]() -> std::optional<std::unique_lock<utils::RWSpinLock>> {
        if (tracking_mtx_) return std::unique_lock{*tracking_mtx_};
        return {};
      });
      tracking_->CreateEdge(from, to, edge_type);
    }

    void DeleteEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type, EdgeRef edge) {
      // Vertices changed by the tx ( no need to lock )
      auto lock = std::invoke([&]() -> std::optional<std::unique_lock<utils::RWSpinLock>> {
        if (tracking_mtx_) return std::unique_lock{*tracking_mtx_};
        return {};
      });
      tracking_->DeleteEdge(edge_type, edge, from, to, properties_on_edges_);
    }

    void SetProperty(Vertex *vertex, PropertyId property, ExtendedPropertyType now, ExtendedPropertyType before) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      auto lock = std::invoke([&]() -> std::optional<std::unique_lock<utils::RWSpinLock>> {
        if (tracking_mtx_) return std::unique_lock{*tracking_mtx_};
        return {};
      });
      tracking_->SetProperty(vertex, property, now, before);
    }

   private:
    Tracking *tracking_{};
    mutable utils::RWSpinLock *tracking_mtx_{};          // TODO Remove
    std::shared_lock<std::shared_mutex> ordering_lock_;  //!< Order guaranteeing lock
    bool properties_on_edges_{};                         //!< As defined by the storage configuration
  };

  ReadAccessor CreateReadAccessor() { return ReadAccessor{*this}; }
  WriteAccessor CreateWriteAccessor() { return WriteAccessor{*this}; }

  VertexModifyingAccessor CreateVertexModifyingAccessor(StorageMode mode, bool prop_on_edges) {
    return VertexModifyingAccessor{*this, mode, prop_on_edges};
  }
  EdgeModifyingAccessor CreateEdgeModifyingAccessor(bool prop_on_edges) {
    return AnalyticalEdgeModifyingAccessor{*this, prop_on_edges};
  }

  static VertexModifyingAccessor CreateVertexModifyingAccessor(Tracking &tracking, bool prop_on_edges) {
    return VertexModifyingAccessor{tracking, prop_on_edges};
  }
  static EdgeModifyingAccessor CreateEdgeModifyingAccessor(Tracking &tracking,
                                                           std::unordered_set<PostProcessPOC> *post_process,
                                                           bool prop_on_edges, uint64_t commit_ts) {
    return TransactionalEdgeModifyingAccessor{tracking, post_process, prop_on_edges, commit_ts};
  }

 private:
  friend ReadAccessor;
  friend WriteAccessor;
  friend VertexModifyingAccessor;
  friend EdgeModifyingAccessor;

  static void UpdateLabel(Tracking &tracking, Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                          const utils::small_vector<LabelId> &new_labels) {
    // Move all stats and edges to new labels
    auto &old_tracking = tracking[old_labels];
    auto &new_tracking = tracking[new_labels];
    --old_tracking.n;
    ++new_tracking.n;
    for (const auto &[property, type] : vertex->properties.ExtendedPropertyTypes()) {
      auto &old_info = old_tracking.properties[property];
      --old_info.n;
      --old_info.types[type];
      auto &new_info = new_tracking.properties[property];
      ++new_info.n;
      ++new_info.types[type];
    }
  }

  Tracking tracking_;                                   //!< Tracking schema stats
  mutable std::shared_mutex operation_ordering_mutex_;  //!< Analytical operations ordering
  // TODO: Use thread-safe structure
  mutable utils::RWSpinLock mtx_;  //!< Underlying schema data protection
};

}  // namespace memgraph::storage
