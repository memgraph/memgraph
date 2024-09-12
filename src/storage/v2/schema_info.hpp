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
#include <boost/container_hash/hash_fwd.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/enum_store.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/small_vector.hpp"

#include <boost/thread/shared_lock_guard.hpp>
#include <boost/thread/shared_mutex.hpp>

// TODO Add namespace schema_info
namespace memgraph::storage {

utils::small_vector<LabelId> GetCommittedLabels(const Vertex &vertex);

utils::small_vector<LabelId> GetLabels(const Vertex &vertex, uint64_t commit_timestamp);

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
  int n{0};
  std::unordered_map<ExtendedPropertyType, int, PropertyTypeHash> types;

  nlohmann::json ToJson(const EnumStore &enum_store, std::string_view key) const;
};
struct TrackingInfo {
  int n{0};
  std::unordered_map<PropertyId, PropertyInfo> properties;

  nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) const;
};

using VertexKey = utils::small_vector<LabelId>;

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

struct VertexKeyEqualTo {
  size_t operator()(const utils::small_vector<LabelId> &lhs, const utils::small_vector<LabelId> &rhs) const {
    return lhs.size() == rhs.size() && std::is_permutation(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
  }
};

struct EdgeKeyRef {
  EdgeTypeId type;
  const VertexKey &from;
  const VertexKey &to;

  EdgeKeyRef(EdgeTypeId id, const VertexKey &from, const VertexKey &to) : type{id}, from{from}, to(to) {}
};

struct EdgeKey {
  EdgeTypeId type;
  VertexKey from;
  VertexKey to;

  EdgeKey() = default;
  // Do not pass by value, since move is expensive on vectors with only a few elements
  EdgeKey(EdgeTypeId id, const VertexKey &from, const VertexKey &to) : type{id}, from{from}, to(to) {}

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

  void ProcessTransaction(Transaction &transaction, bool properties_on_edges);

  void Clear() {
    vertex_state_.clear();
    edge_state_.clear();
  }

  nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store);

 private:
  void CleanUp();

  std::unordered_map<VertexKey, TrackingInfo, VertexKeyHash, VertexKeyEqualTo> vertex_state_;
  std::unordered_map<EdgeKey, TrackingInfo, EdgeKey::hash, EdgeKey::equal_to> edge_state_;
};

class TransactionEdgeHandler {
 public:
  explicit TransactionEdgeHandler(Tracking &tracking, uint64_t commit_timestamp, bool properties_on_edges)
      : tracking_{tracking}, commit_timestamp_{commit_timestamp}, properties_on_edges_{properties_on_edges} {}

  void RemoveEdge(const Delta *delta, EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex);

  void UpdateRemovedEdgeKey(EdgeRef edge, EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex);

  void AddEdge(const Delta *delta, EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex);

  void PostVertexProcess();

  void AppendToPostProcess(EdgeRef ref, EdgeTypeId type, Vertex *from, Vertex *to) {
    post_process_edges_.emplace(ref, type, from, to);
  }

 private:
  std::map<PropertyId, ExtendedPropertyType> GetPrePropertyTypes(const EdgeRef &edge_ref, bool lock) const;

  struct EdgeRefHash {
    size_t operator()(const EdgeRef &ref) const {
      // Both gid and ptr are the same size and unique; just pretend it's the gid
      return ref.gid.AsUint();
    }
  };

  struct EdgeTupleHash {
    size_t operator()(const std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *> &et) const {
      return std::get<0>(et).gid.AsUint();  // Ref should be enough (both gid and ptr are the same size and unique; just
                                            // pretend it's the gid)
    }
  };

  Tracking &tracking_;
  uint64_t commit_timestamp_{-1UL};
  std::unordered_map<EdgeRef, EdgeKeyWrapper, EdgeRefHash> remove_edges_;
  std::unordered_set<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>, EdgeTupleHash> post_process_edges_;
  bool properties_on_edges_{false};
};

class VertexHandler {
 public:
  explicit VertexHandler(Vertex &vertex, Tracking &tracking, uint64_t commit_timestamp)
      : vertex_{vertex}, tracking_{tracking}, commit_timestamp_{commit_timestamp} {}

  void AddVertex() {
    label_update_ = true;
    vertex_added_ = true;
  }

  void RemoveVertex() {
    // vertex already has delete flag
    label_update_ = true;
  }

  void UpdateLabel() { label_update_ = true; }

  void UpdateProperty(PropertyId key, const PropertyValue &value);

  const auto &PreLabels() const;

  // Current labels are the post labels, multiple changes from different transactions to the same vertex would
  // generate a serialization error
  const auto &PostLabels() const { return vertex_.labels; }

  auto GetPropertyDiff() const;

  void Process(TransactionEdgeHandler &edge_handler);

 private:
  void PostProcess(TransactionEdgeHandler &edge_handler);

  Vertex &vertex_;
  Tracking &tracking_;
  uint64_t commit_timestamp_{-1UL};

  mutable std::optional<decltype(vertex_.labels)> pre_labels_;
  mutable std::optional<std::map<PropertyId, PropertyValue::Type>> pre_properties_;

  bool label_update_{false};
  bool vertex_added_{false};
  bool update_property_{false};
};

class EdgeHandler {
 public:
  explicit EdgeHandler(Edge &edge, Tracking &tracking, uint64_t commit_timestamp)
      : edge_{edge}, commit_timestamp_{commit_timestamp}, tracking_{tracking} {}

  void Process();

  bool ExistedPreTx() const;

  auto PropertyType_ActionMethod(
      const PropertyStore &properties,
      std::unordered_map<PropertyId, std::pair<ExtendedPropertyType, ExtendedPropertyType>> &diff);

  auto GetPropertyDiff(const PropertyStore &properties, Delta *delta);

 private:
  Edge &edge_;
  uint64_t commit_timestamp_{-1UL};
  Tracking &tracking_;
  std::optional<EdgeKeyWrapper> edge_key_;
  bool needs_to_lock_{true};
};

struct SchemaInfo {
  //
  //
  // Snapshot recovery
  //
  //
  void RecoverVertex(Vertex *vertex) {
    // No locking, since this should only be used to recover data
    auto &info = tracking_[vertex->labels];
    ++info.n;
    for (const auto &[property, type] : vertex->properties.ExtendedPropertyTypes()) {
      auto &prop_info = info.properties[property];
      ++prop_info.n;
      ++prop_info.types[type];
    }
  }

  void RecoverEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges) {
    auto &tracking_info = tracking_[EdgeKeyRef{edge_type, from->labels, to->labels}];
    ++tracking_info.n;
    if (prop_on_edges) {
      for (const auto &[key, val] : edge.ptr->properties.ExtendedPropertyTypes()) {
        auto &prop_post_info = tracking_info.properties[key];
        ++prop_post_info.n;
        ++prop_post_info.types[val];
      }
    }
  }

  //
  //
  // WAL recovery
  //
  //
  void AddVertex(Vertex *vertex) { ++tracking_[vertex->labels].n; }

  void DeleteVertex(Vertex *vertex) {
    auto &info = tracking_[vertex->labels];
    --info.n;
    for (const auto &[key, val] : vertex->properties.ExtendedPropertyTypes()) {
      auto &prop_info = info.properties[key];
      --prop_info.n;
      --prop_info.types[val];
    }
    // No edges should be present at this point
  }

  void UpdateLabels(Vertex *vertex, const utils::small_vector<LabelId> &old_labels,
                    const utils::small_vector<LabelId> &new_labels, bool prop_on_edges) {
    // Update vertex stats
    SchemaInfo::UpdateLabel(*this, vertex, old_labels, new_labels);
    // Update edge stats
    auto update_edge = [&](EdgeTypeId edge_type, EdgeRef edge_ref, Vertex *from, Vertex *to,
                           const utils::small_vector<LabelId> *old_from_labels,
                           const utils::small_vector<LabelId> *old_to_labels) {
      auto &old_tracking = tracking_[EdgeKeyRef(edge_type, old_from_labels ? *old_from_labels : from->labels,
                                                old_to_labels ? *old_to_labels : to->labels)];
      auto &new_tracking = tracking_[EdgeKeyRef(edge_type, from->labels, to->labels)];
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
    };

    for (const auto &[edge_type, other_vertex, edge_ref] : vertex->in_edges) {
      update_edge(edge_type, edge_ref, other_vertex, vertex, {}, &old_labels);
    }
    for (const auto &[edge_type, other_vertex, edge_ref] : vertex->out_edges) {
      update_edge(edge_type, edge_ref, vertex, other_vertex, &old_labels, {});
    }
  }

  void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type) {
    auto &tracking_info = tracking_[EdgeKeyRef{edge_type, from->labels, to->labels}];
    ++tracking_info.n;
  }

  void DeleteEdge(EdgeTypeId edge_type, EdgeRef edge, Vertex *from, Vertex *to, bool prop_on_edges) {
    auto &tracking_info = tracking_[EdgeKeyRef{edge_type, from->labels, to->labels}];
    --tracking_info.n;
    if (prop_on_edges) {
      for (const auto &[key, type] : edge.ptr->properties.ExtendedPropertyTypes()) {
        auto &prop_info = tracking_info.properties[key];
        --prop_info.n;
        --prop_info.types[type];
      }
    }
  }

  void SetProperty(auto &tracking_info, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before) {
    if (now != before) {
      if (before != ExtendedPropertyType{PropertyValueType::Null}) {
        auto &info = tracking_info.properties[property];
        --info.n;
        --info.types[before];
      }
      if (now != ExtendedPropertyType{PropertyValueType::Null}) {
        auto &info = tracking_info.properties[property];
        ++info.n;
        ++info.types[now];
      }
    }
  }

  void SetProperty(Vertex *vertex, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before) {
    auto &tracking_info = tracking_[vertex->labels];
    SetProperty(tracking_info, property, now, before);
  }

  void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property, const ExtendedPropertyType &now,
                   const ExtendedPropertyType &before, bool prop_on_edges) {
    if (prop_on_edges) {
      auto &tracking_info = tracking_[EdgeKeyRef{type, from->labels, to->labels}];
      SetProperty(tracking_info, property, now, before);
    }
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

    void ProcessTransaction(Transaction &transaction, bool properties_on_edges) {
      schema_info_->tracking_.ProcessTransaction(transaction, properties_on_edges);
    }

   private:
    SchemaInfo *schema_info_;
    std::unique_lock<utils::RWSpinLock> lock_;
  };

  //
  //
  // ANALYTICAL IMPLEMENTATION
  //
  //
  class AnalyticalAccessor;
  class AnalyticalUniqueAccessor {
   public:
    explicit AnalyticalUniqueAccessor(SchemaInfo &si, bool prop_on_edges)
        : schema_info_{&si}, lock_{schema_info_->operation_ordering_mutex_}, properties_on_edges_{prop_on_edges} {}

    // Vertex
    // Calling this after change has been applied
    void AddLabel(Vertex *vertex, LabelId label, std::unique_lock<utils::RWSpinLock> vertex_guard) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      auto old_labels = vertex->labels;
      auto itr = std::find(old_labels.begin(), old_labels.end(), label);
      DMG_ASSERT(itr != old_labels.end(), "Trying to recreate labels pre commit, but label not found!");
      *itr = old_labels.back();
      old_labels.pop_back();
      // Update vertex stats
      SchemaInfo::UpdateLabel(*schema_info_, vertex, old_labels, vertex->labels);
      // Update edge stats
      UpdateEdges<true, true>(vertex, old_labels);
      UpdateEdges<false, true>(vertex, old_labels);
      // Unlock vertex and loop through the ones we couldn't lock
      vertex_guard.unlock();
      UpdateEdges<true, false>(vertex, old_labels);
      UpdateEdges<false, false>(vertex, old_labels);
    }

    void RemoveLabel(Vertex *vertex, LabelId label, std::unique_lock<utils::RWSpinLock> vertex_guard) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // Move all stats and edges to new label
      auto old_labels = vertex->labels;
      old_labels.push_back(label);
      // Update vertex stats
      SchemaInfo::UpdateLabel(*schema_info_, vertex, old_labels, vertex->labels);
      // Update edge stats
      UpdateEdges<true, true>(vertex, old_labels);
      UpdateEdges<false, true>(vertex, old_labels);
      // Unlock vertex and loop through the ones we couldn't lock
      vertex_guard.unlock();
      UpdateEdges<true, false>(vertex, old_labels);
      UpdateEdges<false, false>(vertex, old_labels);
    }

    // Edge
    void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property, ExtendedPropertyType now,
                     ExtendedPropertyType before) {
      auto from_lock = std::unique_lock{from->lock, std::defer_lock};
      auto to_lock = std::unique_lock{to->lock, std::defer_lock};

      if (from->gid == to->gid) {
        from_lock.lock();
      } else if (from->gid < to->gid) {
        from_lock.lock();
        to_lock.lock();
      } else {
        to_lock.lock();
        from_lock.lock();
      }

      auto &tracking_info = schema_info_->tracking_[EdgeKeyRef{type, from->labels, to->labels}];

      from_lock.unlock();
      if (to_lock.owns_lock()) to_lock.unlock();

      if (properties_on_edges_) {
        schema_info_->SetProperty(tracking_info, property, now, before);
      }
    }

    void MarkEdgeAsDeleted(Edge *edge) {}  // Nothing to do (handled via vertex)

   private:
    template <bool InEdges, bool VertexLocked>
    void UpdateEdges(Vertex *vertex, const utils::small_vector<LabelId> &old_labels) {
      // Update edge stats
      // Need to loop though the IN/OUT edges and lock both vertices
      // Locking is done in order of GID
      // First loop through the ones that can lock
      for (const auto &edge : (InEdges ? vertex->in_edges : vertex->out_edges)) {
        const auto edge_type = std::get<0>(edge);
        auto *other_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);

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

        auto &old_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, InEdges ? other_vertex->labels : old_labels,
                                                                InEdges ? old_labels : other_vertex->labels)];
        auto &new_tracking =
            schema_info_->tracking_[EdgeKeyRef(edge_type, InEdges ? other_vertex->labels : vertex->labels,
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

    SchemaInfo *schema_info_;
    boost::unique_lock<boost::shared_mutex> lock_;
    bool properties_on_edges_;

    friend AnalyticalAccessor;
  };

  // tracking_ only needs to be locked here.
  // When processing a transaction, each transaction can commit one at a time; we take a copy of tracking_, process it
  // and update the schema info. When copying, a unique lock is taken.
  // When in analytical mode, we have 2 accessors; UniqueAccessor (which guarantees only a single reader/writer) and
  // Accessor (with shared access). Since multiple operators can be calling the Accessor functions, we need to protect
  // tracking_.
  class AnalyticalAccessor {
   public:
    explicit AnalyticalAccessor(SchemaInfo &si, bool prop_on_edges)
        : schema_info_{&si}, lock_{schema_info_->operation_ordering_mutex_}, property_on_edges_(prop_on_edges) {}

    // Downgrade accessor
    explicit AnalyticalAccessor(AnalyticalUniqueAccessor &&unique)
        : schema_info_{unique.schema_info_},
          upgrade_lock_{std::move(unique.lock_)},
          property_on_edges_{unique.properties_on_edges_} {}

    // Vertex
    void CreateVertex(Vertex *vertex) {
      auto lock = std::unique_lock{schema_info_->mtx_};
      schema_info_->AddVertex(vertex);
    }

    void DeleteVertex(Vertex *vertex) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      auto lock = std::unique_lock{schema_info_->mtx_};
      schema_info_->DeleteVertex(vertex);
    }

    // Calling this after change has been applied
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
      auto lock = std::unique_lock{schema_info_->mtx_};
      SchemaInfo::UpdateLabel(*schema_info_, vertex, old_labels, vertex->labels);
    }

    void RemoveLabel(Vertex *vertex, LabelId label) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // For vertex with edges, this needs to be a unique access....
      DMG_ASSERT(vertex->in_edges.empty() && vertex->out_edges.empty(),
                 "Trying to remove label from vertex with edges; LINE {}", __LINE__);
      // Move all stats and edges to new label
      auto old_labels = vertex->labels;
      old_labels.push_back(label);
      auto lock = std::unique_lock{schema_info_->mtx_};
      SchemaInfo::UpdateLabel(*schema_info_, vertex, old_labels, vertex->labels);
    }

    void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type) {
      DMG_ASSERT(from->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      DMG_ASSERT(to->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // Empty edge; just update the top level stats
      auto lock = std::unique_lock{schema_info_->mtx_};
      schema_info_->CreateEdge(from, to, edge_type);
    }

    void DeleteEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type, EdgeRef edge) {
      // Vertices changed by the tx ( no need to lock )
      auto lock = std::unique_lock{schema_info_->mtx_};
      schema_info_->DeleteEdge(edge_type, edge, from, to, property_on_edges_);
    }

    void SetProperty(Vertex *vertex, PropertyId property, ExtendedPropertyType now, ExtendedPropertyType before) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      auto lock = std::unique_lock{schema_info_->mtx_};
      schema_info_->SetProperty(vertex, property, now, before);
    }

   private:
    SchemaInfo *schema_info_;
    boost::shared_lock<boost::shared_mutex> lock_;
    boost::upgrade_lock<boost::shared_mutex> upgrade_lock_;
    bool property_on_edges_;
  };

  ReadAccessor CreateReadAccessor() { return ReadAccessor{*this}; }
  WriteAccessor CreateWriteAccessor() { return WriteAccessor{*this}; }

  AnalyticalAccessor CreateAccessor(bool prop_on_edges) { return AnalyticalAccessor{*this, prop_on_edges}; }
  AnalyticalUniqueAccessor CreateUniqueAccessor(bool prop_on_edges) {
    return AnalyticalUniqueAccessor{*this, prop_on_edges};
  }

 private:
  friend ReadAccessor;
  friend WriteAccessor;
  friend AnalyticalAccessor;
  friend AnalyticalUniqueAccessor;

  static void UpdateLabel(SchemaInfo &schema_info, Vertex *vertex, const utils::small_vector<LabelId> old_labels,
                          const utils::small_vector<LabelId> new_labels) {
    // Move all stats and edges to new labels
    auto &old_tracking = schema_info.tracking_[old_labels];
    auto &new_tracking = schema_info.tracking_[new_labels];
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

  Tracking tracking_;
  mutable boost::shared_mutex operation_ordering_mutex_;
  mutable utils::RWSpinLock mtx_;
};

}  // namespace memgraph::storage
