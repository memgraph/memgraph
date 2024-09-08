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
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/small_vector.hpp"

// TODO Add namespace schema_info
namespace memgraph::storage {

utils::small_vector<LabelId> GetCommittedLabels(const Vertex &vertex);

utils::small_vector<LabelId> GetLabels(const Vertex &vertex, uint64_t commit_timestamp);

struct PropertyInfo {
  int n{0};
  std::unordered_map<PropertyValue::Type, int> types;

  nlohmann::json ToJson(std::string_view key) const;
};
struct TrackingInfo {
  int n{0};
  std::unordered_map<PropertyId, PropertyInfo> properties;

  nlohmann::json ToJson(NameIdMapper &name_id_mapper) const;
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
  EdgeKey(EdgeTypeId id, VertexKey from, VertexKey to) : type{id}, from{std::move(from)}, to(std::move(to)) {}

  struct hash {
    using is_transparent = void;

    static inline size_t ElementHash(const auto &elem, size_t combined_hash, const auto &hash) {
      size_t element_hash = hash(elem);
      combined_hash ^= element_hash + 0x9e3779b9 + (combined_hash << 6) + (combined_hash >> 2);
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
    // Slow check
    return std::is_permutation(from.begin(), from.end(), other.from.begin(), other.from.end()) &&
           std::is_permutation(to.begin(), to.end(), other.to.begin(), other.to.end());
  }

  bool operator==(const EdgeKeyRef &other) const {
    // Fast check
    if (type != other.type || from.size() != other.from.size() || to.size() != other.to.size()) return false;
    // Slow check
    return std::is_permutation(from.begin(), from.end(), other.from.begin(), other.from.end()) &&
           std::is_permutation(to.begin(), to.end(), other.to.begin(), other.to.end());
  }
};

union EdgeKeyWrapper {
  const static struct RefTag {
  } ref_tag;
  const static struct KeyTag {
  } key_tag;

  EdgeKeyWrapper(RefTag /* tag */, EdgeTypeId id, const VertexKey &from, const VertexKey &to)
      : ref_{.key{id, from, to}} {}
  EdgeKeyWrapper(KeyTag /* tag */, EdgeTypeId id, VertexKey from, VertexKey to)
      : copy_{.key{id, std::move(from), std::move(to)}} {}

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

  void CleanUp();

  void Clear() {
    vertex_state_.clear();
    edge_state_.clear();
  }

  nlohmann::json ToJson(NameIdMapper &name_id_mapper);

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
  std::map<PropertyId, PropertyValueType> GetPrePropertyTypes(const EdgeRef &edge_ref, bool lock) const;

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
      const memgraph::storage::PropertyStore &properties,
      std::unordered_map<memgraph::storage::PropertyId,
                         std::pair<memgraph::storage::PropertyValueType, memgraph::storage::PropertyValueType>> &diff);

  auto GetPropertyDiff(const memgraph::storage::PropertyStore &properties, memgraph::storage::Delta *delta);

 private:
  Edge &edge_;
  uint64_t commit_timestamp_{-1UL};
  Tracking &tracking_;
  std::optional<EdgeKeyWrapper> edge_key_;
  bool needs_to_lock_{true};
};

struct SchemaInfo {
  void ProcessTransaction(Transaction &transaction, bool properties_on_edges) {
    // NOTE: commit_timestamp is actually the transaction ID, sa this is called before we finalize the commit
    const auto commit_timestamp = transaction.commit_timestamp->load(std::memory_order_acquire);

    // Edges have multiple objects that define their changes, this object keeps track of all the changes during a tx
    TransactionEdgeHandler tx_edge_handler{tracking_, commit_timestamp, properties_on_edges};

    // First run through all vertices
    for (const auto &delta : transaction.deltas) {
      // Find VERTEX; handle object's delta chain
      auto prev = delta.prev.Get();
      DMG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type == PreviousPtr::Type::VERTEX) {
        VertexHandler vertex_handler{*prev.vertex, tracking_, commit_timestamp};
        vertex_handler.Process(tx_edge_handler);
      }
    }

    // Handle any lingering transaction-wide changes
    tx_edge_handler.PostVertexProcess();

    // Handle edges only after vertices are handled
    // Here we will handle only edge property changes
    // Other edge changes are handled by vertex deltas
    if (properties_on_edges) {
      for (const auto &delta : transaction.deltas) {
        // Find EDGE; handle object's delta chain
        auto prev = delta.prev.Get();
        DMG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
        if (prev.type == PreviousPtr::Type::EDGE) {
          EdgeHandler edge_handler{*prev.edge, tracking_, commit_timestamp};
          edge_handler.Process();
        }
      }
    }
  }

  auto ToJson(NameIdMapper &name_id_mapper) { return tracking_.ToJson(name_id_mapper); }

  void Clear() { tracking_.Clear(); }

  class Accessor {
   public:
    explicit Accessor(SchemaInfo &si, bool prop_on_edges)
        : schema_info_{&si}, lock_{schema_info_->mtx_}, property_on_edges_(prop_on_edges) {}

    // Vertex
    void CreateVertex(Vertex *vertex) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      ++schema_info_->tracking_[vertex->labels].n;
    }

    void DeleteVertex(Vertex *vertex) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      --schema_info_->tracking_[vertex->labels].n;
      ClearProperties(vertex);
      // No edges should be present at this point
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
      SchemaInfo::UpdateLabel(*schema_info_, vertex, old_labels, vertex->labels);
    }

    void CreateEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type) {
      DMG_ASSERT(from->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      DMG_ASSERT(to->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // Empty edge; just update the top level stats
      auto &tracking_info = schema_info_->tracking_[EdgeKeyRef{edge_type, from->labels, to->labels}];
      ++tracking_info.n;
    }

    void DeleteEdge(Vertex *from, Vertex *to, EdgeTypeId edge_type, EdgeRef edge) {
      // Vertices changed by the tx ( no need to lock )
      auto &tracking_info = schema_info_->tracking_[EdgeKeyRef{edge_type, from->labels, to->labels}];
      --tracking_info.n;
      if (property_on_edges_) {
        for (const auto &[key, type] : edge.ptr->properties.PropertyTypes()) {
          auto &prop_info = tracking_info.properties[key];
          --prop_info.n;
          --prop_info.types[type];
        }
      }
    }

    void SetProperty(Vertex *vertex, PropertyId property, std::pair<PropertyValueType, PropertyValueType> diff) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      auto &tracking_info = schema_info_->tracking_[vertex->labels];
      if (diff.first != diff.second) {
        if (diff.second != PropertyValueType::Null) {
          auto &info = tracking_info.properties[property];
          --info.n;
          --info.types[diff.second];
        }
        if (diff.first != PropertyValueType::Null) {
          auto &info = tracking_info.properties[property];
          ++info.n;
          ++info.types[diff.first];
        }
      }
    }

    void InitProperties(Vertex *vertex, const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // We know this is the first time?
      // TODO Check that you cannot Init mutiple times
      auto &tracking_info = schema_info_->tracking_[vertex->labels];
      for (const auto &[key, val] : properties) {
        auto &info = tracking_info.properties[key];
        ++info.n;
        ++info.types[val.type()];
      }
    }

    void UpdateProperties(Vertex *vertex, std::map<PropertyId, std::pair<PropertyValueType, PropertyValueType>> &diff) {
      DMG_ASSERT(vertex->lock.is_loc.type() ked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      auto &tracking_info = schema_info_->tracking_[vertex->labels];
      for (const auto &[property, diff] : diff) {
        if (diff.first != diff.second) {
          if (diff.second != PropertyValueType::Null) {
            auto &info = tracking_info.properties[property];
            --info.n;
            --info.types[diff.second];
          }
          if (diff.first != PropertyValueType::Null) {
            auto &info = tracking_info.properties[property];
            ++info.n;
            ++info.types[diff.first];
          }
        }
      }
    }

    void ClearProperties(Vertex *vertex) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // TODO Are these the old values
      auto &tracking_info = schema_info_->tracking_[vertex->labels];
      for (const auto &[key, val] : vertex->properties.PropertyTypes()) {
        auto &info = tracking_info.properties[key];
        --info.n;
        --info.types[val];
      }
    }

   private:
    SchemaInfo *schema_info_;
    std::shared_lock<utils::RWSpinLock> lock_;
    bool property_on_edges_;
  };

  class UniqueAccessor {
   public:
    explicit UniqueAccessor(SchemaInfo &si, bool prop_on_edges)
        : schema_info_{&si}, lock_{schema_info_->mtx_}, properties_on_edges_{prop_on_edges} {}

    // Vertex
    // Calling this after change has been applied
    void AddLabel(Vertex *vertex, LabelId label, std::unique_lock<utils::RWSpinLock> &&vertex_guard) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      auto old_labels = vertex->labels;
      auto itr = std::find(old_labels.begin(), old_labels.end(), label);
      DMG_ASSERT(itr != old_labels.end(), "Trying to recreate labels pre commit, but label not found!");
      *itr = old_labels.back();
      old_labels.pop_back();
      // Update vertex stats
      SchemaInfo::UpdateLabel(*schema_info_, vertex, old_labels, vertex->labels);
      // TODO Currently this function is called even if there are no edges or properties are disabled; dont
      if (!properties_on_edges_) return;
      // Update edge stats
      // Need to loop though the IN/OUT edges and lock both vertices
      // Locking is done in order of GID
      // First loop through the ones that can lock
      for (const auto &edge : vertex->in_edges) {
        const auto edge_type = std::get<0>(edge);
        auto *from_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);
        auto from_guard = std::unique_lock{from_vertex->lock, std::defer_lock};
        if (vertex == from_vertex) {
          // Nothing to do, both ends are already locked
        } else if (vertex->gid < from_vertex->gid) {
          from_guard.lock();
        } else {
          continue;
        }
        // No need for edge lock since all edge property operations are unique access  <-
        // TODO This is not true; delete edge is shared access
        auto &old_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, from_vertex->labels, old_labels)];
        auto &new_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, from_vertex->labels, vertex->labels)];

        if (from_guard.owns_lock()) from_guard.unlock();

        --old_tracking.n;
        ++new_tracking.n;
        for (const auto &[property, type] : edge_ref.ptr->properties.PropertyTypes()) {
          auto &old_info = old_tracking.properties[property];
          --old_info.n;
          --old_info.types[type];
          auto &new_info = new_tracking.properties[property];
          ++new_info.n;
          ++new_info.types[type];
        }
      }
      for (const auto &edge : vertex->out_edges) {
        const auto edge_type = std::get<0>(edge);
        auto *to_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);
        auto to_guard = std::unique_lock{to_vertex->lock, std::defer_lock};
        if (vertex == to_vertex) {
          // Nothing to do, both ends are already locked
        } else if (vertex->gid < to_vertex->gid) {
          to_guard.lock();
        } else {
          continue;
        }
        // No need for edge lock since all edge property operations are unique access
        auto &old_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, old_labels, to_vertex->labels)];
        auto &new_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, vertex->labels, to_vertex->labels)];

        if (to_guard.owns_lock()) to_guard.unlock();

        --old_tracking.n;
        ++new_tracking.n;
        for (const auto &[property, type] : edge_ref.ptr->properties.PropertyTypes()) {
          auto &old_info = old_tracking.properties[property];
          --old_info.n;
          --old_info.types[type];
          auto &new_info = new_tracking.properties[property];
          ++new_info.n;
          ++new_info.types[type];
        }
      }
      // Unlock vertex and loop through the ones we couldn't lock
      vertex_guard.unlock();
      for (const auto &edge : vertex->in_edges) {
        const auto edge_type = std::get<0>(edge);
        auto *from_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);
        auto from_guard = std::unique_lock{from_vertex->lock, std::defer_lock};
        auto to_guard = std::unique_lock{vertex->lock, std::defer_lock};
        if (vertex->gid > from_vertex->gid) {
          from_guard.lock();
          to_guard.lock();
        } else {
          // Already handled
          continue;
        }
        // No need for edge lock since all edge property operations are unique access
        auto &old_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, from_vertex->labels, old_labels)];
        auto &new_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, from_vertex->labels, vertex->labels)];

        from_guard.unlock();
        to_guard.unlock();

        --old_tracking.n;
        ++new_tracking.n;
        for (const auto &[property, type] : edge_ref.ptr->properties.PropertyTypes()) {
          auto &old_info = old_tracking.properties[property];
          --old_info.n;
          --old_info.types[type];
          auto &new_info = new_tracking.properties[property];
          ++new_info.n;
          ++new_info.types[type];
        }
      }
      for (const auto &edge : vertex->out_edges) {
        const auto edge_type = std::get<0>(edge);
        auto *to_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);
        auto from_guard = std::unique_lock{vertex->lock, std::defer_lock};
        auto to_guard = std::unique_lock{to_vertex->lock, std::defer_lock};
        if (vertex->gid > to_vertex->gid) {
          to_guard.lock();
          from_guard.lock();
        } else {
          // Already handled
          continue;
        }
        // No need for edge lock since all edge property operations are unique access
        auto &old_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, old_labels, to_vertex->labels)];
        auto &new_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, vertex->labels, to_vertex->labels)];

        to_guard.unlock();
        from_guard.unlock();

        --old_tracking.n;
        ++new_tracking.n;
        for (const auto &[property, type] : edge_ref.ptr->properties.PropertyTypes()) {
          auto &old_info = old_tracking.properties[property];
          --old_info.n;
          --old_info.types[type];
          auto &new_info = new_tracking.properties[property];
          ++new_info.n;
          ++new_info.types[type];
        }
      }
      // TODO Keep track of which edges need to be redone
      // TODO Unify the code...
    }

    void RemoveLabel(Vertex *vertex, LabelId label, std::unique_lock<utils::RWSpinLock> &&vertex_guard) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // Move all stats and edges to new label
      auto old_labels = vertex->labels;
      old_labels.push_back(label);
      // Update vertex stats
      SchemaInfo::UpdateLabel(*schema_info_, vertex, old_labels, vertex->labels);
      // TODO Currently this function is called even if there are no edges or properties are disabled; dont
      if (!properties_on_edges_) return;
      // Update edge stats
      // Need to loop though the IN/OUT edges and lock both vertices
      // Locking is done in order of GID
      // First loop through the ones that can lock
      for (const auto &edge : vertex->in_edges) {
        const auto edge_type = std::get<0>(edge);
        auto *from_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);
        auto from_guard = std::unique_lock{from_vertex->lock, std::defer_lock};
        if (vertex == from_vertex) {
          // Nothing to do, both ends are already locked
        } else if (vertex->gid < from_vertex->gid) {
          from_guard.lock();
        } else {
          continue;
        }
        // No need for edge lock since all edge property operations are unique access
        auto &old_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, from_vertex->labels, old_labels)];
        auto &new_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, from_vertex->labels, vertex->labels)];

        if (from_guard.owns_lock()) from_guard.unlock();

        --old_tracking.n;
        ++new_tracking.n;
        for (const auto &[property, type] : edge_ref.ptr->properties.PropertyTypes()) {
          auto &old_info = old_tracking.properties[property];
          --old_info.n;
          --old_info.types[type];
          auto &new_info = new_tracking.properties[property];
          ++new_info.n;
          ++new_info.types[type];
        }
      }
      for (const auto &edge : vertex->out_edges) {
        const auto edge_type = std::get<0>(edge);
        auto *to_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);
        auto to_guard = std::unique_lock{to_vertex->lock, std::defer_lock};
        if (vertex == to_vertex) {
          // Nothing to do, both ends are already locked
        } else if (vertex->gid < to_vertex->gid) {
          to_guard.lock();
        } else {
          continue;
        }
        // No need for edge lock since all edge property operations are unique access
        auto &old_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, old_labels, to_vertex->labels)];
        auto &new_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, vertex->labels, to_vertex->labels)];

        if (to_guard.owns_lock()) to_guard.unlock();

        --old_tracking.n;
        ++new_tracking.n;
        for (const auto &[property, type] : edge_ref.ptr->properties.PropertyTypes()) {
          auto &old_info = old_tracking.properties[property];
          --old_info.n;
          --old_info.types[type];
          auto &new_info = new_tracking.properties[property];
          ++new_info.n;
          ++new_info.types[type];
        }
      }
      // Unlock vertex and loop through the ones we couldn't lock
      vertex_guard.unlock();
      for (const auto &edge : vertex->in_edges) {
        const auto edge_type = std::get<0>(edge);
        auto *from_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);
        auto from_guard = std::unique_lock{from_vertex->lock, std::defer_lock};
        auto to_guard = std::unique_lock{vertex->lock, std::defer_lock};
        if (vertex->gid > from_vertex->gid) {
          from_guard.lock();
          to_guard.lock();
        } else {
          // Already handled
          continue;
        }
        // No need for edge lock since all edge property operations are unique access
        auto &old_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, from_vertex->labels, old_labels)];
        auto &new_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, from_vertex->labels, vertex->labels)];

        from_guard.unlock();
        to_guard.unlock();

        --old_tracking.n;
        ++new_tracking.n;
        for (const auto &[property, type] : edge_ref.ptr->properties.PropertyTypes()) {
          auto &old_info = old_tracking.properties[property];
          --old_info.n;
          --old_info.types[type];
          auto &new_info = new_tracking.properties[property];
          ++new_info.n;
          ++new_info.types[type];
        }
      }
      for (const auto &edge : vertex->out_edges) {
        const auto edge_type = std::get<0>(edge);
        auto *to_vertex = std::get<1>(edge);
        const auto edge_ref = std::get<2>(edge);
        auto from_guard = std::unique_lock{vertex->lock, std::defer_lock};
        auto to_guard = std::unique_lock{to_vertex->lock, std::defer_lock};
        if (vertex->gid > to_vertex->gid) {
          to_guard.lock();
          from_guard.lock();
        } else {
          // Already handled
          continue;
        }
        // No need for edge lock since all edge property operations are unique access
        auto &old_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, old_labels, to_vertex->labels)];
        auto &new_tracking = schema_info_->tracking_[EdgeKeyRef(edge_type, vertex->labels, to_vertex->labels)];

        to_guard.unlock();
        from_guard.unlock();

        --old_tracking.n;
        ++new_tracking.n;
        for (const auto &[property, type] : edge_ref.ptr->properties.PropertyTypes()) {
          auto &old_info = old_tracking.properties[property];
          --old_info.n;
          --old_info.types[type];
          auto &new_info = new_tracking.properties[property];
          ++new_info.n;
          ++new_info.types[type];
        }
      }
      // TODO Keep track of which edges need to be redone
      // TODO Unify the code...
    }

    void UpdateLabel(Vertex *vertex, const utils::small_vector<LabelId> old_labels,
                     const utils::small_vector<LabelId> new_labels) {
      DMG_ASSERT(vertex->lock.is_locked(), "Trying to read from an unlocked vertex; LINE {}", __LINE__);
      // Update vertex stats
      SchemaInfo::UpdateLabel(*schema_info_, vertex, old_labels, new_labels);
      // Update edge stats
      // TODO...
    }

    // Edge
    void SetProperty(EdgeTypeId type, Vertex *from, Vertex *to, PropertyId property,
                     std::pair<PropertyValueType, PropertyValueType> diff) {
      HandlePropertyDiff(type, from, to, {{property, diff}});
    }

    void InitProperties(EdgeTypeId type, Vertex *from, Vertex *to, Edge *edge) {
      std::map<PropertyId, std::pair<PropertyValueType, PropertyValueType>> diff;
      for (const auto &[key, val] : edge->properties.PropertyTypes()) {
        diff[key] = std::make_pair(val, PropertyValueType::Null);
      }
      HandlePropertyDiff(type, from, to, diff);
    }

    void ClearProperties(EdgeTypeId type, Vertex *from, Vertex *to, Edge *edge) {
      std::map<PropertyId, std::pair<PropertyValueType, PropertyValueType>> diff;
      for (const auto &[key, val] : edge->properties.PropertyTypes()) {
        diff[key] = std::make_pair(PropertyValueType::Null, val);
      }
      HandlePropertyDiff(type, from, to, diff);
    }

    void HandlePropertyDiff(EdgeTypeId type, Vertex *from, Vertex *to,
                            const std::map<PropertyId, std::pair<PropertyValueType, PropertyValueType>> &diff) {
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

      for (const auto &[property, diff] : diff) {
        if (diff.first != diff.second) {
          if (diff.second != PropertyValueType::Null) {
            auto &info = tracking_info.properties[property];
            --info.n;
            --info.types[diff.second];
          }
          if (diff.first != PropertyValueType::Null) {
            auto &info = tracking_info.properties[property];
            ++info.n;
            ++info.types[diff.first];
          }
        }
      }
    }

    void MarkEdgeAsDeleted(Edge *edge) {}  // Nothing to do (handled via vertex)

   private:
    SchemaInfo *schema_info_;
    std::unique_lock<utils::RWSpinLock> lock_;
    bool properties_on_edges_;
  };

  Accessor CreateAccessor(bool prop_on_edges) { return Accessor{*this, prop_on_edges}; }
  UniqueAccessor CreateUniqueAccessor(bool prop_on_edges) { return UniqueAccessor{*this, prop_on_edges}; }

 private:
  friend Accessor;
  friend UniqueAccessor;

  static void UpdateLabel(SchemaInfo &schema_info, Vertex *vertex, const utils::small_vector<LabelId> old_labels,
                          const utils::small_vector<LabelId> new_labels) {
    // Move all stats and edges to new labels
    auto &old_tracking = schema_info.tracking_[old_labels];
    auto &new_tracking = schema_info.tracking_[new_labels];
    --old_tracking.n;
    ++new_tracking.n;
    for (const auto &[property, type] : vertex->properties.PropertyTypes()) {
      auto &old_info = old_tracking.properties[property];
      --old_info.n;
      --old_info.types[type];
      auto &new_info = new_tracking.properties[property];
      ++new_info.n;
      ++new_info.types[type];
    }
  }

  Tracking tracking_;
  mutable utils::RWSpinLock mtx_;
};

}  // namespace memgraph::storage
