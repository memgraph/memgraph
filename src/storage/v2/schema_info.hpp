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
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

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
#include "utils/small_vector.hpp"

namespace memgraph::storage {
struct Tracking;
struct EdgeType;
struct EdgeTypeRef;
}  // namespace memgraph::storage

namespace std {
template <>
struct hash<memgraph::utils::small_vector<memgraph::storage::LabelId>> {
  size_t operator()(const memgraph::utils::small_vector<memgraph::storage::LabelId> &x) const {
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

template <>
struct equal_to<memgraph::utils::small_vector<memgraph::storage::LabelId>> {
  size_t operator()(const memgraph::utils::small_vector<memgraph::storage::LabelId> &lhs,
                    const memgraph::utils::small_vector<memgraph::storage::LabelId> &rhs) const {
    return lhs.size() == rhs.size() && std::is_permutation(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
  }
};

template <>
struct equal_to<memgraph::storage::EdgeType> {
  using is_transparent = void;
  size_t operator()(const memgraph::storage::EdgeType &lhs, const memgraph::storage::EdgeType &rhs) const;
  size_t operator()(const memgraph::storage::EdgeTypeRef &lhs, const memgraph::storage::EdgeType &rhs) const;
  size_t operator()(const memgraph::storage::EdgeType &lhs, const memgraph::storage::EdgeTypeRef &rhs) const;
  size_t operator()(const memgraph::storage::EdgeTypeRef &lhs, const memgraph::storage::EdgeTypeRef &rhs) const;
};

}  // namespace std

// TODO Add namespace schema_info
namespace memgraph::storage {

utils::small_vector<LabelId> GetCommittedLabels(const Vertex &vertex);

utils::small_vector<LabelId> GetLabels(const Vertex &vertex, uint64_t commit_timestamp);

struct PropertyInfo {
  int n{0};
  std::unordered_map<PropertyValue::Type, int> types;
};
struct TrackingInfo {
  int n{0};
  std::unordered_map<PropertyId, PropertyInfo> properties;
};

using v_key_type = utils::small_vector<LabelId>;

struct EdgeType;

struct EdgeTypeRef {
  EdgeTypeId type;
  v_key_type &from_v_type;
  v_key_type &to_v_type;

  EdgeTypeRef(EdgeTypeId id, v_key_type &from, v_key_type &to) : type{id}, from_v_type{from}, to_v_type(to) {}

  inline bool operator==(const EdgeType &type) const;
};

struct EdgeType {
  EdgeTypeId type;
  v_key_type from_v_type;
  v_key_type to_v_type;

  EdgeType(EdgeTypeId id, v_key_type from, v_key_type to)
      : type{id}, from_v_type{std::move(from)}, to_v_type(std::move(to)) {}

  EdgeType() = default;
  EdgeType(const EdgeType &) = default;
  EdgeType(EdgeType &&) noexcept = default;
  EdgeType &operator=(const EdgeType &) = default;
  EdgeType &operator=(EdgeType &&) noexcept = default;

  struct hasher {
    using is_transparent = void;

    size_t operator()(const EdgeType &et) const {
      size_t combined_hash = 0;
      auto element_hash = [&combined_hash](const auto &elem) {
        size_t element_hash = std::hash<std::decay_t<decltype(elem)>>{}(elem);
        combined_hash ^= element_hash + 0x9e3779b9 + (combined_hash << 6) + (combined_hash >> 2);
      };

      element_hash(et.type);
      element_hash(et.from_v_type);
      element_hash(et.to_v_type);
      return combined_hash;
    }

    size_t operator()(const EdgeTypeRef &et) const {
      size_t combined_hash = 0;
      auto element_hash = [&combined_hash](const auto &elem) {
        size_t element_hash = std::hash<std::decay_t<decltype(elem)>>{}(elem);
        combined_hash ^= element_hash + 0x9e3779b9 + (combined_hash << 6) + (combined_hash >> 2);
      };

      element_hash(et.type);
      element_hash(et.from_v_type);
      element_hash(et.to_v_type);
      return combined_hash;
    }
  };

  bool operator==(const EdgeType &other) const {
    // Fast check
    if (type != other.type || from_v_type.size() != other.from_v_type.size() ||
        to_v_type.size() != other.to_v_type.size())
      return false;
    // Slow check
    return std::is_permutation(from_v_type.begin(), from_v_type.end(), other.from_v_type.begin(),
                               other.from_v_type.end()) &&
           std::is_permutation(to_v_type.begin(), to_v_type.end(), other.to_v_type.begin(), other.to_v_type.end());
  }

  bool operator==(const EdgeTypeRef &other) const {
    // Fast check
    if (type != other.type || from_v_type.size() != other.from_v_type.size() ||
        to_v_type.size() != other.to_v_type.size())
      return false;
    // Slow check
    return std::is_permutation(from_v_type.begin(), from_v_type.end(), other.from_v_type.begin(),
                               other.from_v_type.end()) &&
           std::is_permutation(to_v_type.begin(), to_v_type.end(), other.to_v_type.begin(), other.to_v_type.end());
  }
};

struct Tracking {
  // We want the key to be independent of the vector order.
  // Could create a hash and equals function that is independent
  // Difficult to make them performant and low collision
  // Stick to sorting keys for now
  TrackingInfo &operator[](const v_key_type &key) { return vertex_state_[key]; }

  TrackingInfo &operator[](const EdgeType &key) { return edge_state_[key]; }

  TrackingInfo &operator[](const EdgeTypeRef &key) {
    auto itr = edge_state_.find(key);
    if (itr == edge_state_.end()) {
      auto [new_itr, _] = edge_state_.emplace(EdgeType{key.type, key.from_v_type, key.to_v_type}, TrackingInfo{});
      return new_itr->second;
    }
    return itr->second;
  }

  void CleanUp();

  void Print(NameIdMapper &name_id_mapper, bool properties_on_edges);

  nlohmann::json ToJson(NameIdMapper &name_id_mapper, bool properties_on_edges);

  std::unordered_map<v_key_type, TrackingInfo> vertex_state_;
  std::unordered_map<EdgeType, TrackingInfo, EdgeType::hasher, std::equal_to<EdgeType>> edge_state_;
};

inline bool EdgeTypeRef::operator==(const EdgeType &type) const { return type == *this; }

class TransactionEdgeHandler {
 public:
  explicit TransactionEdgeHandler(Tracking &tracking, uint64_t commit_timestamp, bool properties_on_edges)
      : tracking_{tracking}, commit_timestamp_{commit_timestamp}, properties_on_edges_{properties_on_edges} {}

  void RemoveEdge(const Delta *delta, EdgeTypeId edge_type, const Vertex *from_vertex, const Vertex *to_vertex);

  void AddEdge(const Delta *delta, EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex);

  void PostVertexProcess();

  void AppendToPostProcess(EdgeRef ref, EdgeTypeId type, Vertex *from, Vertex *to) {
    post_process_edges_.emplace(ref, type, from, to);
  }

 private:
  std::map<PropertyId, PropertyValueType> GetPrePropertyTypes(const EdgeRef &edge_ref) const;
  std::map<PropertyId, PropertyValueType> GetPostPropertyTypes(const EdgeRef &edge_ref) const;

  Tracking &tracking_;
  uint64_t commit_timestamp_{-1UL};
  bool properties_on_edges_{false};

  struct EdgeRefHasher {
    size_t operator()(const EdgeRef &ref) const {
      // Both gid and ptr are the same size and unique; just pretend it's the gid
      return ref.gid.AsUint();
    }
  };

  std::unordered_map<EdgeRef, EdgeType, EdgeRefHasher> remove_edges_;

  struct hasher {
    size_t operator()(const std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *> &et) const {
      return std::get<0>(et).gid.AsUint();  // Ref should be enough (both gid and ptr are the same size and unique; just
                                            // pretend it's the gid)
    }
  };

  std::unordered_set<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>, hasher> post_process_edges_;
};

class VertexHandler {
 public:
  explicit VertexHandler(Vertex &vertex, Tracking &tracking, uint64_t commit_timestamp)
      : vertex_{vertex}, commit_timestamp_{commit_timestamp}, /*guard_{vertex_.lock},*/ tracking_{tracking} {}

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
  const auto &PreProperties() const;

  // Current labels are the post labels, multiple changes from different transactions to the same vertex would
  // generate a serialization error
  const auto &PostLabels() const { return vertex_.labels; }

  void PostProcess(TransactionEdgeHandler &edge_handler);

  auto PropertyType_ActionMethod(
      const memgraph::storage::PropertyStore &properties,
      std::unordered_map<memgraph::storage::PropertyId,
                         std::pair<memgraph::storage::PropertyValueType, memgraph::storage::PropertyValueType>> &diff);

  auto GetPropertyDiff();

  // TODO Use pointer
  // TODO Hide
  Vertex &vertex_;
  uint64_t commit_timestamp_{-1UL};

 private:
  mutable std::optional<decltype(vertex_.labels)> pre_labels_;
  mutable std::optional<std::map<PropertyId, PropertyValue::Type>> pre_properties_;

  bool label_update_{false};
  bool vertex_added_{false};
  bool update_property_{false};
  std::unordered_map<PropertyId, PropertyValue::Type> remove_property_;
  std::unordered_map<PropertyId, PropertyValue::Type> add_property_;

  Tracking &tracking_;
};

class EdgeHandler {
 public:
  explicit EdgeHandler(Edge &edge, Tracking &tracking, uint64_t commit_timestamp)
      : edge_{edge}, commit_timestamp_{commit_timestamp}, /*guard_{edge.lock},*/ tracking_{tracking} {}

  void Process();

  bool ExistedPreTx() const;

  auto PropertyType_ActionMethod(
      const memgraph::storage::PropertyStore &properties,
      std::unordered_map<memgraph::storage::PropertyId,
                         std::pair<memgraph::storage::PropertyValueType, memgraph::storage::PropertyValueType>> &diff);

  auto GetPropertyDiff(const memgraph::storage::PropertyStore &properties, memgraph::storage::Delta *delta);

 private:
  union edge_type_t {
    edge_type_t(EdgeTypeId id, v_key_type &from, v_key_type &to) : ref_{.ref{id, from, to}} {}
    edge_type_t(EdgeTypeId id, const v_key_type &from, const v_key_type &to, int /* tag */)
        : type_{.type{id, from, to}} {}

    bool is_ref = false;
    struct {
      bool is_ref = true;
      EdgeTypeRef ref;
    } ref_;
    struct {
      bool is_ref = false;
      EdgeType type;
    } type_;
    ~edge_type_t() {
      if (is_ref) {
        std::destroy_at(&ref_.ref);
      } else {
        std::destroy_at(&type_.type);
      }
    }
  };

  Edge &edge_;
  uint64_t commit_timestamp_{-1UL};
  bool needs_to_lock_{true};
  std::optional<edge_type_t> edge_type_;
  Tracking &tracking_;
};

struct SchemaInfo {
  template <typename Func>
  void ProcessTransaction(Transaction &transaction, bool properties_on_edges, Func &&find_edge) {
    const auto commit_timestamp = transaction.commit_timestamp->load(std::memory_order_acquire);
    TransactionEdgeHandler tx_edge_handler{tracking_, commit_timestamp, properties_on_edges};
    // First run through all vertices
    for (const auto &delta : transaction.deltas) {
      // Find VERTEX or EDGE; handle object's delta chain
      auto prev = delta.prev.Get();
      DMG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type == PreviousPtr::Type::VERTEX) {
        VertexHandler vertex_handler{*prev.vertex, tracking_, commit_timestamp};
        ProcessVertex(vertex_handler, tx_edge_handler);
      }
    }
    tx_edge_handler.PostVertexProcess();
    // Handle edges only after vertices are handled
    // Here we will handle only edge property changes
    // Other edge changes are handled by vertex deltas
    if (properties_on_edges) {
      for (const auto &delta : transaction.deltas) {
        // Find VERTEX or EDGE; handle object's delta chain
        auto prev = delta.prev.Get();
        DMG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
        if (prev.type == PreviousPtr::Type::EDGE) {
          EdgeHandler edge_handler{*prev.edge, tracking_, commit_timestamp};
          edge_handler.Process();
        }
      }
    }
  }

  void CleanUp() { tracking_.CleanUp(); }

  void Print(NameIdMapper &name_id_mapper, bool properties_on_edges) {
    tracking_.Print(name_id_mapper, properties_on_edges);
  }

  auto ToJson(NameIdMapper &name_id_mapper, bool properties_on_edges) {
    return tracking_.ToJson(name_id_mapper, properties_on_edges);
  }

  void ProcessVertex(VertexHandler &vertex_handler, TransactionEdgeHandler &edge_handler);

  Tracking tracking_;
};

}  // namespace memgraph::storage
