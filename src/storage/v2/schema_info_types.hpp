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

#include <boost/container_hash/hash_fwd.hpp>
#include <cstdint>
#include <functional>
#include <nlohmann/json.hpp>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "storage/v2/enum_store.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/conccurent_unordered_map.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::storage {

using VertexKey = utils::small_vector<LabelId>;

/**
 * @brief Hash the VertexKey (vector of labels) in an order independent way.
 * This format is needed for boost hasher.
 */
inline size_t hash_value(const VertexKey &x) {
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
  explicit EdgeKey(EdgeKeyRef ref) : type{ref.type}, from{ref.from}, to(ref.to) {}

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
 * @brief
 */
template <template <class...> class TContainer = utils::ConcurrentUnorderedMap>
struct PropertyInfo {
  std::atomic_int n{0};                                     //!< Number of objects with this property
  TContainer<ExtendedPropertyType, std::atomic_int> types;  //!< Numer of property instances with a specific type

  nlohmann::json ToJson(const EnumStore &enum_store, std::string_view key, uint32_t max_count) const {
    nlohmann::json::object_t property_info;
    property_info.emplace("key", key);
    const auto num = n.load();
    property_info.emplace("count", num);
    property_info.emplace("filling_factor", (100.0 * num) / max_count);
    const auto &[types_itr, _] = property_info.emplace("types", nlohmann::json::array_t{});
    for (const auto &type : types) {
      nlohmann::json::object_t type_info;
      std::stringstream ss;
      if (type.first.type == PropertyValueType::TemporalData) {
        ss << type.first.temporal_type;
      } else if (type.first.type == PropertyValueType::Enum) {
        ss << "Enum::" << *enum_store.ToTypeString(type.first.enum_type);
      } else {
        // Unify formatting
        switch (type.first.type) {
          break;
          case PropertyValueType::Null:
            ss << "Null";
            break;
          case PropertyValueType::Bool:
            ss << "Boolean";
            break;
          case PropertyValueType::Int:
            ss << "Integer";
            break;
          case PropertyValueType::Double:
            ss << "Float";
            break;
          case PropertyValueType::String:
            ss << "String";
            break;
          case PropertyValueType::List:
          case PropertyValueType::IntList:
          case PropertyValueType::DoubleList:
          case PropertyValueType::NumericList:
            ss << "List";
            break;
          case PropertyValueType::Map:
            ss << "Map";
            break;
          case PropertyValueType::TemporalData:
            ss << "TemporalData";
            break;
          case PropertyValueType::ZonedTemporalData:
            ss << "ZonedDateTime";
            break;
          case PropertyValueType::Enum:
            ss << "Enum";
            break;
          case PropertyValueType::Point2d:
            ss << "Point2D";
            break;
          case PropertyValueType::Point3d:
            ss << "Point3D";
            break;
        }
      }
      type_info.emplace("type", ss.str());
      type_info.emplace("count", type.second.load());
      types_itr->second.emplace_back(std::move(type_info));
    }
    return property_info;
  }
};

/**
 * @brief
 */
template <template <class...> class TContainer = utils::ConcurrentUnorderedMap>
struct TrackingInfo {
  std::atomic_int n{0};                                         //!< Number of tracked objects
  TContainer<PropertyId, PropertyInfo<TContainer>> properties;  //!< Property statistics defined by the tracked object

  nlohmann::json ToJson(NameIdMapper &name_id_mapper, const EnumStore &enum_store) const {
    nlohmann::json::object_t tracking_info;
    tracking_info.emplace("count", n.load());
    const auto &[prop_itr, _] = tracking_info.emplace("properties", nlohmann::json::array_t{});
    for (const auto &[p, info] : properties) {
      prop_itr->second.emplace_back(
          info.ToJson(enum_store, name_id_mapper.IdToName(p.AsUint()), std::max(n.load(), 1)));
    }
    return tracking_info;
  }

  template <template <class...> class TOtherContainer>
  TrackingInfo &operator+=(const TrackingInfo<TOtherContainer> &rhs) {
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

struct SchemaInfoEdge {
  EdgeRef edge_ref;
  EdgeTypeId edge_type;
  Vertex *from;
  Vertex *to;
};

}  // namespace memgraph::storage

namespace std {

template <>
struct hash<memgraph::storage::VertexKey> {
  size_t operator()(const memgraph::storage::VertexKey &x) const { return memgraph::storage::hash_value(x); }
};

/**
 * @brief Equate VertexKeys (vector of labels) in an order independent way.
 */
template <>
struct equal_to<memgraph::storage::VertexKey> {
  size_t operator()(const memgraph::storage::VertexKey &lhs, const memgraph::storage::VertexKey &rhs) const {
    return lhs.size() == rhs.size() && std::is_permutation(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
  }
};

/**
 * @brief Allow for heterogeneous lookup via EdgeKey and EdgeKeyRef
 */
template <>
struct hash<memgraph::storage::EdgeKey> {
  using is_transparent = void;

  size_t operator()(const memgraph::storage::EdgeKey &et) const {
    size_t seed = 0;
    boost::hash_combine(seed, et.type.AsUint());
    boost::hash_combine(seed, et.from);
    boost::hash_combine(seed, et.to);
    return seed;
  }

  size_t operator()(const memgraph::storage::EdgeKeyRef &et) const {
    size_t seed = 0;
    boost::hash_combine(seed, et.type.AsUint());
    boost::hash_combine(seed, et.from);
    boost::hash_combine(seed, et.to);
    return seed;
  }
};

template <>
struct equal_to<memgraph::storage::EdgeKey> {
  using is_transparent = void;
  size_t operator()(const memgraph::storage::EdgeKey &lhs, const memgraph::storage::EdgeKey &rhs) const {
    return lhs == rhs;
  }
  size_t operator()(const memgraph::storage::EdgeKey &lhs, const memgraph::storage::EdgeKeyRef &rhs) const {
    return lhs == rhs;
  }
  size_t operator()(const memgraph::storage::EdgeKeyRef &lhs, const memgraph::storage::EdgeKey &rhs) const {
    return rhs == lhs;
  }
};

template <>
class hash<memgraph::storage::SchemaInfoEdge> {
 public:
  size_t operator()(const memgraph::storage::SchemaInfoEdge &pp) const {
    return pp.edge_ref.gid.AsUint();  // Both ptr and gid are the same size and unique
  }
};

template <>
class equal_to<memgraph::storage::SchemaInfoEdge> {
 public:
  bool operator()(const memgraph::storage::SchemaInfoEdge &lhs, const memgraph::storage::SchemaInfoEdge &rhs) const {
    // Edge ref is a pointer or gid, both are unique and should completely define the edge
    return lhs.edge_ref == rhs.edge_ref;
  }
};
}  // namespace std

// After hashes/equals are defined
namespace memgraph::storage {
struct SchemaInfoPostProcess {
  std::unordered_set<SchemaInfoEdge> edges;
  std::unordered_map<const Vertex *, VertexKey> vertex_cache;
  std::unordered_map<const Edge *, std::unordered_map<PropertyId, ExtendedPropertyType>>
      edge_cache;  // TODO Do we need this
};
}  // namespace memgraph::storage
