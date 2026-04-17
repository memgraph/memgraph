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

#include <boost/functional/hash.hpp>

#include "query/virtual_node.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/map.hpp"
#include "utils/pmr/string.hpp"

namespace memgraph::query {

class VirtualEdge final {
 public:
  using allocator_type = utils::Allocator<VirtualEdge>;
  using property_map = utils::pmr::map<storage::PropertyId, storage::PropertyValue>;

  VirtualEdge(VirtualNode from, VirtualNode to, utils::pmr::string edge_type_name, allocator_type alloc = {})
      : from_(std::move(from), alloc),
        to_(std::move(to), alloc),
        edge_type_name_(std::move(edge_type_name), alloc),
        gid_(NextSyntheticGid()),
        properties_(alloc) {}

  VirtualEdge(const VirtualEdge &other, allocator_type alloc)
      : from_(other.from_, alloc),
        to_(other.to_, alloc),
        edge_type_name_(other.edge_type_name_, alloc),
        gid_(other.gid_),
        properties_(other.properties_, alloc) {}

  VirtualEdge(VirtualEdge &&other, allocator_type alloc)
      : from_(std::move(other.from_), alloc),
        to_(std::move(other.to_), alloc),
        edge_type_name_(std::move(other.edge_type_name_), alloc),
        gid_(other.gid_),
        properties_(std::move(other.properties_), alloc) {}

  VirtualEdge(const VirtualEdge &other) : VirtualEdge(other, other.edge_type_name_.get_allocator()) {}

  VirtualEdge(VirtualEdge &&) noexcept = default;

  VirtualEdge &operator=(const VirtualEdge &) = default;
  VirtualEdge &operator=(VirtualEdge &&) noexcept = default;
  ~VirtualEdge() = default;

  [[nodiscard]] auto From() const noexcept -> const VirtualNode & { return from_; }

  [[nodiscard]] auto To() const noexcept -> const VirtualNode & { return to_; }

  [[nodiscard]] auto FromGid() const noexcept -> storage::Gid { return from_.Gid(); }

  [[nodiscard]] auto ToGid() const noexcept -> storage::Gid { return to_.Gid(); }

  [[nodiscard]] auto EdgeTypeName() const noexcept -> const utils::pmr::string & { return edge_type_name_; }

  [[nodiscard]] auto Gid() const noexcept -> storage::Gid { return gid_; }

  [[nodiscard]] auto GetProperty(storage::PropertyId key) const -> storage::PropertyValue {
    if (const auto it = properties_.find(key); it != properties_.end()) return it->second;
    return storage::PropertyValue{};
  }

  void SetProperty(storage::PropertyId key, const storage::PropertyValue &value) { properties_[key] = value; }

  [[nodiscard]] auto Properties() const noexcept -> const property_map & { return properties_; }

  // Semantic equality: two VirtualEdges are equal if they connect the same endpoints with the same
  // edge type. This makes VirtualEdgeStore's unordered_set<VirtualEdge> the single source of truth
  // for dedup — no parallel dedup structure needed. The synthetic gid_ is metadata only.
  bool operator==(const VirtualEdge &other) const noexcept {
    return from_.Gid() == other.from_.Gid() && to_.Gid() == other.to_.Gid() && edge_type_name_ == other.edge_type_name_;
  }

 private:
  VirtualNode from_;
  VirtualNode to_;
  utils::pmr::string edge_type_name_;
  storage::Gid gid_;
  property_map properties_;
};

}  // namespace memgraph::query

namespace std {
template <>
struct hash<memgraph::query::VirtualEdge> {
  size_t operator()(const memgraph::query::VirtualEdge &e) const noexcept {
    size_t seed = 0;
    boost::hash_combine(seed, std::hash<memgraph::storage::Gid>{}(e.FromGid()));
    boost::hash_combine(seed, std::hash<memgraph::storage::Gid>{}(e.ToGid()));
    boost::hash_combine(seed, std::hash<std::string_view>{}(std::string_view(e.EdgeTypeName())));
    return seed;
  }
};
}  // namespace std
