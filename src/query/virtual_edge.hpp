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

#include <memory>

#include <boost/functional/hash.hpp>

#include "query/virtual_node.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/unordered_map.hpp"

namespace memgraph::query {

// VirtualEdge borrows its endpoints. `from_` and `to_` point into a VirtualGraph's
// `nodes_` map (pmr::unordered_map gives pointer stability across insertions).
//
// Lifetime: nodes_anchor_ is a type-erased shared_ptr that keeps the referenced
// node map alive. Any edge that escapes its source VirtualGraph (e.g. via
// collect(e) or g.edges returned from a subquery) carries a bumped refcount
// on the map, so the raw from_/to_ pointers stay valid. Edges constructed
// without an anchor (unit tests with local VirtualNode stack variables) are
// valid only while those locals are alive.
class VirtualEdge final {
 public:
  using allocator_type = utils::Allocator<VirtualEdge>;
  using property_map = utils::pmr::unordered_map<storage::PropertyId, storage::PropertyValue>;

  VirtualEdge(const VirtualNode &from, const VirtualNode &to, utils::pmr::string edge_type_name,
              std::shared_ptr<const void> nodes_anchor = {}, allocator_type alloc = {})
      : from_(&from),
        to_(&to),
        edge_type_name_(std::move(edge_type_name), alloc),
        gid_(NextSyntheticGid()),
        properties_(alloc),
        cached_hash_(HashKey(from.Gid(), to.Gid(), edge_type_name_)),
        nodes_anchor_(std::move(nodes_anchor)) {}

  // Rebound-copy ctor: preserves the edge's identity (gid, type, props) but
  // repoints from_/to_ at a different pair of VirtualNodes (and a different
  // anchor). Used by VirtualGraph's copy-with-allocator ctor when the node
  // map is duplicated under a new allocator, and by Merge when alias
  // resolution maps the source edge's endpoints to canonical nodes with
  // different synthetic gids. In the alias case cached_hash_ must be
  // recomputed under the new gids (the hash encodes from_gid/to_gid/type).
  VirtualEdge(const VirtualEdge &other, const VirtualNode &new_from, const VirtualNode &new_to,
              std::shared_ptr<const void> nodes_anchor, allocator_type alloc)
      : from_(&new_from),
        to_(&new_to),
        edge_type_name_(other.edge_type_name_, alloc),
        gid_(other.gid_),
        properties_(other.properties_, alloc),
        cached_hash_(HashKey(new_from.Gid(), new_to.Gid(), edge_type_name_)),
        nodes_anchor_(std::move(nodes_anchor)) {}

  VirtualEdge(const VirtualEdge &other, allocator_type alloc)
      : from_(other.from_),
        to_(other.to_),
        edge_type_name_(other.edge_type_name_, alloc),
        gid_(other.gid_),
        properties_(other.properties_, alloc),
        cached_hash_(other.cached_hash_),
        nodes_anchor_(other.nodes_anchor_) {}

  VirtualEdge(VirtualEdge &&other, allocator_type alloc)
      : from_(other.from_),
        to_(other.to_),
        edge_type_name_(std::move(other.edge_type_name_), alloc),
        gid_(other.gid_),
        properties_(std::move(other.properties_), alloc),
        cached_hash_(other.cached_hash_),
        nodes_anchor_(std::move(other.nodes_anchor_)) {}

  VirtualEdge(const VirtualEdge &other) : VirtualEdge(other, other.edge_type_name_.get_allocator()) {}

  VirtualEdge(VirtualEdge &&) noexcept = default;

  VirtualEdge &operator=(const VirtualEdge &) = default;
  VirtualEdge &operator=(VirtualEdge &&) = default;
  ~VirtualEdge() = default;

  [[nodiscard]] auto From() const noexcept -> const VirtualNode & { return *from_; }

  [[nodiscard]] auto To() const noexcept -> const VirtualNode & { return *to_; }

  [[nodiscard]] auto FromGid() const noexcept -> storage::Gid { return from_->Gid(); }

  [[nodiscard]] auto ToGid() const noexcept -> storage::Gid { return to_->Gid(); }

  [[nodiscard]] auto EdgeTypeName() const noexcept -> const utils::pmr::string & { return edge_type_name_; }

  [[nodiscard]] auto Gid() const noexcept -> storage::Gid { return gid_; }

  [[nodiscard]] size_t Hash() const noexcept { return cached_hash_; }

  [[nodiscard]] auto GetProperty(storage::PropertyId key) const -> storage::PropertyValue {
    if (const auto it = properties_.find(key); it != properties_.end()) return it->second;
    return storage::PropertyValue{};
  }

  void SetProperty(storage::PropertyId key, storage::PropertyValue value) {
    properties_.insert_or_assign(key, std::move(value));
  }

  [[nodiscard]] auto Properties() const noexcept -> const property_map & { return properties_; }

  // Semantic equality on (from_gid, to_gid, type). Drives dedup via unordered_set<VirtualEdge>.
  bool operator==(const VirtualEdge &other) const noexcept {
    return from_->Gid() == other.from_->Gid() && to_->Gid() == other.to_->Gid() &&
           edge_type_name_ == other.edge_type_name_;
  }

 private:
  static size_t HashKey(storage::Gid from_gid, storage::Gid to_gid, std::string_view type) noexcept {
    size_t seed = 0;
    boost::hash_combine(seed, std::hash<storage::Gid>{}(from_gid));
    boost::hash_combine(seed, std::hash<storage::Gid>{}(to_gid));
    boost::hash_combine(seed, std::hash<std::string_view>{}(type));
    return seed;
  }

  const VirtualNode *from_;
  const VirtualNode *to_;
  utils::pmr::string edge_type_name_;
  storage::Gid gid_;
  property_map properties_;
  size_t cached_hash_;
  std::shared_ptr<const void> nodes_anchor_;
};

}  // namespace memgraph::query

namespace std {
template <>
struct hash<memgraph::query::VirtualEdge> {
  size_t operator()(const memgraph::query::VirtualEdge &e) const noexcept { return e.Hash(); }
};
}  // namespace std
