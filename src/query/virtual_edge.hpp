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

#include <atomic>

#include "query/virtual_node.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/map.hpp"
#include "utils/pmr/string.hpp"

namespace memgraph::query {

// Synthetic Gids count down from UINT64_MAX to avoid collision with real Gids (which count up from 0).
inline storage::Gid NextVirtualEdgeGid() {
  static std::atomic<uint64_t> counter{std::numeric_limits<uint64_t>::max()};
  return storage::Gid::FromUint(counter.fetch_sub(1, std::memory_order_relaxed));
}

class VirtualEdge final {
 public:
  using allocator_type = utils::Allocator<VirtualEdge>;
  using property_map = utils::pmr::map<storage::PropertyId, storage::PropertyValue>;

  VirtualEdge(VirtualNode from, VirtualNode to, utils::pmr::string edge_type_name, allocator_type alloc = {})
      : from_(std::move(from), alloc),
        to_(std::move(to), alloc),
        edge_type_name_(std::move(edge_type_name), alloc),
        gid_(NextVirtualEdgeGid()),
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

  auto From() const noexcept -> const VirtualNode & { return from_; }

  auto To() const noexcept -> const VirtualNode & { return to_; }

  auto FromGid() const noexcept -> storage::Gid { return from_.Gid(); }

  auto ToGid() const noexcept -> storage::Gid { return to_.Gid(); }

  auto EdgeTypeName() const noexcept -> const utils::pmr::string & { return edge_type_name_; }

  auto Gid() const noexcept -> storage::Gid { return gid_; }

  auto GetProperty(storage::PropertyId key) const -> storage::PropertyValue {
    if (const auto it = properties_.find(key); it != properties_.end()) return it->second;
    return storage::PropertyValue{};
  }

  void SetProperty(storage::PropertyId key, const storage::PropertyValue &value) { properties_[key] = value; }

  auto Properties() const noexcept -> const property_map & { return properties_; }

  bool operator==(const VirtualEdge &other) const noexcept { return gid_ == other.gid_; }

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
    return std::hash<memgraph::storage::Gid>{}(e.Gid());
  }
};
}  // namespace std
