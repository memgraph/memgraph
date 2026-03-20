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
#include <map>
#include <string>

#include "query/vertex_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query {

// Synthetic Gids count down from UINT64_MAX to avoid collision with real Gids (which count up from 0).
inline storage::Gid NextVirtualEdgeGid() {
  static std::atomic<uint64_t> counter{std::numeric_limits<uint64_t>::max()};
  return storage::Gid::FromUint(counter.fetch_sub(1, std::memory_order_relaxed));
}

class VirtualEdge final {
 public:
  VirtualEdge(VertexAccessor from, VertexAccessor to, std::string edge_type_name)
      : from_(from), to_(to), edge_type_name_(std::move(edge_type_name)), gid_(NextVirtualEdgeGid()) {}

  auto From() const -> VertexAccessor { return from_; }

  auto To() const -> VertexAccessor { return to_; }

  auto EdgeTypeName() const -> const std::string & { return edge_type_name_; }

  auto Gid() const noexcept -> storage::Gid { return gid_; }

  auto GetProperty(storage::PropertyId key) const -> storage::PropertyValue {
    if (auto it = properties_.find(key); it != properties_.end()) {
      return it->second;
    }
    return storage::PropertyValue{};
  }

  auto Properties() const -> const std::map<storage::PropertyId, storage::PropertyValue> & { return properties_; }

  bool operator==(const VirtualEdge &other) const noexcept { return gid_ == other.gid_; }

 private:
  VertexAccessor from_;
  VertexAccessor to_;
  std::string edge_type_name_;
  storage::Gid gid_;
  std::map<storage::PropertyId, storage::PropertyValue> properties_;
};

}  // namespace memgraph::query

namespace std {
template <>
struct hash<memgraph::query::VirtualEdge> {
  size_t operator()(const memgraph::query::VirtualEdge &e) const {
    return std::hash<memgraph::storage::Gid>{}(e.Gid());
  }
};
}  // namespace std
