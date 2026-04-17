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

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/map.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query {

// Synthetic Gids for VirtualNode and VirtualEdge share a single counter counting down from UINT64_MAX,
// so node and edge Gids are drawn from the same space and can never collide with each other
// (nor with real Gids, which count up from 0).
inline storage::Gid NextSyntheticGid() {
  static std::atomic<uint64_t> counter{std::numeric_limits<uint64_t>::max()};
  return storage::Gid::FromUint(counter.fetch_sub(1, std::memory_order_relaxed));
}

class VirtualNode final {
 public:
  using allocator_type = utils::Allocator<VirtualNode>;
  using label_list = utils::pmr::vector<utils::pmr::string>;
  using property_map = utils::pmr::map<storage::PropertyId, storage::PropertyValue>;

  VirtualNode(storage::Gid original_gid, label_list labels, property_map properties, allocator_type alloc = {})
      : gid_(NextSyntheticGid()),
        original_gid_(original_gid),
        labels_(std::move(labels), alloc),
        properties_(std::move(properties), alloc) {}

  VirtualNode(const VirtualNode &other, allocator_type alloc)
      : gid_(other.gid_),
        original_gid_(other.original_gid_),
        labels_(other.labels_, alloc),
        properties_(other.properties_, alloc) {}

  VirtualNode(VirtualNode &&other, allocator_type alloc)
      : gid_(other.gid_),
        original_gid_(other.original_gid_),
        labels_(std::move(other.labels_), alloc),
        properties_(std::move(other.properties_), alloc) {}

  VirtualNode(const VirtualNode &other) : VirtualNode(other, other.labels_.get_allocator()) {}

  VirtualNode(VirtualNode &&) noexcept = default;

  VirtualNode &operator=(const VirtualNode &) = default;
  // Not noexcept: pmr move-assign across mismatched allocators may reallocate and throw.
  VirtualNode &operator=(VirtualNode &&) = default;
  ~VirtualNode() = default;

  [[nodiscard]] auto Gid() const noexcept -> storage::Gid { return gid_; }

  [[nodiscard]] auto OriginalGid() const noexcept -> storage::Gid { return original_gid_; }

  [[nodiscard]] auto CypherId() const noexcept -> int64_t { return gid_.AsInt(); }

  [[nodiscard]] auto Labels() const noexcept -> const label_list & { return labels_; }

  [[nodiscard]] auto GetProperty(storage::PropertyId key) const -> storage::PropertyValue {
    if (const auto it = properties_.find(key); it != properties_.end()) return it->second;
    return storage::PropertyValue{};
  }

  void SetProperty(storage::PropertyId key, const storage::PropertyValue &value) { properties_[key] = value; }

  [[nodiscard]] auto Properties() const noexcept -> const property_map & { return properties_; }

  bool operator==(const VirtualNode &other) const noexcept { return gid_ == other.gid_; }

 private:
  storage::Gid gid_;
  storage::Gid original_gid_;
  label_list labels_;
  property_map properties_;
};

}  // namespace memgraph::query

namespace std {
template <>
struct hash<memgraph::query::VirtualNode> {
  size_t operator()(const memgraph::query::VirtualNode &n) const noexcept {
    return std::hash<memgraph::storage::Gid>{}(n.Gid());
  }
};
}  // namespace std
