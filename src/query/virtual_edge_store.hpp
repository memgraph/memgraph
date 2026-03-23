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

#include <span>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/functional/hash.hpp>

#include "query/virtual_edge.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/unordered_set.hpp"

namespace memgraph::query {

class VirtualEdgeStore {
 public:
  using allocator_type = utils::Allocator<VirtualEdgeStore>;

  explicit VirtualEdgeStore(allocator_type alloc) : edges_(alloc) {}

  VirtualEdgeStore(const VirtualEdgeStore &other, allocator_type alloc)
      : edges_(other.edges_, alloc), dedup_(other.dedup_), out_index_(other.out_index_), in_index_(other.in_index_) {}

  VirtualEdgeStore(VirtualEdgeStore &&other) noexcept = default;

  VirtualEdgeStore(VirtualEdgeStore &&other, allocator_type alloc)
      : edges_(std::move(other.edges_), alloc),
        dedup_(std::move(other.dedup_)),
        out_index_(std::move(other.out_index_)),
        in_index_(std::move(other.in_index_)) {}

  VirtualEdgeStore(const VirtualEdgeStore &other) : VirtualEdgeStore(other, other.edges_.get_allocator()) {}

  VirtualEdgeStore &operator=(const VirtualEdgeStore &) = default;
  VirtualEdgeStore &operator=(VirtualEdgeStore &&) noexcept = default;
  ~VirtualEdgeStore() = default;

  void Insert(const VirtualEdge &edge);
  bool InsertIfNew(VirtualEdge edge);
  bool Contains(const VirtualEdge &edge) const;

  std::span<const VirtualEdge> OutEdges(storage::Gid vertex_gid) const;
  std::span<const VirtualEdge> InEdges(storage::Gid vertex_gid) const;

  auto &edges() { return edges_; }

  auto &edges() const { return edges_; }

  auto size() const { return edges_.size(); }

  auto empty() const { return edges_.empty(); }

 private:
  utils::pmr::unordered_set<VirtualEdge> edges_;

  struct DedupKey {
    storage::Gid from;
    storage::Gid to;
    std::string type;
    bool operator==(const DedupKey &) const = default;
  };

  struct DedupKeyHash {
    size_t operator()(const DedupKey &k) const {
      size_t seed = 0;
      boost::hash_combine(seed, std::hash<storage::Gid>{}(k.from));
      boost::hash_combine(seed, std::hash<storage::Gid>{}(k.to));
      boost::hash_combine(seed, std::hash<std::string>{}(k.type));
      return seed;
    }
  };

  std::unordered_set<DedupKey, DedupKeyHash> dedup_;
  std::unordered_map<storage::Gid, std::vector<VirtualEdge>> out_index_;
  std::unordered_map<storage::Gid, std::vector<VirtualEdge>> in_index_;

  void IndexEdge(const VirtualEdge &edge);
  void RebuildIndexes();
};

}  // namespace memgraph::query
