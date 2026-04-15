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

#include <boost/functional/hash.hpp>

#include "query/virtual_edge.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/unordered_map.hpp"
#include "utils/pmr/unordered_set.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query {

class VirtualEdgeStore {
 public:
  using allocator_type = utils::Allocator<VirtualEdgeStore>;

  explicit VirtualEdgeStore(allocator_type alloc) : edges_(alloc), dedup_(alloc), out_index_(alloc), in_index_(alloc) {}

  VirtualEdgeStore(const VirtualEdgeStore &other, allocator_type alloc)
      : edges_(other.edges_, alloc),
        dedup_(other.dedup_, alloc),
        out_index_(other.out_index_, alloc),
        in_index_(other.in_index_, alloc) {}

  VirtualEdgeStore(VirtualEdgeStore &&other) noexcept = default;

  VirtualEdgeStore(VirtualEdgeStore &&other, allocator_type alloc)
      : edges_(std::move(other.edges_), alloc),
        dedup_(std::move(other.dedup_), alloc),
        out_index_(std::move(other.out_index_), alloc),
        in_index_(std::move(other.in_index_), alloc) {}

  VirtualEdgeStore(const VirtualEdgeStore &other) : VirtualEdgeStore(other, other.edges_.get_allocator()) {}

  VirtualEdgeStore &operator=(const VirtualEdgeStore &) = default;
  VirtualEdgeStore &operator=(VirtualEdgeStore &&) noexcept = default;
  ~VirtualEdgeStore() = default;

  // Returns true iff the (from, to, type) triple was not already present.
  bool InsertIfNew(VirtualEdge edge);
  [[nodiscard]] bool Contains(const VirtualEdge &edge) const;

  [[nodiscard]] std::span<const VirtualEdge> OutEdges(storage::Gid vertex_gid) const;
  [[nodiscard]] std::span<const VirtualEdge> InEdges(storage::Gid vertex_gid) const;

  [[nodiscard]] auto &edges() { return edges_; }

  [[nodiscard]] auto &edges() const { return edges_; }

  [[nodiscard]] auto size() const { return edges_.size(); }

  [[nodiscard]] auto empty() const { return edges_.empty(); }

 private:
  utils::pmr::unordered_set<VirtualEdge> edges_;

  struct DedupKey {
    using allocator_type = utils::Allocator<DedupKey>;

    DedupKey(storage::Gid from, storage::Gid to, std::string_view type, allocator_type alloc = {})
        : from(from), to(to), type(type, alloc) {}

    DedupKey(const DedupKey &other, allocator_type alloc) : from(other.from), to(other.to), type(other.type, alloc) {}

    DedupKey(DedupKey &&other, allocator_type alloc) noexcept
        : from(other.from), to(other.to), type(std::move(other.type), alloc) {}

    DedupKey(const DedupKey &) = default;
    DedupKey(DedupKey &&) noexcept = default;
    DedupKey &operator=(const DedupKey &) = default;
    DedupKey &operator=(DedupKey &&) noexcept = default;
    ~DedupKey() = default;

    storage::Gid from;
    storage::Gid to;
    utils::pmr::string type;

    bool operator==(const DedupKey &) const = default;
  };

  struct DedupKeyHash {
    size_t operator()(const DedupKey &k) const noexcept {
      size_t seed = 0;
      boost::hash_combine(seed, std::hash<storage::Gid>{}(k.from));
      boost::hash_combine(seed, std::hash<storage::Gid>{}(k.to));
      boost::hash_combine(seed, std::hash<std::string_view>{}(k.type));
      return seed;
    }
  };

  utils::pmr::unordered_set<DedupKey, DedupKeyHash> dedup_;
  utils::pmr::unordered_map<storage::Gid, utils::pmr::vector<VirtualEdge>> out_index_;
  utils::pmr::unordered_map<storage::Gid, utils::pmr::vector<VirtualEdge>> in_index_;

  void IndexEdge(const VirtualEdge &edge);
};

}  // namespace memgraph::query
