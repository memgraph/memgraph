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
#include <optional>
#include <variant>

#include <boost/functional/hash.hpp>

#include "query/synthetic_gid.hpp"
#include "query/virtual_node.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/unordered_map.hpp"

namespace memgraph::query {

// A typed edge in a derived view, with its own synthetic gid. Each endpoint is either a resolved
// virtual node or an unresolved import handle (an int64_t the user passed to virtualEdge by gid),
// settled per endpoint so the two may be mixed. A resolved endpoint is a shared_ptr into a
// VirtualGraph's node map, so each edge owns a refcount on its endpoint nodes and edges escaping
// their source graph (collect(e), subquery returns) keep them alive after the graph is destroyed.
// An unresolved endpoint is bound to a node at projection assembly, by matching its handle against
// the node handles.
class VirtualEdge final {
 public:
  using allocator_type = utils::Allocator<VirtualEdge>;
  using property_map = utils::pmr::unordered_map<storage::PropertyId, storage::PropertyValue>;
  // A resolved node, or an unresolved import handle awaiting binding at projection assembly.
  using Endpoint = std::variant<std::shared_ptr<const VirtualNode>, int64_t>;

  VirtualEdge(Endpoint from, Endpoint to, utils::pmr::string edge_type_name, allocator_type alloc = {})
      : impl_(std::make_unique<Impl>(std::move(from), std::move(to), std::move(edge_type_name), property_map{alloc},
                                     alloc)),
        gid_(NextSyntheticGid()) {}

  // Rebound copy: preserves identity (gid, type, props) but repoints the endpoints
  // at different VirtualNodes. Used when VirtualGraph duplicates its node map
  // under a new allocator, and by Merge when alias resolution maps the source
  // edge's endpoints to canonical nodes with different synthetic gids.
  VirtualEdge(const VirtualEdge &other, std::shared_ptr<const VirtualNode> new_from,
              std::shared_ptr<const VirtualNode> new_to, allocator_type alloc)
      : impl_(std::make_unique<Impl>(Endpoint{std::move(new_from)}, Endpoint{std::move(new_to)},
                                     other.impl_->edge_type_name, other.impl_->properties, alloc)),
        gid_(other.gid_) {}

  VirtualEdge(const VirtualEdge &other, allocator_type alloc)
      : impl_(std::make_unique<Impl>(other.impl_->from, other.impl_->to, other.impl_->edge_type_name,
                                     other.impl_->properties, alloc)),
        gid_(other.gid_) {}

  VirtualEdge(VirtualEdge &&other, allocator_type alloc)
      : impl_(std::make_unique<Impl>(std::move(other.impl_->from), std::move(other.impl_->to),
                                     std::move(other.impl_->edge_type_name), std::move(other.impl_->properties),
                                     alloc)),
        gid_(other.gid_) {}

  VirtualEdge(const VirtualEdge &other) : VirtualEdge(other, other.impl_->edge_type_name.get_allocator()) {}

  VirtualEdge(VirtualEdge &&) noexcept = default;

  VirtualEdge &operator=(const VirtualEdge &other) {
    if (this != &other) {
      DMG_ASSERT(impl_ && other.impl_, "Assignment to/from moved-from VirtualEdge");
      *impl_ = *other.impl_;
      gid_ = other.gid_;
    }
    return *this;
  }

  VirtualEdge &operator=(VirtualEdge &&) = default;
  ~VirtualEdge() = default;

  // The endpoint node, when resolved. An unresolved (handle) endpoint has no node yet; reaching for
  // one before assembly binds it (startNode/endNode) is a query error.
  [[nodiscard]] auto From() const -> const VirtualNode & { return EndpointNode(impl_->from); }

  [[nodiscard]] auto To() const -> const VirtualNode & { return EndpointNode(impl_->to); }

  // The endpoint gid: a resolved endpoint's synthetic node gid, or the handle itself when
  // unresolved. Bolt serialization, the VirtualGraph in/out index, and edge equality read through
  // these, so they work for both endpoint forms.
  [[nodiscard]] auto FromGid() const noexcept -> storage::Gid { return EndpointGid(impl_->from); }

  [[nodiscard]] auto ToGid() const noexcept -> storage::Gid { return EndpointGid(impl_->to); }

  // The unresolved import handle on an endpoint, or none if it is a resolved node. Projection
  // assembly reads these to bind the edge's endpoints to nodes by matching handles.
  [[nodiscard]] auto FromHandle() const noexcept -> std::optional<int64_t> { return EndpointHandle(impl_->from); }

  [[nodiscard]] auto ToHandle() const noexcept -> std::optional<int64_t> { return EndpointHandle(impl_->to); }

  [[nodiscard]] auto EdgeTypeName() const noexcept -> const utils::pmr::string & { return impl_->edge_type_name; }

  [[nodiscard]] auto Gid() const noexcept -> storage::Gid { return gid_; }

  [[nodiscard]] size_t Hash() const noexcept { return HashKey(FromGid(), ToGid(), impl_->edge_type_name); }

  // Read, in the EdgeAccessor shape, so one call site reads a property from either a real edge or a
  // virtual edge. A virtual edge has no origin to read through, so the view is unused and the read
  // cannot fail; the Result is returned for one uniform shape.
  [[nodiscard]] auto GetProperty(storage::View /*view*/, storage::PropertyId key) const
      -> storage::Result<storage::PropertyValue> {
    return GetProperty(key);
  }

  [[nodiscard]] auto GetProperty(storage::PropertyId key) const -> storage::PropertyValue {
    if (const auto it = impl_->properties.find(key); it != impl_->properties.end()) return it->second;
    return storage::PropertyValue{};
  }

  void SetProperty(storage::PropertyId key, storage::PropertyValue value) {
    impl_->properties.insert_or_assign(key, std::move(value));
  }

  [[nodiscard]] auto Properties() const noexcept -> const property_map & { return impl_->properties; }

  // Semantic equality on (from_gid, to_gid, type). Drives dedup via unordered_set<VirtualEdge>.
  bool operator==(const VirtualEdge &other) const noexcept {
    return FromGid() == other.FromGid() && ToGid() == other.ToGid() &&
           impl_->edge_type_name == other.impl_->edge_type_name;
  }

 private:
  static storage::Gid EndpointGid(const Endpoint &endpoint) noexcept {
    if (const auto *node = std::get_if<std::shared_ptr<const VirtualNode>>(&endpoint)) return (*node)->Gid();
    return storage::Gid::FromInt(std::get<int64_t>(endpoint));
  }

  static const VirtualNode &EndpointNode(const Endpoint &endpoint) {
    if (const auto *node = std::get_if<std::shared_ptr<const VirtualNode>>(&endpoint)) return **node;
    throw QueryRuntimeException(
        "This virtual edge has an unresolved import-handle endpoint; bind it by assembling a "
        "projection from node and edge lists before reaching for its endpoint nodes.");
  }

  static std::optional<int64_t> EndpointHandle(const Endpoint &endpoint) noexcept {
    if (const auto *handle = std::get_if<int64_t>(&endpoint)) return *handle;
    return std::nullopt;
  }

  static size_t HashKey(storage::Gid from_gid, storage::Gid to_gid, std::string_view type) noexcept {
    size_t seed = 0;
    boost::hash_combine(seed, std::hash<storage::Gid>{}(from_gid));
    boost::hash_combine(seed, std::hash<storage::Gid>{}(to_gid));
    boost::hash_combine(seed, std::hash<std::string_view>{}(type));
    return seed;
  }

  // Endpoints live in the heap-allocated Impl, not inline, so a VirtualEdge stays small and the
  // mgp_edge size budget that embeds it does not grow.
  struct Impl {
    Endpoint from;
    Endpoint to;
    utils::pmr::string edge_type_name;
    property_map properties;

    Impl(Endpoint from, Endpoint to, const utils::pmr::string &name, const property_map &props, allocator_type alloc)
        : from(std::move(from)), to(std::move(to)), edge_type_name(name, alloc), properties(props, alloc) {}

    Impl(Endpoint from, Endpoint to, utils::pmr::string &&name, property_map &&props, allocator_type alloc)
        : from(std::move(from)),
          to(std::move(to)),
          edge_type_name(std::move(name), alloc),
          properties(std::move(props), alloc) {}
  };

  std::unique_ptr<Impl> impl_;
  storage::Gid gid_;
};

}  // namespace memgraph::query

namespace std {
template <>
struct hash<memgraph::query::VirtualEdge> {
  size_t operator()(const memgraph::query::VirtualEdge &e) const noexcept { return e.Hash(); }
};
}  // namespace std
