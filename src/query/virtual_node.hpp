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

#include <algorithm>
#include <memory>
#include <optional>

#include "query/exceptions.hpp"
#include "query/synthetic_gid.hpp"
#include "query/vertex_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/unordered_map.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query {

// The static, shared description of one derive() projection's binding: which keys are hidden and
// which are overlay-bound by the propertyPolicy, plus the provenance ref. Built once from the
// derive() config and shared (via shared_ptr) by every overlay node the projection produces - the
// policy and ref are role-independent, so a projection's source and target nodes share one instance.
// Node-local overlay values and the node's labels stay on the node; only this role-independent part
// is shared. Construction-time property overrides are overlay-bound through the node's own overlay
// store (they carry a value there), so they need not be listed here.
class PropertyBinding {
 public:
  using key_set = utils::pmr::vector<storage::PropertyId>;
  static constexpr int64_t kNoRef = -1;

  PropertyBinding(key_set hidden, key_set overlay_bound, int64_t ref = kNoRef)
      : hidden_(std::move(hidden)), overlay_bound_(std::move(overlay_bound)), ref_(ref) {}

  [[nodiscard]] bool IsHidden(storage::PropertyId key) const {
    return std::ranges::find(hidden_, key) != hidden_.end();
  }

  // True if the propertyPolicy bound this key 'overlay'. A construction-time override key is
  // overlay-bound through the node's overlay store (it carries a value), not here.
  [[nodiscard]] bool IsOverlayBoundByPolicy(storage::PropertyId key) const {
    return std::ranges::find(overlay_bound_, key) != overlay_bound_.end();
  }

  [[nodiscard]] bool HasRef() const noexcept { return ref_ != kNoRef; }

  [[nodiscard]] int64_t Ref() const noexcept { return ref_; }

 private:
  key_set hidden_;
  key_set overlay_bound_;
  int64_t ref_;
};

// A node in a derived view. Holds its own overlay property store and, optionally, a reference to an
// origin real vertex. With no origin it is a synthetic node (the overlay is its only store). With an
// origin it is an overlay node: property reads fall through to the origin lazily (never copied),
// and an overlay key shadows the origin value for that key.
class VirtualNode final {
 public:
  using allocator_type = utils::Allocator<VirtualNode>;
  using label_list = utils::pmr::vector<utils::pmr::string>;
  using property_map = utils::pmr::unordered_map<storage::PropertyId, storage::PropertyValue>;
  using key_set = utils::pmr::vector<storage::PropertyId>;
  using hidden_keys = key_set;

  // A node carrying no projection reference belongs to no derive() projection (a synthetic node, or
  // an overlay node built before provenance was wired). Overlay nodes from one derive() site share a
  // single non-negative reference into the result's projection-schema table.
  static constexpr int64_t kNoProjectionRef = -1;

  VirtualNode(label_list labels, property_map properties, allocator_type alloc = {},
              std::optional<VertexAccessor> origin = std::nullopt,
              std::shared_ptr<const PropertyBinding> schema = nullptr)
      : gid_(NextSyntheticGid()),
        impl_(std::make_unique<Impl>(std::move(labels), std::move(properties), std::move(origin), std::move(schema),
                                     std::nullopt, alloc)) {}

  VirtualNode(const VirtualNode &other, allocator_type alloc)
      : gid_(other.gid_),
        impl_(std::make_unique<Impl>(other.impl_->labels, other.impl_->properties, other.impl_->origin,
                                     other.impl_->schema, other.impl_->handle, alloc)) {}

  VirtualNode(VirtualNode &&other, allocator_type alloc)
      : gid_(other.gid_),
        impl_(std::make_unique<Impl>(std::move(other.impl_->labels), std::move(other.impl_->properties),
                                     std::move(other.impl_->origin), std::move(other.impl_->schema),
                                     other.impl_->handle, alloc)) {}

  VirtualNode(const VirtualNode &other) : VirtualNode(other, other.impl_->labels.get_allocator()) {}

  VirtualNode(VirtualNode &&) noexcept = default;

  VirtualNode &operator=(const VirtualNode &other) {
    if (this != &other) {
      DMG_ASSERT(impl_ && other.impl_, "Assignment to/from moved-from VirtualNode");
      gid_ = other.gid_;
      *impl_ = *other.impl_;
    }
    return *this;
  }

  VirtualNode &operator=(VirtualNode &&) = default;
  ~VirtualNode() = default;

  [[nodiscard]] auto Gid() const noexcept -> storage::Gid { return gid_; }

  [[nodiscard]] auto CypherId() const noexcept -> int64_t { return gid_.AsInt(); }

  [[nodiscard]] auto Labels() const noexcept -> const label_list & { return impl_->labels; }

  [[nodiscard]] auto HasOrigin() const noexcept -> bool { return impl_->origin.has_value(); }

  // The import handle: the integer a user passed to virtualNode(), used at list assembly to wire
  // edge endpoints to this node by reference. It is not the node's identity (that is the synthetic
  // gid) and is never serialized. A node built by derive() carries none.
  [[nodiscard]] auto Handle() const noexcept -> std::optional<int64_t> { return impl_->handle; }

  void SetHandle(std::optional<int64_t> handle) noexcept { impl_->handle = handle; }

  [[nodiscard]] auto Origin() const noexcept -> const std::optional<VertexAccessor> & { return impl_->origin; }

  // True if this node references a projection-schema entry (set for overlay nodes from a derive()
  // whose schema is statically known). The reference is the schema table key carried on the wire.
  [[nodiscard]] auto HasProjectionRef() const noexcept -> bool { return impl_->schema && impl_->schema->HasRef(); }

  [[nodiscard]] auto ProjectionRef() const noexcept -> int64_t {
    return impl_->schema ? impl_->schema->Ref() : kNoProjectionRef;
  }

  // A hidden key is invisible to reads and to function calls over the node, regardless of whether
  // the origin or the overlay holds a value for it. A synthetic node (no schema) hides nothing.
  [[nodiscard]] auto IsHidden(storage::PropertyId key) const noexcept -> bool {
    return impl_->schema && impl_->schema->IsHidden(key);
  }

  // An overlay-bound key reads from and writes to this node's overlay store. A key is overlay-bound
  // if it holds an overlay value here (a construction-time override, or a value set on the node) or
  // the schema's propertyPolicy bound it 'overlay'; every other key on an overlay node is
  // origin-bound. Read source and write target are coupled to one store per key, so this single
  // predicate decides both. A synthetic node has no origin, so its writes always hit the overlay
  // regardless of this predicate.
  [[nodiscard]] auto IsOverlayBound(storage::PropertyId key) const noexcept -> bool {
    return impl_->properties.find(key) != impl_->properties.end() ||
           (impl_->schema && impl_->schema->IsOverlayBoundByPolicy(key));
  }

  // Read, in the VertexAccessor shape: a single call site reads a property from either a real
  // vertex or a projected node. Hidden keys yield null; an overlay key shadows the origin;
  // otherwise the read falls through to the origin lazily under the given view (never cached);
  // otherwise null. An overlay-only read cannot fail but still returns a Result for one uniform
  // shape, and an origin read-through forwards both the view and the origin's Result.
  [[nodiscard]] auto GetProperty(storage::View view, storage::PropertyId key) const
      -> storage::Result<storage::PropertyValue> {
    if (IsHidden(key)) return storage::PropertyValue{};
    if (const auto it = impl_->properties.find(key); it != impl_->properties.end()) return it->second;
    if (impl_->origin) return impl_->origin->GetProperty(view, key);
    return storage::PropertyValue{};
  }

  // The bare read used by callers without a view (the procedure and function paths): the origin is
  // read under the latest transaction view, and an origin failure throws as before.
  [[nodiscard]] auto GetProperty(storage::PropertyId key) const -> storage::PropertyValue {
    auto maybe_value = GetProperty(storage::View::NEW, key);
    if (!maybe_value.has_value()) {
      throw QueryRuntimeException("Reading a property of a projected node's origin failed.");
    }
    return std::move(*maybe_value);
  }

  // Write: a synthetic node (no origin) always writes its overlay. On an overlay node, an
  // overlay-bound key writes the overlay (compute-only, never persisted); an origin-bound or
  // undeclared key persists to the origin vertex, and any stale overlay entry for that key is
  // cleared so a later read does not shadow the just-persisted value.
  void SetProperty(storage::PropertyId key, storage::PropertyValue value) {
    if (impl_->origin && !IsOverlayBound(key)) {
      auto result = impl_->origin->SetProperty(key, value);
      if (!result.has_value()) {
        throw QueryRuntimeException("Writing a property to a projected node's origin failed.");
      }
      impl_->properties.erase(key);
      return;
    }
    impl_->properties.insert_or_assign(key, std::move(value));
  }

  void RemoveProperty(storage::PropertyId key) {
    if (impl_->origin && !IsOverlayBound(key)) {
      auto result = impl_->origin->RemoveProperty(key);
      if (!result.has_value()) {
        throw QueryRuntimeException("Removing a property from a projected node's origin failed.");
      }
      impl_->properties.erase(key);
      return;
    }
    impl_->properties.erase(key);
  }

  void ClearProperties() { impl_->properties.clear(); }

  // Merged view in the VertexAccessor shape: origin properties (read lazily under the given view)
  // with overlay keys shadowing them, hidden keys omitted. Returned by value because the merge is
  // not stored - origin properties are never copied into the overlay. An origin read-through
  // forwards both the view and the origin's Result.
  [[nodiscard]] auto Properties(storage::View view) const -> storage::Result<property_map> {
    property_map merged{impl_->properties.get_allocator()};
    if (impl_->origin) {
      auto maybe_props = impl_->origin->Properties(view);
      if (!maybe_props.has_value()) return std::unexpected{maybe_props.error()};
      for (auto &[id, value] : *maybe_props) {
        if (!IsHidden(id)) merged.insert_or_assign(id, std::move(value));
      }
    }
    for (const auto &[id, value] : impl_->properties) {
      if (!IsHidden(id)) merged.insert_or_assign(id, value);
    }
    return merged;
  }

  // The bare merged view used by callers without a view (the procedure and function paths): the
  // origin is read under the latest transaction view, and an origin failure throws as before.
  [[nodiscard]] auto Properties() const -> property_map {
    auto maybe_props = Properties(storage::View::NEW);
    if (!maybe_props.has_value()) {
      throw QueryRuntimeException("Reading properties of a projected node's origin failed.");
    }
    return std::move(*maybe_props);
  }

  bool operator==(const VirtualNode &other) const noexcept { return gid_ == other.gid_; }

 private:
  // The origin lives in the heap-allocated Impl, not inline, so a VirtualNode stays small and the
  // mgp_vertex/mgp_edge size budgets that embed it do not grow.
  struct Impl {
    label_list labels;
    property_map properties;
    std::optional<VertexAccessor> origin;
    // The static per-projection binding + ref, shared across the projection's nodes; null for a
    // synthetic node (which hides nothing and binds every overlay-valued key).
    std::shared_ptr<const PropertyBinding> schema;
    std::optional<int64_t> handle;

    Impl(const label_list &lbls, const property_map &props, const std::optional<VertexAccessor> &orig,
         std::shared_ptr<const PropertyBinding> sch, std::optional<int64_t> hndl, allocator_type alloc)
        : labels(lbls, alloc), properties(props, alloc), origin(orig), schema(std::move(sch)), handle(hndl) {}

    Impl(label_list &&lbls, property_map &&props, std::optional<VertexAccessor> &&orig,
         std::shared_ptr<const PropertyBinding> sch, std::optional<int64_t> hndl, allocator_type alloc)
        : labels(std::move(lbls), alloc),
          properties(std::move(props), alloc),
          origin(std::move(orig)),
          schema(std::move(sch)),
          handle(hndl) {}
  };

  storage::Gid gid_;
  std::unique_ptr<Impl> impl_;
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
