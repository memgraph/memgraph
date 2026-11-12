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

#include "storage/v2/vertex_info_cache.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "utils/flag_validation.hpp"
#include "utils/small_vector.hpp"

#include <boost/unordered/unordered_flat_map.hpp>
#include "absl/hash/hash.h"

#include <algorithm>
#include <functional>
#include <optional>
#include <span>

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(delta_chain_cache_threshold, 128,
                        "The threshold for when to cache long delta chains. This is used for heavy read + write "
                        "workloads where repeated processing of delta chains can become costly.",
                        { return value > 0; });

namespace memgraph::storage {

/// Note: not a tuple because need a canonical form for the edge types
struct VertexInfoCache::EdgeKey {
  EdgeKey(Vertex const *src_vertex, Vertex const *dst_vertex, std::vector<EdgeTypeId> edge_types);

  friend bool operator==(EdgeKey const &, EdgeKey const &) = default;

  template <typename H>
  friend H AbslHashValue(H h, EdgeKey const &key) {
    return H::combine(std::move(h), key.src_vertex_, key.dst_vertex_, key.edge_types_);
  }

 private:
  Vertex const *src_vertex_;
  Vertex const *dst_vertex_;
  std::vector<EdgeTypeId> edge_types_;  // TODO: use a SBO set type
};

struct VertexInfoCache::Caches {
  void Clear();

  template <typename K, typename V>
  using map = boost::unordered_flat_map<K, V, absl::Hash<K>>;

  // TODO: Why are exists + deleted separate, is there a good reason?
  map<Vertex const *, bool> existsCache_;
  map<Vertex const *, bool> deletedCache_;
  map<std::tuple<Vertex const *, LabelId>, bool> hasLabelCache_;
  map<std::tuple<Vertex const *, PropertyId>, PropertyValue> propertyValueCache_;
  map<Vertex const *, VertexKey> labelCache_;
  map<Vertex const *, std::map<PropertyId, PropertyValue>> propertiesCache_;
  // TODO: nest keys (edge_types) -> (src+dst) -> EdgeStore
  map<EdgeKey, EdgeStore> inEdgesCache_;
  // TODO: nest keys (edge_types) -> (src+dst) -> EdgeStore
  map<EdgeKey, EdgeStore> outEdgesCache_;
  map<Vertex const *, size_t> inDegreeCache_;
  map<Vertex const *, size_t> outDegreeCache_;
};

struct VertexInfoCache::Impl {
  Caches old_;
  Caches new_;
};

/// Helpers to reduce typo errors
template <typename Ret, typename Func, typename... Keys>
auto FetchHelper(VertexInfoCache const &caches, Func &&getCache, View view, Keys &&...keys)
    -> std::optional<std::conditional_t<std::is_trivially_copyable_v<Ret>, Ret, std::reference_wrapper<Ret const>>> {
  if (!caches.impl_) return std::nullopt;
  auto const &cache = (view == View::OLD) ? getCache(caches.impl_->old_) : getCache(caches.impl_->new_);
  // check empty first, cheaper than the relative cost of doing an actual hash + find
  if (cache.empty()) return std::nullopt;

  // defer building the key, maybe a cost at construction
  using key_type = typename std::remove_cvref_t<decltype(cache)>::key_type;
  auto const it = cache.find(key_type{std::forward<Keys>(keys)...});
  if (it == cache.end()) return std::nullopt;

  if constexpr (std::is_trivially_copyable_v<Ret>) {
    return {it->second};
  } else {
    // ensure we return a reference
    return std::cref(it->second);
  }
}

template <typename Value, typename Func, typename... Keys>
void Store(Value &&value, VertexInfoCache &caches, Func &&getCache, View view, Keys &&...keys) {
  if (!caches.impl_) caches.impl_ = std::make_unique<VertexInfoCache::Impl>();
  auto &cache = (view == View::OLD) ? getCache(caches.impl_->old_) : getCache(caches.impl_->new_);
  using key_type = typename std::remove_cvref_t<decltype(cache)>::key_type;
  cache.emplace(key_type{std::forward<Keys>(keys)...}, std::forward<Value>(value));
}

VertexInfoCache::VertexInfoCache() = default;
VertexInfoCache::~VertexInfoCache() = default;
VertexInfoCache::VertexInfoCache(VertexInfoCache &&) noexcept = default;
VertexInfoCache &VertexInfoCache::operator=(VertexInfoCache &&) noexcept = default;

auto VertexInfoCache::GetExists(View view, Vertex const *vertex) const -> std::optional<bool> {
  return FetchHelper<bool>(*this, std::mem_fn(&Caches::existsCache_), view, vertex);
}

void VertexInfoCache::StoreExists(View view, Vertex const *vertex, bool res) {
  Store(res, *this, std::mem_fn(&Caches::existsCache_), view, vertex);
}

auto VertexInfoCache::GetDeleted(View view, Vertex const *vertex) const -> std::optional<bool> {
  return FetchHelper<bool>(*this, std::mem_fn(&Caches::deletedCache_), view, vertex);
}

void VertexInfoCache::StoreDeleted(View view, Vertex const *vertex, bool res) {
  Store(res, *this, std::mem_fn(&Caches::deletedCache_), view, vertex);
}

void VertexInfoCache::Invalidate(Vertex const *vertex) {
  if (!impl_) return;
  auto &fresh = impl_->new_;
  fresh.existsCache_.erase(vertex);
  fresh.deletedCache_.erase(vertex);
  fresh.labelCache_.erase(vertex);
  fresh.propertiesCache_.erase(vertex);
  fresh.inDegreeCache_.erase(vertex);
  fresh.outDegreeCache_.erase(vertex);

  // aggressive cache invalidation, TODO: be smarter
  fresh.hasLabelCache_.clear();
  fresh.propertyValueCache_.clear();
  fresh.inEdgesCache_.clear();
  fresh.outEdgesCache_.clear();
}

auto VertexInfoCache::GetLabels(View view, Vertex const *vertex) const
    -> std::optional<std::reference_wrapper<VertexKey const>> {
  return FetchHelper<VertexKey>(*this, std::mem_fn(&VertexInfoCache::Caches::labelCache_), view, vertex);
}

void VertexInfoCache::StoreLabels(View view, Vertex const *vertex, std::span<LabelId const> res) {
  Store(res, *this, std::mem_fn(&Caches::labelCache_), view, vertex);
}

auto VertexInfoCache::GetHasLabel(View view, Vertex const *vertex, LabelId label) const -> std::optional<bool> {
  return FetchHelper<bool>(*this, std::mem_fn(&Caches::hasLabelCache_), view, vertex, label);
}

void VertexInfoCache::StoreHasLabel(View view, Vertex const *vertex, LabelId label, bool res) {
  Store(res, *this, std::mem_fn(&Caches::hasLabelCache_), view, vertex, label);
}

void VertexInfoCache::Invalidate(Vertex const *vertex, LabelId label) {
  if (!impl_) return;
  auto &fresh = impl_->new_;
  fresh.labelCache_.erase(vertex);
  fresh.hasLabelCache_.erase(std::tuple{vertex, label});
}

auto VertexInfoCache::GetProperty(View view, Vertex const *vertex, PropertyId property) const
    -> std::optional<std::reference_wrapper<PropertyValue const>> {
  return FetchHelper<PropertyValue>(*this, std::mem_fn(&Caches::propertyValueCache_), view, vertex, property);
}

void VertexInfoCache::StoreProperty(View view, Vertex const *vertex, PropertyId property, PropertyValue value) {
  Store(std::move(value), *this, std::mem_fn(&Caches::propertyValueCache_), view, vertex, property);
}

auto VertexInfoCache::GetProperties(View view, Vertex const *vertex) const
    -> std::optional<std::reference_wrapper<std::map<PropertyId, PropertyValue> const>> {
  return FetchHelper<std::map<PropertyId, PropertyValue>>(*this, std::mem_fn(&Caches::propertiesCache_), view, vertex);
}

void VertexInfoCache::StoreProperties(View view, Vertex const *vertex, std::map<PropertyId, PropertyValue> properties) {
  Store(std::move(properties), *this, std::mem_fn(&Caches::propertiesCache_), view, vertex);
}

void VertexInfoCache::Invalidate(Vertex const *vertex, PropertyId property_key) {
  if (!impl_) return;
  auto &fresh = impl_->new_;
  fresh.propertiesCache_.erase(vertex);
  fresh.propertyValueCache_.erase(std::tuple{vertex, property_key});
}

auto VertexInfoCache::GetInEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex,
                                 const std::vector<EdgeTypeId> &edge_types) const
    -> std::optional<std::reference_wrapper<EdgeStore const>> {
  return FetchHelper<EdgeStore>(*this, std::mem_fn(&Caches::inEdgesCache_), view, src_vertex, dst_vertex, edge_types);
}

void VertexInfoCache::StoreInEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex,
                                   std::vector<EdgeTypeId> edge_types, EdgeStore in_edges) {
  Store(std::move(in_edges),
        *this,
        std::mem_fn(&Caches::inEdgesCache_),
        view,
        src_vertex,
        dst_vertex,
        std::move(edge_types));
}

auto VertexInfoCache::GetOutEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex,
                                  const std::vector<EdgeTypeId> &edge_types) const
    -> std::optional<std::reference_wrapper<const EdgeStore>> {
  return FetchHelper<EdgeStore>(*this, std::mem_fn(&Caches::outEdgesCache_), view, src_vertex, dst_vertex, edge_types);
}

void VertexInfoCache::StoreOutEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex,
                                    std::vector<EdgeTypeId> edge_types, EdgeStore out_edges) {
  Store(std::move(out_edges),
        *this,
        std::mem_fn(&Caches::outEdgesCache_),
        view,
        src_vertex,
        dst_vertex,
        std::move(edge_types));
}

auto VertexInfoCache::GetInDegree(View view, Vertex const *vertex) const -> std::optional<std::size_t> {
  return FetchHelper<std::size_t>(*this, std::mem_fn(&Caches::inDegreeCache_), view, vertex);
}

void VertexInfoCache::StoreInDegree(View view, Vertex const *vertex, std::size_t in_degree) {
  Store(in_degree, *this, std::mem_fn(&Caches::inDegreeCache_), view, vertex);
}

auto VertexInfoCache::GetOutDegree(View view, Vertex const *vertex) const -> std::optional<std::size_t> {
  return FetchHelper<std::size_t>(*this, std::mem_fn(&Caches::outDegreeCache_), view, vertex);
}

void VertexInfoCache::StoreOutDegree(View view, Vertex const *vertex, std::size_t out_degree) {
  Store(out_degree, *this, std::mem_fn(&Caches::outDegreeCache_), view, vertex);
}

void VertexInfoCache::Invalidate(Vertex const *vertex, EdgeTypeId /*unused*/, EdgeDirection direction) {
  if (!impl_) return;
  auto &fresh = impl_->new_;
  // EdgeTypeId is currently unused but could be used to be more precise in future
  if (direction == EdgeDirection::IN) {
    fresh.inDegreeCache_.erase(vertex);
    fresh.inEdgesCache_.clear();  // TODO: be more precise
  } else {
    fresh.outDegreeCache_.erase(vertex);
    fresh.outEdgesCache_.clear();  // TODO: be more precise
  }
}

void VertexInfoCache::Clear() {
  if (!impl_) return;
  impl_->old_.Clear();
  impl_->new_.Clear();
}

void VertexInfoCache::Caches::Clear() {
  existsCache_.clear();
  deletedCache_.clear();
  hasLabelCache_.clear();
  propertyValueCache_.clear();
  labelCache_.clear();
  propertiesCache_.clear();
  inEdgesCache_.clear();
  outEdgesCache_.clear();
  inDegreeCache_.clear();
  outDegreeCache_.clear();
}

VertexInfoCache::EdgeKey::EdgeKey(Vertex const *src_vertex, Vertex const *dst_vertex,
                                  std::vector<EdgeTypeId> edge_types)
    : src_vertex_{src_vertex}, dst_vertex_{dst_vertex}, edge_types_{std::move(edge_types)} {
  // needed for a canonical form
  std::ranges::sort(edge_types_);
}
}  // namespace memgraph::storage
