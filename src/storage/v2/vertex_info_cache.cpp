// Copyright 2024 Memgraph Ltd.
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

#include "storage/v2/property_value.hpp"
#include "utils/flag_validation.hpp"

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(delta_chain_cache_threshold, 128,
                        "The threshold for when to cache long delta chains. This is used for heavy read + write "
                        "workloads where repeated processing of delta chains can become costly.",
                        { return value > 0; });

namespace memgraph::storage {

/// Helpers to reduce typo errors
template <typename Ret, typename Func, typename... Keys>
auto FetchHelper(VertexInfoCache const &caches, Func &&getCache, View view, Keys &&...keys)
    -> std::optional<std::conditional_t<std::is_trivially_copyable_v<Ret>, Ret, std::reference_wrapper<Ret const>>> {
  auto const &cache = (view == View::OLD) ? getCache(caches.old_) : getCache(caches.new_);
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
  auto &cache = (view == View::OLD) ? getCache(caches.old_) : getCache(caches.new_);
  using key_type = typename std::remove_cvref_t<decltype(cache)>::key_type;
  cache.emplace(key_type{std::forward<Keys>(keys)...}, std::forward<Value>(value));
}

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
  new_.existsCache_.erase(vertex);
  new_.deletedCache_.erase(vertex);
  new_.labelCache_.erase(vertex);
  new_.propertiesCache_.erase(vertex);
  new_.inDegreeCache_.erase(vertex);
  new_.outDegreeCache_.erase(vertex);

  // aggressive cache invalidation, TODO: be smarter
  new_.hasLabelCache_.clear();
  new_.propertyValueCache_.clear();
  new_.inEdgesCache_.clear();
  new_.outEdgesCache_.clear();
}

auto VertexInfoCache::GetLabels(View view, Vertex const *vertex) const
    -> std::optional<std::reference_wrapper<label_set const>> {
  return FetchHelper<label_set>(*this, std::mem_fn(&VertexInfoCache::Caches::labelCache_), view, vertex);
}
void VertexInfoCache::StoreLabels(View view, Vertex const *vertex, const label_set &res) {
  Store(res, *this, std::mem_fn(&Caches::labelCache_), view, vertex);
}
auto VertexInfoCache::GetHasLabel(View view, Vertex const *vertex, LabelId label) const -> std::optional<bool> {
  return FetchHelper<bool>(*this, std::mem_fn(&Caches::hasLabelCache_), view, vertex, label);
}
void VertexInfoCache::StoreHasLabel(View view, Vertex const *vertex, LabelId label, bool res) {
  Store(res, *this, std::mem_fn(&Caches::hasLabelCache_), view, vertex, label);
}
void VertexInfoCache::Invalidate(Vertex const *vertex, LabelId label) {
  new_.labelCache_.erase(vertex);
  new_.hasLabelCache_.erase(std::tuple{vertex, label});
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
  new_.propertiesCache_.erase(vertex);
  new_.propertyValueCache_.erase(std::tuple{vertex, property_key});
}

auto VertexInfoCache::GetInEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex,
                                 const std::vector<EdgeTypeId> &edge_types) const
    -> std::optional<std::reference_wrapper<EdgeStore const>> {
  return FetchHelper<EdgeStore>(*this, std::mem_fn(&Caches::inEdgesCache_), view, src_vertex, dst_vertex, edge_types);
}
void VertexInfoCache::StoreInEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex,
                                   std::vector<EdgeTypeId> edge_types, EdgeStore in_edges) {
  Store(std::move(in_edges), *this, std::mem_fn(&Caches::inEdgesCache_), view, src_vertex, dst_vertex,
        std::move(edge_types));
}
auto VertexInfoCache::GetOutEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex,
                                  const std::vector<EdgeTypeId> &edge_types) const
    -> std::optional<std::reference_wrapper<const EdgeStore>> {
  return FetchHelper<EdgeStore>(*this, std::mem_fn(&Caches::outEdgesCache_), view, src_vertex, dst_vertex, edge_types);
}
void VertexInfoCache::StoreOutEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex,
                                    std::vector<EdgeTypeId> edge_types, EdgeStore out_edges) {
  Store(std::move(out_edges), *this, std::mem_fn(&Caches::outEdgesCache_), view, src_vertex, dst_vertex,
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
  // EdgeTypeId is currently unused but could be used to be more precise in future
  if (direction == EdgeDirection::IN) {
    new_.inDegreeCache_.erase(vertex);
    new_.inEdgesCache_.clear();  // TODO: be more precise
  } else {
    new_.outDegreeCache_.erase(vertex);
    new_.outEdgesCache_.clear();  // TODO: be more precise
  }
}

void VertexInfoCache::Clear() {
  old_.Clear();
  new_.Clear();
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
  std::sort(edge_types_.begin(), edge_types_.end());
}
}  // namespace memgraph::storage
