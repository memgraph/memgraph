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
#pragma once

#include "storage/v2/edge_direction.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/tco_vector.hpp"
#include "storage/v2/view.hpp"

#include "absl/container/flat_hash_map.h"

#include <gflags/gflags.h>
#include <tuple>

DECLARE_uint64(delta_chain_cache_threshold);

namespace memgraph::storage {

namespace detail {
// TODO: move to a general C++ utility header
template <typename T>
using optref = std::optional<std::reference_wrapper<T>>;
}  // namespace detail

// forward declarations
struct Vertex;
struct Transaction;
class PropertyValue;

/** For vertices with long delta chains, its possible that its expensive
 * to rebuild state for the relevant transaction. This cache is used to
 * store information that would be expensive to repeatedly rebuild.
 *
 * The cache is assumed to be used in reference to a given:
 * - transaction_id
 * - command_id
 * - only for View::OLD
 */
struct VertexInfoCache final {
  VertexInfoCache() = default;
  ~VertexInfoCache() = default;

  // By design would be a mistake to copy the cache
  VertexInfoCache(VertexInfoCache const &) = delete;
  VertexInfoCache &operator=(VertexInfoCache const &) = delete;

  VertexInfoCache(VertexInfoCache &&) noexcept;
  VertexInfoCache &operator=(VertexInfoCache &&) noexcept;

  auto GetExists(View view, Vertex const *vertex) const -> std::optional<bool>;

  void StoreExists(View view, Vertex const *vertex, bool res);

  auto GetDeleted(View view, Vertex const *vertex) const -> std::optional<bool>;

  void StoreDeleted(View view, Vertex const *vertex, bool res);

  void Invalidate(Vertex const *vertex);

  auto GetLabels(View view, Vertex const *vertex) const -> detail::optref<std::vector<LabelId> const>;

  void StoreLabels(View view, Vertex const *vertex, std::vector<LabelId> const &res);

  auto GetHasLabel(View view, Vertex const *vertex, LabelId label) const -> std::optional<bool>;

  void StoreHasLabel(View view, Vertex const *vertex, LabelId label, bool res);

  void Invalidate(Vertex const *vertex, LabelId label);

  auto GetProperty(View view, Vertex const *vertex, PropertyId property) const -> detail::optref<PropertyValue const>;

  void StoreProperty(View view, Vertex const *vertex, PropertyId property, PropertyValue value);

  auto GetProperties(View view, Vertex const *vertex) const
      -> detail::optref<std::map<PropertyId, PropertyValue> const>;

  void StoreProperties(View view, Vertex const *vertex, std::map<PropertyId, PropertyValue> properties);

  void Invalidate(Vertex const *vertex, PropertyId property_key);

  using EdgeStore = TcoVector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>>;

  auto GetInEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex,
                  const std::vector<EdgeTypeId> &edge_types) const -> detail::optref<const EdgeStore>;

  void StoreInEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex, std::vector<EdgeTypeId> edge_types,
                    EdgeStore in_edges);

  auto GetOutEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex,
                   const std::vector<EdgeTypeId> &edge_types) const -> detail::optref<const EdgeStore>;

  void StoreOutEdges(View view, Vertex const *src_vertex, Vertex const *dst_vertex, std::vector<EdgeTypeId> edge_types,
                     EdgeStore out_edges);

  auto GetInDegree(View view, Vertex const *vertex) const -> std::optional<std::size_t>;

  void StoreInDegree(View view, Vertex const *vertex, std::size_t in_degree);

  auto GetOutDegree(View view, Vertex const *vertex) const -> std::optional<std::size_t>;

  void StoreOutDegree(View view, Vertex const *vertex, std::size_t out_degree);

  void Invalidate(Vertex const *vertex, EdgeTypeId /*unused*/, EdgeDirection direction);

  void Clear();

 private:
  /// Note: not a tuple because need a canonical form for the edge types
  struct EdgeKey {
    EdgeKey(Vertex const *src_vertex, Vertex const *dst_vertex, std::vector<EdgeTypeId> edge_types);

    friend bool operator==(EdgeKey const &, EdgeKey const &) = default;

    friend bool operator==(EdgeKey const &lhs, std::tuple<Vertex const *, EdgeTypeId> const &rhs) {
      return lhs.src_vertex_ == std::get<0>(rhs) &&
             std::find(lhs.edge_types_.begin(), lhs.edge_types_.end(), std::get<1>(rhs)) != lhs.edge_types_.end();
    }

    template <typename H>
    friend H AbslHashValue(H h, EdgeKey const &key) {
      return H::combine(std::move(h), key.src_vertex_, key.dst_vertex_, key.edge_types_);
    }

   private:
    Vertex const *src_vertex_;
    Vertex const *dst_vertex_;
    std::vector<EdgeTypeId> edge_types_;  // TODO: use a SBO set type
  };

  struct Caches {
    void Clear();

    template <typename K, typename V>
    using map = absl::flat_hash_map<K, V>;

    // TODO: Why are exists + deleted separate, is there a good reason?
    map<Vertex const *, bool> existsCache_;
    map<Vertex const *, bool> deletedCache_;
    map<std::tuple<Vertex const *, LabelId>, bool> hasLabelCache_;
    map<std::tuple<Vertex const *, PropertyId>, PropertyValue> propertyValueCache_;
    map<Vertex const *, std::vector<LabelId>> labelCache_;
    map<Vertex const *, std::map<PropertyId, PropertyValue>> propertiesCache_;
    // TODO: nest keys (edge_types) -> (src+dst) -> EdgeStore
    map<EdgeKey, EdgeStore> inEdgesCache_;
    // TODO: nest keys (edge_types) -> (src+dst) -> EdgeStore
    map<EdgeKey, EdgeStore> outEdgesCache_;
    map<Vertex const *, size_t> inDegreeCache_;
    map<Vertex const *, size_t> outDegreeCache_;
  };
  Caches old_;
  Caches new_;

  // Helpers
  template <typename Ret, typename Func, typename... Keys>
  friend auto FetchHelper(VertexInfoCache const &caches, Func &&getCache, View view, Keys &&...keys)
      -> std::optional<std::conditional_t<std::is_trivially_copyable_v<Ret>, Ret, std::reference_wrapper<Ret const>>>;

  template <typename Value, typename Func, typename... Keys>
  friend void Store(Value &&value, VertexInfoCache &caches, Func &&getCache, View view, Keys &&...keys);
};
}  // namespace memgraph::storage
