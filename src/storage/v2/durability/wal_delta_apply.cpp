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

#include "storage/v2/durability/wal_delta_apply.hpp"

#include <functional>
#include <shared_mutex>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/exceptions.hpp"

import memgraph.utils.fnv;

namespace memgraph::storage {

size_t EdgeSetPropertyCacheKeyHash::operator()(EdgeSetPropertyCacheKey const &key) const {
  return utils::HashCombine<uint64_t, uint64_t>{}(key.edge_gid, key.delta_timestamp);
}

namespace {

// Ported verbatim from InMemoryReplicationHandlers::ReadAndApplyDeltasSingleTxn's WalVertexCreate arm
// (S3b extraction -- see wal_delta_apply.hpp file header).
void ApplyVertexCreate(ReplicationAccessor *accessor, durability::WalVertexCreate const &data,
                       uint64_t current_delta_idx) {
  auto const gid = data.gid.AsUint();
  spdlog::trace("  Delta {}. Create vertex {}", current_delta_idx, gid);
  if (!accessor->CreateVertexEx(data.gid)) {
    throw utils::BasicException("Vertex with gid {} already exists at replica.", gid);
  }
}

void ApplyVertexDelete(ReplicationAccessor *accessor, durability::WalVertexDelete const &data,
                       uint64_t current_delta_idx) {
  auto const gid = data.gid.AsUint();
  spdlog::trace("  Delta {}. Delete vertex {}", current_delta_idx, gid);
  auto vertex = accessor->FindVertex(data.gid, View::NEW);
  if (!vertex) {
    throw utils::BasicException("Vertex with gid {} couldn't be found while trying to delete vertex.", gid);
  }
  auto ret = accessor->DeleteVertex(&*vertex);
  if (!ret || !ret.value()) {
    throw utils::BasicException("Deleting vertex with gid {} failed.", gid);
  }
}

void ApplyVertexAddLabel(ReplicationAccessor *accessor, durability::WalVertexAddLabel const &data,
                         uint64_t current_delta_idx) {
  auto const gid = data.gid.AsUint();
  spdlog::trace("   Delta {}. Vertex {} add label {}", current_delta_idx, gid, data.label);
  auto vertex = accessor->FindVertex(data.gid, View::NEW);
  if (!vertex) {
    throw utils::BasicException("Couldn't find vertex {} when adding label.", gid);
  }
  auto ret = vertex->AddLabel(accessor->NameToLabel(data.label));
  if (!ret || !ret.value()) {
    throw utils::BasicException("Failed to add label to vertex {}.", gid);
  }
}

void ApplyVertexRemoveLabel(ReplicationAccessor *accessor, durability::WalVertexRemoveLabel const &data,
                            uint64_t current_delta_idx) {
  auto const gid = data.gid.AsUint();
  spdlog::trace("   Delta {}. Vertex {} remove label {}", current_delta_idx, gid, data.label);
  auto vertex = accessor->FindVertex(data.gid, View::NEW);
  if (!vertex) throw utils::BasicException("Failed to find vertex {} when removing label.", gid);
  auto ret = vertex->RemoveLabel(accessor->NameToLabel(data.label));
  if (!ret || !ret.value()) {
    throw utils::BasicException("Failed to remove label from vertex {}.", gid);
  }
}

void ApplyVertexSetProperty(ReplicationAccessor *accessor, NameIdMapper *mapper,
                            durability::WalVertexSetProperty const &data, uint64_t current_delta_idx) {
  auto const gid = data.gid.AsUint();
  spdlog::trace("   Delta {}. Vertex {} set property", current_delta_idx, gid);
  auto vertex = accessor->FindVertex(data.gid, View::NEW);
  if (!vertex) {
    throw utils::BasicException("Failed to find vertex {} when setting property.", gid);
  }
  auto ret = vertex->SetProperty(accessor->NameToProperty(data.property), ToPropertyValue(data.value, mapper));
  if (!ret) {
    throw utils::BasicException("Failed to set property label from vertex {}.", gid);
  }
}

void ApplyEdgeCreate(ReplicationAccessor *accessor, durability::WalEdgeCreate const &data, uint64_t delta_timestamp,
                     uint64_t current_delta_idx, EdgeSetPropertyCache &edge_set_property_cache) {
  auto const edge_gid = data.gid.AsUint();
  auto const from_vertex_gid = data.from_vertex.AsUint();
  auto const to_vertex_gid = data.to_vertex.AsUint();
  spdlog::trace("   Delta {}. Create edge {} of type {} from vertex {} to vertex {}",
                current_delta_idx,
                edge_gid,
                data.edge_type,
                from_vertex_gid,
                to_vertex_gid);
  auto from_vertex = accessor->FindVertex(data.from_vertex, View::NEW);
  if (!from_vertex) {
    throw utils::BasicException("Failed to find vertex {} when adding edge {}.", from_vertex_gid, edge_gid);
  }
  auto to_vertex = accessor->FindVertex(data.to_vertex, View::NEW);
  if (!to_vertex) {
    throw utils::BasicException("Failed to find vertex {} when adding edge {}.", to_vertex_gid, edge_gid);
  }
  auto edge_result =
      accessor->CreateEdgeEx(&*from_vertex, &*to_vertex, accessor->NameToEdgeType(data.edge_type), data.gid);
  if (!edge_result) {
    throw utils::BasicException(
        "Failed to add edge {} between vertices {} and {}.", edge_gid, from_vertex_gid, to_vertex_gid);
  }
  // Pre-fill cache for subsequent SET_PROPERTY on this edge in the same transaction.
  auto &edge = *edge_result;
  EdgeSetPropertyCacheKey key{.edge_gid = edge_gid, .delta_timestamp = delta_timestamp};
  edge_set_property_cache.emplace(key, edge);
}

void ApplyEdgeDelete(ReplicationAccessor *accessor, durability::WalEdgeDelete const &data, uint64_t current_delta_idx) {
  auto const edge_gid = data.gid.AsUint();
  auto const from_vertex_gid = data.from_vertex.AsUint();
  auto const to_vertex_gid = data.to_vertex.AsUint();
  spdlog::trace("   Delta {}. Delete edge {} of type {} from vertex {} to vertex {}",
                current_delta_idx,
                edge_gid,
                data.edge_type,
                from_vertex_gid,
                to_vertex_gid);
  auto from_vertex = accessor->FindVertex(data.from_vertex, View::NEW);
  if (!from_vertex) {
    throw utils::BasicException("Failed to find vertex {} when deleting edge {}.", from_vertex_gid, edge_gid);
  }
  auto to_vertex = accessor->FindVertex(data.to_vertex, View::NEW);
  if (!to_vertex) {
    throw utils::BasicException("Failed to find vertex {} when deleting edge {}.", from_vertex_gid, edge_gid);
  }
  auto edgeType = accessor->NameToEdgeType(data.edge_type);
  auto edge = accessor->FindEdge(data.gid, View::NEW, edgeType, &*from_vertex, &*to_vertex);
  if (!edge) {
    throw utils::BasicException("Couldn't find edge {} when deleting edge.", edge_gid);
  }
  if (auto ret = accessor->DeleteEdge(&*edge); !ret.has_value()) {
    throw utils::BasicException(
        "Failed to delete edge {} between vertices {} and {}.", edge_gid, from_vertex_gid, to_vertex_gid);
  }
}

void ApplyEdgeSetProperty(ReplicationAccessor *accessor, InMemoryStorage *storage, NameIdMapper *mapper,
                          durability::WalEdgeSetProperty const &data, uint64_t delta_timestamp,
                          uint64_t current_delta_idx, EdgeSetPropertyCache &edge_set_property_cache,
                          FindEdgeFallback const &find_edge_fallback) {
  auto const edge_gid = data.gid.AsUint();
  spdlog::trace("   Delta {}. Edge {} set property (from_gid={} to_gid={})",
                current_delta_idx,
                edge_gid,
                data.from_gid.has_value() ? static_cast<int64_t>(data.from_gid->AsUint()) : -1,
                data.to_gid.has_value() ? static_cast<int64_t>(data.to_gid->AsUint()) : -1);
  if (!storage->config_.salient.items.properties_on_edges)
    throw utils::BasicException(
        "Can't set properties on edges because properties on edges "
        "are disabled!");

  EdgeSetPropertyCacheKey const cache_key{.edge_gid = edge_gid, .delta_timestamp = delta_timestamp};

  // Fast path: use cached edge accessor.
  auto it = edge_set_property_cache.find(cache_key);
  if (it != edge_set_property_cache.end()) {
    auto ret = it->second.SetProperty(accessor->NameToProperty(data.property), ToPropertyValue(data.value, mapper));
    if (!ret) {
      throw utils::BasicException("Setting property on edge {} failed.", edge_gid);
    }
    return;
  }

  // Resolve EdgeInfo using the best available WAL data (newest to oldest format).
  // Edge is alive by WAL ordering — a SET_PROPERTY delta proves it was not GC-collectable
  // at main-side commit.
  // Case 1 (newest WAL): from_gid + to_gid + edge_type → type-filtered out_edges scan via
  //                      accessor->FindEdge (public), fastest. Light and heavy edges both supported.
  // Case 2:              from_gid only → find_edge_fallback(gid, from_gid), which the caller
  //                      implements via InMemoryStorage's private FindEdge(gid, from_gid) --
  //                      handles both light and heavy edges internally. O(log V + deg).
  // Case 3 (oldest WAL): gid only → find_edge_fallback(gid, nullopt), which the caller implements
  //                      via InMemoryStorage's private FindEdge(gid) -- handles both light and
  //                      heavy edges internally. Full scan fallback.
  const auto cached_edge_info = std::invoke([&]() -> FindEdgeResult {
    if (data.from_gid.has_value() && data.to_gid.has_value() && data.edge_type.has_value() &&
        *data.to_gid != kInvalidGid && !data.edge_type->empty()) {
      auto to_v = accessor->FindVertex(*data.to_gid, View::NEW);
      if (!to_v)
        throw utils::BasicException("Failed to find to vertex {} when setting edge property.", data.to_gid->AsUint());
      auto from_v = accessor->FindVertex(*data.from_gid, View::NEW);
      if (!from_v)
        throw utils::BasicException("Failed to find from vertex {} when setting edge property.",
                                    data.from_gid->AsUint());
      auto const edge_type_id = accessor->NameToEdgeType(*data.edge_type);
      auto found = accessor->FindEdge(data.gid, View::NEW, edge_type_id, &*from_v, &*to_v);
      if (!found) {
        throw utils::BasicException("Failed to find edge {} when setting edge property.", edge_gid);
      }
      return FindEdgeResult{std::in_place, found->edge_, found->edge_type_, found->from_vertex_, found->to_vertex_};
    } else if (data.from_gid.has_value()) {
      auto info = find_edge_fallback(data.gid, *data.from_gid);
      if (!info)
        throw utils::BasicException(
            "Failed to find edge {} from vertex {} when setting edge property.", edge_gid, data.from_gid->AsUint());
      return info;
    } else {
      auto info = find_edge_fallback(data.gid, std::nullopt);
      if (!info) throw utils::BasicException("Failed to find edge {} when setting edge property.", edge_gid);
      return info;
    }
  });

  auto const &[er, et, fv, tv] = *cached_edge_info;
  auto *edge_raw = er.ptr;
  {
    bool is_visible = true;
    Delta *local_delta = nullptr;
    {
      auto guard = std::shared_lock{edge_raw->lock};
      is_visible = !edge_raw->deleted();
      local_delta = edge_raw->delta();
    }
    ApplyDeltasForRead(&accessor->GetTransaction(), local_delta, View::NEW, [&is_visible](const Delta &delta) {
      switch (delta.action) {
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          break;
        case Delta::Action::RECREATE_OBJECT: {
          is_visible = true;
          break;
        }
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
        case Delta::Action::DELETE_OBJECT: {
          is_visible = false;
          break;
        }
      }
    });
    if (!is_visible) {
      throw utils::BasicException("Edge {} isn't visible when setting edge property.", edge_gid);
    }
  }
  EdgeAccessor ea{er, et, fv, tv, storage, &accessor->GetTransaction()};
  edge_set_property_cache.emplace(cache_key, ea);  // Fast edge accessor lookup cache
  auto ret = ea.SetProperty(accessor->NameToProperty(data.property), ToPropertyValue(data.value, mapper));
  if (!ret) {
    throw utils::BasicException("Setting property on edge {} failed.", edge_gid);
  }
}

}  // namespace

void ApplyWalDataDelta(ReplicationAccessor *accessor, InMemoryStorage *storage, durability::WalDeltaData const &delta,
                       uint64_t delta_timestamp, uint64_t current_delta_idx,
                       EdgeSetPropertyCache &edge_set_property_cache, FindEdgeFallback const &find_edge_fallback) {
  using namespace durability;  // NOLINT (google-build-using-namespace) -- matches the original
                               // ReadAndApplyDeltasSingleTxn dispatch this was extracted from.
  auto *mapper = storage->name_id_mapper_.get();

  if (auto const *data = std::get_if<WalVertexCreate>(&delta.data_)) {
    ApplyVertexCreate(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalVertexDelete>(&delta.data_)) {
    ApplyVertexDelete(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalVertexAddLabel>(&delta.data_)) {
    ApplyVertexAddLabel(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalVertexRemoveLabel>(&delta.data_)) {
    ApplyVertexRemoveLabel(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalVertexSetProperty>(&delta.data_)) {
    ApplyVertexSetProperty(accessor, mapper, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalEdgeCreate>(&delta.data_)) {
    ApplyEdgeCreate(accessor, *data, delta_timestamp, current_delta_idx, edge_set_property_cache);
    return;
  }
  if (auto const *data = std::get_if<WalEdgeDelete>(&delta.data_)) {
    ApplyEdgeDelete(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalEdgeSetProperty>(&delta.data_)) {
    ApplyEdgeSetProperty(accessor,
                         storage,
                         mapper,
                         *data,
                         delta_timestamp,
                         current_delta_idx,
                         edge_set_property_cache,
                         find_edge_fallback);
    return;
  }

  // Unreachable in practice: callers (replica-apply today; window-replay from S3d) only invoke this
  // function for the eight data-plane delta kinds handled above. Framing (WalTransactionStart/End)
  // and schema/index/constraint/enum/TTL/description deltas are dispatched by the caller itself --
  // see the file header comment in wal_delta_apply.hpp for why.
  throw utils::BasicException("ApplyWalDataDelta called with a non-data-plane WAL delta.");
}

}  // namespace memgraph::storage
