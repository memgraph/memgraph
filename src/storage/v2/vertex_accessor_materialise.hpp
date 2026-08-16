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

#include "storage/v2/indexed_property_decoder.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "storage/v2/vertex_read_lock.hpp"
#include "utils/logging.hpp"

/// The materialising read, kept out of `vertex_accessor.hpp` so that only a caller who wants it
/// pays for the delta machinery its definition needs.
namespace memgraph::storage {

template <typename Materialiser>
Result<void> VertexAccessor::ReadPropertyValuesInto(std::span<PropertyId const> properties, View view,
                                                    std::span<PropertyValue> scratch, PropertyPlanMemo &memo,
                                                    Materialiser &out) const {
  DMG_ASSERT(scratch.size() == properties.size(), "Scratch buffer size must match the number of properties");

  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  // Whether the values already reached `out`, or are still sitting in `scratch` waiting for
  // deltas to be replayed over them.
  bool materialised = false;

  VertexReadLock read_lock{vertex_};
  {
    auto const guard = read_lock.AcquireLock();
    deleted = vertex_->deleted();
    // Read before the values rather than after, so the choice between the two paths is made on
    // the same state the values are read under. Both sit inside the one lock, so this is the
    // order the existing read has, just observed earlier.
    delta = vertex_->delta();

    auto const decoder = IndexedPropertyDecoder<Vertex>{
        .indices = &storage_->indices_, .name_id_mapper = storage_->name_id_mapper_.get(), .entity = vertex_};

    auto const no_deltas_to_apply =
        delta == nullptr || transaction_->isolation_level == IsolationLevel::READ_UNCOMMITTED;
    if (no_deltas_to_apply) {
      auto decoding = DecodingMaterialiser{decoder, out};
      vertex_->properties.ExtractPropertiesInto(storage_->manifest_registry(), properties, memo, decoding);
      materialised = true;
    } else {
      vertex_->properties.ExtractPropertyValuesMissingAsNull(storage_->manifest_registry(), properties, scratch);
      for (auto &value : scratch) decoder.DecodeProperty(value);
    }
  }

  if (!materialised) {
    auto const useCache = transaction_->UseCache();
    if (useCache) {
      auto const &cache = transaction_->manyDeltasCache;
      if (auto resError = HasError(view, cache, vertex_, for_deleted_); resError) return std::unexpected{*resError};
    }

    auto const n_processed =
        ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, properties, scratch](const Delta &delta) {
          // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Deleted_ActionMethod(deleted),
            Exists_ActionMethod(exists),
            PropertyValues_ActionMethod(properties, scratch)
          });
          // clang-format on
        });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction_->manyDeltasCache;
      cache.StoreExists(view, vertex_, exists);
      cache.StoreDeleted(view, vertex_, deleted);
    }

    for (std::size_t index = 0; index != scratch.size(); ++index) out.Emit(index, std::move(scratch[index]));
  }

  if (!exists) return std::unexpected{Error::NONEXISTENT_OBJECT};
  if (!for_deleted_ && deleted) return std::unexpected{Error::DELETED_OBJECT};
  return {};
}

template <typename Materialiser>
Result<void> VertexAccessor::ReadPropertyValueInto(PropertyId property, View view, PropertyLocationMemo &memo,
                                                   Materialiser &out) const {
  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  bool materialised = false;
  // Only the delta path needs one, and only until the values are handed on.
  auto scratch = PropertyValue{};

  VertexReadLock read_lock{vertex_};
  {
    auto const guard = read_lock.AcquireLock();
    deleted = vertex_->deleted();
    delta = vertex_->delta();

    auto const decoder = IndexedPropertyDecoder<Vertex>{
        .indices = &storage_->indices_, .name_id_mapper = storage_->name_id_mapper_.get(), .entity = vertex_};

    auto const no_deltas_to_apply =
        delta == nullptr || transaction_->isolation_level == IsolationLevel::READ_UNCOMMITTED;
    if (no_deltas_to_apply) {
      auto decoding = DecodingMaterialiser{decoder, out};
      vertex_->properties.ExtractPropertyInto(storage_->manifest_registry(), property, memo, decoding);
      materialised = true;
    } else {
      scratch = vertex_->properties.GetProperty(storage_->manifest_registry(), property, decoder, &memo);
    }
  }

  if (!materialised) {
    auto const useCache = transaction_->UseCache();
    if (useCache) {
      auto const &cache = transaction_->manyDeltasCache;
      if (auto resError = HasError(view, cache, vertex_, for_deleted_); resError) return std::unexpected{*resError};
      // A chain long enough to have been cached is long enough that replaying it again would cost
      // more than the copy handing the cached value over costs.
      if (auto resProperty = cache.GetProperty(view, vertex_, property); resProperty) {
        out.Emit(0, PropertyValue{*resProperty});
        return {};
      }
    }

    auto const properties = std::span{&property, 1};
    auto const values = std::span{&scratch, 1};
    auto const n_processed =
        ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, properties, values](const Delta &delta) {
          // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Deleted_ActionMethod(deleted),
            Exists_ActionMethod(exists),
            PropertyValues_ActionMethod(properties, values)
          });
          // clang-format on
        });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction_->manyDeltasCache;
      cache.StoreExists(view, vertex_, exists);
      cache.StoreDeleted(view, vertex_, deleted);
      cache.StoreProperty(view, vertex_, property, scratch);
    }

    out.Emit(0, std::move(scratch));
  }

  if (!exists) return std::unexpected{Error::NONEXISTENT_OBJECT};
  if (!for_deleted_ && deleted) return std::unexpected{Error::DELETED_OBJECT};
  return {};
}

}  // namespace memgraph::storage
