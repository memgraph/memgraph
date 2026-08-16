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

#include <shared_mutex>
#include <span>

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/indexed_property_decoder.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_info_helpers.hpp"
#include "utils/logging.hpp"

/// The edge's materialising read. See `vertex_accessor_materialise.hpp`; kept out of the accessor
/// header for the same reason.
namespace memgraph::storage {

template <typename Materialiser>
Result<void> EdgeAccessor::ReadPropertyValuesInto(std::span<PropertyId const> properties, View view,
                                                  std::span<PropertyValue> scratch, PropertyPlanMemo &memo,
                                                  Materialiser &out) const {
  DMG_ASSERT(scratch.size() == properties.size(), "Scratch buffer size must match the number of properties");

  // Without properties on edges there is no record to read: `edge_` holds a gid rather than a
  // pointer, so this has to answer before anything dereferences it. Every property is Null, as
  // the single-property read has it.
  if (!storage_->config_.salient.items.properties_on_edges) {
    for (std::size_t index = 0; index != properties.size(); ++index) out.EmitNull(index);
    return {};
  }

  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  // Whether the values already reached `out`, or are still in `scratch` waiting for deltas.
  bool materialised = false;

  {
    auto guard = std::shared_lock{edge_.ptr->lock};
    deleted = edge_.ptr->deleted();
    delta = edge_.ptr->delta();

    auto const decoder = IndexedPropertyDecoder<Edge>{
        .indices = &storage_->indices_, .name_id_mapper = storage_->name_id_mapper_.get(), .entity = edge_.ptr};

    if (delta == nullptr) {
      auto decoding = DecodingMaterialiser{decoder, out};
      edge_.ptr->properties.ExtractPropertiesInto(storage_->manifest_registry(), properties, memo, decoding);
      materialised = true;
    } else {
      edge_.ptr->properties.ExtractPropertyValuesMissingAsNull(storage_->manifest_registry(), properties, scratch);
      for (auto &value : scratch) decoder.DecodeProperty(value);
    }
  }

  if (!materialised) {
    ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, properties, scratch](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Deleted_ActionMethod(deleted),
        Exists_ActionMethod(exists),
        PropertyValues_ActionMethod(properties, scratch)
      });
      // clang-format on
    });

    for (std::size_t index = 0; index != scratch.size(); ++index) out.Emit(index, std::move(scratch[index]));
  }

  if (!exists) return std::unexpected{Error::NONEXISTENT_OBJECT};
  if (!for_deleted_ && deleted) return std::unexpected{Error::DELETED_OBJECT};
  return {};
}

template <typename Materialiser>
Result<void> EdgeAccessor::ReadPropertyValueInto(PropertyId property, View view, PropertyLocationMemo &memo,
                                                 Materialiser &out) const {
  // Without properties on edges `edge_` holds a gid rather than a pointer, so this has to answer
  // before anything dereferences it.
  if (!storage_->config_.salient.items.properties_on_edges) {
    out.EmitNull(0);
    return {};
  }

  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  bool materialised = false;
  auto scratch = PropertyValue{};

  {
    auto guard = std::shared_lock{edge_.ptr->lock};
    deleted = edge_.ptr->deleted();
    delta = edge_.ptr->delta();

    auto const decoder = IndexedPropertyDecoder<Edge>{
        .indices = &storage_->indices_, .name_id_mapper = storage_->name_id_mapper_.get(), .entity = edge_.ptr};

    if (delta == nullptr) {
      auto decoding = DecodingMaterialiser{decoder, out};
      edge_.ptr->properties.ExtractPropertyInto(storage_->manifest_registry(), property, memo, decoding);
      materialised = true;
    } else {
      scratch = edge_.ptr->properties.GetProperty(storage_->manifest_registry(), property, decoder, &memo);
    }
  }

  if (!materialised) {
    auto const properties = std::span{&property, 1};
    auto const values = std::span{&scratch, 1};
    ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, properties, values](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Deleted_ActionMethod(deleted),
        Exists_ActionMethod(exists),
        PropertyValues_ActionMethod(properties, values)
      });
      // clang-format on
    });

    out.Emit(0, std::move(scratch));
  }

  if (!exists) return std::unexpected{Error::NONEXISTENT_OBJECT};
  if (!for_deleted_ && deleted) return std::unexpected{Error::DELETED_OBJECT};
  return {};
}

}  // namespace memgraph::storage
