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

#include <concepts>
#include <cstddef>
#include <utility>

#include "edge.hpp"
#include "indices/indices.hpp"
#include "name_id_mapper.hpp"
#include "property_value.hpp"
#include "vertex.hpp"

namespace memgraph::storage {

template <typename T>
struct IndexedPropertyDecoder {
  Indices *indices;
  NameIdMapper *name_id_mapper;
  T *entity;

  void DecodeProperty(PropertyValue &value) const {
    switch (value.type()) {
      case PropertyValueType::VectorIndexId: {
        DMG_ASSERT(!value.ValueVectorIndexIds().empty(), "VectorIndexId property has no index IDs");
        if constexpr (std::is_same_v<T, Vertex>) {
          value.ValueVectorIndexList() = indices->vector_index_.GetVectorPropertyFromIndex(
              entity, name_id_mapper->IdToName(value.ValueVectorIndexIds()[0]), name_id_mapper);
        } else if constexpr (std::is_same_v<T, Edge>) {
          value.ValueVectorIndexList() = indices->vector_edge_index_.GetVectorPropertyFromEdgeIndex(
              entity, name_id_mapper->IdToName(value.ValueVectorIndexIds()[0]), name_id_mapper);
        }
        break;
      }
      default:
        break;
    }
  }
};

/// A materialiser that resolves what a record only holds a handle to, and hands everything else
/// on untouched.
///
/// A read that builds values into the caller's own type has the same obligation as one that
/// produces a storage value: a property living in an index is a handle in the record, and the
/// caller must be given the value, not the handle. Only a value arriving as a `PropertyValue`
/// can be one, so every type the shape describes outright is forwarded without a test.
///
/// Wraps the record's values alone. A value replayed from a delta chain is already what it was
/// when it was written, and resolving it would answer with what the index holds now.
template <typename T, typename Materialiser>
class DecodingMaterialiser {
 public:
  DecodingMaterialiser(IndexedPropertyDecoder<T> decoder, Materialiser &out) : decoder_{decoder}, out_{&out} {}

  void EmitNull(std::size_t index) { out_->EmitNull(index); }

  template <typename Value>
    requires(!std::same_as<std::remove_cvref_t<Value>, PropertyValue>)
  void Emit(std::size_t index, Value &&value) {
    out_->Emit(index, std::forward<Value>(value));
  }

  void Emit(std::size_t index, PropertyValue &&value) {
    decoder_.DecodeProperty(value);
    out_->Emit(index, std::move(value));
  }

 private:
  IndexedPropertyDecoder<T> decoder_;
  Materialiser *out_;
};

}  // namespace memgraph::storage
