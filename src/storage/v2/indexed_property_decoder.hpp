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

#include "indices/indices.hpp"
#include "name_id_mapper.hpp"
#include "property_value.hpp"

namespace memgraph::storage {

template <typename T>
struct IndexedPropertyDecoder {
  Indices *indices;
  NameIdMapper *name_id_mapper;
  T *entity;

  void DecodeProperty(PropertyValue &value) const {
    switch (value.type()) {
      case PropertyValueType::VectorIndexId: {
        value.ValueVectorIndexList() = indices->vector_index_.GetVectorPropertyFromIndex(
            entity, name_id_mapper->IdToName(value.ValueVectorIndexIds()[0]), name_id_mapper);
        break;
      }
      default:
        break;
    }
  }
};

}  // namespace memgraph::storage
