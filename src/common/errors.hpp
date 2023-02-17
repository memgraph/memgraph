// Copyright 2022 Memgraph Ltd.
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

#include <cstdint>
#include <string_view>

namespace memgraph::common {

enum class ErrorCode : uint8_t {
  SERIALIZATION_ERROR,
  NONEXISTENT_OBJECT,
  DELETED_OBJECT,
  VERTEX_HAS_EDGES,
  PROPERTIES_DISABLED,
  VERTEX_ALREADY_INSERTED,
  // Schema Violations
  SCHEMA_NO_SCHEMA_DEFINED_FOR_LABEL,
  SCHEMA_VERTEX_PROPERTY_WRONG_TYPE,
  SCHEMA_VERTEX_UPDATE_PRIMARY_KEY,
  SCHEMA_VERTEX_UPDATE_PRIMARY_LABEL,
  SCHEMA_VERTEX_SECONDARY_LABEL_IS_PRIMARY,
  SCHEMA_VERTEX_PRIMARY_PROPERTIES_UNDEFINED,
  // Distributed race conditions
  STALE_SHARD_MAP,

  OBJECT_NOT_FOUND,
};

constexpr std::string_view ErrorCodeToString(const ErrorCode code) {
  switch (code) {
    case ErrorCode::SERIALIZATION_ERROR:
      return "SERIALIZATION_ERROR";
    case ErrorCode::NONEXISTENT_OBJECT:
      return "NONEXISTENT_OBJECT";
    case ErrorCode::DELETED_OBJECT:
      return "DELETED_OBJECT";
    case ErrorCode::VERTEX_HAS_EDGES:
      return "VERTEX_HAS_EDGES";
    case ErrorCode::PROPERTIES_DISABLED:
      return "PROPERTIES_DISABLED";
    case ErrorCode::VERTEX_ALREADY_INSERTED:
      return "VERTEX_ALREADY_INSERTED";
    case ErrorCode::SCHEMA_NO_SCHEMA_DEFINED_FOR_LABEL:
      return "SCHEMA_NO_SCHEMA_DEFINED_FOR_LABEL";
    case ErrorCode::SCHEMA_VERTEX_PROPERTY_WRONG_TYPE:
      return "SCHEMA_VERTEX_PROPERTY_WRONG_TYPE";
    case ErrorCode::SCHEMA_VERTEX_UPDATE_PRIMARY_KEY:
      return "SCHEMA_VERTEX_UPDATE_PRIMARY_KEY";
    case ErrorCode::SCHEMA_VERTEX_UPDATE_PRIMARY_LABEL:
      return "SCHEMA_VERTEX_UPDATE_PRIMARY_LABEL";
    case ErrorCode::SCHEMA_VERTEX_SECONDARY_LABEL_IS_PRIMARY:
      return "SCHEMA_VERTEX_SECONDARY_LABEL_IS_PRIMARY";
    case ErrorCode::SCHEMA_VERTEX_PRIMARY_PROPERTIES_UNDEFINED:
      return "SCHEMA_VERTEX_PRIMARY_PROPERTIES_UNDEFINED";
    case ErrorCode::OBJECT_NOT_FOUND:
      return "OBJECT_NOT_FOUND";
    case ErrorCode::STALE_SHARD_MAP:
      return "STALE_SHARD_MAP";
  }
}

}  // namespace memgraph::common
