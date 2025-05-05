// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/id_types.hpp"

#include "utils/string.hpp"

namespace memgraph::storage {
#define STORAGE_DEFINE_ID_TYPE_STRING_METHODS(name, parse)               \
  name name::FromString(std::string_view id) { return name{parse(id)}; } \
  std::string name::ToString() const { return std::to_string(id_); }

STORAGE_DEFINE_ID_TYPE_STRING_METHODS(Gid, utils::ParseStringToUint64);
STORAGE_DEFINE_ID_TYPE_STRING_METHODS(LabelId, utils::ParseStringToUint32);
STORAGE_DEFINE_ID_TYPE_STRING_METHODS(PropertyId, utils::ParseStringToUint32);
STORAGE_DEFINE_ID_TYPE_STRING_METHODS(EdgeTypeId, utils::ParseStringToUint32);
}  // namespace memgraph::storage
