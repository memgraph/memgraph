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

#include <map>

#include "query/exceptions.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "versioning/branch_engine.hpp"

namespace memgraph::query {

[[noreturn]] inline void ThrowReserved(const char *what) {
  throw QueryRuntimeException(
      "Names starting with '__mg_' are reserved for versioning and cannot be used as a {} on a branch.", what);
}

inline void RejectReservedProperties(versioning::BranchContext &ctx,
                                     const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  for (const auto &[pid, _] : properties) {
    if (ctx.IsReservedProperty(pid)) ThrowReserved("property");
  }
}

}  // namespace memgraph::query
