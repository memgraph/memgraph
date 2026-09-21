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

#include "versioning/gate.hpp"

namespace memgraph::versioning {

std::optional<GateError> CheckVersioningGate(bool flag_enabled, bool enterprise_valid, bool is_in_memory_transactional,
                                             bool wal_enabled) {
  if (!flag_enabled) return GateError::kDisabled;
  if (!enterprise_valid) return GateError::kNoLicense;
  if (!is_in_memory_transactional) return GateError::kUnsupportedStorageMode;
  if (!wal_enabled) return GateError::kWalDisabled;
  return std::nullopt;
}

std::string_view GateErrorMessage(GateError err) {
  using namespace std::string_view_literals;
  switch (err) {
    using enum GateError;
    case kDisabled:
      return "Versioning is disabled. Start Memgraph with --versioning-enabled=true to use branch queries."sv;
    case kNoLicense:
      return "Graph versioning is an enterprise feature. A valid Memgraph Enterprise License is required."sv;
    case kUnsupportedStorageMode:
      return "Graph versioning requires IN_MEMORY_TRANSACTIONAL storage mode. It is not available in "
             "IN_MEMORY_ANALYTICAL or ON_DISK_TRANSACTIONAL."sv;
    case kWalDisabled:
      return "Graph versioning requires durability with WAL enabled (--storage-wal-enabled=true). Enable "
             "periodic snapshots + WAL to use branches."sv;
  }
  return "unknown"sv;
}

}  // namespace memgraph::versioning
