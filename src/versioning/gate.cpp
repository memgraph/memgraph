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

std::optional<GateError> CheckVersioningGate(GateOperation op, GateState const &state) {
  const bool creating = op == GateOperation::kCreate;
  if (!state.flag_enabled) return GateError::kDisabled;
  if (creating && !state.enterprise_valid) return GateError::kNoLicense;
  if (!state.is_in_memory_transactional) return GateError::kUnsupportedStorageMode;
  if (creating && !state.durable) return GateError::kNotDurable;
  return std::nullopt;
}

bool BranchesDurable(const storage::Config &config) {
  return config.durability.recover_on_startup &&
         config.durability.snapshot_wal_mode ==
             storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
}

std::optional<std::string> StartupConfigError(storage::StorageMode mode, const storage::Config &config) {
  const auto error =
      CheckVersioningGate(GateOperation::kCreate,
                          {.flag_enabled = true,
                           .enterprise_valid = true,
                           .is_in_memory_transactional = mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL,
                           .durable = BranchesDurable(config)});
  if (!error) return std::nullopt;
  return std::string{GateErrorMessage(*error)} +
         " Memgraph does not start with --experimental-enabled=versioning and these settings: change them, or start "
         "without the flag.";
}

std::string_view GateErrorMessage(GateError err) {
  using namespace std::string_view_literals;
  switch (err) {
    using enum GateError;
    case kDisabled:
      return "Graph versioning is disabled. Start Memgraph with --experimental-enabled=versioning to use branch queries."sv;
    case kNoLicense:
      return "Graph versioning is an enterprise feature. A valid Memgraph Enterprise License is required."sv;
    case kUnsupportedStorageMode:
      return "Graph versioning requires IN_MEMORY_TRANSACTIONAL storage mode. It is not available in "
             "IN_MEMORY_ANALYTICAL or ON_DISK_TRANSACTIONAL."sv;
    case kNotDurable:
      return "Graph versioning requires the WAL (PERIODIC_SNAPSHOT_WITH_WAL) and --data-recovery-on-startup=true."sv;
  }
  return "unknown"sv;
}

}  // namespace memgraph::versioning
