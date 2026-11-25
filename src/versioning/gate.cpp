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

std::expected<void, GateError> CheckVersioningGate(GateOperation op, GateState const &state) {
  const bool creating = op == GateOperation::kCreate;
  if (!state.flag_enabled) return std::unexpected(GateError::kDisabled);
  if (creating && !state.enterprise_valid) return std::unexpected(GateError::kNoLicense);
  if (!state.is_in_memory_transactional) return std::unexpected(GateError::kUnsupportedStorageMode);
  return {};
}

bool BranchesDurable(const storage::Config &config) {
  return config.durability.recover_on_startup &&
         config.durability.snapshot_wal_mode ==
             storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
}

std::expected<void, std::string> ValidateStartupConfig(storage::StorageMode mode, const storage::Config &config) {
  constexpr std::string_view kSuffix =
      " Memgraph does not start with --experimental-enabled=versioning and these settings: change them, or start "
      "without the flag.";
  const auto error =
      CheckVersioningGate(GateOperation::kCreate,
                          {.flag_enabled = true,
                           .enterprise_valid = true,
                           .is_in_memory_transactional = mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL});
  if (!error) return std::unexpected(std::string{GateErrorMessage(error.error())} + std::string{kSuffix});
  if (!BranchesDurable(config)) {
    return std::unexpected(std::string{"Graph versioning requires the WAL (PERIODIC_SNAPSHOT_WITH_WAL) and "
                                       "--data-recovery-on-startup=true."} +
                           std::string{kSuffix});
  }
  return {};
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
  }
  return "unknown"sv;
}

}  // namespace memgraph::versioning
