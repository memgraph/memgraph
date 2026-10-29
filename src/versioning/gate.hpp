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

#include <cstdint>
#include <optional>
#include <string_view>

#include "storage/v2/config.hpp"

namespace memgraph::versioning {

enum class GateError : uint8_t { kDisabled, kNoLicense, kUnsupportedStorageMode, kNotDurable, kCommitLockNarrowing };

// Unlicensed branches are read and drop only, so only kCreate needs the license (and durable branches); branch writes
// are checked in AccessorCompliance. SHOW, USE, USING and DROP are kObserve.
enum class GateOperation : uint8_t { kObserve, kCreate };

struct GateState {
  bool flag_enabled;
  bool enterprise_valid;
  bool is_in_memory_transactional;
  bool durable;
  bool commit_lock_narrowing;
};

// Returns std::nullopt when `op` is allowed. First failure wins: flag, license (kCreate only), storage mode,
// durability (kCreate only), commit-lock narrowing. The replica refusal is checked by the caller afterwards.
// Takes primitives so it is callable without Storage or license machinery.
std::optional<GateError> CheckVersioningGate(GateOperation op, GateState const &state);

std::string_view GateErrorMessage(GateError err);

// Branches survive a restart: WAL on (PERIODIC_SNAPSHOT_WITH_WAL) and --data-recovery-on-startup=true.
bool BranchesDurable(const storage::Config &config);

}  // namespace memgraph::versioning
