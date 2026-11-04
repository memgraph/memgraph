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

enum class GateError : uint8_t { kDisabled, kNoLicense, kUnsupportedStorageMode, kNotDurable };

// Only kCreate needs the license and durable branches; kObserve needs neither.
enum class GateOperation : uint8_t { kObserve, kCreate };

struct GateState {
  bool flag_enabled;
  bool enterprise_valid;
  bool is_in_memory_transactional;
  bool durable;
};

// Returns std::nullopt when `op` is allowed. First failure wins: flag, license (kCreate only), storage mode,
// durability (kCreate only). The caller refuses replicas afterwards. Takes primitives to stay free of Storage/license.
std::optional<GateError> CheckVersioningGate(GateOperation op, GateState const &state);

std::string_view GateErrorMessage(GateError err);

// Branches survive a restart: WAL on (PERIODIC_SNAPSHOT_WITH_WAL) and --data-recovery-on-startup=true.
bool BranchesDurable(const storage::Config &config);

}  // namespace memgraph::versioning
