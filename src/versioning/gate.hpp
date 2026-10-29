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

namespace memgraph::versioning {

enum class GateError : uint8_t { kDisabled, kNoLicense, kUnsupportedStorageMode, kNotDurable, kCommitLockNarrowing };

// What the caller is about to do. Without a valid license branches are read-only and droppable, so only creating a
// branch (and writing on one, checked where the plan's read/write type is known) needs the license; creating also
// needs durable branches. Every other branch query (SHOW, USE, USING, DROP) is kObserve.
enum class GateOperation : uint8_t { kObserve, kCreate };

struct GateState {
  bool flag_enabled;
  bool enterprise_valid;
  bool is_in_memory_transactional;
  // WAL on (PERIODIC_SNAPSHOT_WITH_WAL) and --data-recovery-on-startup=true.
  bool durable;
  bool commit_lock_narrowing;
};

// Returns std::nullopt when `op` is allowed. First failure wins, in the order of the spec's prerequisites: flag,
// license (kCreate only), storage mode, durability (kCreate only), commit-lock narrowing. The replica refusal is
// last and checked by the caller.
// Takes primitives so it is callable without Storage or license machinery.
std::optional<GateError> CheckVersioningGate(GateOperation op, GateState const &state);

std::string_view GateErrorMessage(GateError err);

}  // namespace memgraph::versioning
