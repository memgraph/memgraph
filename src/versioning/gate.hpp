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

enum class GateError : uint8_t { kDisabled, kNoLicense, kUnsupportedStorageMode, kCommitLockNarrowing };

// Returns std::nullopt when versioning is allowed. First failure wins, in argument order: flag,
// license, storage mode, commit-lock narrowing.
// Takes primitives so it is callable without Storage or license machinery.
std::optional<GateError> CheckVersioningGate(bool flag_enabled, bool enterprise_valid, bool is_in_memory_transactional,
                                             bool commit_lock_narrowing);

std::string_view GateErrorMessage(GateError err);

}  // namespace memgraph::versioning
