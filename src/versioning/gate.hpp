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

enum class GateError : uint8_t { kDisabled, kNoLicense, kUnsupportedStorageMode, kWalDisabled };

// Pure predicate for whether graph versioning (branches) may be used, given the current
// startup/runtime configuration expressed as primitives. Returns std::nullopt when versioning
// is ALLOWED. Checked in this precedence (first failure wins):
//   1. flag_enabled == false             -> kDisabled
//   2. enterprise_valid == false         -> kNoLicense
//   3. is_in_memory_transactional==false -> kUnsupportedStorageMode
//   4. wal_enabled == false              -> kWalDisabled
// Deliberately takes primitives (not Storage/StorageMode/license singletons) so callers (and unit
// tests) can invoke it without any Storage/DB/license machinery; chunk 7 computes these four bools
// from the real singletons/Storage at the call site.
std::optional<GateError> CheckVersioningGate(bool flag_enabled, bool enterprise_valid, bool is_in_memory_transactional,
                                             bool wal_enabled);

std::string_view GateErrorMessage(GateError err);

}  // namespace memgraph::versioning
