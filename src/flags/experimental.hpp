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
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <string_view>

#include "gflags/gflags.h"

// Short help flag.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(experimental_enabled);

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(experimental_config);

// Standalone experimental bool flag (not part of the --experimental-enabled=x,y,z bitmask above):
// gates IP-1 "parkable Prepare via C++20 coroutine" at every level -- the storage-side wake hook
// (installed only when on), the Bolt entry point, the acquire coroutine, and the shutdown drain.
// Default ON; off restores the synchronous blocking acquire exactly.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(experimental_coro_prepare_accessor_yield);

namespace memgraph::flags {

/// Single source of truth for the flag's default. BOTH the gflag definition (flags/experimental.cpp)
/// and the cached-atomic initialiser (flags/run_time_configurable.cpp) must use this and nothing
/// else: as independently hardcoded literals they desynced once, and a process that never calls
/// RefreshCoroPrepareAccessorYieldEnabled (unit tests, embedded uses) then disagrees with the server.
constexpr bool kCoroPrepareAccessorYieldDefault = true;

// Each bit is an enabled experiment
// old experiments can be reused once code cleanup has happened
enum class Experiments : uint8_t {
  NONE = 0,
  PLANNER_V2 = 1 << 0,
};

bool AreExperimentsEnabled(Experiments experiments);

auto ReadExperimental(std::string const &) -> Experiments;
void SetExperimental(Experiments const &);
void AppendExperimental(Experiments const &);
auto ValidExperimentalFlag(std::string_view value) -> bool;
auto ValidExperimentalConfig(std::string_view json_config) -> bool;
auto ParseExperimentalConfig(Experiments experiments) -> nlohmann::json;

}  // namespace memgraph::flags
