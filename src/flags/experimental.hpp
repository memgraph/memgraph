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
// gates the storage-side wake hook AND the query/session parking (HandlePrepareCoro,
// Session::RunLoop/DrivePreparedRun, Interpreter::PrepareCoro) for IP-1 "parkable Prepare via C++20
// coroutine" -- see opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md.
//
// Default ON. Release-path cost, stated to match Storage::NotifyMainLockReleased() exactly: TWO
// relaxed atomic loads when on and nobody is parked (the flag, then the waiter count), ONE when the
// flag is off. No mutex is touched in either case.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(experimental_coro_prepare_accessor_yield);

namespace memgraph::flags {

/// Single source of truth for --experimental-coro-prepare-accessor-yield's default. BOTH the gflag
/// definition (flags/experimental.cpp) and the cached-atomic initialiser
/// (flags/run_time_configurable.cpp) must use this and nothing else. Those two lived as independently
/// hardcoded literals in separate TUs and desynced once already -- the gflag said true while the cache
/// said false, so any process that never calls RefreshCoroPrepareAccessorYieldEnabled (unit tests,
/// embedded uses) silently disagreed with the server, and only review caught it. With one constant the
/// disagreement is not expressible.
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
