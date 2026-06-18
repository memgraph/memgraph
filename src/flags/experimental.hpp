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

namespace memgraph::flags {

// Each bit is an enabled experiment
// old experiments can be reused once code cleanup has happened
enum class Experiments : uint8_t {
  NONE = 0,
  PLANNER_V2 = 1 << 0,
  // Hot/cold tenants (SUSPEND/RESUME DATABASE). This flag is RESTART-ONLY (read once at startup) and MUST
  // be set consistently across every member of a replication cluster. A flag-enabled MAIN with a
  // flag-disabled replica is unsupported: the replica still applies the MAIN's suspend RPCs (so the data
  // streams stay consistent), leaving it holding COLD tenants it cannot surface or RESUME locally; if such
  // a replica is promoted before a restart those tenants are stranded until the next boot (which reheats
  // them HOT). The engine logs a loud warning on promotion in that case — see DbmsHandler::PromoteColdTenants.
  HOT_COLD_DATABASES = 1 << 1,
};

bool AreExperimentsEnabled(Experiments experiments);

auto ReadExperimental(std::string const &) -> Experiments;
void SetExperimental(Experiments const &);
void AppendExperimental(Experiments const &);
auto ValidExperimentalFlag(std::string_view value) -> bool;
auto ValidExperimentalConfig(std::string_view json_config) -> bool;
auto ParseExperimentalConfig(Experiments experiments) -> nlohmann::json;

}  // namespace memgraph::flags
