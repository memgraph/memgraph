// Copyright 2025 Memgraph Ltd.
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

namespace memgraph::utils::sysinfo {

/**
 * Gets the amount of logical CPU cores. If the information is
 * unavailable an empty value is returned.
 */

std::optional<uint64_t> LogicalCPUCores();

}  // namespace memgraph::utils::sysinfo
