// Copyright 2021 Memgraph Ltd.
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

#include <json/json.hpp>

namespace telemetry {

// TODO (mferencevic): merge with `utils/sysinfo`

/**
 * This function returs a dictionary containing some basic system information
 * (eg. operating system name, cpu information, memory information, etc.).
 */
const nlohmann::json GetSystemInfo();

}  // namespace telemetry
