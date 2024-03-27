// Copyright 2024 Memgraph Ltd.
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

#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::flags::run_time {

/**
 * @brief Initialize the run-time flags (must be done before run-time flags are used).
 *
 */
void Initialize();

/**
 * @brief Get the bolt server name value
 *
 * @return std::string
 */
std::string GetServerName();

/**
 * @brief Get the query execution timeout value
 *
 * @return double
 */
double GetExecutionTimeout();

/**
 * @brief Get the cartesian product enabled value
 *
 * @return bool
 */
bool GetCartesianProductEnabled();

}  // namespace memgraph::flags::run_time
