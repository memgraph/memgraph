// Copyright 2023 Memgraph Ltd.
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

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern utils::Synchronized<std::string, utils::SpinLock> bolt_server_name_;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern std::atomic<double> execution_timeout_sec_;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern std::atomic<bool> cartesian_product_enabled_;

void Initialize();

}  // namespace memgraph::flags::run_time
