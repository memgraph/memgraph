// Copyright 2022 Memgraph Ltd.
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

///
/// Physical Execute/Next/Emit Async Architecture Implementation
///
/// The whole new set of possibilities!
///

#include "utils/logging.hpp"

namespace memgraph::query::v2::physical {

struct NonCopyable {
  NonCopyable() = default;
  NonCopyable(const NonCopyable &) = delete;
  NonCopyable &operator=(const NonCopyable &) = delete;
  NonCopyable(NonCopyable &&) = default;
  NonCopyable &operator=(NonCopyable &&) = default;
};

struct Once : private NonCopyable {};

struct ScanAll : private NonCopyable {};

struct Produce : private NonCopyable {};

using OperatorStates = std::variant<Once, ScanAll, Produce>;

void Execute(Once &state) { SPDLOG_INFO("Once"); }

void Execute(ScanAll &state) { SPDLOG_INFO("ScanAll"); }

void Execute(Produce &state) { SPDLOG_INFO("Produce"); }

}  // namespace memgraph::query::v2::physical
