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

#include "gflags/gflags.h"

// Short help flag.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(experimental_enabled);

namespace memgraph::flags {

// Each bit is an enabled experiment
// old experiments can be reused once code cleanup has happened
enum class Experiments : uint8_t {
  SYSTEM_REPLICATION = 1 << 0,
  TEXT_SEARCH = 1 << 1,
};

bool AreExperimentsEnabled(Experiments experiments);

void InitializeExperimental();

}  // namespace memgraph::flags
