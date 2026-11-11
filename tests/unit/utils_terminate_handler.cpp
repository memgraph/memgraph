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

#include <gtest/gtest.h>

#include <csignal>
#include <iostream>

#include "utils/terminate_handler.hpp"

namespace {
[[noreturn]] void FatalAs() {
  std::cerr << "fatal-message";
  memgraph::utils::AbortProcess();
}
}  // namespace

// The PID-1 exit path is not unit-testable (a gtest death-test child is never PID 1).
TEST(TerminateHandler, AbortsAfterFlushingMessage) {
  EXPECT_EXIT(FatalAs(), ::testing::KilledBySignal(SIGABRT), "fatal-message");
}
