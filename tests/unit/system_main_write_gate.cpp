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

#include <chrono>
#include <future>

#include <gtest/gtest.h>

#include "system/main_write_gate.hpp"

using memgraph::system::MainWriteGate;
using namespace std::chrono_literals;

TEST(MainWriteGate, AdmitsWhileOpen) {
  MainWriteGate gate;
  EXPECT_TRUE(gate.TryEnter());
}

TEST(MainWriteGate, RefusesUntilOpenedAgain) {
  MainWriteGate gate;
  ASSERT_TRUE(gate.CloseAndDrain(0ms));
  EXPECT_FALSE(gate.TryEnter());
  gate.Open();
  EXPECT_TRUE(gate.TryEnter());
}

TEST(MainWriteGate, DrainWaitsForAdmittedWrites) {
  MainWriteGate gate;
  auto pass = gate.TryEnter();
  auto drained = std::async(std::launch::async, [&] { return gate.CloseAndDrain(5s); });
  EXPECT_EQ(drained.wait_for(100ms), std::future_status::timeout);
  pass.reset();
  EXPECT_TRUE(drained.get());
}

TEST(MainWriteGate, StaysClosedWhenDrainTimesOut) {
  MainWriteGate gate;
  auto pass = gate.TryEnter();
  EXPECT_FALSE(gate.CloseAndDrain(10ms));
  pass.reset();
  EXPECT_FALSE(gate.TryEnter());
}
