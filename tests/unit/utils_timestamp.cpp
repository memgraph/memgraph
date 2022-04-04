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

#include <chrono>
#include <iostream>
#include <thread>

#include <gtest/gtest.h>
#include <utils/timestamp.hpp>

TEST(TimestampTest, BasicUsage) {
  auto timestamp = memgraph::utils::Timestamp::Now();

  std::cout << timestamp << std::endl;
  std::cout << memgraph::utils::Timestamp::Now() << std::endl;

  std::this_thread::sleep_for(std::chrono::milliseconds(250));

  std::cout << memgraph::utils::Timestamp::Now().ToIso8601() << std::endl;

  ASSERT_GT(memgraph::utils::Timestamp::Now(), timestamp);

  std::cout << std::boolalpha;

  std::cout << (timestamp == memgraph::utils::Timestamp::Now()) << std::endl;

  ASSERT_NE(timestamp, memgraph::utils::Timestamp::Now());
}
