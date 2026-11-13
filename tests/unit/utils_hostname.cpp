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

#include <unistd.h>
#include <array>
#include <climits>
#include <string>

#include "utils/system_info.hpp"

TEST(Hostname, MatchesKernelHostname) {
  std::array<char, HOST_NAME_MAX + 1> buffer{};
  ASSERT_EQ(::gethostname(buffer.data(), buffer.size() - 1), 0);
  const std::string expected{buffer.data()};
  if (expected.empty() || expected == "(none)") GTEST_SKIP() << "The kernel hostname isn't set on this machine";
  EXPECT_EQ(memgraph::utils::GetHostname(), expected);
}

TEST(Hostname, IsNeverEmptyOrPadded) {
  const auto hostname = memgraph::utils::GetHostname();
  ASSERT_FALSE(hostname.empty());
  EXPECT_EQ(hostname.find_first_of(" \t\r\n"), std::string::npos) << "hostname: '" << hostname << "'";
}
