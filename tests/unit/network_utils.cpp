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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "io/network/utils.hpp"

using namespace io::network;

TEST(ResolveHostname, Simple) {
  auto result = ResolveHostname("localhost");
  EXPECT_TRUE(result == "127.0.0.1" || result == "::1");
}

TEST(ResolveHostname, PassThroughIpv4) {
  auto result = ResolveHostname("127.0.0.1");
  EXPECT_EQ(result, "127.0.0.1");
}

TEST(ResolveHostname, PassThroughIpv6) {
  auto result = ResolveHostname("::1");
  EXPECT_EQ(result, "::1");
}
