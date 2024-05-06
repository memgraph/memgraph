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

#include "gtest/gtest.h"

#include "io/network/endpoint.hpp"
#include "io/network/network_error.hpp"
#include "utils/logging.hpp"

using endpoint_t = memgraph::io::network::Endpoint;

TEST(Endpoint, IPv4) {
  endpoint_t endpoint;

  // test constructor
  endpoint = endpoint_t("127.0.0.1", 12347);
  EXPECT_EQ(endpoint.GetAddress(), "127.0.0.1");
  EXPECT_EQ(endpoint.GetPort(), 12347);
  EXPECT_EQ(endpoint.GetIpFamily(), endpoint_t::IpFamily::IP4);

  // test address invalid
  EXPECT_THROW(endpoint_t("invalid", 12345), memgraph::io::network::NetworkError);
}

TEST(Endpoint, IPv6) {
  endpoint_t endpoint;

  // test constructor
  endpoint = endpoint_t("ab:cd:ef::3", 12347);
  EXPECT_EQ(endpoint.GetAddress(), "ab:cd:ef::3");
  EXPECT_EQ(endpoint.GetPort(), 12347);
  EXPECT_EQ(endpoint.GetIpFamily(), endpoint_t::IpFamily::IP6);

  // test address invalid
  EXPECT_THROW(endpoint_t("::g", 12345), memgraph::io::network::NetworkError);
}
