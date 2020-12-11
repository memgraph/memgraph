#include <iostream>

#include "gtest/gtest.h"

#include "io/network/endpoint.hpp"
#include "io/network/network_error.hpp"

using endpoint_t = io::network::Endpoint;

TEST(Endpoint, IPv4) {
  endpoint_t endpoint;

  // test constructor
  endpoint = endpoint_t("127.0.0.1", 12347);
  EXPECT_EQ(endpoint.address, "127.0.0.1");
  EXPECT_EQ(endpoint.port, 12347);
  EXPECT_EQ(endpoint.family, endpoint_t::IpFamily::IP4);

  // test address invalid
  EXPECT_DEATH(endpoint_t("invalid", 12345), "address");
}

TEST(Endpoint, IPv6) {
  endpoint_t endpoint;

  // test constructor
  endpoint = endpoint_t("ab:cd:ef::3", 12347);
  EXPECT_EQ(endpoint.address, "ab:cd:ef::3");
  EXPECT_EQ(endpoint.port, 12347);
  EXPECT_EQ(endpoint.family, endpoint_t::IpFamily::IP6);

  // test address invalid
  EXPECT_DEATH(endpoint_t("::g", 12345), "address");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
