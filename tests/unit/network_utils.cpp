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
