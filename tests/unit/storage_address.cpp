#include "gtest/gtest.h"

#include "storage/address.hpp"
#include "storage/gid.hpp"

using storage::Address;

TEST(Address, Local) {
  std::string a{"bla"};
  Address<std::string> address(&a);

  EXPECT_TRUE(address.is_local());
  EXPECT_FALSE(address.is_remote());
  EXPECT_EQ(address.local(), &a);
}

TEST(Address, CopyCompare) {
  int a = 12;
  int b = 13;
  Address<int> addr_a{&a};
  EXPECT_EQ(Address<int>{&a}, addr_a);
  EXPECT_FALSE(Address<int>{&b} == addr_a);
}

TEST(Address, Global) {
  uint64_t worker_id{13};
  uint64_t local_id{31};
  auto global_id = gid::Create(worker_id, local_id);
  Address<int> address{global_id};

  EXPECT_TRUE(address.is_remote());
  EXPECT_FALSE(address.is_local());
  EXPECT_EQ(gid::WorkerId(address.global_id()), worker_id);
  EXPECT_EQ(address.global_id(), global_id);
}
