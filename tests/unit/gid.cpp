#include <gtest/gtest.h>

#include "storage/distributed/gid.hpp"

TEST(Generator, GeneratedOnCorrectWorker) {
  int my_worker = 775;
  gid::Generator generator(my_worker);
  auto gid = generator.Next();
  EXPECT_EQ(gid::CreatorWorker(gid), my_worker);
}

TEST(Generator, DontReuseIds) {
  gid::Generator generator(0);
  auto gid = generator.Next();
  auto gid_gt = generator.Next();
  EXPECT_NE(gid_gt, gid);
}

TEST(Generator, FromOtherGenerator) {
  gid::Generator generator_0(0);
  gid::Generator generator_1(1);
  for (int i = 0; i < 10; ++i) generator_1.Next();
  auto gid1 = generator_1.Next();
  auto gid = generator_0.Next(gid1);
  EXPECT_EQ(gid::CreatorWorker(gid), 1);
  EXPECT_EQ(gid, gid1);
}

TEST(Generator, FromSameGenerator) {
  gid::Generator generator(0);
  std::vector<gid::Gid> gids;
  for (int i = 0; i < 10; ++i) gids.push_back(generator.Next());
  for (int i = 0; i < 10; ++i) EXPECT_EQ(gids[i], generator.Next(gids[i]));
}

TEST(Generator, AdvanceGenerator) {
  gid::Generator generator_00(0);
  gid::Generator generator_01(0);
  for (int i = 0; i < 10; ++i) generator_00.Next();
  auto gid00 = generator_00.Next();
  auto gid01 = generator_01.Next(gid00);
  EXPECT_EQ(gid00, gid01);
  auto gid10 = generator_00.Next();
  auto gid11 = generator_01.Next();
  EXPECT_EQ(gid10, gid11);
}
