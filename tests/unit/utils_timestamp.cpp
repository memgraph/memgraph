#include <chrono>
#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "utils/timestamp.hpp"

TEST(TimestampTest, BasicUsage) {
  auto timestamp = utils::Timestamp::Now();

  std::cout << timestamp << std::endl;
  std::cout << utils::Timestamp::Now() << std::endl;

  std::this_thread::sleep_for(std::chrono::milliseconds(250));

  std::cout << utils::Timestamp::Now().ToIso8601() << std::endl;

  ASSERT_GT(utils::Timestamp::Now(), timestamp);

  std::cout << std::boolalpha;

  std::cout << (timestamp == utils::Timestamp::Now()) << std::endl;

  ASSERT_NE(timestamp, utils::Timestamp::Now());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
