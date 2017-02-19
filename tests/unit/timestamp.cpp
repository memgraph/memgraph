#include <chrono>
#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "utils/datetime/timestamp.hpp"

TEST(TimestampTest, BasicUsage) {
  auto timestamp = Timestamp::now();

  std::cout << timestamp << std::endl;
  std::cout << Timestamp::now() << std::endl;

  std::this_thread::sleep_for(std::chrono::milliseconds(250));

  std::cout << Timestamp::now().to_iso8601() << std::endl;

  ASSERT_GT(Timestamp::now(), timestamp);

  std::cout << std::boolalpha;

  std::cout << (timestamp == Timestamp::now()) << std::endl;

  ASSERT_NE(timestamp, Timestamp::now());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
