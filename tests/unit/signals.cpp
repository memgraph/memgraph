#include "gtest/gtest.h"

#include <iostream>
#include <string>
#include <utility>

#include "utils/signals.hpp"
#include "utils/stacktrace.hpp"

TEST(Signals, Handler) {
  ASSERT_TRUE(utils::SignalHandler::RegisterHandler(
      utils::Signal::SegmentationFault, []() {
        std::cout << "Segmentation Fault" << std::endl;
        Stacktrace stacktrace;
        std::cout << stacktrace.dump() << std::endl;
      }));

  std::raise(SIGSEGV);
}

TEST(Signals, Ignore) {
  ASSERT_TRUE(utils::SignalIgnore(utils::Signal::Pipe));
  std::raise(SIGPIPE);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
