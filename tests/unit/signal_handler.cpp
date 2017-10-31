#include "gtest/gtest.h"

#include <iostream>
#include <string>
#include <utility>

#include "utils/signals/handler.hpp"
#include "utils/stacktrace.hpp"

TEST(SignalHandler, SegmentationFaultTest) {
  SignalHandler::RegisterHandler(Signal::SegmentationFault, []() {
    std::cout << "Segmentation Fault" << std::endl;
    Stacktrace stacktrace;
    std::cout << stacktrace.dump() << std::endl;
  });

  std::raise(SIGSEGV);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
