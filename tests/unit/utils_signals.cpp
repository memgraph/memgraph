#include "gtest/gtest.h"

#include <iostream>
#include <string>
#include <thread>
#include <utility>

#include "utils/signals.hpp"
#include "utils/stacktrace.hpp"

/**
 * NOTE: The signals used in these tests must be unique because signal handlers
 * installed in one test are preserved during the other tests and you might not
 * get desired results.
 */

TEST(Signals, Handler) {
  ASSERT_TRUE(utils::SignalHandler::RegisterHandler(
      utils::Signal::SegmentationFault, []() {
        std::cout << "Segmentation Fault" << std::endl;
        utils::Stacktrace stacktrace;
        std::cout << stacktrace.dump() << std::endl;
      }));

  std::raise(SIGSEGV);
}

TEST(Signals, Ignore) {
  ASSERT_TRUE(utils::SignalIgnore(utils::Signal::Pipe));
  std::raise(SIGPIPE);
}

/** In this test the signal is ignored from the main process and a signal is
 * raised in a thread. We want to check that the signal really is ignored
 * globally.
 */
TEST(SignalsMultithreaded, Ignore) {
  ASSERT_TRUE(utils::SignalIgnore(utils::Signal::BusError));
  std::thread thread([] { std::raise(SIGBUS); });
  thread.join();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
