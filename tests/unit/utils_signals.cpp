// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <iostream>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "utils/signals.hpp"
#include "utils/stacktrace.hpp"

/**
 * NOTE: The signals used in these tests must be unique because signal handlers
 * installed in one test are preserved during the other tests and you might not
 * get desired results.
 */

TEST(Signals, Handler) {
  ASSERT_TRUE(memgraph::utils::SignalHandler::RegisterHandler(memgraph::utils::Signal::SegmentationFault, []() {
    std::cout << "Segmentation Fault" << std::endl;
#if !defined(__SANITIZE_THREAD__) && !__has_feature(thread_sanitizer)
    // Creating a stack-trace is not async signal safe, and TSAN
    // complains about it. For tests with TSAN enabled, the cout output
    // above suffices to confirm the test is successful.
    memgraph::utils::Stacktrace stacktrace;
    std::cout << stacktrace.dump() << std::endl;
#endif
  }));

  std::raise(SIGSEGV);
}

TEST(Signals, Ignore) {
  ASSERT_TRUE(memgraph::utils::SignalIgnore(memgraph::utils::Signal::Pipe));
  std::raise(SIGPIPE);
}

/** In this test the signal is ignored from the main process and a signal is
 * raised in a thread. We want to check that the signal really is ignored
 * globally.
 */
TEST(SignalsMultithreaded, Ignore) {
  ASSERT_TRUE(memgraph::utils::SignalIgnore(memgraph::utils::Signal::BusError));
  std::thread thread([] { std::raise(SIGBUS); });
  thread.join();
}
