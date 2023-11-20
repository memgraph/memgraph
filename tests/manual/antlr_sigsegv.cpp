// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "query/frontend/opencypher/parser.hpp"
#include "utils/signals.hpp"
#include "utils/stacktrace.hpp"

// This test was introduced because Antlr Cpp runtime doesn't work well in a
// highly concurrent environment. Interpreter `interpret.hpp` contains
// `antlr_lock` used to avoid crashes.
//   v4.6 and before -> Crashes.
//   v4.8            -> Does NOT crash but sometimes this tests does NOT finish.
//                      Looks like a deadlock. -> The lock is still REQUIRED.
//   v4.9            -> Seems to be working.
//   v4.10           -> Seems to be working as well. -> antlr_lock removed

using namespace std::chrono_literals;

TEST(Antlr, Sigsegv) {
  // start clients
  int N = 8;
  std::vector<std::thread> threads;

  std::atomic<bool> run{false};

  threads.reserve(N);
  for (int i = 0; i < N; ++i) {
    threads.emplace_back([&run]() {
      while (!run)
        ;
      while (run) {
        memgraph::query::frontend::opencypher::Parser parser(
            "CREATE (:Label_T7 {x: 903}) CREATE (:Label_T7 {x: 720}) CREATE "
            "(:Label_T7 {x: 13}) CREATE (:Label_T7 {x: 643}) CREATE (:Label_T7 "
            "{x: 245}) CREATE (:Label_T7 {x: 441}) CREATE (:Label_T7 {x: 47}) "
            "CREATE (:Label_T7 {x: 583}) CREATE (:Label_T7 {x: 262}) CREATE "
            "(:Label_T7 {x: 472}) CREATE (:Label_T7 {x: 443}) CREATE "
            "(:Label_T7 {x: 955}) CREATE (:Label_T7 {x: 197}) CREATE "
            "(:Label_T7 {x: 524}) CREATE (:Label_T7 {x: 416}) CREATE "
            "(:Label_T7 {x: 403}) CREATE (:Label_T7 {x: 100}) CREATE "
            "(:Label_T7 {x: 524}) CREATE (:Label_T7 {x: 702}) CREATE "
            "(:Label_T7 {x: 586}) CREATE (:Label_T7 {x: 459}) CREATE "
            "(:Label_T7 {x: 844}) CREATE (:Label_T7 {x: 959}) CREATE "
            "(:Label_T7 {x: 81}) CREATE (:Label_T7 {x: 423}) CREATE (:Label_T7 "
            "{x: 553}) CREATE (:Label_T7 {x: 987}) CREATE (:Label_T7 {x: 579}) "
            "CREATE (:Label_T7 {x: 980}) CREATE (:Label_T7 {x: 869}) CREATE "
            "(:Label_T7 {x: 86}) CREATE (:Label_T7 {x: 521}) CREATE (:Label_T7 "
            "{x: 171}) CREATE (:Label_T7 {x: 161}) CREATE (:Label_T7 {x: 260}) "
            "CREATE (:Label_T7 {x: 857}) CREATE (:Label_T7 {x: 34}) CREATE "
            "(:Label_T7 {x: 969}) CREATE (:Label_T7 {x: 948}) CREATE "
            "(:Label_T7 {x: 519}) CREATE (:Label_T7 {x: 333}) CREATE "
            "(:Label_T7 {x: 733}) CREATE (:Label_T7 {x: 870}) CREATE "
            "(:Label_T7 {x: 129}) CREATE (:Label_T7 {x: 77}) CREATE (:Label_T7 "
            "{x: 54}) CREATE (:Label_T7 {x: 109}) CREATE (:Label_T7 {x: 908}) "
            "CREATE (:Label_T7 {x: 560}) CREATE (:Label_T7 {x: 928}) CREATE "
            "(:Label_T7 {x: 934}) CREATE (:Label_T7 {x: 475}) CREATE "
            "(:Label_T7 {x: 619}) CREATE (:Label_T7 {x: 787}) CREATE "
            "(:Label_T7 {x: 739}) CREATE (:Label_T7 {x: 622}) CREATE "
            "(:Label_T7 {x: 588}) CREATE (:Label_T7 {x: 917}) CREATE "
            "(:Label_T7 {x: 45}) CREATE (:Label_T7 {x: 696}) CREATE (:Label_T7 "
            "{x: 998}) CREATE (:Label_T7 {x: 232}) CREATE (:Label_T7 {x: 600}) "
            "CREATE (:Label_T7 {x: 763}) CREATE (:Label_T7 {x: 511}) CREATE "
            "(:Label_T7 {x: 501}) CREATE (:Label_T7 {x: 555}) CREATE "
            "(:Label_T7 {x: 491}) CREATE (:Label_T7 {x: 896}) CREATE "
            "(:Label_T7 {x: 95}) CREATE (:Label_T7 {x: 361}) CREATE (:Label_T7 "
            "{x: 132}) CREATE (:Label_T7 {x: 637}) CREATE (:Label_T7 {x: 427}) "
            "CREATE (:Label_T7 {x: 37}) CREATE (:Label_T7 {x: 78}) CREATE "
            "(:Label_T7 {x: 405}) CREATE (:Label_T7 {x: 821}) CREATE "
            "(:Label_T7 {x: 186}) CREATE (:Label_T7 {x: 66}) CREATE (:Label_T7 "
            "{x: 450}) CREATE (:Label_T7 {x: 709}) CREATE (:Label_T7 {x: 424}) "
            "CREATE (:Label_T7 {x: 698}) CREATE (:Label_T7 {x: 42}) CREATE "
            "(:Label_T7 {x: 629}) CREATE (:Label_T7 {x: 121}) CREATE "
            "(:Label_T7 {x: 891}) CREATE (:Label_T7 {x: 823}) CREATE "
            "(:Label_T7 {x: 337}) CREATE (:Label_T7 {x: 139}) CREATE "
            "(:Label_T7 {x: 704}) CREATE (:Label_T7 {x: 141}) CREATE "
            "(:Label_T7 {x: 438}) CREATE (:Label_T7 {x: 793}) CREATE "
            "(:Label_T7 {x: 42}) CREATE (:Label_T7 {x: 142}) CREATE (:Label_T7 "
            "{x: 526}) CREATE (:Label_T7 {x: 732}) CREATE (:Label_T7 {x: "
            "336})");
        parser.tree();
      }
    });
  }

  run = true;
  std::this_thread::sleep_for(10ms);
  run = false;

  // cleanup threads
  for (int i = 0; i < N; ++i) threads[i].join();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Signal handling init.
  memgraph::utils::SignalHandler::RegisterHandler(memgraph::utils::Signal::SegmentationFault, []() {
    // Log that we got SIGSEGV and abort the program, because returning from
    // SIGSEGV handler is undefined behaviour.
    std::cerr << "SegmentationFault signal raised" << std::endl;
    std::abort();  // This will continue into our SIGABRT handler.
  });
  memgraph::utils::SignalHandler::RegisterHandler(memgraph::utils::Signal::Abort, []() {
    // Log the stacktrace and let the abort continue.
    memgraph::utils::Stacktrace stacktrace;
    std::cerr << "Abort signal raised" << std::endl << stacktrace.dump() << std::endl;
  });

  return RUN_ALL_TESTS();
}
