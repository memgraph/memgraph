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
#include "distributed/lamport_clock.hpp"

#include <iostream>

// TODO: replace with doctest / catch2
int main() try {
  namespace mgd = memgraph::distributed;
  struct test_tag;
  auto sut_m1 = mgd::LamportClock<test_tag>{};
  auto sut_m2 = mgd::LamportClock<test_tag>{};
  auto ts1 = sut_m1.get_timestamp(mgd::internal);
  auto ts2 = sut_m1.get_timestamp(mgd::internal);
  if (!(ts1 < ts2)) throw __LINE__;  // NOLINT
  auto ts3 = sut_m1.get_timestamp(mgd::send);
  auto ts4 = sut_m2.get_timestamp(mgd::receive, ts3);
  if (!(ts3 < ts4)) throw __LINE__;  // NOLINT
} catch (int line) {
  std::cerr << "not correct, line: " << line << '\n';
  return EXIT_FAILURE;
}
