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

// TODO: replace with doctest / catch2
int main() {
  using namespace memgraph::distributed;
  struct test_tag;
  auto sut_m1 = LamportClock<test_tag>{};
  auto sut_m2 = LamportClock<test_tag>{};
  auto ts1 = sut_m1.get_timestamp(internal);
  auto ts2 = sut_m1.get_timestamp(internal);
  if (!(ts1 < ts2)) throw "not correct";
  auto ts3 = sut_m1.get_timestamp(send);
  auto ts4 = sut_m2.get_timestamp(receive, ts3);
  if (!(ts3 < ts4)) throw "not correct";
}
