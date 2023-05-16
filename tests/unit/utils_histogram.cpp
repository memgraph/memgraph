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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils/event_histogram.hpp"
#include "utils/logging.hpp"

TEST(Histogram, BasicFunctionality) {
  memgraph::metrics::Histogram histo{};

  for (int i = 0; i < 9000; i++) {
    histo.Measure(10);
  }
  for (int i = 0; i < 900; i++) {
    histo.Measure(25);
  }
  for (int i = 0; i < 90; i++) {
    histo.Measure(33);
  }
  for (int i = 0; i < 9; i++) {
    histo.Measure(47);
  }
  histo.Measure(500);

  ASSERT_EQ(histo.Percentile(0.0), 10);
  ASSERT_EQ(histo.Percentile(99.0), 25);
  ASSERT_EQ(histo.Percentile(99.89), 32);
  ASSERT_EQ(histo.Percentile(99.99), 46);
  ASSERT_EQ(histo.Percentile(100.0), 500);

  uint64_t max = std::numeric_limits<uint64_t>::max();
  histo.Measure(max);
  auto observed_max = static_cast<double>(histo.Percentile(100.0));
  auto diff = (max - observed_max) / max;

  ASSERT_NEAR(diff, 0, 0.01);
}
