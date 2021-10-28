// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "stats/metrics.hpp"

#include <thread>

#include "gtest/gtest.h"

using namespace std::chrono_literals;

using namespace stats;

TEST(Metrics, Counter) {
  Counter &x = GetCounter("counter");
  EXPECT_EQ(*x.Flush(), 0);
  EXPECT_EQ(x.Value(), 0);
  x.Bump();
  EXPECT_EQ(*x.Flush(), 1);
  EXPECT_EQ(x.Value(), 1);

  Counter &y = GetCounter("counter");
  EXPECT_EQ(*y.Flush(), 1);
  EXPECT_EQ(y.Value(), 1);

  y.Bump(5);
  EXPECT_EQ(*x.Flush(), 6);
  EXPECT_EQ(x.Value(), 6);
  EXPECT_EQ(*y.Flush(), 6);
  EXPECT_EQ(y.Value(), 6);
}

TEST(Metrics, Gauge) {
  Gauge &x = GetGauge("gauge");
  EXPECT_EQ(*x.Flush(), 0);
  x.Set(1);
  EXPECT_EQ(*x.Flush(), 1);

  Gauge &y = GetGauge("gauge");
  EXPECT_EQ(*y.Flush(), 1);

  x.Set(2);
  EXPECT_EQ(*x.Flush(), 2);
  EXPECT_EQ(*y.Flush(), 2);
}

TEST(Metrics, IntervalMin) {
  IntervalMin &x = GetIntervalMin("min");
  EXPECT_EQ(x.Flush(), std::nullopt);
  x.Add(5);
  x.Add(3);
  EXPECT_EQ(*x.Flush(), 3);
  EXPECT_EQ(x.Flush(), std::nullopt);
  x.Add(3);
  x.Add(5);
  EXPECT_EQ(*x.Flush(), 3);
  EXPECT_EQ(x.Flush(), std::nullopt);
}

TEST(Metrics, IntervalMax) {
  IntervalMax &x = GetIntervalMax("max");
  EXPECT_EQ(x.Flush(), std::nullopt);
  x.Add(5);
  x.Add(3);
  EXPECT_EQ(*x.Flush(), 5);
  EXPECT_EQ(x.Flush(), std::nullopt);
  x.Add(3);
  x.Add(5);
  EXPECT_EQ(*x.Flush(), 5);
  EXPECT_EQ(x.Flush(), std::nullopt);
}

TEST(Metrics, Stopwatch) {
  auto d1 = Stopwatch("stopwatch", [] { std::this_thread::sleep_for(150ms); });
  EXPECT_TRUE(140 <= d1 && d1 <= 160);

  auto d2 = Stopwatch("stopwatch", [] { std::this_thread::sleep_for(300ms); });
  EXPECT_TRUE(290 <= d2 && d2 <= 310);

  Counter &total_time = GetCounter("stopwatch.total_time");
  Counter &count = GetCounter("stopwatch.count");
  IntervalMin &min = GetIntervalMin("stopwatch.min");
  IntervalMax &max = GetIntervalMax("stopwatch.max");

  EXPECT_TRUE(430 <= total_time.Value() && total_time.Value() <= 470);
  EXPECT_EQ(count.Value(), 2);

  auto m = *min.Flush();
  EXPECT_TRUE(140 <= m && m <= 160);

  auto M = *max.Flush();
  EXPECT_TRUE(290 <= M && M <= 310);
}
