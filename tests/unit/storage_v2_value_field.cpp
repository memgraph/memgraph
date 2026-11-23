// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <string>

#include <gtest/gtest.h>

#include "storage/v2/value_field.hpp"
#include "utils/temporal.hpp"

using memgraph::storage::FieldNamed;
using memgraph::storage::PropertyValue;
using memgraph::storage::ReadField;
using memgraph::storage::ReadFieldNamed;
using memgraph::storage::TemporalData;
using memgraph::storage::TemporalType;

// A name on its own says which field is wanted. What that field is depends on the value it is
// read off, which is why naming one and reading it are two steps.
TEST(ValueField, ADateHasAYear) {
  auto const field = FieldNamed("year");
  ASSERT_TRUE(field.has_value());

  auto const date = PropertyValue{TemporalData{TemporalType::Date, 1'709'596'800'000'000}};
  EXPECT_EQ(ReadField(date, *field), PropertyValue{int64_t{2024}});
}

namespace {
auto Zoned(std::string_view text) {
  return memgraph::utils::ZonedDateTime(memgraph::utils::ParseZonedDateTimeParameters(text));
}
}  // namespace

TEST(ValueField, AZonedDateTimeHasEpochOffsetAndNanosecond) {
  auto const zoned = Zoned("2024-01-02T03:04:05.123456-05:30");
  EXPECT_EQ(ReadFieldNamed(zoned, "epochSeconds"), PropertyValue{int64_t{1'704'184'445}});
  EXPECT_EQ(ReadFieldNamed(zoned, "epochMillis"), PropertyValue{int64_t{1'704'184'445'123}});
  EXPECT_EQ(ReadFieldNamed(zoned, "offset"), PropertyValue{std::string{"-05:30"}});
  EXPECT_EQ(ReadFieldNamed(zoned, "nanosecond"), PropertyValue{int64_t{123'456'000}});
}

// Neo4j prints a zero offset as java.time.ZoneOffset does.
TEST(ValueField, AUtcOffsetIsZ) {
  EXPECT_EQ(ReadFieldNamed(Zoned("2024-01-02T03:04:05Z"), "offset"), PropertyValue{std::string{"Z"}});
}

TEST(ValueField, ANamedZoneOffsetFollowsDaylightSaving) {
  EXPECT_EQ(ReadFieldNamed(Zoned("2024-07-01T12:00:00[Europe/Zagreb]"), "offset"),
            PropertyValue{std::string{"+02:00"}});
}

// Neo4j: epochMillis = Instant::toEpochMilli (floored); epochSeconds = epochMillis / 1000 in Java long
// division (toward zero).
TEST(ValueField, APreEpochInstantMatchesNeo4jRounding) {
  auto const half_second_before = Zoned("1969-12-31T23:59:59.500Z");
  EXPECT_EQ(ReadFieldNamed(half_second_before, "epochSeconds"), PropertyValue{int64_t{0}});
  EXPECT_EQ(ReadFieldNamed(half_second_before, "epochMillis"), PropertyValue{int64_t{-500}});

  auto const half_milli_before = Zoned("1969-12-31T23:59:59.999500Z");
  EXPECT_EQ(ReadFieldNamed(half_milli_before, "epochMillis"), PropertyValue{int64_t{-1}});
}

TEST(ValueField, ALocalTemporalHasNanosecondButNoEpochOrOffset) {
  auto const time = memgraph::utils::LocalTime(memgraph::utils::ParseLocalTimeParameters("03:04:05.123456").first);
  EXPECT_EQ(ReadFieldNamed(time, "nanosecond"), PropertyValue{int64_t{123'456'000}});
  auto const [date, local_time] = memgraph::utils::ParseLocalDateTimeParameters("2024-01-02T03:04:05.123456");
  auto const moment = memgraph::utils::LocalDateTime(date, local_time);
  EXPECT_EQ(ReadFieldNamed(moment, "nanosecond"), PropertyValue{int64_t{123'456'000}});
  EXPECT_EQ(ReadFieldNamed(moment, "epochSeconds"), std::nullopt);
  EXPECT_EQ(ReadFieldNamed(moment, "offset"), std::nullopt);
}
