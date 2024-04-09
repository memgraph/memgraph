// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>
#include <format>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>

#include <gtest/gtest.h>

#include "utils/exceptions.hpp"
#include "utils/memory.hpp"
#include "utils/temporal.hpp"

namespace {

std::string ToString(const memgraph::utils::DateParameters &date_parameters) {
  return fmt::format("{:04d}-{:02d}-{:02d}", date_parameters.year, date_parameters.month, date_parameters.day);
}

std::string ToString(const memgraph::utils::LocalTimeParameters &local_time_parameters) {
  return fmt::format("{:02}:{:02d}:{:02d}", local_time_parameters.hour, local_time_parameters.minute,
                     local_time_parameters.second);
}

struct TestDateParameters {
  memgraph::utils::DateParameters date_parameters;
  bool should_throw;
};

inline constexpr std::array test_dates{
    TestDateParameters{{-1996, 11, 22}, true}, TestDateParameters{{1996, -11, 22}, true},
    TestDateParameters{{1996, 11, -22}, true}, TestDateParameters{{1, 13, 3}, true},
    TestDateParameters{{1, 12, 32}, true},     TestDateParameters{{1, 2, 29}, true},
    TestDateParameters{{2020, 2, 29}, false},  TestDateParameters{{1700, 2, 29}, true},
    TestDateParameters{{1200, 2, 29}, false},  TestDateParameters{{10000, 12, 3}, true}};

struct TestLocalTimeParameters {
  memgraph::utils::LocalTimeParameters local_time_parameters;
  bool should_throw;
};

inline constexpr std::array test_local_times{TestLocalTimeParameters{{.hour = 24}, true},
                                             TestLocalTimeParameters{{.hour = -1}, true},
                                             TestLocalTimeParameters{{.minute = -1}, true},
                                             TestLocalTimeParameters{{.minute = 60}, true},
                                             TestLocalTimeParameters{{.second = -1}, true},
                                             TestLocalTimeParameters{{.minute = 60}, true},
                                             TestLocalTimeParameters{{.millisecond = -1}, true},
                                             TestLocalTimeParameters{{.millisecond = 1000}, true},
                                             TestLocalTimeParameters{{.microsecond = -1}, true},
                                             TestLocalTimeParameters{{.microsecond = 1000}, true},
                                             TestLocalTimeParameters{{23, 59, 59, 999, 999}, false},
                                             TestLocalTimeParameters{{0, 0, 0, 0, 0}, false}};
}  // namespace

TEST(TemporalTest, DateConstruction) {
  std::optional<memgraph::utils::Date> test_date;

  for (const auto [date_parameters, should_throw] : test_dates) {
    if (should_throw) {
      EXPECT_THROW(test_date.emplace(date_parameters), memgraph::utils::BasicException) << ToString(date_parameters);
    } else {
      EXPECT_NO_THROW(test_date.emplace(date_parameters)) << ToString(date_parameters);
    }
  }
}

TEST(TemporalTest, DateMicrosecondsSinceEpochConversion) {
  const auto check_microseconds = [](const auto date_parameters) {
    memgraph::utils::Date initial_date{date_parameters};
    const auto microseconds = initial_date.MicrosecondsSinceEpoch();
    memgraph::utils::Date new_date{microseconds};
    ASSERT_EQ(initial_date, new_date);
  };

  check_microseconds(memgraph::utils::DateParameters{2020, 11, 22});
  check_microseconds(memgraph::utils::DateParameters{1900, 2, 22});
  check_microseconds(memgraph::utils::DateParameters{0, 1, 1});
  check_microseconds(memgraph::utils::DateParameters{1994, 12, 7});

  ASSERT_THROW(check_microseconds(memgraph::utils::DateParameters{-10, 1, 1}), memgraph::utils::BasicException);

  {
    memgraph::utils::Date date{memgraph::utils::DateParameters{1970, 1, 1}};
    ASSERT_EQ(date.MicrosecondsSinceEpoch(), 0);
  }
  {
    memgraph::utils::Date date{memgraph::utils::DateParameters{1910, 1, 1}};
    ASSERT_LT(date.MicrosecondsSinceEpoch(), 0);
  }
  {
    memgraph::utils::Date date{memgraph::utils::DateParameters{2021, 1, 1}};
    ASSERT_GT(date.MicrosecondsSinceEpoch(), 0);
  }
}

TEST(TemporalTest, LocalTimeConstruction) {
  std::optional<memgraph::utils::LocalTime> test_local_time;

  for (const auto [local_time_parameters, should_throw] : test_local_times) {
    if (should_throw) {
      ASSERT_THROW(test_local_time.emplace(local_time_parameters), memgraph::utils::BasicException);
    } else {
      ASSERT_NO_THROW(test_local_time.emplace(local_time_parameters));
    }
  }
}

TEST(TemporalTest, LocalTimeMicrosecondsSinceEpochConversion) {
  const auto check_microseconds = [](const memgraph::utils::LocalTimeParameters &parameters) {
    memgraph::utils::LocalTime initial_local_time{parameters};
    const auto microseconds = initial_local_time.MicrosecondsSinceEpoch();
    memgraph::utils::LocalTime new_local_time{microseconds};
    ASSERT_EQ(initial_local_time, new_local_time);
  };

  check_microseconds(memgraph::utils::LocalTimeParameters{23, 59, 59, 999, 999});
  check_microseconds(memgraph::utils::LocalTimeParameters{0, 0, 0, 0, 0});
  check_microseconds(memgraph::utils::LocalTimeParameters{14, 8, 55, 321, 452});
}

TEST(TemporalTest, LocalDateTimeMicrosecondsSinceEpochConversion) {
  const auto check_microseconds = [](const memgraph::utils::DateParameters date_parameters,
                                     const memgraph::utils::LocalTimeParameters &local_time_parameters) {
    memgraph::utils::LocalDateTime initial_local_date_time{date_parameters, local_time_parameters};
    const auto microseconds = initial_local_date_time.MicrosecondsSinceEpoch();
    memgraph::utils::LocalDateTime new_local_date_time{microseconds};
    ASSERT_EQ(initial_local_date_time, new_local_date_time);
  };

  check_microseconds(memgraph::utils::DateParameters{2020, 11, 22},
                     memgraph::utils::LocalTimeParameters{0, 0, 0, 0, 0});
  check_microseconds(memgraph::utils::DateParameters{1900, 2, 22}, memgraph::utils::LocalTimeParameters{0, 0, 0, 0, 0});
  check_microseconds(memgraph::utils::DateParameters{0, 1, 1}, memgraph::utils::LocalTimeParameters{0, 0, 0, 0, 0});

  check_microseconds(memgraph::utils::DateParameters{1961, 1, 1}, memgraph::utils::LocalTimeParameters{15, 44, 12});
  check_microseconds(memgraph::utils::DateParameters{1969, 12, 31}, memgraph::utils::LocalTimeParameters{23, 59, 59});
  {
    memgraph::utils::LocalDateTime local_date_time(memgraph::utils::DateParameters{1970, 1, 1},
                                                   memgraph::utils::LocalTimeParameters{0, 0, 0, 0, 0});
    ASSERT_EQ(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
  {
    memgraph::utils::LocalDateTime local_date_time(memgraph::utils::DateParameters{1970, 1, 1},
                                                   memgraph::utils::LocalTimeParameters{0, 0, 0, 0, 1});
    ASSERT_GT(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
  {
    memgraph::utils::LocalTimeParameters local_time_parameters{12, 10, 40, 42, 42};
    memgraph::utils::LocalDateTime local_date_time{memgraph::utils::DateParameters{1970, 1, 1}, local_time_parameters};
    ASSERT_EQ(local_date_time.MicrosecondsSinceEpoch(),
              memgraph::utils::LocalTime{local_time_parameters}.MicrosecondsSinceEpoch());
  }
  {
    memgraph::utils::LocalDateTime local_date_time(memgraph::utils::DateParameters{1910, 1, 1},
                                                   memgraph::utils::LocalTimeParameters{0, 0, 0, 0, 0});
    ASSERT_LT(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
  {
    memgraph::utils::LocalDateTime local_date_time(memgraph::utils::DateParameters{2021, 1, 1},
                                                   memgraph::utils::LocalTimeParameters{0, 0, 0, 0, 0});
    ASSERT_GT(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
  {
    // Assert ordering for dates prior the unix epoch.
    // If this test fails, our storage indexes will be incorrect.
    memgraph::utils::LocalDateTime ldt({1969, 12, 31}, {0, 0, 0});
    memgraph::utils::LocalDateTime ldt2({1969, 12, 31}, {23, 59, 59});
    ASSERT_LT(ldt.MicrosecondsSinceEpoch(), ldt2.MicrosecondsSinceEpoch());
  }
}

TEST(TemporalTest, ZonedDateTimeMicrosecondsSinceEpochConversion) {
  using namespace memgraph::utils;

  const auto date_parameters = DateParameters{2024, 3, 22};
  const auto local_time_parameters = LocalTimeParameters{12, 06, 03, 500, 500};

  const auto local_date_time = LocalDateTime{date_parameters, local_time_parameters};

  std::array timezone_offsets{
      std::chrono::minutes{0},   std::chrono::minutes{60},  std::chrono::minutes{75},  std::chrono::minutes{90},
      std::chrono::minutes{-60}, std::chrono::minutes{-75}, std::chrono::minutes{-90},
  };

  const auto check_conversion = [&date_parameters, &local_time_parameters, &local_date_time](const auto &cases) {
    for (const auto &timezone_offset : cases) {
      const auto zdt = ZonedDateTime({date_parameters, local_time_parameters, Timezone(timezone_offset)});

      EXPECT_EQ(zdt.MicrosecondsSinceEpoch(),
                local_date_time.MicrosecondsSinceEpoch() -
                    std::chrono::duration_cast<std::chrono::microseconds>(timezone_offset).count());
    }
  };

  check_conversion(timezone_offsets);

  const std::array named_timezones{
      std::make_pair("GMT", std::chrono::minutes{0}),
      std::make_pair("Europe/Zagreb", std::chrono::minutes{60}),          // local_date_time in standard time
      std::make_pair("America/Los_Angeles", std::chrono::minutes{-420}),  // local_date_time in daylight saving time
  };

  const auto check_conversion_from_named = [&date_parameters, &local_time_parameters,
                                            &local_date_time](const auto &cases) {
    for (const auto &[timezone_name, timezone_offset] : cases) {
      const auto zdt = ZonedDateTime({date_parameters, local_time_parameters, Timezone(timezone_name)});

      EXPECT_EQ(zdt.MicrosecondsSinceEpoch(),
                local_date_time.MicrosecondsSinceEpoch() -
                    std::chrono::duration_cast<std::chrono::microseconds>(timezone_offset).count());
    }
  };

  check_conversion_from_named(named_timezones);
}

TEST(TemporalTest, AmbiguousZonedDateTimeDescription) {
  // Ambiguity caused by the switch from daylight saving time to standard time (Europe/Zagreb: 3 AM local time on the
  // last Sunday in October)
  // Memgraph chooses the earlier of the two possible instants

  using namespace memgraph::utils;

  const auto ambiguous_timestamp = "2023-10-29T02:30:00[Europe/Zagreb]";
  auto r = ZonedDateTime(ParseZonedDateTimeParameters(ambiguous_timestamp));
  std::cout << r.ToString() << std::endl;
}

TEST(TemporalTest, ZonedDateTimeDescriptionInGap) {
  // Ambiguity caused by the switch from standard time to daylight saving time (Europe/Zagreb: 2 AM local time on the
  // last Sunday in March)
  // Std::chrono adjusts the time to the next valid instant

  using namespace memgraph::utils;

  const auto nonexistent_timestamp = "2024-03-31T02:30:00[Europe/Zagreb]";  // 02:00 → 03:00
  auto s = ZonedDateTime(ParseZonedDateTimeParameters(nonexistent_timestamp));
  std::cout << s.ToString() << std::endl;
}

TEST(TemporalTest, DurationConversion) {
  {
    memgraph::utils::Duration duration{{.minute = 123.25}};
    const auto microseconds = duration.microseconds;
    memgraph::utils::LocalDateTime local_date_time{microseconds};
    ASSERT_EQ(local_date_time.date.year, 1970);
    ASSERT_EQ(local_date_time.date.month, 1);
    ASSERT_EQ(local_date_time.date.day, 1);
    ASSERT_EQ(local_date_time.local_time.hour, 2);
    ASSERT_EQ(local_date_time.local_time.minute, 3);
    ASSERT_EQ(local_date_time.local_time.second, 15);
  };
}

TEST(TemporalTest, LocalDateTimeToDate) {
  memgraph::utils::LocalDateTime local_date_time{memgraph::utils::DateParameters{2020, 11, 22},
                                                 memgraph::utils::LocalTimeParameters{13, 21, 40, 123, 456}};
  memgraph::utils::Date date{local_date_time.date};
  ASSERT_EQ(date.year, 2020);
  ASSERT_EQ(date.month, 11);
  ASSERT_EQ(date.day, 22);
}

TEST(TemporalTest, LocalDateTimeToLocalTime) {
  memgraph::utils::LocalDateTime local_date_time{memgraph::utils::DateParameters{2020, 11, 22},
                                                 memgraph::utils::LocalTimeParameters{13, 21, 40, 123, 456}};
  memgraph::utils::LocalTime local_time{local_date_time.local_time};
  ASSERT_EQ(local_time.hour, 13);
  ASSERT_EQ(local_time.minute, 21);
  ASSERT_EQ(local_time.second, 40);
  ASSERT_EQ(local_time.millisecond, 123);
  ASSERT_EQ(local_time.microsecond, 456);
}

namespace {
using namespace std::literals;
inline constexpr std::array parsing_test_dates_extended{
    std::make_pair("2020-11-22"sv, memgraph::utils::DateParameters{2020, 11, 22}),
    std::make_pair("2020-11"sv, memgraph::utils::DateParameters{2020, 11}),
};

inline constexpr std::array parsing_test_dates_basic{
    std::make_pair("20201122"sv, memgraph::utils::DateParameters{2020, 11, 22})};

inline constexpr std::array parsing_test_local_time_extended{
    std::make_pair("19:23:21.123456"sv, memgraph::utils::LocalTimeParameters{19, 23, 21, 123, 456}),
    std::make_pair("19:23:21.123"sv, memgraph::utils::LocalTimeParameters{19, 23, 21, 123}),
    std::make_pair("19:23:21"sv, memgraph::utils::LocalTimeParameters{19, 23, 21}),
    std::make_pair("19:23"sv, memgraph::utils::LocalTimeParameters{19, 23}),
    std::make_pair("00:00:00.000000"sv, memgraph::utils::LocalTimeParameters{0, 0, 0, 0, 0}),
    std::make_pair("01:02:03.004005"sv, memgraph::utils::LocalTimeParameters{1, 2, 3, 4, 5}),
};

inline constexpr std::array parsing_test_local_time_basic{
    std::make_pair("192321.123456"sv, memgraph::utils::LocalTimeParameters{19, 23, 21, 123, 456}),
    std::make_pair("192321.123"sv, memgraph::utils::LocalTimeParameters{19, 23, 21, 123}),
    std::make_pair("192321"sv, memgraph::utils::LocalTimeParameters{19, 23, 21}),
    std::make_pair("1923"sv, memgraph::utils::LocalTimeParameters{19, 23}),
    std::make_pair("19"sv, memgraph::utils::LocalTimeParameters{19}),
    std::make_pair("000000.000000"sv, memgraph::utils::LocalTimeParameters{0, 0, 0, 0, 0}),
    std::make_pair("010203.004005"sv, memgraph::utils::LocalTimeParameters{1, 2, 3, 4, 5}),
};
}  // namespace

TEST(TemporalTest, DateParsing) {
  for (const auto &[string, date_parameters] : parsing_test_dates_extended) {
    ASSERT_EQ(memgraph::utils::ParseDateParameters(string).first, date_parameters);
  }

  for (const auto &[string, date_parameters] : parsing_test_dates_basic) {
    ASSERT_EQ(memgraph::utils::ParseDateParameters(string).first, date_parameters);
  }

  ASSERT_THROW(memgraph::utils::ParseDateParameters("202-011-22"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDateParameters("2020-1-022"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDateParameters("2020-11-2-"), memgraph::utils::BasicException);
}

TEST(TemporalTest, LocalTimeParsing) {
  for (const auto &[string, local_time_parameters] : parsing_test_local_time_extended) {
    ASSERT_EQ(memgraph::utils::ParseLocalTimeParameters(string).first, local_time_parameters)
        << ToString(local_time_parameters);
    const auto time_string = fmt::format("T{}", string);
    ASSERT_EQ(memgraph::utils::ParseLocalTimeParameters(time_string).first, local_time_parameters)
        << ToString(local_time_parameters);
  }

  for (const auto &[string, local_time_parameters] : parsing_test_local_time_basic) {
    ASSERT_EQ(memgraph::utils::ParseLocalTimeParameters(string).first, local_time_parameters)
        << ToString(local_time_parameters);
    ASSERT_EQ(memgraph::utils::ParseLocalTimeParameters(fmt::format("T{}", string)).first, local_time_parameters)
        << ToString(local_time_parameters);
  }

  ASSERT_THROW(memgraph::utils::ParseLocalTimeParameters("19:20:21s"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseLocalTimeParameters("1920:21"), memgraph::utils::BasicException);
}

TEST(TemporalTest, LocalDateTimeParsing) {
  const auto check_local_date_time_combinations = [](const auto &dates, const auto &local_times, const bool is_valid) {
    for (const auto &[date_string, date_parameters] : dates) {
      for (const auto &[local_time_string, local_time_parameters] : local_times) {
        const auto local_date_time_string = fmt::format("{}T{}", date_string, local_time_string);
        if (is_valid) {
          const auto parsed = memgraph::utils::ParseLocalDateTimeParameters(local_date_time_string);
          EXPECT_EQ(parsed.first, date_parameters);
          EXPECT_EQ(parsed.second, local_time_parameters);
        }
      }
    }
  };

  check_local_date_time_combinations(parsing_test_dates_basic, parsing_test_local_time_basic, true);
  check_local_date_time_combinations(parsing_test_dates_extended, parsing_test_local_time_extended, true);
  check_local_date_time_combinations(parsing_test_dates_basic, parsing_test_local_time_extended, false);
  check_local_date_time_combinations(parsing_test_dates_extended, parsing_test_local_time_basic, false);
}

TEST(TemporalTest, ZonedDateTimeParsing) {
  // The ZonedDateTime format is the LocalDateTime format & the timezone designation. As the first part is parsed with
  // the existing LocalDateTime parser, the LocalDateTime data is shared and the test cases focus on timezone parsing,
  // except for two cases that test ambiguous/nonexistent local times caused by daylight saving time changes.

  using namespace memgraph::utils;

  const auto shared_date_time = "2020-11-22T19:23:21.123456"sv;
  const auto shared_expected_date_params = DateParameters{2020, 11, 22};
  const auto shared_expected_local_time_params = LocalTimeParameters{19, 23, 21, 123, 456};

  const std::array timezone_parsing_cases{
      std::make_pair("Z"sv, Timezone("Etc/UTC")),
      std::make_pair("+01:00"sv, Timezone(std::chrono::minutes{60})),
      std::make_pair("+01:00[Europe/Zagreb]"sv, Timezone("Europe/Zagreb")),
      std::make_pair("-08:00"sv, Timezone(std::chrono::minutes{-480})),
      std::make_pair("-08:00[America/Los_Angeles]"sv, Timezone("America/Los_Angeles")),
      std::make_pair("+0100"sv, Timezone(std::chrono::minutes{60})),
      std::make_pair("+0100[Europe/Zagreb]"sv, Timezone("Europe/Zagreb")),
      std::make_pair("-0800"sv, Timezone(std::chrono::minutes{-480})),
      std::make_pair("-0800[America/Los_Angeles]"sv, Timezone("America/Los_Angeles")),
      std::make_pair("+01"sv, Timezone(std::chrono::minutes{60})),
      std::make_pair("+01[Europe/Zagreb]"sv, Timezone("Europe/Zagreb")),
      std::make_pair("-08"sv, Timezone(std::chrono::minutes{-480})),
      std::make_pair("-08[America/Los_Angeles]"sv, Timezone("America/Los_Angeles")),
      std::make_pair("[Europe/Zagreb]"sv, Timezone("Europe/Zagreb")),
      std::make_pair("[US/Pacific]"sv, Timezone("America/Los_Angeles")),  // US/Pacific links to America/Los_Angeles
      std::make_pair("[GMT]"sv, Timezone("GMT")),
  };

  const std::array faulty_timezone_cases{
      "Z_extra_text"sv,
      "+01:00_extra_text"sv,
      "+01:00[Europe/Zagreb]_extra_text"sv,
      "+01:00[America/Los_Angeles]"sv,
      "+01:00[America/Los_Angeles"sv,
      "+01:00[nonexistent/timezone]"sv,
      "+01.44"sv,
      "01:00"sv,
      "+"sv,
      "+[America/Los_Angeles]"sv,
      "[]"sv,
      "+01:00[]"sv,
      "-19:00"sv,
      "+19:00"sv,
      "+00:60"sv,
      "-00:60"sv,
  };

  const auto join_strings = [](const auto &date_time, const auto &timezone) {
    return std::format("{0}{1}", date_time, timezone);
  };

  const auto check_timezone_parsing_cases = [&shared_date_time, &shared_expected_date_params,
                                             &shared_expected_local_time_params, &join_strings](const auto &cases) {
    for (const auto &[timezone_string, timezone_parameter] : cases) {
      auto zdt_string = join_strings(shared_date_time, timezone_string);
      auto zdt_parameters =
          ZonedDateTimeParameters{shared_expected_date_params, shared_expected_local_time_params, timezone_parameter};
      EXPECT_EQ(ParseZonedDateTimeParameters(zdt_string), zdt_parameters);
    }
  };

  const auto check_faulty_timezones = [&shared_date_time, &join_strings](const auto &cases) {
    for (const auto &timezone_string : cases) {
      auto zdt_string = join_strings(shared_date_time, timezone_string);
      EXPECT_ANY_THROW(ParseZonedDateTimeParameters(zdt_string));
    }
  };

  check_timezone_parsing_cases(timezone_parsing_cases);
  check_faulty_timezones(faulty_timezone_cases);
}

void CheckDurationParameters(const auto &values, const auto &expected) {
  ASSERT_EQ(values.day, expected.day);
  ASSERT_EQ(values.hour, expected.hour);
  ASSERT_EQ(values.minute, expected.minute);
  ASSERT_EQ(values.second, expected.second);
  ASSERT_EQ(values.millisecond, expected.millisecond);
  ASSERT_EQ(values.microsecond, expected.microsecond);
}

TEST(TemporalTest, DurationParsing) {
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("P12Y"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("P12Y32DT2M"), memgraph::utils::BasicException);

  CheckDurationParameters(memgraph::utils::ParseDurationParameters("PT26H"),
                          memgraph::utils::DurationParameters{.hour = 26});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("PT2M"),
                          memgraph::utils::DurationParameters{.minute = 2.0});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("PT22S"),
                          memgraph::utils::DurationParameters{.second = 22});

  CheckDurationParameters(memgraph::utils::ParseDurationParameters("PT.33S"),
                          memgraph::utils::DurationParameters{.second = 0.33});

  CheckDurationParameters(memgraph::utils::ParseDurationParameters("PT2M3S"),
                          memgraph::utils::DurationParameters{.minute = 2.0, .second = 3.0});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("PT2.5H"),
                          memgraph::utils::DurationParameters{.hour = 2.5});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("P2DT2.5H"),
                          memgraph::utils::DurationParameters{.day = 2.0, .hour = 2.5});

  ASSERT_THROW(memgraph::utils::ParseDurationParameters("P2M3S"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("PTM3S"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("P2M3Y"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("PT2Y3M"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("12Y32DT2M"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("PY"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters(""), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("PT2M3SX"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("PT2M3S32"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("PT2.5M3S"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("PT2.5M3.5S"), memgraph::utils::BasicException);
  ASSERT_THROW(memgraph::utils::ParseDurationParameters("PT2.5M3.-5S"), memgraph::utils::BasicException);
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("P1256.5D"),
                          memgraph::utils::DurationParameters{1256.5});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("P1222DT2H"),
                          memgraph::utils::DurationParameters{1222, 2});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("P1222DT2H44M"),
                          memgraph::utils::DurationParameters{1222, 2, 44});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("P22DT1H9M20S"),
                          memgraph::utils::DurationParameters{22, 1, 9, 20});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("P22DT1H9M20.100S"),
                          memgraph::utils::DurationParameters{
                              22,
                              1,
                              9,
                              20.1,
                          });
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("P-22222222DT1H9M20.100S"),
                          memgraph::utils::DurationParameters{-22222222, 1, 9, 20.1});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("P-22222222DT-10H8M21.200S"),
                          memgraph::utils::DurationParameters{-22222222, -10, 8, 21.2});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("P-22222222DT-1H-7M22.300S"),
                          memgraph::utils::DurationParameters{-22222222, -1, -7, 22.3});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("P-22222222DT-1H-6M-20.100S"),
                          memgraph::utils::DurationParameters{-22222222, -1, -6, -20.1});
  CheckDurationParameters(memgraph::utils::ParseDurationParameters("P-22222222DT-1H-5M-20.100S"),
                          memgraph::utils::DurationParameters{-22222222, -1, -5, -20.1});
}

TEST(TemporalTest, PrintDate) {
  const auto unix_epoch = memgraph::utils::Date(memgraph::utils::DateParameters{1970, 1, 1});
  std::ostringstream stream;
  stream << unix_epoch;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "1970-01-01");
}

TEST(TemporalTest, PrintLocalTime) {
  const auto lt = memgraph::utils::LocalTime({13, 2, 40, 100, 50});
  std::ostringstream stream;
  stream << lt;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "13:02:40.100050");
}

TEST(TemporalTest, PrintDuration) {
  const auto dur = memgraph::utils::Duration({1, 0, 0, 0, 0, 0});
  std::ostringstream stream;
  stream << dur;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "P1DT0H0M0.000000S");
  stream.str("");
  stream.clear();
  const auto complex_dur = memgraph::utils::Duration({10, 3, 30, 33, 100, 50});
  stream << complex_dur;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "P10DT3H30M33.100050S");
  stream.str("");
  stream.clear();
  const auto negative_dur = memgraph::utils::Duration({-10, -3, -30, -33, -100, -50});
  stream << negative_dur;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "P-10DT-3H-30M-33.100050S");
}

TEST(TemporalTest, PrintLocalDateTime) {
  const auto unix_epoch = memgraph::utils::Date(memgraph::utils::DateParameters{1970, 1, 1});
  const auto lt = memgraph::utils::LocalTime({13, 2, 40, 100, 50});
  memgraph::utils::LocalDateTime ldt(unix_epoch, lt);
  std::ostringstream stream;
  stream << ldt;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "1970-01-01T13:02:40.100050");
}

TEST(TemporalTest, PrintZonedDateTime) {
  using namespace memgraph::utils;

  const std::array cases{
      // Standard time
      std::make_pair(ZonedDateTime({{2024, 1, 1}, {13, 2, 40, 100, 50}, Timezone("Europe/Zagreb")}),
                     "2024-01-01T13:02:40.100050+01:00[Europe/Zagreb]"),
      // Daylight saving time
      std::make_pair(ZonedDateTime({{2024, 7, 1}, {13, 2, 40, 100, 50}, Timezone("Europe/Zagreb")}),
                     "2024-07-01T13:02:40.100050+02:00[Europe/Zagreb]"),
      // Timezone links to another
      std::make_pair(ZonedDateTime({{2024, 7, 1}, {13, 2, 40, 100, 50}, Timezone("US/Pacific")}),
                     "2024-07-01T13:02:40.100050-07:00[America/Los_Angeles]"),
      // Timezone from offset (no name specified)
      std::make_pair(ZonedDateTime({{2024, 1, 1}, {13, 2, 40, 100, 50}, Timezone(std::chrono::minutes{60})}),
                     "2024-01-01T13:02:40.100050+01:00"),
  };

  auto check_to_string = [](const auto &cases) {
    for (const auto &[zdt, expected_string] : cases) {
      std::ostringstream stream;
      stream << zdt;
      ASSERT_TRUE(stream);
      ASSERT_EQ(stream.view(), expected_string);
    }
  };

  check_to_string(cases);
}

TEST(TemporalTest, DurationAddition) {
  // a >= 0 && b >= 0
  const auto zero = memgraph::utils::Duration(0);
  const auto one = memgraph::utils::Duration(1);
  const auto two = one + one;
  const auto four = two + two;
  ASSERT_EQ(two.microseconds, 2);
  ASSERT_EQ(four.microseconds, 4);
  const auto max = memgraph::utils::Duration(std::numeric_limits<int64_t>::max());
  ASSERT_THROW(max + one, memgraph::utils::BasicException);
  ASSERT_EQ(zero + zero, zero);
  ASSERT_EQ(max + zero, max);

  // a < 0 && b < 0
  const auto neg_one = -one;
  const auto neg_two = neg_one + neg_one;
  const auto neg_four = neg_two + neg_two;
  ASSERT_EQ(neg_two.microseconds, -2);
  ASSERT_EQ(neg_four.microseconds, -4);
  const auto min = memgraph::utils::Duration(std::numeric_limits<int64_t>::min());
  ASSERT_THROW(min + neg_one, memgraph::utils::BasicException);
  ASSERT_EQ(min + zero, min);

  // a < 0, b > 0 && a > 0 , b < 0
  ASSERT_EQ(neg_one + one, zero);
  ASSERT_EQ(neg_two + one, neg_one);
  ASSERT_EQ(neg_two + four, two);
  ASSERT_EQ(four + neg_two, two);

  // a {min, max} && b {min, max}
  ASSERT_EQ(min + max, neg_one);
  ASSERT_EQ(max + min, neg_one);
  ASSERT_THROW(min + min, memgraph::utils::BasicException);
  ASSERT_THROW(max + max, memgraph::utils::BasicException);
}

TEST(TemporalTest, DurationSubtraction) {
  // a >= 0 && b >= 0
  const auto one = memgraph::utils::Duration(1);
  const auto two = one + one;
  const auto zero = one - one;
  ASSERT_EQ(zero.microseconds, 0);
  const auto neg_one = zero - one;
  const auto neg_two = zero - two;
  ASSERT_EQ(neg_one.microseconds, -1);
  ASSERT_EQ(neg_two.microseconds, -2);
  const auto max = memgraph::utils::Duration(std::numeric_limits<int64_t>::max());
  const auto min_minus_one = memgraph::utils::Duration(std::numeric_limits<int64_t>::min() + 1);
  ASSERT_EQ(max - zero, max);
  ASSERT_EQ(zero - max, min_minus_one);

  // a < 0 && b < 0
  ASSERT_EQ(neg_two - neg_two, zero);
  const auto min = memgraph::utils::Duration(std::numeric_limits<int64_t>::min());
  ASSERT_THROW(min - one, memgraph::utils::BasicException);
  ASSERT_EQ(min - zero, min);

  // a < 0, b > 0 && a > 0 , b < 0
  ASSERT_EQ(neg_one - one, neg_two);
  ASSERT_EQ(one - neg_one, two);
  const auto neg_three = memgraph::utils::Duration(-3);
  const auto three = -neg_three;
  ASSERT_EQ(neg_two - one, neg_three);
  ASSERT_EQ(one - neg_two, three);

  // a {min, max} && b {min, max}
  ASSERT_THROW(min - max, memgraph::utils::BasicException);
  ASSERT_THROW(max - min, memgraph::utils::BasicException);
  // There is no positive representation of min
  ASSERT_THROW(min - min, memgraph::utils::BasicException);
  ASSERT_EQ(max - max, zero);
}

TEST(TemporalTest, LocalTimeAndDurationAddition) {
  const auto half_past_one = memgraph::utils::LocalTime({1, 30, 10});
  const auto three = half_past_one + memgraph::utils::Duration({10, 1, 30, 2, 22, 45});
  const auto three_symmetrical = memgraph::utils::Duration({10, 1, 30, 2, 22, 45}) + half_past_one;
  ASSERT_EQ(three, memgraph::utils::LocalTime({3, 0, 12, 22, 45}));
  ASSERT_EQ(three_symmetrical, memgraph::utils::LocalTime({3, 0, 12, 22, 45}));

  const auto half_an_hour_before_midnight = memgraph::utils::LocalTime({23, 30, 10});
  {
    const auto half_past_midnight = half_an_hour_before_midnight + memgraph::utils::Duration({1, 1, 0, 0});
    ASSERT_EQ(half_past_midnight, memgraph::utils::LocalTime({.minute = 30, .second = 10}));
  }
  const auto identity = half_an_hour_before_midnight + memgraph::utils::Duration({.day = 1});
  ASSERT_EQ(identity, half_an_hour_before_midnight);
  ASSERT_EQ(identity, half_an_hour_before_midnight + memgraph::utils::Duration({.day = 1, .hour = 24}));
  const auto an_hour_and_a_half_before_midnight = memgraph::utils::LocalTime({22, 30, 10});
  ASSERT_EQ(half_an_hour_before_midnight + memgraph::utils::Duration({.hour = 23}), an_hour_and_a_half_before_midnight);

  const auto minus_one_hour = memgraph::utils::Duration({-10, -1, 0, 0, -20, -20});
  const auto minus_one_hour_exact = memgraph::utils::Duration({.day = -10, .hour = -1});
  {
    const auto half_past_midnight = half_past_one + minus_one_hour;
    ASSERT_EQ(half_past_midnight, memgraph::utils::LocalTime({0, 30, 9, 979, 980}));
    ASSERT_EQ(half_past_midnight + minus_one_hour_exact, memgraph::utils::LocalTime({23, 30, 9, 979, 980}));

    const auto minus_two_hours_thirty_mins = memgraph::utils::Duration({-10, -2, -30, -9});
    ASSERT_EQ(half_past_midnight + minus_two_hours_thirty_mins, memgraph::utils::LocalTime({22, 0, 0, 979, 980}));

    ASSERT_NO_THROW(half_past_midnight + (memgraph::utils::Duration(std::numeric_limits<int64_t>::max())));
    ASSERT_EQ(half_past_midnight + (memgraph::utils::Duration(std::numeric_limits<int64_t>::max())),
              memgraph::utils::LocalTime({4, 31, 4, 755, 787}));
    ASSERT_NO_THROW(half_past_midnight + (memgraph::utils::Duration(std::numeric_limits<int64_t>::min())));
    ASSERT_EQ(half_past_midnight + (memgraph::utils::Duration(std::numeric_limits<int64_t>::min())),
              memgraph::utils::LocalTime({20, 29, 15, 204, 172}));
  }
}

TEST(TemporalTest, LocalTimeAndDurationSubtraction) {
  const auto half_past_one = memgraph::utils::LocalTime({1, 30, 10});
  const auto midnight = half_past_one - memgraph::utils::Duration({10, 1, 30, 10});
  ASSERT_EQ(midnight, memgraph::utils::LocalTime());
  ASSERT_EQ(midnight - memgraph::utils::Duration({-10, -1, -30, -10}), memgraph::utils::LocalTime({1, 30, 10}));

  const auto almost_an_hour_and_a_half_before_midnight = midnight - memgraph::utils::Duration({10, 1, 30, 1, 20, 20});
  ASSERT_EQ(almost_an_hour_and_a_half_before_midnight, memgraph::utils::LocalTime({22, 29, 58, 979, 980}));

  ASSERT_NO_THROW(midnight - (memgraph::utils::Duration(std::numeric_limits<int64_t>::max())));
  ASSERT_EQ(midnight - (memgraph::utils::Duration(std::numeric_limits<int64_t>::max())),
            memgraph::utils::LocalTime({19, 59, 5, 224, 193}));
  ASSERT_THROW(midnight - memgraph::utils::Duration(std::numeric_limits<int64_t>::min()),
               memgraph::utils::BasicException);
}

TEST(TemporalTest, LocalTimeDeltaDuration) {
  const auto half_past_one = memgraph::utils::LocalTime({1, 30, 10});
  const auto half_past_two = memgraph::utils::LocalTime({2, 30, 10});
  const auto an_hour_negative = half_past_one - half_past_two;
  ASSERT_EQ(an_hour_negative, memgraph::utils::Duration({.hour = -1}));
  const auto an_hour = half_past_two - half_past_one;
  ASSERT_EQ(an_hour, memgraph::utils::Duration({.hour = 1}));
}

TEST(TemporalTest, DateAddition) {
  const auto unix_epoch = memgraph::utils::Date({1970, 1, 1});
  const auto one_day_after_unix_epoch = unix_epoch + memgraph::utils::Duration({.day = 1});
  const auto one_day_after_unix_epoch_symmetrical = memgraph::utils::Duration({.day = 1}) + unix_epoch;
  ASSERT_EQ(one_day_after_unix_epoch, memgraph::utils::Date({1970, 1, 2}));
  ASSERT_EQ(one_day_after_unix_epoch_symmetrical, one_day_after_unix_epoch);

  const auto one_month_after_unix_epoch = unix_epoch + memgraph::utils::Duration({.day = 31});
  ASSERT_EQ(one_month_after_unix_epoch, memgraph::utils::Date({1970, 2, 1}));

  const auto one_year_after_unix_epoch = unix_epoch + memgraph::utils::Duration({.day = 365});
  ASSERT_EQ(one_year_after_unix_epoch, memgraph::utils::Date({1971, 1, 1}));

  const auto last_day_of_unix_epoch = one_year_after_unix_epoch + memgraph::utils::Duration({.day = -1});
  ASSERT_EQ(last_day_of_unix_epoch, memgraph::utils::Date({1970, 12, 31}));

  const auto one_day_before_unix_epoch = unix_epoch + memgraph::utils::Duration({.day = -1});
  ASSERT_EQ(one_day_before_unix_epoch, memgraph::utils::Date({1969, 12, 31}));

  ASSERT_EQ(last_day_of_unix_epoch + memgraph::utils::Duration({.day = -31}), memgraph::utils::Date({1970, 11, 30}));
  ASSERT_THROW(unix_epoch + memgraph::utils::Duration(std::numeric_limits<int64_t>::max()),
               memgraph::utils::BasicException);
  ASSERT_THROW(unix_epoch + memgraph::utils::Duration(std::numeric_limits<int64_t>::min()),
               memgraph::utils::BasicException);
}

TEST(TemporalTest, DateSubstraction) {
  const auto day_after_unix_epoch = memgraph::utils::Date({1970, 1, 2});
  const auto unix_epoch = day_after_unix_epoch - memgraph::utils::Duration({.day = 1});
  ASSERT_EQ(unix_epoch, memgraph::utils::Date({1970, 1, 1}));
  ASSERT_EQ(memgraph::utils::Date({1971, 1, 1}) - memgraph::utils::Duration({.day = 1}),
            memgraph::utils::Date({1970, 12, 31}));
  ASSERT_EQ(memgraph::utils::Date({1971, 1, 1}) - memgraph::utils::Duration({.day = -1}),
            memgraph::utils::Date({1971, 1, 2}));
  ASSERT_THROW(unix_epoch - memgraph::utils::Duration(std::numeric_limits<int64_t>::max()),
               memgraph::utils::BasicException);
  ASSERT_THROW(unix_epoch - memgraph::utils::Duration(std::numeric_limits<int64_t>::min()),
               memgraph::utils::BasicException);
}

TEST(TemporalTest, DateDelta) {
  const auto unix_epoch = memgraph::utils::Date({1970, 1, 1});
  const auto one_year_after_unix_epoch = memgraph::utils::Date({1971, 1, 1});
  ASSERT_EQ(one_year_after_unix_epoch - unix_epoch, memgraph::utils::Duration({.day = 365}));
  ASSERT_EQ(unix_epoch - one_year_after_unix_epoch, memgraph::utils::Duration({.day = -365}));
}

TEST(TemporalTest, LocalDateTimeAdditionSubtraction) {
  const auto unix_epoch = memgraph::utils::LocalDateTime({1970, 1, 1}, {.hour = 12});
  auto one_day_after_unix_epoch = unix_epoch + memgraph::utils::Duration({.hour = 24});
  auto one_day_after_unix_epoch_symmetrical = memgraph::utils::Duration({.hour = 24}) + unix_epoch;
  ASSERT_EQ(one_day_after_unix_epoch, memgraph::utils::LocalDateTime({1970, 1, 2}, {.hour = 12}));
  ASSERT_EQ(one_day_after_unix_epoch_symmetrical, one_day_after_unix_epoch);

  const auto one_day_before_unix_epoch = memgraph::utils::LocalDateTime({1969, 12, 31}, {23, 59, 59});
  ASSERT_EQ(one_day_before_unix_epoch + memgraph::utils::Duration({.second = 1}),
            memgraph::utils::LocalDateTime({1970, 1, 1}, {}));

  one_day_after_unix_epoch = unix_epoch + memgraph::utils::Duration({.day = 1});
  ASSERT_EQ(one_day_after_unix_epoch, memgraph::utils::LocalDateTime({1970, 1, 2}, {.hour = 12}));

  ASSERT_EQ(one_day_after_unix_epoch + memgraph::utils::Duration({.day = -1}), unix_epoch);
  ASSERT_EQ(one_day_after_unix_epoch - memgraph::utils::Duration({.day = 1}), unix_epoch);
  ASSERT_THROW(one_day_after_unix_epoch + memgraph::utils::Duration(std::numeric_limits<int64_t>::max()),
               memgraph::utils::BasicException);
  ASSERT_THROW(one_day_after_unix_epoch + memgraph::utils::Duration(std::numeric_limits<int64_t>::min()),
               memgraph::utils::BasicException);
  ASSERT_THROW(one_day_after_unix_epoch - memgraph::utils::Duration(std::numeric_limits<int64_t>::max()),
               memgraph::utils::BasicException);
  ASSERT_THROW(one_day_after_unix_epoch - memgraph::utils::Duration(std::numeric_limits<int64_t>::min()),
               memgraph::utils::BasicException);
}

TEST(TemporalTest, ZonedDateTimeAdditionSubtraction) {
  using namespace memgraph::utils;

  const auto zdt = ZonedDateTime({{2024, 3, 22}, {12, 00, 00}, Timezone("Europe/Zagreb")});
  const auto one_day = Duration({.day = 1});

  EXPECT_EQ(zdt + one_day, ZonedDateTime({{2024, 3, 23}, {12, 00, 00}, Timezone("Europe/Zagreb")}));
  EXPECT_EQ(one_day + zdt, ZonedDateTime({{2024, 3, 23}, {12, 00, 00}, Timezone("Europe/Zagreb")}));

  EXPECT_EQ(zdt - one_day, ZonedDateTime({{2024, 3, 21}, {12, 00, 00}, Timezone("Europe/Zagreb")}));
}

TEST(TemporalTest, LocalDateTimeDelta) {
  const auto unix_epoch = memgraph::utils::LocalDateTime({1970, 1, 1}, {1, 1, 1});
  const auto one_year_after_unix_epoch = memgraph::utils::LocalDateTime({1971, 2, 1}, {12, 1, 1});
  const auto two_years_after_unix_epoch = memgraph::utils::LocalDateTime({1972, 2, 1}, {1, 1, 1, 20, 34});
  ASSERT_EQ(one_year_after_unix_epoch - unix_epoch, memgraph::utils::Duration({.day = 396, .hour = 11}));
  ASSERT_EQ(unix_epoch - one_year_after_unix_epoch, memgraph::utils::Duration({.day = -396, .hour = -11}));
  ASSERT_EQ(two_years_after_unix_epoch - unix_epoch,
            memgraph::utils::Duration({.day = 761, .millisecond = 20, .microsecond = 34}));
}

TEST(TemporalTest, ZonedDateTimeDelta) {
  using namespace memgraph::utils;

  const auto zdt = ZonedDateTime({{2024, 3, 22}, {12, 00, 00}, Timezone("Europe/Zagreb")});
  const auto zdt_plus_time = ZonedDateTime({{2024, 3, 25}, {14, 18, 13, 206, 22}, Timezone("Europe/Zagreb")});

  EXPECT_EQ(zdt_plus_time - zdt,
            Duration({.day = 3, .hour = 2, .minute = 18, .second = 13, .millisecond = 206, .microsecond = 22}));
}

TEST(TemporalTest, DateConvertsToString) {
  const auto date1 = memgraph::utils::Date({1970, 1, 2});
  const std::string date1_expected_str = "1970-01-02";
  const auto date2 = memgraph::utils::Date({0000, 1, 1});
  const std::string date2_expected_str = "0000-01-01";
  const auto date3 = memgraph::utils::Date({2022, 7, 4});
  const std::string date3_expected_str = "2022-07-04";

  ASSERT_EQ(date1_expected_str, date1.ToString());
  ASSERT_EQ(date2_expected_str, date2.ToString());
  ASSERT_EQ(date3_expected_str, date3.ToString());
}

TEST(TemporalTest, LocalTimeConvertsToString) {
  const auto lt1 = memgraph::utils::LocalTime({13, 2, 40, 100, 50});
  const std::string lt1_expected_str = "13:02:40.100050";
  const auto lt2 = memgraph::utils::LocalTime({13, 2, 40});
  const std::string lt2_expected_str = "13:02:40.000000";
  const auto lt3 = memgraph::utils::LocalTime({0, 0, 0});
  const std::string lt3_expected_str = "00:00:00.000000";
  const auto lt4 = memgraph::utils::LocalTime({3, 2, 4, 6, 7});
  const std::string lt4_expected_str = "03:02:04.006007";

  ASSERT_EQ(lt1_expected_str, lt1.ToString());
  ASSERT_EQ(lt2_expected_str, lt2.ToString());
  ASSERT_EQ(lt3_expected_str, lt3.ToString());
  ASSERT_EQ(lt4_expected_str, lt4.ToString());
}

TEST(TemporalTest, LocalDateTimeConvertsToString) {
  const auto ldt1 = memgraph::utils::LocalDateTime({1970, 1, 2}, {23, 02, 59});
  const std::string ldt1_expected_str = "1970-01-02T23:02:59.000000";
  const auto ldt2 = memgraph::utils::LocalDateTime({1970, 1, 2}, {23, 02, 59, 456, 123});
  const std::string ldt2_expected_str = "1970-01-02T23:02:59.456123";
  const auto ldt3 = memgraph::utils::LocalDateTime({1997, 8, 24}, {16, 32, 9});
  const std::string ldt3_expected_str = "1997-08-24T16:32:09.000000";

  ASSERT_EQ(ldt1_expected_str, ldt1.ToString());
  ASSERT_EQ(ldt2_expected_str, ldt2.ToString());
  ASSERT_EQ(ldt3_expected_str, ldt3.ToString());
}

TEST(TemporalTest, ZonedDateTimeConvertsToString) {
  using namespace memgraph::utils;

  const std::array cases{
      // Standard time
      std::make_pair(ZonedDateTime({{2024, 1, 1}, {13, 2, 40, 100, 50}, Timezone("Europe/Zagreb")}),
                     "2024-01-01T13:02:40.100050+01:00[Europe/Zagreb]"),
      // Daylight saving time
      std::make_pair(ZonedDateTime({{2024, 7, 1}, {13, 2, 40, 100, 50}, Timezone("Europe/Zagreb")}),
                     "2024-07-01T13:02:40.100050+02:00[Europe/Zagreb]"),
      // Timezone links to another
      std::make_pair(ZonedDateTime({{2024, 7, 1}, {13, 2, 40, 100, 50}, Timezone("US/Pacific")}),
                     "2024-07-01T13:02:40.100050-07:00[America/Los_Angeles]"),
      // Timezone from offset (no name specified)
      std::make_pair(ZonedDateTime({{2024, 1, 1}, {13, 2, 40, 100, 50}, Timezone(std::chrono::minutes{60})}),
                     "2024-01-01T13:02:40.100050+01:00"),
  };

  auto check_to_string = [](const auto &cases) {
    for (const auto &[zdt, expected_string] : cases) {
      ASSERT_EQ(zdt.ToString(), expected_string);
    }
  };

  check_to_string(cases);
}

TEST(TemporalTest, DurationConvertsToString) {
  memgraph::utils::Duration duration1{{.minute = 2, .second = 2, .microsecond = 33}};
  const std::string duration1_expected_str = "P0DT0H2M2.000033S";
  memgraph::utils::Duration duration2{{.hour = 2.5, .minute = 2, .second = 2, .microsecond = 33}};
  const std::string duration2_expected_str = "P0DT2H32M2.000033S";
  memgraph::utils::Duration duration3{{.hour = 1.25, .minute = 2, .second = 2}};
  const std::string duration3_expected_str = "P0DT1H17M2.000000S";
  memgraph::utils::Duration duration4{{.day = 20, .hour = 1.25, .minute = 2, .second = 2}};
  const std::string duration4_expected_str = "P20DT1H17M2.000000S";
  memgraph::utils::Duration duration5{{.hour = -3, .minute = 2, .second = 2, .microsecond = -33}};
  const std::string duration5_expected_str = "P0DT-2H-57M-58.000033S";
  memgraph::utils::Duration duration6{{.day = -2, .hour = 3, .minute = 2, .second = 2, .microsecond = 33}};
  const std::string duration6_expected_str = "P-1DT-20H-57M-57.999967S";
  memgraph::utils::Duration duration7{{.day = 20, .hour = 72, .minute = 154, .second = 312}};
  const std::string duration7_expected_str = "P23DT2H39M12.000000S";
  memgraph::utils::Duration duration8{{.day = 1, .hour = 23, .minute = 59, .second = 60}};
  const std::string duration8_expected_str = "P2DT0H0M0.000000S";

  ASSERT_EQ(duration1_expected_str, duration1.ToString());
  ASSERT_EQ(duration2_expected_str, duration2.ToString());
  ASSERT_EQ(duration3_expected_str, duration3.ToString());
  ASSERT_EQ(duration4_expected_str, duration4.ToString());
  ASSERT_EQ(duration5_expected_str, duration5.ToString());
  ASSERT_EQ(duration6_expected_str, duration6.ToString());
  ASSERT_EQ(duration7_expected_str, duration7.ToString());
  ASSERT_EQ(duration8_expected_str, duration8.ToString());
}
