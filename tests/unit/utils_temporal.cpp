#include <chrono>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>

#include <gtest/gtest.h>

#include "utils/exceptions.hpp"
#include "utils/temporal.hpp"

namespace {

std::string ToString(const utils::DateParameters &date_parameters) {
  return fmt::format("{:04d}-{:02d}-{:02d}", date_parameters.years, date_parameters.months, date_parameters.days);
}

std::string ToString(const utils::LocalTimeParameters &local_time_parameters) {
  return fmt::format("{:02}:{:02d}:{:02d}", local_time_parameters.hours, local_time_parameters.minutes,
                     local_time_parameters.seconds);
}

struct TestDateParameters {
  utils::DateParameters date_parameters;
  bool should_throw;
};

constexpr std::array test_dates{TestDateParameters{{-1996, 11, 22}, true}, TestDateParameters{{1996, -11, 22}, true},
                                TestDateParameters{{1996, 11, -22}, true}, TestDateParameters{{1, 13, 3}, true},
                                TestDateParameters{{1, 12, 32}, true},     TestDateParameters{{1, 2, 29}, true},
                                TestDateParameters{{2020, 2, 29}, false},  TestDateParameters{{1700, 2, 29}, true},
                                TestDateParameters{{1200, 2, 29}, false},  TestDateParameters{{10000, 12, 3}, true}};

struct TestLocalTimeParameters {
  utils::LocalTimeParameters local_time_parameters;
  bool should_throw;
};

constexpr std::array test_local_times{
    TestLocalTimeParameters{{.hours = 24}, true},           TestLocalTimeParameters{{.hours = -1}, true},
    TestLocalTimeParameters{{.minutes = -1}, true},         TestLocalTimeParameters{{.minutes = 60}, true},
    TestLocalTimeParameters{{.seconds = -1}, true},         TestLocalTimeParameters{{.minutes = 60}, true},
    TestLocalTimeParameters{{.milliseconds = -1}, true},    TestLocalTimeParameters{{.milliseconds = 1000}, true},
    TestLocalTimeParameters{{.microseconds = -1}, true},    TestLocalTimeParameters{{.microseconds = 1000}, true},
    TestLocalTimeParameters{{23, 59, 59, 999, 999}, false}, TestLocalTimeParameters{{0, 0, 0, 0, 0}, false}};
}  // namespace

TEST(TemporalTest, DateConstruction) {
  std::optional<utils::Date> test_date;

  for (const auto [date_parameters, should_throw] : test_dates) {
    if (should_throw) {
      EXPECT_THROW(test_date.emplace(date_parameters), utils::BasicException) << ToString(date_parameters);
    } else {
      EXPECT_NO_THROW(test_date.emplace(date_parameters)) << ToString(date_parameters);
    }
  }
}

TEST(TemporalTest, DateMicrosecondsSinceEpochConversion) {
  const auto check_microseconds = [](const auto date_parameters) {
    utils::Date initial_date{date_parameters};
    const auto microseconds = initial_date.MicrosecondsSinceEpoch();
    utils::Date new_date{microseconds};
    ASSERT_EQ(initial_date, new_date);
  };

  check_microseconds(utils::DateParameters{2020, 11, 22});
  check_microseconds(utils::DateParameters{1900, 2, 22});
  check_microseconds(utils::DateParameters{0, 1, 1});
  check_microseconds(utils::DateParameters{1994, 12, 7});

  ASSERT_THROW(check_microseconds(utils::DateParameters{-10, 1, 1}), utils::BasicException);

  {
    utils::Date date{utils::DateParameters{1970, 1, 1}};
    ASSERT_EQ(date.MicrosecondsSinceEpoch(), 0);
  }
  {
    utils::Date date{utils::DateParameters{1910, 1, 1}};
    ASSERT_LT(date.MicrosecondsSinceEpoch(), 0);
  }
  {
    utils::Date date{utils::DateParameters{2021, 1, 1}};
    ASSERT_GT(date.MicrosecondsSinceEpoch(), 0);
  }
}

TEST(TemporalTest, LocalTimeConstruction) {
  std::optional<utils::LocalTime> test_local_time;

  for (const auto [local_time_parameters, should_throw] : test_local_times) {
    if (should_throw) {
      ASSERT_THROW(test_local_time.emplace(local_time_parameters), utils::BasicException);
    } else {
      ASSERT_NO_THROW(test_local_time.emplace(local_time_parameters));
    }
  }
}

TEST(TemporalTest, LocalTimeMicrosecondsSinceEpochConversion) {
  const auto check_microseconds = [](const utils::LocalTimeParameters &parameters) {
    utils::LocalTime initial_local_time{parameters};
    const auto microseconds = initial_local_time.MicrosecondsSinceEpoch();
    utils::LocalTime new_local_time{microseconds};
    ASSERT_EQ(initial_local_time, new_local_time);
  };

  check_microseconds(utils::LocalTimeParameters{23, 59, 59, 999, 999});
  check_microseconds(utils::LocalTimeParameters{0, 0, 0, 0, 0});
  check_microseconds(utils::LocalTimeParameters{14, 8, 55, 321, 452});
}

TEST(TemporalTest, LocalDateTimeMicrosecondsSinceEpochConversion) {
  const auto check_microseconds = [](const utils::DateParameters date_parameters,
                                     const utils::LocalTimeParameters &local_time_parameters) {
    utils::LocalDateTime initial_local_date_time{date_parameters, local_time_parameters};
    const auto microseconds = initial_local_date_time.MicrosecondsSinceEpoch();
    utils::LocalDateTime new_local_date_time{microseconds};
    ASSERT_EQ(initial_local_date_time, new_local_date_time);
  };

  check_microseconds(utils::DateParameters{2020, 11, 22}, utils::LocalTimeParameters{0, 0, 0, 0, 0});
  check_microseconds(utils::DateParameters{1900, 2, 22}, utils::LocalTimeParameters{0, 0, 0, 0, 0});
  check_microseconds(utils::DateParameters{0, 1, 1}, utils::LocalTimeParameters{0, 0, 0, 0, 0});
  {
    utils::LocalDateTime local_date_time(utils::DateParameters{1970, 1, 1}, utils::LocalTimeParameters{0, 0, 0, 0, 0});
    ASSERT_EQ(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
  {
    utils::LocalDateTime local_date_time(utils::DateParameters{1970, 1, 1}, utils::LocalTimeParameters{0, 0, 0, 0, 1});
    ASSERT_GT(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
  {
    utils::LocalTimeParameters local_time_parameters{12, 10, 40, 42, 42};
    utils::LocalDateTime local_date_time{utils::DateParameters{1970, 1, 1}, local_time_parameters};
    ASSERT_EQ(local_date_time.MicrosecondsSinceEpoch(),
              utils::LocalTime{local_time_parameters}.MicrosecondsSinceEpoch());
  }
  {
    utils::LocalDateTime local_date_time(utils::DateParameters{1910, 1, 1}, utils::LocalTimeParameters{0, 0, 0, 0, 0});
    ASSERT_LT(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
  {
    utils::LocalDateTime local_date_time(utils::DateParameters{2021, 1, 1}, utils::LocalTimeParameters{0, 0, 0, 0, 0});
    ASSERT_GT(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
}

TEST(TemporalTest, DurationConversion) {
  {
    utils::Duration duration{{.years = 2.5}};
    const auto microseconds = duration.microseconds;
    utils::LocalDateTime local_date_time{microseconds};
    ASSERT_EQ(local_date_time.date.years, 2 + 1970);
    ASSERT_EQ(local_date_time.date.months, 6 + 1);
  };
  {
    utils::Duration duration{{.months = 26}};
    const auto microseconds = duration.microseconds;
    utils::LocalDateTime local_date_time{microseconds};
    ASSERT_EQ(local_date_time.date.years, 2 + 1970);
    ASSERT_EQ(local_date_time.date.months, 2 + 1);
  };
  {
    utils::Duration duration{{.minutes = 123.25}};
    const auto microseconds = duration.microseconds;
    utils::LocalDateTime local_date_time{microseconds};
    ASSERT_EQ(local_date_time.date.years, 1970);
    ASSERT_EQ(local_date_time.date.months, 1);
    ASSERT_EQ(local_date_time.date.days, 1);
    ASSERT_EQ(local_date_time.local_time.hours, 2);
    ASSERT_EQ(local_date_time.local_time.minutes, 3);
    ASSERT_EQ(local_date_time.local_time.seconds, 15);
  };
}

namespace {
using namespace std::literals;
constexpr std::array parsing_test_dates_extended{
    std::make_pair("2020-11-22"sv, utils::DateParameters{2020, 11, 22}),
    std::make_pair("2020-11"sv, utils::DateParameters{2020, 11}),
};

constexpr std::array parsing_test_dates_basic{std::make_pair("20201122"sv, utils::DateParameters{2020, 11, 22})};

constexpr std::array parsing_test_local_time_extended{
    std::make_pair("19:23:21.123456"sv, utils::LocalTimeParameters{19, 23, 21, 123, 456}),
    std::make_pair("19:23:21.123"sv, utils::LocalTimeParameters{19, 23, 21, 123}),
    std::make_pair("19:23:21"sv, utils::LocalTimeParameters{19, 23, 21}),
    std::make_pair("19:23"sv, utils::LocalTimeParameters{19, 23}),
    std::make_pair("00:00:00.000000"sv, utils::LocalTimeParameters{0, 0, 0, 0, 0}),
    std::make_pair("01:02:03.004005"sv, utils::LocalTimeParameters{1, 2, 3, 4, 5}),
};

constexpr std::array parsing_test_local_time_basic{
    std::make_pair("192321.123456"sv, utils::LocalTimeParameters{19, 23, 21, 123, 456}),
    std::make_pair("192321.123"sv, utils::LocalTimeParameters{19, 23, 21, 123}),
    std::make_pair("192321"sv, utils::LocalTimeParameters{19, 23, 21}),
    std::make_pair("1923"sv, utils::LocalTimeParameters{19, 23}),
    std::make_pair("19"sv, utils::LocalTimeParameters{19}),
    std::make_pair("000000.000000"sv, utils::LocalTimeParameters{0, 0, 0, 0, 0}),
    std::make_pair("010203.004005"sv, utils::LocalTimeParameters{1, 2, 3, 4, 5}),
};
}  // namespace

TEST(TemporalTest, DateParsing) {
  for (const auto &[string, date_parameters] : parsing_test_dates_extended) {
    ASSERT_EQ(utils::ParseDateParameters(string).first, date_parameters);
  }

  for (const auto &[string, date_parameters] : parsing_test_dates_basic) {
    ASSERT_EQ(utils::ParseDateParameters(string).first, date_parameters);
  }

  ASSERT_THROW(utils::ParseDateParameters("202-011-22"), utils::BasicException);
  ASSERT_THROW(utils::ParseDateParameters("2020-1-022"), utils::BasicException);
  ASSERT_THROW(utils::ParseDateParameters("2020-11-2-"), utils::BasicException);
}

TEST(TemporalTest, LocalTimeParsing) {
  for (const auto &[string, local_time_parameters] : parsing_test_local_time_extended) {
    ASSERT_EQ(utils::ParseLocalTimeParameters(string).first, local_time_parameters) << ToString(local_time_parameters);
    ASSERT_EQ(utils::ParseLocalTimeParameters(fmt::format("T{}", string)).first, local_time_parameters)
        << ToString(local_time_parameters);
  }

  for (const auto &[string, local_time_parameters] : parsing_test_local_time_basic) {
    ASSERT_EQ(utils::ParseLocalTimeParameters(string).first, local_time_parameters) << ToString(local_time_parameters);
    ASSERT_EQ(utils::ParseLocalTimeParameters(fmt::format("T{}", string)).first, local_time_parameters)
        << ToString(local_time_parameters);
  }

  ASSERT_THROW(utils::ParseLocalTimeParameters("19:20:21s"), utils::BasicException);
  ASSERT_THROW(utils::ParseLocalTimeParameters("1920:21"), utils::BasicException);
}

TEST(TemporalTest, LocalDateTimeParsing) {
  const auto check_local_date_time_combinations = [](const auto &dates, const auto &local_times, const bool is_valid) {
    for (const auto &[date_string, date_parameters] : dates) {
      for (const auto &[local_time_string, local_time_parameters] : local_times) {
        const auto local_date_time_string = fmt::format("{}T{}", date_string, local_time_string);
        if (is_valid) {
          EXPECT_EQ(utils::ParseLocalDateTimeParameters(local_date_time_string),
                    (std::pair{date_parameters, local_time_parameters}));
        }
      }
    }
  };

  check_local_date_time_combinations(parsing_test_dates_basic, parsing_test_local_time_basic, true);
  check_local_date_time_combinations(parsing_test_dates_extended, parsing_test_local_time_extended, true);
  check_local_date_time_combinations(parsing_test_dates_basic, parsing_test_local_time_extended, false);
  check_local_date_time_combinations(parsing_test_dates_extended, parsing_test_local_time_basic, false);
}

void CheckDurationParameters(const auto &values, const auto &expected) {
  ASSERT_NEAR(values.years, expected.years, 0.01);
  ASSERT_NEAR(values.months, expected.months, 0.01);
  ASSERT_NEAR(values.days, expected.days, 0.01);
  ASSERT_NEAR(values.hours, expected.hours, 0.01);
  ASSERT_NEAR(values.minutes, expected.minutes, 0.01);
  ASSERT_NEAR(values.seconds, expected.seconds, 0.01);
}

TEST(TemporalTest, DurationParsing) {
  CheckDurationParameters(utils::ParseDurationParameters("P12Y"), utils::DurationParameters{.years = 12.0});
  CheckDurationParameters(utils::ParseDurationParameters("P12Y32DT2M"),
                          utils::DurationParameters{.years = 12.0, .days = 32.0, .minutes = 2.0});
  CheckDurationParameters(utils::ParseDurationParameters("PT2M"), utils::DurationParameters{.minutes = 2.0});
  CheckDurationParameters(utils::ParseDurationParameters("PT2M3S"),
                          utils::DurationParameters{.minutes = 2.0, .seconds = 3.0});
  CheckDurationParameters(utils::ParseDurationParameters("PT2.5H"), utils::DurationParameters{.hours = 2.5});
  CheckDurationParameters(utils::ParseDurationParameters("P2DT2.5H"),
                          utils::DurationParameters{.days = 2.0, .hours = 2.5});

  ASSERT_THROW(utils::ParseDurationParameters("P2M3S"), utils::BasicException);
  ASSERT_THROW(utils::ParseDurationParameters("PTM3S"), utils::BasicException);
  ASSERT_THROW(utils::ParseDurationParameters("P2M3Y"), utils::BasicException);
  ASSERT_THROW(utils::ParseDurationParameters("PT2Y3M"), utils::BasicException);
  ASSERT_THROW(utils::ParseDurationParameters("12Y32DT2M"), utils::BasicException);
  ASSERT_THROW(utils::ParseDurationParameters("PY"), utils::BasicException);
  ASSERT_THROW(utils::ParseDurationParameters(""), utils::BasicException);
  ASSERT_THROW(utils::ParseDurationParameters("PT2M3SX"), utils::BasicException);
  ASSERT_THROW(utils::ParseDurationParameters("PT2M3S32"), utils::BasicException);
  ASSERT_THROW(utils::ParseDurationParameters("PT2.5M3S"), utils::BasicException);
  ASSERT_THROW(utils::ParseDurationParameters("PT2.5M3.5S"), utils::BasicException);

  CheckDurationParameters(utils::ParseDurationParameters("P20201122T192032"),
                          utils::DurationParameters{2020, 11, 22, 19, 20, 32});
  CheckDurationParameters(utils::ParseDurationParameters("P20201122T192032.333"),
                          utils::DurationParameters{2020, 11, 22, 19, 20, 32, 333});
  CheckDurationParameters(utils::ParseDurationParameters("P2020-11-22T19:20:32"),
                          utils::DurationParameters{2020, 11, 22, 19, 20, 32});
}

TEST(TemporalTest, PrintDate) {
  const auto unix_epoch = utils::Date(utils::DateParameters{1970, 1, 1});
  std::ostringstream stream;
  stream << unix_epoch;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "1970-01-01");
}

TEST(TemporalTest, PrintLocalTime) {
  const auto lt = utils::LocalTime({13, 2, 40, 100, 50});
  std::ostringstream stream;
  stream << lt;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "13:02:40.100050");
}

TEST(TemporalTest, PrintDuration) {
  const auto dur = utils::Duration({1, 0, 0, 0, 0, 0, 0, 0});
  std::ostringstream stream;
  stream << dur;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "P0001-00-00T00:00:00.000000");
  stream.str("");
  stream.clear();
  const auto complex_dur = utils::Duration({1, 5, 10, 3, 30, 33, 100, 50});
  stream << complex_dur;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "P0001-05-10T03:30:33.100050");
  /// stream.str("");
  /// stream.clear();
  /// TODO (kostasrim)
  /// We do not support pasring negative Durations yet. We are the only ones we have access
  /// to the constructor below.
  /*
  const auto negative_dur = utils::Duration({-1, 5, -10, -3, -30, -33});
  stream << negative_dur;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "P0001-05-10T03:30:33.000000");
  */
}

TEST(TemporalTest, PrintLocalDateTime) {
  const auto unix_epoch = utils::Date(utils::DateParameters{1970, 1, 1});
  const auto lt = utils::LocalTime({13, 2, 40, 100, 50});
  utils::LocalDateTime ldt(unix_epoch, lt);
  std::ostringstream stream;
  stream << ldt;
  ASSERT_TRUE(stream);
  ASSERT_EQ(stream.view(), "1970-01-01T13:02:40.100050");
}

TEST(TemporalTest, DurationAddition) {
  // a >= 0 && b >= 0
  const auto zero = utils::Duration(0);
  const auto one = utils::Duration(1);
  const auto two = one + one;
  const auto four = two + two;
  ASSERT_EQ(two.microseconds, 2);
  ASSERT_EQ(four.microseconds, 4);
  const auto max = utils::Duration(std::numeric_limits<int64_t>::max());
  ASSERT_THROW(max + one, utils::BasicException);
  ASSERT_EQ(zero + zero, zero);
  ASSERT_EQ(max + zero, max);

  // a < 0 && b < 0
  const auto neg_one = -one;
  const auto neg_two = neg_one + neg_one;
  const auto neg_four = neg_two + neg_two;
  ASSERT_EQ(neg_two.microseconds, -2);
  ASSERT_EQ(neg_four.microseconds, -4);
  const auto min = utils::Duration(std::numeric_limits<int64_t>::min());
  ASSERT_THROW(min + neg_one, utils::BasicException);
  ASSERT_EQ(min + zero, min);

  // a < 0, b > 0 && a > 0 , b < 0
  ASSERT_EQ(neg_one + one, zero);
  ASSERT_EQ(neg_two + one, neg_one);
  ASSERT_EQ(neg_two + four, two);
  ASSERT_EQ(four + neg_two, two);

  // a {min, max} && b {min, max}
  ASSERT_EQ(min + max, neg_one);
  ASSERT_EQ(max + min, neg_one);
  ASSERT_THROW(min + min, utils::BasicException);
  ASSERT_THROW(max + max, utils::BasicException);
}

TEST(TemporalTest, DurationSubtraction) {
  // a >= 0 && b >= 0
  const auto one = utils::Duration(1);
  const auto two = one + one;
  const auto zero = one - one;
  ASSERT_EQ(zero.microseconds, 0);
  const auto neg_one = zero - one;
  const auto neg_two = zero - two;
  ASSERT_EQ(neg_one.microseconds, -1);
  ASSERT_EQ(neg_two.microseconds, -2);
  const auto max = utils::Duration(std::numeric_limits<int64_t>::max());
  const auto min_minus_one = utils::Duration(std::numeric_limits<int64_t>::min() + 1);
  ASSERT_EQ(max - zero, max);
  ASSERT_EQ(zero - max, min_minus_one);

  // a < 0 && b < 0
  ASSERT_EQ(neg_two - neg_two, zero);
  const auto min = utils::Duration(std::numeric_limits<int64_t>::min());
  ASSERT_THROW(min - one, utils::BasicException);
  ASSERT_EQ(min - zero, min);

  // a < 0, b > 0 && a > 0 , b < 0
  ASSERT_EQ(neg_one - one, neg_two);
  ASSERT_EQ(one - neg_one, two);
  const auto neg_three = utils::Duration(-3);
  const auto three = -neg_three;
  ASSERT_EQ(neg_two - one, neg_three);
  ASSERT_EQ(one - neg_two, three);

  // a {min, max} && b {min, max}
  ASSERT_THROW(min - max, utils::BasicException);
  ASSERT_THROW(max - min, utils::BasicException);
  // There is no positive representation of min
  ASSERT_THROW(min - min, utils::BasicException);
  ASSERT_EQ(max - max, zero);
}

TEST(TemporalTest, LocalTimeAndDurationAddition) {
  const auto half_past_one = utils::LocalTime({1, 30, 10});
  const auto three = half_past_one + utils::Duration({1994, 2, 10, 1, 30, 2, 22, 45});
  const auto three_symmetrical = utils::Duration({1994, 2, 10, 1, 30, 2, 22, 45}) + half_past_one;
  ASSERT_EQ(three, utils::LocalTime({3, 0, 12, 22, 45}));
  ASSERT_EQ(three_symmetrical, utils::LocalTime({3, 0, 12, 22, 45}));

  const auto half_an_hour_before_midnight = utils::LocalTime({23, 30, 10});
  {
    const auto half_past_midnight = half_an_hour_before_midnight + utils::Duration({1994, 1, 10, 1});
    ASSERT_EQ(half_past_midnight, utils::LocalTime({.minutes = 30, .seconds = 10}));
  }
  const auto identity = half_an_hour_before_midnight + utils::Duration({.days = 1});
  ASSERT_EQ(identity, half_an_hour_before_midnight);
  ASSERT_EQ(identity, half_an_hour_before_midnight + utils::Duration({.days = 1, .hours = 24}));
  const auto an_hour_and_a_half_before_midnight = utils::LocalTime({22, 30, 10});
  ASSERT_EQ(half_an_hour_before_midnight + utils::Duration({.hours = 23}), an_hour_and_a_half_before_midnight);

  const auto minus_one_hour = utils::Duration({-1994, -2, -10, -1, 0, 0, -20, -20});
  const auto minus_one_hour_exact = utils::Duration({-1994, -2, -10, -1});
  {
    const auto half_past_midnight = half_past_one + minus_one_hour;
    ASSERT_EQ(half_past_midnight, utils::LocalTime({0, 30, 9, 979, 980}));
    ASSERT_EQ(half_past_midnight + minus_one_hour_exact, utils::LocalTime({23, 30, 9, 979, 980}));

    const auto minus_two_hours_thirty_mins = utils::Duration({-1994, -2, -10, -2, -30, -9});
    ASSERT_EQ(half_past_midnight + minus_two_hours_thirty_mins, utils::LocalTime({22, 0, 0, 979, 980}));

    ASSERT_NO_THROW(half_past_midnight + (utils::Duration(std::numeric_limits<int64_t>::max())));
    ASSERT_EQ(half_past_midnight + (utils::Duration(std::numeric_limits<int64_t>::max())),
              utils::LocalTime({0, 22, 40, 755, 787}));
    ASSERT_NO_THROW(half_past_midnight + (utils::Duration(std::numeric_limits<int64_t>::min())));
    ASSERT_EQ(half_past_midnight + (utils::Duration(std::numeric_limits<int64_t>::min())),
              utils::LocalTime({0, 37, 39, 204, 172}));
  }
}

TEST(TemporalTest, LocalTimeAndDurationSubtraction) {
  const auto half_past_one = utils::LocalTime({1, 30, 10});
  const auto midnight = half_past_one - utils::Duration({1994, 2, 10, 1, 30, 10});
  ASSERT_EQ(midnight, utils::LocalTime());
  ASSERT_EQ(midnight - utils::Duration({-1994, -2, -10, -1, -30, -10}), utils::LocalTime({1, 30, 10}));

  const auto almost_an_hour_and_a_half_before_midnight = midnight - utils::Duration({1994, 2, 10, 1, 30, 1, 20, 20});
  ASSERT_EQ(almost_an_hour_and_a_half_before_midnight, utils::LocalTime({22, 29, 58, 979, 980}));

  ASSERT_NO_THROW(midnight - (utils::Duration(std::numeric_limits<int64_t>::max())));
  ASSERT_EQ(midnight - (utils::Duration(std::numeric_limits<int64_t>::max())), utils::LocalTime({0, 7, 29, 224, 193}));
  ASSERT_THROW(midnight - utils::Duration(std::numeric_limits<int64_t>::min()), utils::BasicException);
}

TEST(TemporalTest, LocalTimeDeltaDuration) {
  const auto half_past_one = utils::LocalTime({1, 30, 10});
  const auto half_past_two = utils::LocalTime({2, 30, 10});
  const auto an_hour_negative = half_past_one - half_past_two;
  ASSERT_EQ(an_hour_negative, utils::Duration({.hours = -1}));
  const auto an_hour = half_past_two - half_past_one;
  ASSERT_EQ(an_hour, utils::Duration({.hours = 1}));
}

TEST(TemporalTest, DateAddition) {
  const auto unix_epoch = utils::Date({1970, 1, 1});
  const auto one_day_after_unix_epoch = unix_epoch + utils::Duration({.days = 1});
  const auto one_day_after_unix_epoch_symmetrical = utils::Duration({.days = 1}) + unix_epoch;
  ASSERT_EQ(one_day_after_unix_epoch, utils::Date({1970, 1, 2}));
  ASSERT_EQ(one_day_after_unix_epoch_symmetrical, one_day_after_unix_epoch);

  const auto one_month_after_unix_epoch = unix_epoch + utils::Duration({.days = 31});
  ASSERT_EQ(one_month_after_unix_epoch, utils::Date({1970, 2, 1}));

  const auto one_year_after_unix_epoch = unix_epoch + utils::Duration({.days = 365});
  ASSERT_EQ(one_year_after_unix_epoch, utils::Date({1971, 1, 1}));

  const auto last_day_of_unix_epoch = one_year_after_unix_epoch + utils::Duration({.days = -1});
  ASSERT_EQ(last_day_of_unix_epoch, utils::Date({1970, 12, 31}));

  const auto one_day_before_unix_epoch = unix_epoch + utils::Duration({.days = -1});
  ASSERT_EQ(one_day_before_unix_epoch, utils::Date({1969, 12, 31}));

  ASSERT_EQ(last_day_of_unix_epoch + utils::Duration({.days = -31}), utils::Date({1970, 11, 30}));
  ASSERT_THROW(unix_epoch + utils::Duration(std::numeric_limits<int64_t>::max()), utils::BasicException);
  ASSERT_THROW(unix_epoch + utils::Duration(std::numeric_limits<int64_t>::min()), utils::BasicException);
}

TEST(TemporalTest, DateSubstraction) {
  const auto day_after_unix_epoch = utils::Date({1970, 1, 2});
  const auto unix_epoch = day_after_unix_epoch - utils::Duration({.days = 1});
  ASSERT_EQ(unix_epoch, utils::Date({1970, 1, 1}));
  ASSERT_EQ(utils::Date({1971, 1, 1}) - utils::Duration({.days = 1}), utils::Date({1970, 12, 31}));
  ASSERT_EQ(utils::Date({1971, 1, 1}) - utils::Duration({.days = -1}), utils::Date({1971, 1, 2}));
  ASSERT_THROW(unix_epoch - utils::Duration(std::numeric_limits<int64_t>::max()), utils::BasicException);
  ASSERT_THROW(unix_epoch - utils::Duration(std::numeric_limits<int64_t>::min()), utils::BasicException);
}

TEST(TemporalTest, DateDelta) {
  const auto unix_epoch = utils::Date({1970, 1, 1});
  const auto one_year_after_unix_epoch = utils::Date({1971, 1, 1});
  ASSERT_EQ(one_year_after_unix_epoch - unix_epoch, utils::Duration({.days = 365}));
  ASSERT_EQ(unix_epoch - one_year_after_unix_epoch, utils::Duration({.days = -365}));
}

TEST(TemporalTest, LocalDateTimeAdditionSubtraction) {
  const auto unix_epoch = utils::LocalDateTime({1970, 1, 1}, {.hours = 12});
  auto one_day_after_unix_epoch = unix_epoch + utils::Duration({.hours = 24});
  auto one_day_after_unix_epoch_symmetrical = utils::Duration({.hours = 24}) + unix_epoch;
  ASSERT_EQ(one_day_after_unix_epoch, utils::LocalDateTime({1970, 1, 2}, {.hours = 12}));
  ASSERT_EQ(one_day_after_unix_epoch_symmetrical, one_day_after_unix_epoch);

  one_day_after_unix_epoch = unix_epoch + utils::Duration({.days = 1});
  ASSERT_EQ(one_day_after_unix_epoch, utils::LocalDateTime({1970, 1, 2}, {.hours = 12}));

  ASSERT_EQ(one_day_after_unix_epoch + utils::Duration({.days = -1}), unix_epoch);
  ASSERT_EQ(one_day_after_unix_epoch - utils::Duration({.days = 1}), unix_epoch);
  ASSERT_THROW(one_day_after_unix_epoch + utils::Duration(std::numeric_limits<int64_t>::max()), utils::BasicException);
  ASSERT_THROW(one_day_after_unix_epoch + utils::Duration(std::numeric_limits<int64_t>::min()), utils::BasicException);
  ASSERT_THROW(one_day_after_unix_epoch - utils::Duration(std::numeric_limits<int64_t>::max()), utils::BasicException);
  ASSERT_THROW(one_day_after_unix_epoch - utils::Duration(std::numeric_limits<int64_t>::min()), utils::BasicException);
}

TEST(TemporalTest, LocalDateTimeDelta) {
  const auto unix_epoch = utils::LocalDateTime({1970, 1, 1}, {1, 1, 1});
  const auto one_year_after_unix_epoch = utils::LocalDateTime({1971, 2, 1}, {12, 1, 1});
  const auto two_years_after_unix_epoch = utils::LocalDateTime({1972, 2, 1}, {1, 1, 1, 20, 34});
  ASSERT_EQ(one_year_after_unix_epoch - unix_epoch, utils::Duration({.days = 396, .hours = 11}));
  ASSERT_EQ(unix_epoch - one_year_after_unix_epoch, utils::Duration({.days = -396, .hours = -11}));
  ASSERT_EQ(two_years_after_unix_epoch - unix_epoch,
            utils::Duration({.days = 761, .milliseconds = 20, .microseconds = 34}));
}
