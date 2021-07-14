#include <chrono>
#include <optional>

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
