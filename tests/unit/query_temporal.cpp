#include <optional>

#include <gtest/gtest.h>

#include "query/temporal.hpp"
#include "utils/exceptions.hpp"

namespace {
struct TestDateParameters {
  query::DateParameters date_parameters;
  bool should_throw;
};

constexpr std::array test_dates{TestDateParameters{{-1996, 11, 22}, true}, TestDateParameters{{1996, -11, 22}, true},
                                TestDateParameters{{1996, 11, -22}, true}, TestDateParameters{{1, 13, 3}, true},
                                TestDateParameters{{1, 12, 32}, true},     TestDateParameters{{1, 2, 29}, true},
                                TestDateParameters{{2020, 2, 29}, false},  TestDateParameters{{1700, 2, 29}, true},
                                TestDateParameters{{1200, 2, 29}, false},  TestDateParameters{{10000, 12, 3}, true}};

struct TestLocalTimeParameters {
  query::LocalTimeParameters local_time_parameters;
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
  std::optional<query::Date> test_date;

  for (const auto [date_parameters, should_throw] : test_dates) {
    if (should_throw) {
      ASSERT_THROW(test_date.emplace(date_parameters), utils::BasicException);
    } else {
      ASSERT_NO_THROW(test_date.emplace(date_parameters));
    }
  }
}

TEST(TemporalTest, DateMicrosecondsSinceEpochConversion) {
  const auto check_microseconds = [](const auto date_parameters) {
    query::Date initial_date{date_parameters};
    const auto microseconds = initial_date.MicrosecondsSinceEpoch();
    query::Date new_date{microseconds};
    ASSERT_EQ(initial_date, new_date);
  };

  check_microseconds(query::DateParameters{2020, 11, 22});
  check_microseconds(query::DateParameters{1900, 2, 22});
  check_microseconds(query::DateParameters{0, 1, 1});

  ASSERT_THROW(check_microseconds(query::DateParameters{-10, 1, 1}), utils::BasicException);

  {
    query::Date date{query::DateParameters{1970, 1, 1}};
    ASSERT_EQ(date.MicrosecondsSinceEpoch(), 0);
  }
  {
    query::Date date{query::DateParameters{1910, 1, 1}};
    ASSERT_LT(date.MicrosecondsSinceEpoch(), 0);
  }
  {
    query::Date date{query::DateParameters{2021, 1, 1}};
    ASSERT_GT(date.MicrosecondsSinceEpoch(), 0);
  }
}

TEST(TemporalTest, LocalTimeConstruction) {
  std::optional<query::LocalTime> test_local_time;

  for (const auto [local_time_parameters, should_throw] : test_local_times) {
    if (should_throw) {
      ASSERT_THROW(test_local_time.emplace(local_time_parameters), utils::BasicException);
    } else {
      ASSERT_NO_THROW(test_local_time.emplace(local_time_parameters));
    }
  }
}

TEST(TemporalTest, LocalTimeMicrosecondsSinceEpochConversion) {
  const auto check_microseconds = [](const query::LocalTimeParameters &parameters) {
    query::LocalTime initial_local_time{parameters};
    const auto microseconds = initial_local_time.MicrosecondsSinceEpoch();
    query::LocalTime new_local_time{microseconds};
    ASSERT_EQ(initial_local_time, new_local_time);
  };

  check_microseconds(query::LocalTimeParameters{23, 59, 59, 999, 999});
  check_microseconds(query::LocalTimeParameters{0, 0, 0, 0, 0});
  check_microseconds(query::LocalTimeParameters{14, 8, 55, 321, 452});
}

TEST(TemporalTest, LocalDateTimeMicrosecondsSinceEpochConversion) {
  const auto check_microseconds = [](const query::DateParameters date_parameters,
                                     const query::LocalTimeParameters &local_time_parameters) {
    query::LocalDateTime initial_local_date_time{date_parameters, local_time_parameters};
    const auto microseconds = initial_local_date_time.MicrosecondsSinceEpoch();
    query::LocalDateTime new_local_date_time{microseconds};
    ASSERT_EQ(initial_local_date_time, new_local_date_time);
  };

  check_microseconds(query::DateParameters{2020, 11, 22}, query::LocalTimeParameters{0, 0, 0, 0, 0});
  check_microseconds(query::DateParameters{1900, 2, 22}, query::LocalTimeParameters{0, 0, 0, 0, 0});
  check_microseconds(query::DateParameters{0, 1, 1}, query::LocalTimeParameters{0, 0, 0, 0, 0});
  {
    query::LocalDateTime local_date_time(query::DateParameters{1970, 1, 1}, query::LocalTimeParameters{0, 0, 0, 0, 0});
    ASSERT_EQ(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
  {
    query::LocalDateTime local_date_time(query::DateParameters{1970, 1, 1}, query::LocalTimeParameters{0, 0, 0, 0, 1});
    ASSERT_GT(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
  {
    query::LocalTimeParameters local_time_parameters{12, 10, 40, 42, 42};
    query::LocalDateTime local_date_time{query::DateParameters{1970, 1, 1}, local_time_parameters};
    ASSERT_EQ(local_date_time.MicrosecondsSinceEpoch(),
              query::LocalTime{local_time_parameters}.MicrosecondsSinceEpoch());
  }
  {
    query::LocalDateTime local_date_time(query::DateParameters{1910, 1, 1}, query::LocalTimeParameters{0, 0, 0, 0, 0});
    ASSERT_LT(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
  {
    query::LocalDateTime local_date_time(query::DateParameters{2021, 1, 1}, query::LocalTimeParameters{0, 0, 0, 0, 0});
    ASSERT_GT(local_date_time.MicrosecondsSinceEpoch(), 0);
  }
}

TEST(TemporalTest, DurationConversion) {
  {
    query::Duration duration{{.years = 2.5}};
    const auto microseconds = duration.microseconds;
    query::LocalDateTime local_date_time{microseconds};
    ASSERT_EQ(local_date_time.date.years, 2 + 1970);
    ASSERT_EQ(local_date_time.date.months, 6 + 1);
  };
  {
    query::Duration duration{{.months = 26}};
    const auto microseconds = duration.microseconds;
    query::LocalDateTime local_date_time{microseconds};
    ASSERT_EQ(local_date_time.date.years, 2 + 1970);
    ASSERT_EQ(local_date_time.date.months, 2 + 1);
  };
  {
    query::Duration duration{{.minutes = 123.25}};
    const auto microseconds = duration.microseconds;
    query::LocalDateTime local_date_time{microseconds};
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
    std::make_pair("2020-11-22"sv, query::DateParameters{2020, 11, 22}),
    std::make_pair("2020-11"sv, query::DateParameters{2020, 11}),
};

constexpr std::array parsing_test_dates_basic{std::make_pair("20201122"sv, query::DateParameters{2020, 11, 22})};

constexpr std::array parsing_test_local_time_extended{
    std::make_pair("19:23:21.123456"sv, query::LocalTimeParameters{19, 23, 21, 123, 456}),
    std::make_pair("19:23:21.123"sv, query::LocalTimeParameters{19, 23, 21, 123}),
    std::make_pair("19:23:21"sv, query::LocalTimeParameters{19, 23, 21}),
    std::make_pair("19:23"sv, query::LocalTimeParameters{19, 23}),
};

constexpr std::array parsing_test_local_time_basic{
    std::make_pair("T192321.123456"sv, query::LocalTimeParameters{19, 23, 21, 123, 456}),
    std::make_pair("T192321.123"sv, query::LocalTimeParameters{19, 23, 21, 123}),
    std::make_pair("T192321"sv, query::LocalTimeParameters{19, 23, 21}),
    std::make_pair("T1923"sv, query::LocalTimeParameters{19, 23}),
};
}  // namespace

TEST(TemporalTest, DateParsing) {
  for (const auto &[string, date_parameters] : parsing_test_dates_extended) {
    ASSERT_EQ(query::ParseDateParameters(string).first, date_parameters);
  }

  for (const auto &[string, date_parameters] : parsing_test_dates_basic) {
    ASSERT_EQ(query::ParseDateParameters(string).first, date_parameters);
  }

  ASSERT_THROW(query::ParseDateParameters("202-011-22"), utils::BasicException);
  ASSERT_THROW(query::ParseDateParameters("2020-1-022"), utils::BasicException);
  ASSERT_THROW(query::ParseDateParameters("2020-11-2-"), utils::BasicException);
}

TEST(TemporalTest, LocalTimeParsing) {
  for (const auto &[string, local_time_parameters] : parsing_test_local_time_extended) {
    ASSERT_EQ(query::ParseLocalTimeParameters(string), local_time_parameters);
  }

  for (const auto &[string, local_time_parameters] : parsing_test_local_time_basic) {
    ASSERT_EQ(query::ParseLocalTimeParameters(string), local_time_parameters);
  }
}

TEST(TemporalTest, LocalDateTimeParsing) {
  const auto check_local_date_time_combinations = [](const auto &dates, const auto &local_times, const bool is_valid) {
    for (const auto &[date_string, date_parameters] : dates) {
      for (const auto &[local_time_string, local_time_parameters] : local_times) {
        const auto local_date_time_string = fmt::format(
            "{}T{}", date_string, local_time_string.starts_with("T") ? local_time_string.substr(1) : local_time_string);
        if (is_valid) {
          EXPECT_EQ(query::ParseLocalDateTimeParameters(local_date_time_string),
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
  CheckDurationParameters(query::ParseDurationParameters("P12Y"), query::DurationParameters{.years = 12.0});
  CheckDurationParameters(query::ParseDurationParameters("P12Y32DT2M"),
                          query::DurationParameters{.years = 12.0, .days = 32.0, .minutes = 2.0});
  CheckDurationParameters(query::ParseDurationParameters("PT2M"), query::DurationParameters{.minutes = 2.0});
  CheckDurationParameters(query::ParseDurationParameters("PT2M3S"),
                          query::DurationParameters{.minutes = 2.0, .seconds = 3.0});
  CheckDurationParameters(query::ParseDurationParameters("PT2.5H"), query::DurationParameters{.hours = 2.5});
  CheckDurationParameters(query::ParseDurationParameters("P2DT2.5H"),
                          query::DurationParameters{.days = 2.0, .hours = 2.5});

  ASSERT_THROW(query::ParseDurationParameters("P2M3S"), utils::BasicException);
  ASSERT_THROW(query::ParseDurationParameters("P2M3S"), utils::BasicException);
  ASSERT_THROW(query::ParseDurationParameters("PTM3S"), utils::BasicException);
  ASSERT_THROW(query::ParseDurationParameters("P2M3Y"), utils::BasicException);
  ASSERT_THROW(query::ParseDurationParameters("PT2Y3M"), utils::BasicException);
  ASSERT_THROW(query::ParseDurationParameters("12Y32DT2M"), utils::BasicException);
  ASSERT_THROW(query::ParseDurationParameters("PY"), utils::BasicException);
  ASSERT_THROW(query::ParseDurationParameters(""), utils::BasicException);
  ASSERT_THROW(query::ParseDurationParameters("PT2M3SX"), utils::BasicException);
  ASSERT_THROW(query::ParseDurationParameters("PT2M3S32"), utils::BasicException);
  ASSERT_THROW(query::ParseDurationParameters("PT2.5M3S"), utils::BasicException);
  ASSERT_THROW(query::ParseDurationParameters("PT2.5M3.5S"), utils::BasicException);

  CheckDurationParameters(query::ParseDurationParameters("P20201122T192032"),
                          query::DurationParameters{2020, 11, 22, 19, 20, 32});
  CheckDurationParameters(query::ParseDurationParameters("P2020-11-22T19:20:32"),
                          query::DurationParameters{2020, 11, 22, 19, 20, 32});
}
