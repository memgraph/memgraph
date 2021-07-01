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
