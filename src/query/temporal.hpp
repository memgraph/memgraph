#pragma once
#include <chrono>
#include <cstdint>

#include "utils/exceptions.hpp"
#include "utils/hash_combine.hpp"
#include "utils/logging.hpp"

namespace query {

struct DateParameters {
  int64_t years{0};
  int64_t months{1};
  int64_t days{1};
};

struct Date {
  explicit Date() : Date{DateParameters{}} {}
  // we assume we accepted date in microseconds which was normilized using the epoch time point
  explicit Date(int64_t microseconds);
  explicit Date(const DateParameters &date_parameters);

  // return microseconds normilized with regard to epoch time point
  int64_t Microseconds() const;

  auto operator<=>(const Date &) const = default;

  uint64_t years;
  uint8_t months;
  uint8_t days;
};

struct DateHash {
  size_t operator()(const Date &date) const {
    size_t result = 0;
    utils::BoostHashCombine(result, date.years);
    utils::BoostHashCombine(result, date.months);
    utils::BoostHashCombine(result, date.days);
    return result;
  }
};

struct LocalTimeParameters {
  int64_t hours{0};
  int64_t minutes{0};
  int64_t seconds{0};
  int64_t milliseconds{0};
  int64_t microseconds{0};
};

struct LocalTime {
  explicit LocalTime() : LocalTime{LocalTimeParameters{}} {}
  explicit LocalTime(int64_t microseconds);
  explicit LocalTime(const LocalTimeParameters &local_time_parameters);

  int64_t Microseconds() const;

  auto operator<=>(const LocalTime &) const = default;

  uint8_t hours;
  uint8_t minutes;
  uint8_t seconds;
  uint16_t milliseconds;
  uint16_t microseconds;
};

struct LocalTimeHash {
  size_t operator()(const LocalTime &local_time) const {
    size_t result = 0;
    utils::BoostHashCombine(result, local_time.hours);
    utils::BoostHashCombine(result, local_time.minutes);
    utils::BoostHashCombine(result, local_time.seconds);
    utils::BoostHashCombine(result, local_time.milliseconds);
    utils::BoostHashCombine(result, local_time.microseconds);
    return result;
  }
};

struct LocalDateTime {
  explicit LocalDateTime(int64_t microseconds);
  explicit LocalDateTime(DateParameters date, const LocalTimeParameters &local_time);

  // return microseconds normilized with regard to epoch time point
  int64_t Microseconds() const;

  auto operator<=>(const LocalDateTime &) const = default;

  Date date;
  LocalTime local_time;
};

struct LocalDateTimeHash {
  size_t operator()(const LocalDateTime &local_date_time) const {
    size_t result = 0;
    utils::BoostHashCombine(result, LocalTimeHash{}(local_date_time.local_time));
    utils::BoostHashCombine(result, DateHash{}(local_date_time.date));
    return result;
  }
};

struct DurationParameters {
  double years{0};
  double months{0};
  double days{0};
  double hours{0};
  double minutes{0};
  double seconds{0};
  // TODO(antonio2368): Check how to include milliseconds/microseconds
  // ISO 8601 does not specify string format for them
};

struct Duration {
  explicit Duration(int64_t microseconds);
  explicit Duration(const DurationParameters &parameters);

  auto operator<=>(const Duration &) const = default;

  Duration operator-() const;

  int64_t microseconds;
};

struct DurationHash {
  size_t operator()(const Duration &duration) const {
    size_t result = 0;
    utils::BoostHashCombine(result, duration.microseconds);
    return result;
  }
};

}  // namespace query
