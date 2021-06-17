#pragma once
#include <bits/stdint-intn.h>
#include <chrono>
#include <cstdint>
#include <ratio>

#include "utils/exceptions.hpp"
#include "utils/hash_combine.hpp"
#include "utils/logging.hpp"

namespace query {

struct Date {
  // we assume we accepted date in microseconds which was normilized using the epoch time point
  explicit Date(int64_t microseconds);

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

struct LocalTime {
  explicit LocalTime(int64_t microseconds);

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

  // return microseconds normilized with regard to epoch time point
  int64_t Microseconds() const;

  auto operator<=>(const LocalDateTime &) const = default;

  uint64_t years;
  uint8_t months;
  uint8_t days;
  uint8_t hours;
  uint8_t minutes;
  uint8_t seconds;
  uint16_t milliseconds;
  uint16_t microseconds;
};

struct LocalDateTimeHash {
  size_t operator()(const LocalDateTime &local_date_time) const {
    size_t result = 0;
    utils::BoostHashCombine(result, local_date_time.years);
    utils::BoostHashCombine(result, local_date_time.months);
    utils::BoostHashCombine(result, local_date_time.days);
    utils::BoostHashCombine(result, local_date_time.hours);
    utils::BoostHashCombine(result, local_date_time.minutes);
    utils::BoostHashCombine(result, local_date_time.seconds);
    utils::BoostHashCombine(result, local_date_time.milliseconds);
    utils::BoostHashCombine(result, local_date_time.microseconds);
    return result;
  }
};

struct Duration {
  // we assume we accepted date in microseconds which was normilized using the epoch time point
  explicit Duration(int64_t microseconds);

  // return microseconds normilized with regard to epoch time point
  int64_t Microseconds() const;

  auto operator<=>(const Duration &) const = default;

  Duration operator-() const;

  uint64_t years;
  uint64_t months;
  uint64_t days;
  uint64_t hours;
  uint64_t minutes;
  uint64_t seconds;
  uint64_t milliseconds;
  uint64_t microseconds;
  bool negative{false};
};

struct DurationHash {
  size_t operator()(const Duration &duration) const {
    size_t result = 0;
    utils::BoostHashCombine(result, duration.years);
    utils::BoostHashCombine(result, duration.months);
    utils::BoostHashCombine(result, duration.days);
    utils::BoostHashCombine(result, duration.hours);
    utils::BoostHashCombine(result, duration.minutes);
    utils::BoostHashCombine(result, duration.seconds);
    utils::BoostHashCombine(result, duration.milliseconds);
    utils::BoostHashCombine(result, duration.microseconds);
    utils::BoostHashCombine(result, duration.negative);
    return result;
  }
};

}  // namespace query
