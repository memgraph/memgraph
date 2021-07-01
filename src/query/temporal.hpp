#pragma once
#include <chrono>
#include <cstdint>

#include "utils/exceptions.hpp"
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

  int64_t MicrosecondsSinceEpoch() const;

  auto operator<=>(const Date &) const = default;

  uint16_t years;
  uint8_t months;
  uint8_t days;
};

struct DateHash {
  size_t operator()(const Date &date) const;
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

  int64_t MicrosecondsSinceEpoch() const;

  auto operator<=>(const LocalTime &) const = default;

  uint8_t hours;
  uint8_t minutes;
  uint8_t seconds;
  uint16_t milliseconds;
  uint16_t microseconds;
};

struct LocalTimeHash {
  size_t operator()(const LocalTime &local_time) const;
};

struct LocalDateTime {
  explicit LocalDateTime(int64_t microseconds);
  explicit LocalDateTime(DateParameters date, const LocalTimeParameters &local_time);

  int64_t MicrosecondsSinceEpoch() const;

  auto operator<=>(const LocalDateTime &) const = default;

  Date date;
  LocalTime local_time;
};

struct LocalDateTimeHash {
  size_t operator()(const LocalDateTime &local_date_time) const;
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
  size_t operator()(const Duration &duration) const;
};

}  // namespace query
