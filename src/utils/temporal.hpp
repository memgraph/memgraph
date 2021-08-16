#pragma once

#include <cstdint>

#include <chrono>
#include <iostream>

#include "fmt/format.h"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"

namespace utils {

template <typename T>
concept Chrono = requires(T) {
  typename T::rep;
  typename T::period;
};

template <Chrono TFirst, Chrono TSecond>
constexpr auto GetAndSubtractDuration(TSecond &base_duration) {
  const auto duration = std::chrono::duration_cast<TFirst>(base_duration);
  base_duration -= duration;
  return duration.count();
}

struct DateParameters {
  int64_t years{0};
  int64_t months{1};
  int64_t days{1};

  bool operator==(const DateParameters &) const = default;
};

// boolean indicates whether the parsed string was in extended format
std::pair<DateParameters, bool> ParseDateParameters(std::string_view date_string);

struct Date {
  explicit Date() : Date{DateParameters{}} {}
  // we assume we accepted date in microseconds which was normilized using the epoch time point
  explicit Date(int64_t microseconds);
  explicit Date(const DateParameters &date_parameters);

  friend std::ostream &operator<<(std::ostream &os, const Date &date) {
    return os << fmt::format("{:0>2}-{:0>2}-{:0>2}", date.years, static_cast<int>(date.months),
                             static_cast<int>(date.days));
  }

  int64_t MicrosecondsSinceEpoch() const;
  int64_t DaysSinceEpoch() const;

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

  bool operator==(const LocalTimeParameters &) const = default;
};

// boolean indicates whether the parsed string was in extended format
std::pair<LocalTimeParameters, bool> ParseLocalTimeParameters(std::string_view string);

struct LocalTime {
  explicit LocalTime() : LocalTime{LocalTimeParameters{}} {}
  explicit LocalTime(int64_t microseconds);
  explicit LocalTime(const LocalTimeParameters &local_time_parameters);

  std::chrono::microseconds SumLocalTimeParts() const;

  // Epoch means the start of the day, i,e, midnight
  int64_t MicrosecondsSinceEpoch() const;
  int64_t NanosecondsSinceEpoch() const;

  auto operator<=>(const LocalTime &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const LocalTime &lt) {
    namespace chrono = std::chrono;
    using milli = chrono::milliseconds;
    using micro = chrono::microseconds;
    const auto subseconds = milli(lt.milliseconds) + micro(lt.microseconds);
    return os << fmt::format("{:0>2}:{:0>2}:{:0>2}.{:0>6}", static_cast<int>(lt.hours), static_cast<int>(lt.minutes),
                             static_cast<int>(lt.seconds), subseconds.count());
  }

  uint8_t hours;
  uint8_t minutes;
  uint8_t seconds;
  uint16_t milliseconds;
  uint16_t microseconds;
};

struct LocalTimeHash {
  size_t operator()(const LocalTime &local_time) const;
};

std::pair<DateParameters, LocalTimeParameters> ParseLocalDateTimeParameters(std::string_view string);

struct LocalDateTime {
  explicit LocalDateTime(int64_t microseconds);
  explicit LocalDateTime(DateParameters date_parameters, const LocalTimeParameters &local_time_parameters);
  explicit LocalDateTime(const Date &date, const LocalTime &local_time);

  int64_t MicrosecondsSinceEpoch() const;
  int64_t SecondsSinceEpoch() const;  // seconds since epoch
  int64_t SubSecondsAsNanoseconds() const;

  auto operator<=>(const LocalDateTime &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const LocalDateTime &ldt) {
    os << ldt.date << 'T' << ldt.local_time;
    return os;
  }

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
  double milliseconds{0};
  double microseconds{0};
};

DurationParameters ParseDurationParameters(std::string_view string);

struct Duration {
  explicit Duration(int64_t microseconds);
  explicit Duration(const DurationParameters &parameters);

  auto operator<=>(const Duration &) const = default;

  int64_t Months() const;
  int64_t SubMonthsAsDays() const;
  int64_t SubDaysAsSeconds() const;
  int64_t SubSecondsAsNanoseconds() const;

  friend std::ostream &operator<<(std::ostream &os, const Duration &dur) {
    // ISO 8601 extended format: P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss].
    namespace chrono = std::chrono;
    auto micros = chrono::microseconds(dur.microseconds);
    const auto y = GetAndSubtractDuration<chrono::years>(micros);
    const auto mo = GetAndSubtractDuration<chrono::months>(micros);
    const auto dd = GetAndSubtractDuration<chrono::days>(micros);
    const auto h = GetAndSubtractDuration<chrono::hours>(micros);
    const auto m = GetAndSubtractDuration<chrono::minutes>(micros);
    const auto s = GetAndSubtractDuration<chrono::seconds>(micros);
    return os << fmt::format("P{:0>4}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}.{:0>6}", y, mo, dd, h, m, s, micros.count());
  }

  Duration operator-() const;

  int64_t microseconds;
};

struct DurationHash {
  size_t operator()(const Duration &duration) const;
};

constexpr std::chrono::days DaysSinceEpoch(uint16_t years, uint8_t months, uint8_t days) {
  namespace chrono = std::chrono;
  const auto ymd = chrono::year_month_day(chrono::year(years), chrono::month(months), chrono::day(days));
  return chrono::sys_days{ymd}.time_since_epoch();
}

Date UtcToday();
LocalTime UtcLocalTime();
LocalDateTime UtcLocalDateTime();
}  // namespace utils
