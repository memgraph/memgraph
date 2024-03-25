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

#pragma once

#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>

#include "fmt/format.h"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"

namespace memgraph::utils {

class Timezone;

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

template <typename TType>
bool Overflows(const TType &lhs, const TType &rhs) {
  if (lhs > 0 && rhs > 0 && lhs > (std::numeric_limits<TType>::max() - rhs)) [[unlikely]] {
    return true;
  }
  return false;
}

template <typename TType>
bool Underflows(const TType &lhs, const TType &rhs) {
  if (lhs < 0 && rhs < 0 && lhs < (std::numeric_limits<TType>::min() - rhs)) [[unlikely]] {
    return true;
  }
  return false;
}

namespace temporal {
struct InvalidArgumentException : public utils::BasicException {
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(InvalidArgumentException)
};
}  // namespace temporal

struct DurationParameters {
  double day{0};
  double hour{0};
  double minute{0};
  double second{0};
  double millisecond{0};
  double microsecond{0};
};

DurationParameters ParseDurationParameters(std::string_view string);

struct Date;
struct LocalTime;
struct LocalDateTime;

struct Duration {
  explicit Duration(int64_t microseconds);
  explicit Duration(const DurationParameters &parameters);

  auto operator<=>(const Duration &) const = default;

  int64_t Days() const;
  int64_t SubDaysAsSeconds() const;
  int64_t SubDaysAsHours() const;
  int64_t SubDaysAsMinutes() const;
  int64_t SubDaysAsMilliseconds() const;
  int64_t SubDaysAsMicroseconds() const;
  int64_t SubDaysAsNanoseconds() const;
  int64_t SubSecondsAsNanoseconds() const;

  std::string ToString() const;

  friend std::ostream &operator<<(std::ostream &os, const Duration &dur) { return os << dur.ToString(); }

  Duration operator-() const;

  friend Duration operator+(const Duration &lhs, const Duration rhs) {
    if (Overflows(lhs.microseconds, rhs.microseconds)) {
      throw utils::BasicException("Duration arithmetic overflows");
    }

    if (Underflows(lhs.microseconds, rhs.microseconds)) {
      throw utils::BasicException("Duration arithmetic underflows");
    }

    return Duration(lhs.microseconds + rhs.microseconds);
  }

  friend Duration operator-(const Duration &lhs, const Duration rhs) { return lhs + (-rhs); }

  int64_t microseconds;
};

struct DurationHash {
  size_t operator()(const Duration &duration) const;
};

struct DateParameters {
  int64_t year{0};
  int64_t month{1};
  int64_t day{1};

  bool operator==(const DateParameters &) const = default;
};

// boolean indicates whether the parsed string was in extended format
std::pair<DateParameters, bool> ParseDateParameters(std::string_view date_string);

constexpr std::chrono::year_month_day ToChronoYMD(uint16_t year, uint8_t month, uint8_t day) {
  namespace chrono = std::chrono;
  return chrono::year_month_day(chrono::year(year), chrono::month(month), chrono::day(day));
}

constexpr std::chrono::sys_days ToChronoSysDaysYMD(uint16_t year, uint8_t month, uint8_t day) {
  return std::chrono::sys_days(ToChronoYMD(year, month, day));
}

constexpr std::chrono::days DaysSinceEpoch(uint16_t year, uint8_t month, uint8_t day) {
  return ToChronoSysDaysYMD(year, month, day).time_since_epoch();
}

struct Date {
  explicit Date() : Date{DateParameters{}} {}
  // we assume we accepted date in microseconds which was normalized using the epoch time point
  explicit Date(int64_t microseconds);
  explicit Date(const DateParameters &date_parameters);

  friend std::ostream &operator<<(std::ostream &os, const Date &date) { return os << date.ToString(); }

  int64_t MicrosecondsSinceEpoch() const;
  int64_t DaysSinceEpoch() const;
  std::string ToString() const;

  friend Date operator+(const Date &date, const Duration &dur) {
    namespace chrono = std::chrono;
    const auto date_as_duration = Duration(date.MicrosecondsSinceEpoch());
    const auto result = date_as_duration + dur;
    const auto ymd = chrono::year_month_day(chrono::sys_days(chrono::days(result.Days())));
    return Date({static_cast<int>(ymd.year()), static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day())});
  }

  friend Date operator+(const Duration &dur, const Date &date) { return date + dur; }

  friend Date operator-(const Date &date, const Duration &dur) { return date + (-dur); }

  friend Duration operator-(const Date &lhs, const Date &rhs) {
    namespace chrono = std::chrono;
    const auto lhs_days = utils::DaysSinceEpoch(lhs.year, lhs.month, lhs.day);
    const auto rhs_days = utils::DaysSinceEpoch(rhs.year, rhs.month, rhs.day);
    const auto days_elapsed = lhs_days - rhs_days;
    return Duration(chrono::duration_cast<chrono::microseconds>(days_elapsed).count());
  }

  auto operator<=>(const Date &) const = default;

  uint16_t year;
  uint8_t month;
  uint8_t day;
};

struct DateHash {
  size_t operator()(const Date &date) const;
};

struct LocalTimeParameters {
  int64_t hour{0};
  int64_t minute{0};
  int64_t second{0};
  int64_t millisecond{0};
  int64_t microsecond{0};

  bool operator==(const LocalTimeParameters &) const = default;
};

std::pair<LocalTimeParameters, bool> ParseLocalTimeParameters(std::string_view string);

struct LocalTime {
  explicit LocalTime() : LocalTime{LocalTimeParameters{}} {}
  explicit LocalTime(int64_t microseconds);
  explicit LocalTime(const LocalTimeParameters &local_time_parameters);

  std::chrono::microseconds SumLocalTimeParts() const;

  // Epoch means the start of the day, i,e, midnight
  int64_t MicrosecondsSinceEpoch() const;
  int64_t NanosecondsSinceEpoch() const;
  std::string ToString() const;

  auto operator<=>(const LocalTime &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const LocalTime &lt) { return os << lt.ToString(); }

  friend LocalTime operator+(const LocalTime &local_time, const Duration &dur) {
    namespace chrono = std::chrono;
    auto rhs = dur.SubDaysAsMicroseconds();
    auto abs = [](auto value) { return (value >= 0) ? value : -value; };
    const auto lhs = local_time.MicrosecondsSinceEpoch();
    if (rhs < 0 && lhs < abs(rhs)) {
      static constexpr int64_t one_day_in_microseconds =
          chrono::duration_cast<chrono::microseconds>(chrono::days(1)).count();
      rhs = one_day_in_microseconds + rhs;
    }
    auto result = chrono::microseconds(lhs + rhs);
    const auto h = GetAndSubtractDuration<chrono::hours>(result) % 24;
    const auto m = GetAndSubtractDuration<chrono::minutes>(result);
    const auto s = GetAndSubtractDuration<chrono::seconds>(result);
    const auto milli = GetAndSubtractDuration<chrono::milliseconds>(result);
    const auto micro = result.count();
    return LocalTime(LocalTimeParameters{h, m, s, milli, micro});
  }

  friend LocalTime operator+(const Duration &dur, const LocalTime &local_time) { return local_time + dur; }

  friend LocalTime operator-(const LocalTime &local_time, const Duration &duration) { return local_time + (-duration); }

  friend Duration operator-(const LocalTime &lhs, const LocalTime &rhs) {
    Duration lhs_dur(lhs.MicrosecondsSinceEpoch());
    Duration rhs_dur(rhs.MicrosecondsSinceEpoch());
    return lhs_dur - rhs_dur;
  }

  uint8_t hour;
  uint8_t minute;
  uint8_t second;
  uint16_t millisecond;
  uint16_t microsecond;
};

struct LocalTimeHash {
  size_t operator()(const LocalTime &local_time) const;
};

std::pair<DateParameters, LocalTimeParameters> ParseLocalDateTimeParameters(std::string_view string);

struct LocalDateTime {
  explicit LocalDateTime(int64_t microseconds);
  explicit LocalDateTime(const DateParameters &date_parameters, const LocalTimeParameters &local_time_parameters);
  explicit LocalDateTime(const Date &date, const LocalTime &local_time);

  int64_t MicrosecondsSinceEpoch() const;
  int64_t SecondsSinceEpoch() const;  // seconds since epoch
  int64_t SubSecondsAsNanoseconds() const;
  std::string ToString() const;

  auto operator<=>(const LocalDateTime &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const LocalDateTime &ldt) { return os << ldt.ToString(); }

  friend LocalDateTime operator+(const LocalDateTime &dt, const Duration &dur) {
    const auto local_date_time_as_duration = Duration(dt.MicrosecondsSinceEpoch());
    const auto result = local_date_time_as_duration + dur;
    namespace chrono = std::chrono;
    const auto ymd = chrono::year_month_day(chrono::sys_days(chrono::days(result.Days())));
    const auto date_part =
        Date({static_cast<int>(ymd.year()), static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day())});
    const auto local_time_part = LocalTime(result.SubDaysAsMicroseconds());
    return LocalDateTime(date_part, local_time_part);
  }

  friend LocalDateTime operator+(const Duration &dur, const LocalDateTime &dt) { return dt + dur; }

  friend LocalDateTime operator-(const LocalDateTime &dt, const Duration &dur) { return dt + (-dur); }

  friend Duration operator-(const LocalDateTime &lhs, const LocalDateTime &rhs) {
    return Duration(lhs.MicrosecondsSinceEpoch()) - Duration(rhs.MicrosecondsSinceEpoch());
  }

  Date date;
  LocalTime local_time;
};

struct LocalDateTimeHash {
  size_t operator()(const LocalDateTime &local_date_time) const;
};

class Timezone {
 private:
  std::variant<std::chrono::minutes, const std::chrono::time_zone *> offset_;

 public:
  explicit Timezone(const std::chrono::minutes offset) : offset_{offset} {}
  explicit Timezone(const std::chrono::time_zone *timezone) : offset_{timezone} {}
  explicit Timezone(const std::string &timezone_name) : offset_{std::chrono::locate_zone(timezone_name)} {}

  const Timezone *operator->() const { return this; }

  bool operator==(const Timezone &) const = default;

  template <class DurationT>
  std::chrono::minutes OffsetInMinutes(std::chrono::sys_time<DurationT> time_point) const {
    if (std::holds_alternative<std::chrono::minutes>(offset_)) {
      return std::get<std::chrono::minutes>(offset_);
    }

    return std::chrono::duration_cast<std::chrono::minutes>(
        std::get<const std::chrono::time_zone *>(offset_)->get_info(time_point).offset);
  }

  template <class DurationT>
  std::chrono::sys_info get_info(std::chrono::sys_time<DurationT> time_point) const {
    if (std::holds_alternative<std::chrono::minutes>(offset_)) {
      const auto offset = std::get<std::chrono::minutes>(offset_);
      return std::chrono::sys_info{
          .begin = std::chrono::sys_seconds::min(),
          .end = std::chrono::sys_seconds::max(),
          .offset = std::chrono::duration_cast<std::chrono::seconds>(offset),
          .save = std::chrono::minutes{0},
          .abbrev = "",  // custom timezones specified by offset don’t have names
      };
    }
    return std::get<const std::chrono::time_zone *>(offset_)->get_info(time_point);
  }

  template <class DurationT>
  auto to_local(std::chrono::sys_time<DurationT> time_point) const {
    if (std::holds_alternative<std::chrono::minutes>(offset_)) {
      using local_time = std::chrono::local_time<std::common_type_t<DurationT, std::chrono::minutes>>;
      return local_time{(time_point + OffsetInMinutes(time_point)).time_since_epoch()};
    }
    return std::get<const std::chrono::time_zone *>(offset_)->to_local(time_point);
  }

  template <class DurationT>
  auto to_sys(std::chrono::local_time<DurationT> time_point) const {
    if (std::holds_alternative<std::chrono::minutes>(offset_)) {
      using sys_time = std::chrono::sys_time<std::common_type_t<DurationT, std::chrono::minutes>>;
      return sys_time{(time_point - std::get<std::chrono::minutes>(offset_)).time_since_epoch()};
    }
    return std::get<const std::chrono::time_zone *>(offset_)->to_sys(time_point);
  }

  std::string_view TimezoneName() const {
    if (std::holds_alternative<std::chrono::minutes>(offset_)) {
      return "";
    }
    return std::get<const std::chrono::time_zone *>(offset_)->name();
  }
};
}  // namespace memgraph::utils

namespace std::chrono {

template <>
struct zoned_traits<memgraph::utils::Timezone> {
  static memgraph::utils::Timezone default_zone() { return memgraph::utils::Timezone{minutes{0}}; }
};

}  // namespace std::chrono

namespace memgraph::utils {

struct ZonedDateTimeParameters {
  DateParameters date;
  LocalTimeParameters local_time;
  Timezone timezone;

  bool operator==(const ZonedDateTimeParameters &) const = default;
};

ZonedDateTimeParameters ParseZonedDateTimeParameters(std::string_view string);

struct ZonedDateTime {
  explicit ZonedDateTime(const ZonedDateTimeParameters &zoned_date_time_parameters);
  explicit ZonedDateTime(const ZonedDateTime &zoned_date_time);
  explicit ZonedDateTime(const std::chrono::zoned_time<std::chrono::microseconds, Timezone> &zoned_time);

  int64_t MicrosecondsSinceEpoch() const;
  int64_t SecondsSinceEpoch() const;
  int64_t SubSecondsAsNanoseconds() const;
  std::string ToString() const;

  bool operator==(const ZonedDateTime &other) const;

  std::strong_ordering operator<=>(const ZonedDateTime &other) const;

  std::chrono::minutes OffsetInMinutes() const {
    return zoned_time.get_time_zone().OffsetInMinutes(zoned_time.get_sys_time());
  }

  std::string_view TimezoneName() const { return zoned_time.get_time_zone().TimezoneName(); }

  friend std::ostream &operator<<(std::ostream &os, const ZonedDateTime &zdt) { return os << zdt.ToString(); }

  friend ZonedDateTime operator+(const ZonedDateTime &zdt, const Duration &dur) {
    return ZonedDateTime(std::chrono::zoned_time(
        zdt.zoned_time.get_time_zone(), zdt.zoned_time.get_sys_time() + std::chrono::microseconds(dur.microseconds)));
  }

  friend ZonedDateTime operator+(const Duration &dur, const ZonedDateTime &zdt) { return zdt + dur; }

  friend ZonedDateTime operator-(const ZonedDateTime &zdt, const Duration &dur) { return zdt + (-dur); }

  friend Duration operator-(const ZonedDateTime &lhs, const ZonedDateTime &rhs) {
    return Duration(lhs.MicrosecondsSinceEpoch()) - Duration(rhs.MicrosecondsSinceEpoch());
  }

  std::chrono::zoned_time<std::chrono::microseconds, Timezone> zoned_time;
};

Date CurrentDate();
LocalTime CurrentLocalTime();
LocalDateTime CurrentLocalDateTime();
}  // namespace memgraph::utils
