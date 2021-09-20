#pragma once

#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>

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

struct DurationParameters {
  double days{0};
  double hours{0};
  double minutes{0};
  double seconds{0};
  double milliseconds{0};
  double microseconds{0};
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

  friend std::ostream &operator<<(std::ostream &os, const Duration &dur) {
    // Format [nD]T[nH]:[nM]:[nS].
    namespace chrono = std::chrono;
    auto micros = chrono::microseconds(dur.microseconds);
    const auto dd = GetAndSubtractDuration<chrono::days>(micros);
    const auto h = GetAndSubtractDuration<chrono::hours>(micros);
    const auto m = GetAndSubtractDuration<chrono::minutes>(micros);
    const auto s = GetAndSubtractDuration<chrono::seconds>(micros);
    os << fmt::format("P{}DT{}H{}M", dd, h, m);
    if (s == 0 && micros.count() < 0) {
      os << '-';
    }
    return os << fmt::format("{}.{:0>6}S", s, std::abs(micros.count()));
  }

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
  int64_t years{0};
  int64_t months{1};
  int64_t days{1};

  bool operator==(const DateParameters &) const = default;
};

// boolean indicates whether the parsed string was in extended format
std::pair<DateParameters, bool> ParseDateParameters(std::string_view date_string);

constexpr std::chrono::year_month_day ToChronoYMD(uint16_t years, uint8_t months, uint8_t days) {
  namespace chrono = std::chrono;
  return chrono::year_month_day(chrono::year(years), chrono::month(months), chrono::day(days));
}

constexpr std::chrono::sys_days ToChronoSysDaysYMD(uint16_t years, uint8_t months, uint8_t days) {
  return std::chrono::sys_days(ToChronoYMD(years, months, days));
}

constexpr std::chrono::days DaysSinceEpoch(uint16_t years, uint8_t months, uint8_t days) {
  return ToChronoSysDaysYMD(years, months, days).time_since_epoch();
}

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
    const auto lhs_days = utils::DaysSinceEpoch(lhs.years, lhs.months, lhs.days);
    const auto rhs_days = utils::DaysSinceEpoch(rhs.years, rhs.months, rhs.days);
    const auto days_elapsed = lhs_days - rhs_days;
    return Duration(chrono::duration_cast<chrono::microseconds>(days_elapsed).count());
  }

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

  friend LocalTime operator+(const LocalTime &local_time, const Duration &dur) {
    namespace chrono = std::chrono;
    auto rhs = dur.SubDaysAsMicroseconds();
    auto abs = [](auto value) { return (value >= 0) ? value : -value; };
    const auto lhs = local_time.MicrosecondsSinceEpoch();
    if (rhs < 0 && lhs < abs(rhs)) {
      constexpr int64_t one_day_in_microseconds = 24LL * 60 * 60 * 1000 * 1000;
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

Date UtcToday();
LocalTime UtcLocalTime();
LocalDateTime UtcLocalDateTime();
}  // namespace utils
