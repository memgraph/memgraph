#pragma once

#include <chrono>
#include <cstdint>
#include <iomanip>
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
constexpr void ThrowIfOverflows(const TType &lhs, const TType &rhs) {
  if (lhs > 0 && rhs > 0 && lhs > (std::numeric_limits<TType>::max() - rhs)) [[unlikely]] {
    throw utils::BasicException("Overflow of durations");
  }
}

template <typename TType>
constexpr void ThrowIfUnderflows(const TType &lhs, const TType &rhs) {
  if (lhs < 0 && rhs < 0 && lhs < (std::numeric_limits<int64_t>::min() + (-rhs))) [[unlikely]] {
    throw utils::BasicException("Underflow of durations");
  }
}

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

struct Date;
struct LocalTime;
struct LocalDateTime;

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
    os << std::setfill('0');
    os << "P" << std::setw(4) << y << "-";
    os << std::setw(2) << mo << "-";
    os << std::setw(2) << dd << "";
    os << "T" << std::setw(2) << h << ":";
    os << std::setw(2) << m << ":";
    os << std::setw(2) << s << ".";
    os << std::setw(6) << micros.count();
    return os;
  }

  Duration operator-() const;

  friend Duration operator+(const Duration &lhs, const Duration rhs) {
    // Overflow
    if (lhs.microseconds > 0 && rhs.microseconds > 0 &&
        lhs.microseconds > (std::numeric_limits<int64_t>::max() - rhs.microseconds)) [[unlikely]] {
      throw utils::BasicException("Overflow of durations");
    }

    if (lhs.microseconds < 0 && rhs.microseconds < 0 &&
        lhs.microseconds < (std::numeric_limits<int64_t>::min() + (-rhs.microseconds))) [[unlikely]] {
      throw utils::BasicException("Underflow of durations");
    }
    return Duration(lhs.microseconds + rhs.microseconds);
  }

  friend Duration operator-(const Duration &lhs, const Duration rhs) { return lhs + (-rhs); }

  friend LocalTime operator+(const LocalTime &lhs, const Duration &dur);
  friend LocalTime operator-(const LocalTime &lhs, const Duration &rhs);

  friend Date operator+(const Date &date, const Duration &dur);
  friend Date operator-(const Date &date, const Duration &dur);

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
  const auto ymd = chrono::year_month_day(chrono::year(years), chrono::month(months), chrono::day(days));
  return chrono::sys_days{ymd};
}

constexpr std::chrono::days DaysSinceEpoch(uint16_t years, uint8_t months, uint8_t days) {
  namespace chrono = std::chrono;
  const auto ymd = chrono::year_month_day(chrono::year(years), chrono::month(months), chrono::day(days));
  return chrono::sys_days{ymd}.time_since_epoch();
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
    auto result = Date(date);

    namespace chrono = std::chrono;
    auto micros = chrono::microseconds(dur.microseconds);
    const auto y = GetAndSubtractDuration<chrono::years>(micros);
    const auto mo = GetAndSubtractDuration<chrono::months>(micros);
    const auto dd = GetAndSubtractDuration<chrono::days>(micros);
    result.years += y;
    result.months += mo;

    auto ymd = utils::DaysSinceEpoch(result.years, result.months, result.days);
    ymd += chrono::days(dd);

    return Date(chrono::duration_cast<chrono::microseconds>(ymd).count());
  }

  friend Date operator-(const Date &date, const Duration &dur) { return date + (-dur); }

  // friend Duration operator-(const Date &lhs, const Date &rhs);

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

  int64_t ToNanoseconds() const;
  int64_t ToMicroseconds() const;

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
    auto rhs = chrono::duration_cast<chrono::microseconds>(chrono::seconds(dur.SubDaysAsSeconds())).count();
    auto abs = [](auto value) { return (value >= 0) ? value : -value; };
    const auto lhs = local_time.ToMicroseconds();
    if (rhs < 0 && lhs < abs(rhs)) {
      constexpr int64_t one_day_in_microseconds = 86400000000;
      rhs = one_day_in_microseconds + rhs;
    }
    auto result = chrono::microseconds(lhs + rhs);
    const auto h = GetAndSubtractDuration<chrono::hours>(result) % 24;
    const auto m = GetAndSubtractDuration<chrono::minutes>(result) % 60;
    const auto s = GetAndSubtractDuration<chrono::seconds>(result) % 60;
    const auto milli = GetAndSubtractDuration<chrono::milliseconds>(result);
    const auto micro = result.count();
    return LocalTime(LocalTimeParameters{h, m, s, milli, micro});
  }

  friend LocalTime operator-(const LocalTime &local_time, const Duration &duration) { return local_time + (-duration); }

  friend Duration operator-(const LocalTime &lhs, const LocalTime &rhs) {
    Duration lhs_dur(lhs.ToMicroseconds());
    Duration rhs_dur(rhs.ToMicroseconds());
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
    return LocalDateTime(dt.MicrosecondsSinceEpoch() + dur.microseconds);
  }

  friend LocalDateTime operator-(const LocalDateTime &dt, const Duration &dur) {
    return LocalDateTime(dt.MicrosecondsSinceEpoch() + (-dur.microseconds));
  }

  // friend Duration operator-(const LocalDateTime &lhs, const LocalDateTime &rhs);

  Date date;
  LocalTime local_time;
};

struct LocalDateTimeHash {
  size_t operator()(const LocalDateTime &local_date_time) const;
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
