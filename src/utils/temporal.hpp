#pragma once

#include <cstdint>

#include <chrono>
#include <iomanip>
#include <iostream>
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"

namespace utils {

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
    os << std::setfill('0') << std::setw(4) << date.years << '-';
    os << std::setw(2) << static_cast<int>(date.months) << '-';
    os << std::setw(2) << static_cast<int>(date.days);
    return os;
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
  int64_t ToNanoseconds() const;

  auto operator<=>(const LocalTime &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const LocalTime &lt) {
    namespace chrono = std::chrono;
    using milli = chrono::milliseconds;
    using micro = chrono::microseconds;
    const auto subseconds = milli(lt.milliseconds) + chrono::duration_cast<milli>(micro(lt.microseconds));
    os << std::setfill('0') << std::setw(2) << static_cast<int>(lt.hours) << ':';
    os << std::setw(2) << static_cast<int>(lt.minutes) << ':';
    os << std::setw(2) << static_cast<int>(lt.seconds) << '.';
    os << std::setw(6) << subseconds.count();
    return os;
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

  LocalDateTime(const Date &dt, const LocalTime &lt) : date(dt), local_time(lt) {}

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
    const auto micros = chrono::microseconds(dur.microseconds);
    const auto y = chrono::duration_cast<chrono::years>(micros);
    const auto mo = chrono::duration_cast<chrono::months>(micros - y);
    const auto dd = chrono::duration_cast<chrono::days>(micros - y - mo);
    const auto h = chrono::duration_cast<chrono::hours>(micros - y - mo - dd);
    const auto m = chrono::duration_cast<chrono::minutes>(micros - y - mo - dd - h);
    const auto s = chrono::duration_cast<chrono::seconds>(micros - y - mo - dd - h - m);
    os << std::setfill('0');
    os << "P[" << std::setw(4) << y.count() << "]-";
    os << "[" << std::setw(2) << mo.count() << "]-";
    os << "[" << std::setw(2) << dd.count() << "]";
    os << "T[" << std::setw(2) << h.count() << "]:";
    os << "[" << std::setw(2) << m.count() << "]:";
    os << "[" << std::setw(2) << s.count() << "]";
    return os;
  }

  Duration operator-() const;

  int64_t microseconds;
};

struct DurationHash {
  size_t operator()(const Duration &duration) const;
};

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

constexpr std::chrono::days DaysSinceEpoch(uint16_t years, uint8_t months, uint8_t days) {
  namespace chrono = std::chrono;
  const auto ymd = chrono::year_month_day(chrono::year(years), chrono::month(months), chrono::day(days));
  return chrono::sys_days{ymd}.time_since_epoch();
}

}  // namespace utils
