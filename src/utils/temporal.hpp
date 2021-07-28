#pragma once

#include <cstdint>

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
    os << std::setw(2) << date.months << '-' << date.days;
    return os;
  }

  int64_t MicrosecondsSinceEpoch() const;
  int64_t ToDays() const;

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

  int64_t MicrosecondsSinceEpoch() const;

  int64_t ToNanoseconds() const;

  auto operator<=>(const LocalTime &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const LocalTime &lt) {
    os << std::setfill('0') << std::setw(2);
    os << lt.hours << ':' << lt.minutes << ':' << lt.seconds;
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
  int64_t ToSeconds() const;
  int64_t ToNanoseconds() const;

  auto operator<=>(const LocalDateTime &) const = default;

  friend std::ostream &operator<<(std::ostream &os, const LocalDateTime &ldt) {
    os << ldt.date << '\n' << ldt.local_time;
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

  friend std::ostream &operator<<(std::ostream &os, const Duration &dur) {
    os << dur.microseconds;
    return os;
  }

  int64_t ToMonths() const;
  int64_t ToDays() const;
  int64_t ToSeconds() const;
  int64_t ToNanoseconds() const;

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

template <Chrono From, Chrono To, typename T = double>
constexpr To CastChrono(const T value) {
  return std::chrono::duration_cast<To>(std::chrono::duration<T, typename From::period>(value));
};
}  // namespace utils
