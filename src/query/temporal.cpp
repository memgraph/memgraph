#include "query/temporal.hpp"

#include <chrono>

#include "utils/exceptions.hpp"
#include "utils/fnv.hpp"

namespace query {
namespace {
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

constexpr std::chrono::microseconds epoch{std::chrono::years{1970} + std::chrono::months{1} + std::chrono::days{1}};

constexpr bool IsInBounds(const auto low, const auto high, const auto value) { return low <= value && value <= high; }

constexpr bool IsValidDay(const auto day, const auto month, const auto year) {
  constexpr std::array<uint8_t, 12> days{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  MG_ASSERT(IsInBounds(1, 12, month), "Invalid month!");

  if (day <= 0) {
    return false;
  }

  const auto is_leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);

  uint8_t leap_day = month == 2 && is_leap_year;

  return day <= (days[month - 1] + leap_day);
}

}  // namespace

Date::Date(const int64_t microseconds) {
  auto chrono_microseconds = std::chrono::microseconds(microseconds);
  chrono_microseconds += epoch;
  MG_ASSERT(chrono_microseconds.count() >= 0, "Invalid Date specified in microseconds");
  years = GetAndSubtractDuration<std::chrono::years>(chrono_microseconds);
  months = GetAndSubtractDuration<std::chrono::months>(chrono_microseconds);
  days = GetAndSubtractDuration<std::chrono::days>(chrono_microseconds);
}

Date::Date(const DateParameters &date_parameters) {
  if (!IsInBounds(0, 9999, date_parameters.years)) {
    throw utils::BasicException("Creating a Date with invalid year parameter.");
  }

  // TODO(antonio2368): Replace with year_month_day when it's implemented
  // https://en.cppreference.com/w/cpp/chrono/year_month_day/ok
  if (!IsInBounds(1, 12, date_parameters.months)) {
    throw utils::BasicException("Creating a Date with invalid month parameter.");
  }

  if (!IsValidDay(date_parameters.days, date_parameters.months, date_parameters.years)) {
    throw utils::BasicException("Creating a Date with invalid day parameter.");
  }

  years = date_parameters.years;
  months = date_parameters.months;
  days = date_parameters.days;
}

int64_t Date::MicrosecondsSinceEpoch() const {
  auto result = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::years{years} + std::chrono::months{months} + std::chrono::days{days});

  result -= epoch;
  return result.count();
}

size_t DateHash::operator()(const Date &date) const {
  utils::HashCombine<uint64_t, uint64_t> hasher;
  size_t result = hasher(0, date.years);
  result = hasher(result, date.months);
  result = hasher(result, date.days);
  return result;
}

LocalTime::LocalTime(const int64_t microseconds) {
  auto chrono_microseconds = std::chrono::microseconds(microseconds);
  MG_ASSERT(chrono_microseconds.count() >= 0, "Negative LocalTime specified in microseconds");

  const auto parsed_hours = GetAndSubtractDuration<std::chrono::hours>(chrono_microseconds);
  MG_ASSERT(parsed_hours <= 23, "Invalid LocalTime specified in microseconds");

  hours = parsed_hours;
  minutes = GetAndSubtractDuration<std::chrono::minutes>(chrono_microseconds);
  seconds = GetAndSubtractDuration<std::chrono::seconds>(chrono_microseconds);
  milliseconds = GetAndSubtractDuration<std::chrono::milliseconds>(chrono_microseconds);
  this->microseconds = chrono_microseconds.count();
}

LocalTime::LocalTime(const LocalTimeParameters &local_time_parameters) {
  if (!IsInBounds(0, 23, local_time_parameters.hours)) {
    throw utils::BasicException("Creating a LocalTime with invalid hour parameter.");
  }

  if (!IsInBounds(0, 59, local_time_parameters.minutes)) {
    throw utils::BasicException("Creating a LocalTime with invalid minutes parameter.");
  }

  if (!IsInBounds(0, 59, local_time_parameters.seconds)) {
    throw utils::BasicException("Creating a LocalTime with invalid seconds parameter.");
  }

  if (!IsInBounds(0, 999, local_time_parameters.milliseconds)) {
    throw utils::BasicException("Creating a LocalTime with invalid seconds parameter.");
  }

  if (!IsInBounds(0, 999, local_time_parameters.microseconds)) {
    throw utils::BasicException("Creating a LocalTime with invalid seconds parameter.");
  }

  hours = local_time_parameters.hours;
  minutes = local_time_parameters.minutes;
  seconds = local_time_parameters.seconds;
  milliseconds = local_time_parameters.milliseconds;
  microseconds = local_time_parameters.microseconds;
}

int64_t LocalTime::MicrosecondsSinceEpoch() const {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::hours{hours} + std::chrono::minutes{minutes} + std::chrono::seconds{seconds} +
             std::chrono::milliseconds{milliseconds} + std::chrono::microseconds{microseconds})
      .count();
}

size_t LocalTimeHash::operator()(const LocalTime &local_time) const {
  utils::HashCombine<uint64_t, uint64_t> hasher;
  size_t result = hasher(0, local_time.hours);
  result = hasher(result, local_time.minutes);
  result = hasher(result, local_time.seconds);
  result = hasher(result, local_time.milliseconds);
  result = hasher(result, local_time.microseconds);
  return result;
}

LocalDateTime::LocalDateTime(const int64_t microseconds) {
  auto chrono_microseconds = std::chrono::microseconds(microseconds);
  date = Date(chrono_microseconds.count());
  chrono_microseconds -= std::chrono::microseconds{date.MicrosecondsSinceEpoch()};
  local_time = LocalTime(chrono_microseconds.count());
}

// return microseconds normilized with regard to epoch time point
int64_t LocalDateTime::MicrosecondsSinceEpoch() const {
  return date.MicrosecondsSinceEpoch() + local_time.MicrosecondsSinceEpoch();
}

LocalDateTime::LocalDateTime(const DateParameters date_parameters, const LocalTimeParameters &local_time_parameters)
    : date(date_parameters), local_time(local_time_parameters) {}

size_t LocalDateTimeHash::operator()(const LocalDateTime &local_date_time) const {
  utils::HashCombine<uint64_t, uint64_t> hasher;
  size_t result = hasher(0, LocalTimeHash{}(local_date_time.local_time));
  result = hasher(result, DateHash{}(local_date_time.date));
  return result;
}

Duration::Duration(int64_t microseconds) { this->microseconds = microseconds; }

namespace {
template <Chrono From, Chrono To>
constexpr To CastChronoDouble(const double value) {
  return std::chrono::duration_cast<To>(std::chrono::duration<double, typename From::period>(value));
};

}  // namespace

Duration::Duration(const DurationParameters &parameters) {
  microseconds = (CastChronoDouble<std::chrono::years, std::chrono::microseconds>(parameters.years) +
                  CastChronoDouble<std::chrono::months, std::chrono::microseconds>(parameters.months) +
                  CastChronoDouble<std::chrono::days, std::chrono::microseconds>(parameters.days) +
                  CastChronoDouble<std::chrono::hours, std::chrono::microseconds>(parameters.hours) +
                  CastChronoDouble<std::chrono::minutes, std::chrono::microseconds>(parameters.minutes) +
                  CastChronoDouble<std::chrono::seconds, std::chrono::microseconds>(parameters.seconds))
                     .count();
}

Duration Duration::operator-() const {
  Duration result{-microseconds};
  return result;
}

size_t DurationHash::operator()(const Duration &duration) const { return std::hash<int64_t>{}(duration.microseconds); }

}  // namespace query
