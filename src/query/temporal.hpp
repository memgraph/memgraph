#pragma once
#include <chrono>
#include <cstdint>
#include <ratio>

#include "utils/exceptions.hpp"
#include "utils/hash_combine.hpp"
#include "utils/logging.hpp"

namespace query {

template <typename TFirst, typename TSecond>
auto GetAndSubtractDuration(TSecond &base_duration) {
  const auto duration = std::chrono::duration_cast<TFirst>(base_duration);
  base_duration -= duration;
  return duration.count();
}

constexpr std::chrono::microseconds epoch{std::chrono::years{1970} + std::chrono::months{1} + std::chrono::days{1}};

struct Date {
  // we assume we accepted date in microseconds which was normilized using the epoch time point
  explicit Date(const int64_t microseconds) {
    auto chrono_microseconds = std::chrono::microseconds(microseconds);
    chrono_microseconds += epoch;
    MG_ASSERT(chrono_microseconds.count() >= 0, "Invalid Date specified in microseconds");
    years = GetAndSubtractDuration<std::chrono::years>(chrono_microseconds);
    months = GetAndSubtractDuration<std::chrono::months>(chrono_microseconds);
    days = GetAndSubtractDuration<std::chrono::days>(chrono_microseconds);
  }

  // return microseconds normilized with regard to epoch time point
  auto Microseconds() const {
    auto result = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::years{years} + std::chrono::months{months} + std::chrono::days{days});

    result -= epoch;
    return result.count();
  }

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
  explicit LocalTime(const int64_t microseconds) {
    auto chrono_microseconds = std::chrono::microseconds(microseconds);
    MG_ASSERT(chrono_microseconds.count() >= 0, "Negative LocalTime specified in microseconds");

    const uint64_t parsed_hours = GetAndSubtractDuration<std::chrono::hours>(chrono_microseconds);
    MG_ASSERT(parsed_hours <= 23, "invalid LocalTime specified in microseconds");

    minutes = GetAndSubtractDuration<std::chrono::minutes>(chrono_microseconds);
    seconds = GetAndSubtractDuration<std::chrono::seconds>(chrono_microseconds);
    milliseconds = GetAndSubtractDuration<std::chrono::milliseconds>(chrono_microseconds);
    this->microseconds = chrono_microseconds.count();
  }

  auto Microseconds() const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::hours{hours} + std::chrono::minutes{minutes} + std::chrono::seconds{seconds} +
               std::chrono::milliseconds{milliseconds} + std::chrono::microseconds{microseconds})
        .count();
  }

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
  explicit LocalDateTime(const int64_t microseconds) {
    auto chrono_microseconds = std::chrono::microseconds(microseconds);
    chrono_microseconds += epoch;
    MG_ASSERT(chrono_microseconds.count() >= 0, "Negative LocalDateTime specified in microseconds");
    years = GetAndSubtractDuration<std::chrono::years>(chrono_microseconds);
    months = GetAndSubtractDuration<std::chrono::months>(chrono_microseconds);
    days = GetAndSubtractDuration<std::chrono::days>(chrono_microseconds);
    hours = GetAndSubtractDuration<std::chrono::hours>(chrono_microseconds);
    minutes = GetAndSubtractDuration<std::chrono::minutes>(chrono_microseconds);
    seconds = GetAndSubtractDuration<std::chrono::seconds>(chrono_microseconds);
    milliseconds = GetAndSubtractDuration<std::chrono::milliseconds>(chrono_microseconds);
    this->microseconds = chrono_microseconds.count();
  }

  // return microseconds normilized with regard to epoch time point
  auto Microseconds() const {
    auto result = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::years{years} + std::chrono::months{months} + std::chrono::days{days} + std::chrono::hours{hours} +
        std::chrono::minutes{minutes} + std::chrono::seconds{seconds} + std::chrono::milliseconds{milliseconds} +
        std::chrono::microseconds{microseconds});

    result -= epoch;
    return result.count();
  }

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
  explicit Duration(int64_t microseconds) {
    if (microseconds < 0) {
      negative = true;
      microseconds = std::abs(microseconds);
    }

    auto chrono_microseconds = std::chrono::microseconds(microseconds);
    chrono_microseconds += epoch;
    years = GetAndSubtractDuration<std::chrono::years>(chrono_microseconds);
    months = GetAndSubtractDuration<std::chrono::months>(chrono_microseconds);
    days = GetAndSubtractDuration<std::chrono::days>(chrono_microseconds);
    hours = GetAndSubtractDuration<std::chrono::hours>(chrono_microseconds);
    minutes = GetAndSubtractDuration<std::chrono::minutes>(chrono_microseconds);
    seconds = GetAndSubtractDuration<std::chrono::seconds>(chrono_microseconds);
    milliseconds = GetAndSubtractDuration<std::chrono::milliseconds>(chrono_microseconds);
    this->microseconds = chrono_microseconds.count();
  }

  // return microseconds normilized with regard to epoch time point
  auto Microseconds() const {
    auto result = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::years{years} + std::chrono::months{months} + std::chrono::days{days} + std::chrono::hours{hours} +
        std::chrono::minutes{minutes} + std::chrono::seconds{seconds} + std::chrono::milliseconds{milliseconds} +
        std::chrono::microseconds{microseconds});

    result -= epoch;
    return result.count();
  }

  auto operator<=>(const Duration &) const = default;

  Duration operator-() const {
    Duration result{*this};
    result.negative = !negative;
    return result;
  }

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
