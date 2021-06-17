#include "query/temporal.hpp"

namespace query {
namespace {
template <typename TFirst, typename TSecond>
auto GetAndSubtractDuration(TSecond &base_duration) {
  const auto duration = std::chrono::duration_cast<TFirst>(base_duration);
  base_duration -= duration;
  return duration.count();
}

constexpr std::chrono::microseconds epoch{std::chrono::years{1970} + std::chrono::months{1} + std::chrono::days{1}};
}  // namespace

Date::Date(const int64_t microseconds) {
  auto chrono_microseconds = std::chrono::microseconds(microseconds);
  chrono_microseconds += epoch;
  MG_ASSERT(chrono_microseconds.count() >= 0, "Invalid Date specified in microseconds");
  years = GetAndSubtractDuration<std::chrono::years>(chrono_microseconds);
  months = GetAndSubtractDuration<std::chrono::months>(chrono_microseconds);
  days = GetAndSubtractDuration<std::chrono::days>(chrono_microseconds);
}

int64_t Date::Microseconds() const {
  auto result = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::years{years} + std::chrono::months{months} + std::chrono::days{days});

  result -= epoch;
  return result.count();
}

LocalTime::LocalTime(const int64_t microseconds) {
  auto chrono_microseconds = std::chrono::microseconds(microseconds);
  MG_ASSERT(chrono_microseconds.count() >= 0, "Negative LocalTime specified in microseconds");

  const uint64_t parsed_hours = GetAndSubtractDuration<std::chrono::hours>(chrono_microseconds);
  MG_ASSERT(parsed_hours <= 23, "invalid LocalTime specified in microseconds");

  minutes = GetAndSubtractDuration<std::chrono::minutes>(chrono_microseconds);
  seconds = GetAndSubtractDuration<std::chrono::seconds>(chrono_microseconds);
  milliseconds = GetAndSubtractDuration<std::chrono::milliseconds>(chrono_microseconds);
  this->microseconds = chrono_microseconds.count();
}

int64_t LocalTime::Microseconds() const {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::hours{hours} + std::chrono::minutes{minutes} + std::chrono::seconds{seconds} +
             std::chrono::milliseconds{milliseconds} + std::chrono::microseconds{microseconds})
      .count();
}

LocalDateTime::LocalDateTime(const int64_t microseconds) {
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
int64_t LocalDateTime::Microseconds() const {
  auto result = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::years{years} + std::chrono::months{months} + std::chrono::days{days} + std::chrono::hours{hours} +
      std::chrono::minutes{minutes} + std::chrono::seconds{seconds} + std::chrono::milliseconds{milliseconds} +
      std::chrono::microseconds{microseconds});

  result -= epoch;
  return result.count();
}

Duration::Duration(int64_t microseconds) {
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
int64_t Duration::Microseconds() const {
  auto result = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::years{years} + std::chrono::months{months} + std::chrono::days{days} + std::chrono::hours{hours} +
      std::chrono::minutes{minutes} + std::chrono::seconds{seconds} + std::chrono::milliseconds{milliseconds} +
      std::chrono::microseconds{microseconds});

  result -= epoch;
  return result.count();
}

Duration Duration::operator-() const {
  Duration result{*this};
  result.negative = !negative;
  return result;
}

}  // namespace query
