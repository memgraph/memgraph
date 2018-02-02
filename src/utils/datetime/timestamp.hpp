#pragma once

#include <chrono>
#include <ctime>
#include <iomanip>
#include <ostream>

#include <fmt/format.h>

#include "utils/datetime/datetime_error.hpp"
#include "utils/total_ordering.hpp"

class Timestamp : public TotalOrdering<Timestamp> {
 public:
  Timestamp() : Timestamp(0, 0) {}

  Timestamp(std::time_t time, long nsec = 0) : unix_time(time), nsec(nsec) {
    auto result = gmtime_r(&time, &this->time);

    if (result == nullptr)
      throw DatetimeError("Unable to construct from {}", time);
  }

  Timestamp(const Timestamp&) = default;
  Timestamp(Timestamp&&) = default;

  static Timestamp Now() {
    timespec time;
    clock_gettime(CLOCK_REALTIME, &time);

    return {time.tv_sec, time.tv_nsec};
  }

  auto SecSinceTheEpoch() const { return unix_time; }

  auto NanoSec() const { return nsec; }

  long Year() const { return time.tm_year + 1900; }

  long Month() const { return time.tm_mon + 1; }

  long Day() const { return time.tm_mday; }

  long Hour() const { return time.tm_hour; }

  long Min() const { return time.tm_min; }

  long Sec() const { return time.tm_sec; }

  long Subsec() const { return nsec / 10000; }

  const std::string ToIso8601() const {
    return fmt::format(fiso8601, Year(), Month(), Day(), Hour(), Min(), Sec(),
                       Subsec());
  }

  const std::string ToString(const std::string& format = fiso8601) const {
    return fmt::format(format, Year(), Month(), Day(), Hour(), Min(), Sec(),
                       Subsec());
  }

  friend std::ostream& operator<<(std::ostream& stream, const Timestamp& ts) {
    return stream << ts.ToIso8601();
  }

  operator std::string() const { return ToString(); }

  constexpr friend bool operator==(const Timestamp& a, const Timestamp& b) {
    return a.unix_time == b.unix_time && a.nsec == b.nsec;
  }

  constexpr friend bool operator<(const Timestamp& a, const Timestamp& b) {
    return a.unix_time < b.unix_time ||
           (a.unix_time == b.unix_time && a.nsec < b.nsec);
  }

 private:
  std::tm time;

  /** http://en.cppreference.com/w/cpp/chrono/c/time_t */
  std::time_t unix_time;
  long nsec;

  static constexpr auto fiso8601 =
      "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}.{:05d}Z";
};
