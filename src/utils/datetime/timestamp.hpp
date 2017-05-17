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

  Timestamp(std::time_t time, long nsec = 0)
      : unix_time(time), nsec(nsec) {
    auto result = gmtime_r(&time, &this->time);

    if (result == nullptr)
      throw DatetimeError("Unable to construct from {}", time);
  }

  Timestamp(const Timestamp&) = default;
  Timestamp(Timestamp&&) = default;

  static Timestamp now() {
    timespec time;
    clock_gettime(CLOCK_REALTIME, &time);

    return {time.tv_sec, time.tv_nsec};
  }

  long year() const { return time.tm_year + 1900; }

  long month() const { return time.tm_mon + 1; }

  long day() const { return time.tm_mday; }

  long hour() const { return time.tm_hour; }

  long min() const { return time.tm_min; }

  long sec() const { return time.tm_sec; }

  long subsec() const { return nsec / 10000; }

  const std::string to_iso8601() const {
    return fmt::format(fiso8601, year(), month(), day(), hour(), min(), sec(),
                       subsec());
  }

  const std::string to_string(const std::string &format = fiso8601) const {
    return fmt::format(format, year(), month(), day(), hour(), min(), sec(),
                       subsec()); 
  }

  friend std::ostream& operator<<(std::ostream& stream, const Timestamp& ts) {
    return stream << ts.to_iso8601();
  }

  operator std::string() const { return to_string(); }

  constexpr friend bool operator==(const Timestamp& a, const Timestamp& b) {
    return a.unix_time == b.unix_time && a.nsec == b.nsec;
  }

  constexpr friend bool operator<(const Timestamp& a, const Timestamp& b) {
    return a.unix_time < b.unix_time ||
           (a.unix_time == b.unix_time && a.nsec < b.nsec);
  }

 private:
  std::tm time;

  std::time_t unix_time;
  long nsec;

  static constexpr auto fiso8601 =
      "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}.{:05d}Z";
};
