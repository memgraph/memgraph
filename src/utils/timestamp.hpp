// Copyright 2023 Memgraph Ltd.
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
#include <ctime>
#include <iomanip>
#include <ostream>

#include <fmt/format.h>

#include "utils/exceptions.hpp"

namespace memgraph::utils {

class TimestampError : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};

class Timestamp final {
 public:
  Timestamp() : Timestamp(0, 0) {}

  Timestamp(std::time_t time, long nsec = 0) : unix_time(time), nsec(nsec) {
    auto *result = gmtime_r(&time, &this->time);

    if (result == nullptr) throw TimestampError("Unable to construct from {}", time);
  }

  static Timestamp Now() {
    timespec time;
    clock_gettime(CLOCK_REALTIME, &time);

    return {time.tv_sec, time.tv_nsec};
  }

  auto SecSinceTheEpoch() const { return unix_time; }

  double SecWithNsecSinceTheEpoch() const { return (double)unix_time + (double)nsec / 1e9; }

  auto NanoSec() const { return nsec; }

  long Year() const { return time.tm_year + 1900; }

  long Month() const { return time.tm_mon + 1; }

  long Day() const { return time.tm_mday; }

  long Hour() const { return time.tm_hour; }

  long Min() const { return time.tm_min; }

  long Sec() const { return time.tm_sec; }

  long Usec() const { return nsec / 1000; }

  std::string ToIso8601() const { return fmt::format(fiso8601, Year(), Month(), Day(), Hour(), Min(), Sec(), Usec()); }

  std::string ToString(const std::string &format = fiso8601) const {
    return fmt::format(fmt::runtime(format), Year(), Month(), Day(), Hour(), Min(), Sec(), Usec());
  }

  friend std::ostream &operator<<(std::ostream &stream, const Timestamp &ts) { return stream << ts.ToIso8601(); }

  operator std::string() const { return ToString(); }

  constexpr friend bool operator==(const Timestamp &a, const Timestamp &b) {
    return a.unix_time == b.unix_time && a.nsec == b.nsec;
  }

  constexpr friend bool operator<(const Timestamp &a, const Timestamp &b) {
    return a.unix_time < b.unix_time || (a.unix_time == b.unix_time && a.nsec < b.nsec);
  }

  constexpr friend bool operator!=(const Timestamp &a, const Timestamp &b) { return !(a == b); }

  constexpr friend bool operator<=(const Timestamp &a, const Timestamp &b) { return a < b || a == b; }

  constexpr friend bool operator>(const Timestamp &a, const Timestamp &b) { return !(a <= b); }

  constexpr friend bool operator>=(const Timestamp &a, const Timestamp &b) { return !(a < b); }

 private:
  std::tm time;

  /** http://en.cppreference.com/w/cpp/chrono/c/time_t */
  std::time_t unix_time;
  long nsec;

  static constexpr auto fiso8601 = "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}.{:06d}Z";
};

}  // namespace memgraph::utils
