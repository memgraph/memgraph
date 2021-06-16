#pragma once
#include <cstdint>

namespace query {

struct Date {
  uint64_t year;
  uint8_t month;
  uint8_t day;
};

struct LocalTime {
  uint8_t hour;
  uint8_t minute;
  uint8_t second;
  uint16_t milliseconds;
  uint16_t microseconds;
};

struct LocalDateTime {
  Date date;
  LocalTime local_time;
};

struct Duration {
  uint64_t months;
  uint64_t days;
  uint64_t seconds;
  uint64_t milliseconds;
  uint64_t microseconds;
};

}  // namespace query
