#pragma once
#include <cstdint>
#include <iostream>
#include <string_view>

namespace storage {

enum class TemporalType : uint8_t { Date = 0, LocalTime, LocalDateTime, Duration };

constexpr std::string_view TemporalTypeTostring(const TemporalType type) {
  switch (type) {
    case TemporalType::Date:
      return "Date";
    case TemporalType::LocalTime:
      return "LocalTime";
    case TemporalType::LocalDateTime:
      return "LocalDateTime";
    case TemporalType::Duration:
      return "Duration";
  }
}

struct TemporalData {
  explicit TemporalData(TemporalType type, int64_t microseconds);

  auto operator<=>(const TemporalData &) const = default;
  friend std::ostream &operator<<(std::ostream &os, const TemporalData &t) { return os << t.microseconds; }
  TemporalType type;
  int64_t microseconds;
};

}  // namespace storage
