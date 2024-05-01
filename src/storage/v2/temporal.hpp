// Copyright 2024 Memgraph Ltd.
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
#include <cstdint>
#include <iosfwd>
#include <string_view>

#include "utils/temporal.hpp"

namespace memgraph::storage {

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
  friend std::ostream &operator<<(std::ostream &os, const TemporalData &t) {
    switch (t.type) {
      case TemporalType::Date:
        return os << "DATE(\"" << utils::Date(t.microseconds) << "\")";
      case TemporalType::LocalTime:
        return os << "LOCALTIME(\"" << utils::LocalTime(t.microseconds) << "\")";
      case TemporalType::LocalDateTime:
        return os << "LOCALDATETIME(\"" << utils::LocalDateTime(t.microseconds) << "\")";
      case TemporalType::Duration:
        return os << "DURATION(\"" << utils::Duration(t.microseconds) << "\")";
    }
  }
  TemporalType type;
  int64_t microseconds;
};

}  // namespace memgraph::storage
