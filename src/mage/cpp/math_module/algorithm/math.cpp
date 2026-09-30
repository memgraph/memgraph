// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "math.hpp"
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string_view>

namespace Math {

namespace {
RoundingMode StringToRoundingModeImpl(std::string_view mode_str) {
  if (mode_str == "CEILING") return RoundingMode::CEILING;
  if (mode_str == "FLOOR") return RoundingMode::FLOOR;
  if (mode_str == "UP") return RoundingMode::UP;
  if (mode_str == "DOWN") return RoundingMode::DOWN;
  if (mode_str == "HALF_EVEN") return RoundingMode::HALF_EVEN;
  if (mode_str == "HALF_DOWN") return RoundingMode::HALF_DOWN;
  if (mode_str == "HALF_UP") return RoundingMode::HALF_UP;
  if (mode_str == "UNNECESSARY") return RoundingMode::UNNECESSARY;
  throw std::invalid_argument("Invalid rounding mode: " + std::string(mode_str));
}

double ApplyRounding(double value, int precision, RoundingMode mode) {
  const double multiplier = std::pow(10.0, precision);
  const double scaled_value = value * multiplier;

  switch (mode) {
    case RoundingMode::CEILING:
      return std::ceil(scaled_value) / multiplier;

    case RoundingMode::FLOOR:
      return std::floor(scaled_value) / multiplier;

    case RoundingMode::UP:
      return (value >= 0 ? std::ceil(scaled_value) : std::floor(scaled_value)) / multiplier;

    case RoundingMode::DOWN:
      return (value >= 0 ? std::floor(scaled_value) : std::ceil(scaled_value)) / multiplier;

    case RoundingMode::HALF_UP:
      return std::round(scaled_value) / multiplier;

    case RoundingMode::HALF_DOWN: {
      const double fractional_part = scaled_value - std::floor(scaled_value);
      if (fractional_part < 0.5) {
        return std::floor(scaled_value) / multiplier;
      }
      if (fractional_part > 0.5) {
        return std::ceil(scaled_value) / multiplier;
      }
      return std::floor(scaled_value) / multiplier;
    }

    case RoundingMode::HALF_EVEN: {
      const double fractional_part = scaled_value - std::floor(scaled_value);
      if (fractional_part < 0.5) {
        return std::floor(scaled_value) / multiplier;
      }
      if (fractional_part > 0.5) {
        return std::ceil(scaled_value) / multiplier;
      }
      const double floor_val = std::floor(scaled_value);
      if (static_cast<int64_t>(floor_val) % 2 == 0) {
        return floor_val / multiplier;
      }
      return std::ceil(scaled_value) / multiplier;
    }

    case RoundingMode::UNNECESSARY: {
      const double exact_result = scaled_value;
      if (exact_result == std::floor(exact_result)) {
        return exact_result / multiplier;
      }
      throw std::runtime_error("Rounding necessary for UNNECESSARY mode");
    }

    default:
      throw std::invalid_argument("Unknown rounding mode");
  }
}

RoundingMode StringToRoundingMode(std::string_view mode_str) { return StringToRoundingModeImpl(mode_str); }
}  // namespace

void Round(mgp_list *args, mgp_func_context *ctx [[maybe_unused]], mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  auto result = mgp::Result(res);
  const auto arguments = mgp::List(args);

  try {
    const auto value_arg = arguments[0];
    const auto precision_arg = arguments[1];
    const auto mode_arg = arguments[2];

    // invalid type, return null
    if (!value_arg.IsDouble() || !precision_arg.IsInt() || !mode_arg.IsString()) {
      result.SetValue();
      return;
    }

    auto value = value_arg.ValueDouble();
    auto precision = precision_arg.ValueInt();
    auto mode_str = mode_arg.ValueString();

    const RoundingMode mode = StringToRoundingMode(mode_str);
    const double rounded_value = ApplyRounding(value, static_cast<int>(precision), mode);

    result.SetValue(rounded_value);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
  }
}

}  // namespace Math
