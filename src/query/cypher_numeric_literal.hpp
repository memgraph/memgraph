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

#pragma once

#include <cmath>
#include <concepts>
#include <iomanip>
#include <limits>
#include <locale>
#include <optional>
#include <sstream>
#include <string>

#include <fmt/format.h>

namespace memgraph::query {

namespace detail {
// The lexer's FloatingLiteral allows only a '-' exponent sign (CypherLexer.g4:168-170).
inline std::string StripExponentPlus(std::string text) {
  auto const exponent = text.find_first_of("eE");
  if (exponent != std::string::npos && exponent + 1 < text.size() && text[exponent + 1] == '+') {
    text.erase(exponent + 1, 1);
  }
  return text;
}

template <std::floating_point T>
std::optional<std::string> NonFiniteLiteral(T value) {
  if (std::isnan(value)) return "(0.0/0.0)";
  if (std::isinf(value)) return value < T{} ? "(-1.0/0.0)" : "(1.0/0.0)";
  return std::nullopt;
}
}  // namespace detail

// Spell a floating-point value as a Cypher expression that re-parses to the same value and kind.
template <std::floating_point T>
std::string CypherNumericLiteral(T value) {
  if (auto const non_finite = detail::NonFiniteLiteral(value)) return *non_finite;
  std::ostringstream oss;
  oss.imbue(std::locale::classic());
  oss << std::setprecision(std::numeric_limits<T>::max_digits10) << value;
  auto text = detail::StripExponentPlus(oss.str());
  if (text.find_first_of(".eE") == std::string::npos) text += ".0";
  return text;
}

// Shortest round-tripping coordinate spelling; non-finites need an expression because `inf` is not a token.
inline std::string CypherCoordinateLiteral(double value) {
  if (auto const non_finite = detail::NonFiniteLiteral(value)) return *non_finite;
  return detail::StripExponentPlus(fmt::format("{}", value));
}

}  // namespace memgraph::query
