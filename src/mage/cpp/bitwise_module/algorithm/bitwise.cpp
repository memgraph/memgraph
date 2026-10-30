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

#include "bitwise.hpp"

#include <algorithm>
#include <stdexcept>
#include <string>

namespace Bitwise {

namespace {

enum class Operator : uint8_t { AND, OR, XOR, NOT, SHIFT_LEFT, SHIFT_RIGHT, SHIFT_RIGHT_UNSIGNED };

// Every accepted operator is ASCII, so ASCII lowercasing matches Java's String.toLowerCase for them.
std::string ToLower(std::string_view str) {
  std::string lowered(str);
  std::ranges::transform(
      lowered, lowered.begin(), [](char c) { return c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c; });
  return lowered;
}

Operator ParseOperator(std::string_view op) {
  const auto lowered = ToLower(op);
  if (lowered == "&" || lowered == "and") return Operator::AND;
  if (lowered == "|" || lowered == "or") return Operator::OR;
  if (lowered == "^" || lowered == "xor") return Operator::XOR;
  if (lowered == "~" || lowered == "not") return Operator::NOT;
  if (lowered == "<<" || lowered == "left shift") return Operator::SHIFT_LEFT;
  if (lowered == ">>" || lowered == "right shift") return Operator::SHIFT_RIGHT;
  if (lowered == ">>>" || lowered == "right shift unsigned") return Operator::SHIFT_RIGHT_UNSIGNED;
  throw std::invalid_argument("Invalid bitwise operator : '" + std::string(op) + "'");
}

// Java shifts a long by the six low bits of the distance. Taking them here also keeps the C++ shifts defined, as
// shifting by a negative amount or by 64 or more is undefined behavior.
int ShiftDistance(int64_t distance) { return static_cast<int>(distance & 63); }

}  // namespace

std::optional<int64_t> Op(std::optional<int64_t> a, std::optional<std::string_view> op, std::optional<int64_t> b) {
  // The NULL checks come before the operator is parsed, and the "~" comparison is case-sensitive: only the literal
  // "~" may leave the right operand NULL, while "NOT" with a NULL right operand yields NULL. Both are intentional, for
  // compatibility, and they make Op(60, "NAND", NULL) NULL rather than an error.
  if (!a || !op || op->empty()) return std::nullopt;
  if (*op != "~" && !b) return std::nullopt;

  // Shifts go through uint64_t so that bits shifted past the sign wrap around as in Java and `>>>` fills with zeros.
  const auto unsigned_a = static_cast<uint64_t>(*a);
  switch (ParseOperator(*op)) {
    case Operator::AND:
      return *a & *b;
    case Operator::OR:
      return *a | *b;
    case Operator::XOR:
      return *a ^ *b;
    case Operator::NOT:
      return ~*a;
    case Operator::SHIFT_LEFT:
      return static_cast<int64_t>(unsigned_a << ShiftDistance(*b));
    case Operator::SHIFT_RIGHT:
      return *a >> ShiftDistance(*b);
    case Operator::SHIFT_RIGHT_UNSIGNED:
      return static_cast<int64_t>(unsigned_a >> ShiftDistance(*b));
  }
  return std::nullopt;
}

}  // namespace Bitwise
