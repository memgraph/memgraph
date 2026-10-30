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

#include "query/checked_arith.hpp"

#include <string>

#include "query/exceptions.hpp"

namespace memgraph::query {

namespace {
const char *Symbol(ArithOp op) {
  switch (op) {
    case ArithOp::Add:
      return "+";
    case ArithOp::Sub:
    case ArithOp::Neg:
      return "-";
    case ArithOp::Mul:
      return "*";
    case ArithOp::Div:
      return "/";
    case ArithOp::Mod:
      return "%";
    case ArithOp::Abs:
      return "abs";
  }
  return "?";
}
}  // namespace

void ThrowIntegerOverflow(ArithOp op) {
  throw ArithmeticException(std::string("Integer overflow in '") + Symbol(op) + "'.");
}

void ThrowDivisionByZero(ArithOp op) {
  throw ArithmeticException(std::string("Division by zero in '") + Symbol(op) + "'.");
}

}  // namespace memgraph::query
