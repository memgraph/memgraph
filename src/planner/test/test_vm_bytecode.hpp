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

/// GTest wrappers for bytecode validators and test-only assertion helpers.
///
/// The actual validation logic lives in planner/pattern/vm/bytecode_validator.hpp.
/// This header provides:
///   - GTestReporter: adapts the validator Reporter concept to gtest EXPECT_*/FAIL
///   - ExpectValidBytecode: convenience entry points for tests
///   - ExpectBytecode / VerifyBytecode: test-only assertion helpers
///   - R(), N(), At(), Slot(): shorthand constructors for readable test bytecode

#include <gtest/gtest.h>

#include <ApprovalTests.hpp>
#include <span>

#include "planner/pattern/vm/bytecode_validator.hpp"
#include "planner/pattern/vm/tracer.hpp"

namespace memgraph::planner::core {

using namespace pattern::vm;

// ============================================================================
// Bytecode assertion helpers (test-only)
// ============================================================================

/// Short aliases for register/address strong types, keeping expected bytecode readable.
constexpr auto R(uint8_t v) -> EClassReg { return EClassReg{v}; }

constexpr auto N(uint8_t v) -> ENodeReg { return ENodeReg{v}; }

constexpr auto At(uint16_t v) -> InstrAddr { return InstrAddr{v}; }

constexpr auto Slot(uint8_t v) -> SlotIdx { return SlotIdx{v}; }

/// Compare compiled bytecode against an expected instruction sequence.
/// On mismatch, prints a side-by-side diff of expected vs actual using the disassembler.
template <typename Symbol>
void ExpectBytecode(std::span<Instruction const> actual, std::span<Symbol const> symbols,
                    std::initializer_list<Instruction> expected_il, char const *file, int line) {
  auto expected = std::vector<Instruction>(expected_il);
  auto actual_str = disassemble(actual, symbols);
  auto expected_str = disassemble(std::span<Instruction const>{expected}, symbols);

  EXPECT_EQ(actual.size(), expected.size()) << "Instruction count mismatch\n"
                                            << "Expected bytecode:\n"
                                            << expected_str << "\nActual bytecode:\n"
                                            << actual_str;

  auto n = std::min(actual.size(), expected.size());
  for (std::size_t i = 0; i < n; ++i) {
    EXPECT_EQ(actual[i], expected[i]) << "Instruction mismatch at position " << i << "\nExpected bytecode:\n"
                                      << expected_str << "\nActual bytecode:\n"
                                      << actual_str;
  }
}

#define EXPECT_BYTECODE(actual_code, symbols, ...) ExpectBytecode(actual_code, symbols, __VA_ARGS__, __FILE__, __LINE__)

/// Verify compiled bytecode matches an approved snapshot (disassembled text).
template <typename Symbol>
void VerifyBytecode(std::span<Instruction const> code, std::span<Symbol const> symbols) {
  ApprovalTests::Approvals::verify(disassemble(code, symbols));
}

// ============================================================================
// GTestReporter — adapts validator Reporter concept to gtest
// ============================================================================

/// Reporter that delegates to gtest EXPECT_* macros.
/// Messages are formatted lazily — fmt::format is only called on failure (gtest
/// short-circuits the << stream when the assertion passes).
/// assert_* methods return false on failure so validators can early-return.
class GTestReporter {
 public:
  template <typename... Args>
  void expect_true(bool cond, fmt::format_string<Args...> fmt, Args &&...args) {
    EXPECT_TRUE(cond) << fmt::format(fmt, std::forward<Args>(args)...);
  }

  template <typename A, typename B, typename... Args>
  void expect_eq(A const &a, B const &b, fmt::format_string<Args...> fmt, Args &&...args) {
    EXPECT_EQ(a, b) << fmt::format(fmt, std::forward<Args>(args)...);
  }

  template <typename A, typename B, typename... Args>
  void expect_lt(A const &a, B const &b, fmt::format_string<Args...> fmt, Args &&...args) {
    EXPECT_LT(a, b) << fmt::format(fmt, std::forward<Args>(args)...);
  }

  template <typename A, typename B, typename... Args>
  void expect_ne(A const &a, B const &b, fmt::format_string<Args...> fmt, Args &&...args) {
    EXPECT_NE(a, b) << fmt::format(fmt, std::forward<Args>(args)...);
  }

  template <typename A, typename B, typename... Args>
  void expect_gt(A const &a, B const &b, fmt::format_string<Args...> fmt, Args &&...args) {
    EXPECT_GT(a, b) << fmt::format(fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  void fail(fmt::format_string<Args...> fmt, Args &&...args) {
    ADD_FAILURE() << fmt::format(fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  auto assert_true(bool cond, fmt::format_string<Args...> fmt, Args &&...args) -> bool {
    EXPECT_TRUE(cond) << fmt::format(fmt, std::forward<Args>(args)...);
    return cond;
  }

  template <typename A, typename B, typename... Args>
  auto assert_lt(A const &a, B const &b, fmt::format_string<Args...> fmt, Args &&...args) -> bool {
    bool ok = a < b;
    EXPECT_LT(a, b) << fmt::format(fmt, std::forward<Args>(args)...);
    return ok;
  }

  template <typename A, typename B, typename... Args>
  auto assert_gt(A const &a, B const &b, fmt::format_string<Args...> fmt, Args &&...args) -> bool {
    bool ok = a > b;
    EXPECT_GT(a, b) << fmt::format(fmt, std::forward<Args>(args)...);
    return ok;
  }

  template <typename A, typename B, typename... Args>
  auto assert_eq(A const &a, B const &b, fmt::format_string<Args...> fmt, Args &&...args) -> bool {
    bool ok = a == b;
    EXPECT_EQ(a, b) << fmt::format(fmt, std::forward<Args>(args)...);
    return ok;
  }
};

// ============================================================================
// ExpectValidBytecode — gtest entry points
// ============================================================================

/// Run all structural invariant checks on a compiled pattern.
template <typename Symbol>
void ExpectValidBytecode(CompiledMatcher<Symbol> const &compiled) {
  GTestReporter r;
  ValidateBytecodeInvariants(r, compiled);
}

}  // namespace memgraph::planner::core
