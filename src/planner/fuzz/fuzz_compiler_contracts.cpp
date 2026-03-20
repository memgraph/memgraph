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

// ============================================================================
// Contract Verification Fuzzer for PatternCompiler
// ============================================================================
//
// KEY INSIGHT: This fuzzer checks STRUCTURAL correctness of compiled bytecode,
// not SEMANTIC correctness. It answers: "Is this bytecode well-formed?" not
// "Does this bytecode match the right things?"
//
// - Structural correctness: register indices in bounds, jump targets valid,
//   iteration instructions paired, slots bound before use, etc.
//
// - Semantic correctness: pattern `Neg(Neg(?x))` actually matches double
//   negations and binds ?x correctly. (See fuzz_pattern_vm.cpp for this.)
//
// Why both matter: A compiler bug might produce bytecode that "works" but
// violates invariants (e.g., unpaired iteration instructions). The VM might
// not crash today, but such bugs indicate deeper problems. This fuzzer
// catches those structural violations that semantic testing might miss.
//
// Flow:
//   Fuzz input → generate Pattern ASTs → compile → verify contracts → abort if violated
//
// Contracts verified:
//
//   1. Register Allocation Contract
//      - All register indices < reported num_eclass_regs/num_enode_regs
//
//   2. Jump Target Contract
//      - All jump targets are valid instruction indices in [0, code.size())
//      - No 0xFFFF placeholders remain after compilation
//
//   3. Symbol Table Contract
//      - All CheckSymbol.arg < symbols.size()
//
//   4. Slot Binding Contract
//      - All BindSlot/CheckSlot/MarkSeen/Yield.arg < num_slots()
//      - binding_order contains valid slot indices without duplicates
//      - slot_to_order is consistent inverse of binding_order
//
//   5. Iteration Pairing Contract
//      - Every IterENodes(dst) has matching NextENode(dst)
//      - Every IterParents(dst) has matching NextParent(dst)
//      - Every IterAllEClasses(dst) has matching NextEClass(dst)
//
//   6. Code Structure Contract
//      - Last instruction is Halt
//      - Patterns with bindings have Yield followed by Jump
//      - Patterns without bindings have no Yield
//
//   7. Register Liveness Contract
//      - Registers are defined before being read (in linear program order)
//      - Registers are defined by IterX/LoadChild/GetENodeEClass before being read
//
//   8. MarkSeen Ordering Contract
//      - MarkSeen/Yield only reference slots already bound via BindSlot
//
//   9. binding_order Consistency Contract
//      - binding_order matches BindSlot instructions in emission order
//
// Running:
//   ./build/src/planner/fuzz/fuzz_compiler_contracts -max_total_time=60
//
// On contract violation, the fuzzer aborts with detailed diagnostics.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <span>
#include <string>
#include <vector>

#include "fuzz_common.hpp"
#include "planner/pattern/vm/bytecode_validator.hpp"
#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/tracer.hpp"

namespace memgraph::planner::core::pattern::vm {

using fuzz::FuzzSymbol;
using fuzz::MultiPatternGenerator;
using fuzz::pattern_to_memgraph;
using fuzz::pattern_to_string;
using fuzz::PatternASTPtr;

// ============================================================================
// Configuration
// ============================================================================

namespace {
bool g_verbose = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// ============================================================================
// ErrorCollectorReporter — collects validation errors for diagnostic output
// ============================================================================

class ErrorCollectorReporter {
 public:
  template <typename... Args>
  void expect_true(bool cond, fmt::format_string<Args...> f, Args &&...args) {
    if (!cond) errors_.push_back(fmt::format(f, std::forward<Args>(args)...));
  }

  template <typename A, typename B, typename... Args>
  void expect_eq(A const &a, B const &b, fmt::format_string<Args...> f, Args &&...args) {
    if (a != b) errors_.push_back(fmt::format(f, std::forward<Args>(args)...));
  }

  template <typename A, typename B, typename... Args>
  void expect_lt(A const &a, B const &b, fmt::format_string<Args...> f, Args &&...args) {
    if (!(a < b)) errors_.push_back(fmt::format(f, std::forward<Args>(args)...));
  }

  template <typename A, typename B, typename... Args>
  void expect_ne(A const &a, B const &b, fmt::format_string<Args...> f, Args &&...args) {
    if (a == b) errors_.push_back(fmt::format(f, std::forward<Args>(args)...));
  }

  template <typename A, typename B, typename... Args>
  void expect_gt(A const &a, B const &b, fmt::format_string<Args...> f, Args &&...args) {
    if (!(a > b)) errors_.push_back(fmt::format(f, std::forward<Args>(args)...));
  }

  template <typename... Args>
  void fail(fmt::format_string<Args...> f, Args &&...args) {
    errors_.push_back(fmt::format(f, std::forward<Args>(args)...));
  }

  template <typename... Args>
  auto assert_true(bool cond, fmt::format_string<Args...> f, Args &&...args) -> bool {
    if (!cond) errors_.push_back(fmt::format(f, std::forward<Args>(args)...));
    return cond;
  }

  template <typename A, typename B, typename... Args>
  auto assert_lt(A const &a, B const &b, fmt::format_string<Args...> f, Args &&...args) -> bool {
    const bool ok = a < b;
    if (!ok) errors_.push_back(fmt::format(f, std::forward<Args>(args)...));
    return ok;
  }

  template <typename A, typename B, typename... Args>
  auto assert_gt(A const &a, B const &b, fmt::format_string<Args...> f, Args &&...args) -> bool {
    const bool ok = a > b;
    if (!ok) errors_.push_back(fmt::format(f, std::forward<Args>(args)...));
    return ok;
  }

  template <typename A, typename B, typename... Args>
  auto assert_eq(A const &a, B const &b, fmt::format_string<Args...> f, Args &&...args) -> bool {
    const bool ok = a == b;
    if (!ok) errors_.push_back(fmt::format(f, std::forward<Args>(args)...));
    return ok;
  }

  [[nodiscard]] auto errors() const -> std::vector<std::string> const & { return errors_; }

  [[nodiscard]] auto has_errors() const -> bool { return !errors_.empty(); }

 private:
  std::vector<std::string> errors_;
};

}  // anonymous namespace

#define VERBOSE_OUT \
  if (g_verbose) std::cerr

// ============================================================================
// Test Runner
// ============================================================================

class ContractFuzzer {
 public:
  bool run(uint8_t const *data, std::size_t size) {
    if (size < 2) return true;

    std::size_t pos = 0;

    // Generate 1-3 patterns
    auto result = pattern_gen_.generate(data, pos, size);
    if (result.patterns.empty()) return true;

    // Convert to memgraph patterns
    std::vector<Pattern<FuzzSymbol>> patterns;
    patterns.reserve(result.patterns.size());
    for (auto const &ast : result.patterns) {
      patterns.push_back(pattern_to_memgraph(ast));
    }

    // Skip patterns with 0 variables (edge case)
    for (auto const &p : patterns) {
      if (p.num_vars() == 0) {
        VERBOSE_OUT << "Skipping pattern with 0 variables\n";
        return true;
      }
    }

    // Compile
    PatternCompiler<FuzzSymbol> compiler;
    auto compiled = compiler.compile(std::span(patterns));

    // Skip patterns that would overflow uint8_t register indices.
    // Large bytecode sequences risk overflowing the 256 register limit.
    if (compiled.code().size() > 500) {
      VERBOSE_OUT << "Skipping pattern with " << compiled.code().size() << " instructions (limit 500)\n";
      return true;
    }

    // Verify all contracts using shared validator
    ErrorCollectorReporter reporter;
    ValidateBytecodeInvariants(reporter, compiled);
    auto const &errors = reporter.errors();

    if (!errors.empty()) {
      std::cerr << "\n!!! CONTRACT VIOLATIONS !!!\n";
      std::cerr << "Patterns:\n";
      for (std::size_t i = 0; i < result.patterns.size(); ++i) {
        std::cerr << "  " << i << ": " << pattern_to_string(result.patterns[i]) << "\n";
      }

      std::cerr << "\nBytecode:\n";
      std::cerr << disassemble<FuzzSymbol>(compiled.code(), compiled.symbols()) << "\n";

      std::cerr << "\nMetadata:\n";
      std::cerr << "  num_eclass_regs: " << compiled.num_eclass_regs() << "\n";
      std::cerr << "  num_enode_regs: " << compiled.num_enode_regs() << "\n";
      std::cerr << "  num_slots: " << compiled.num_slots() << "\n";
      std::cerr << "  symbols: " << compiled.symbols().size() << "\n";
      std::cerr << "  binding_order: [";
      for (std::size_t i = 0; i < compiled.binding_order().size(); ++i) {
        if (i > 0) std::cerr << ", ";
        std::cerr << compiled.binding_order()[i];
      }
      std::cerr << "]\n";

      std::cerr << "\nErrors:\n";
      for (auto const &err : errors) {
        std::cerr << "  - " << err << "\n";
      }

      abort();
    }

    VERBOSE_OUT << "Verified " << result.patterns.size() << " pattern(s), " << compiled.code().size()
                << " instructions\n";
    return true;
  }

 private:
  MultiPatternGenerator pattern_gen_;
};

// ============================================================================
// Main Fuzzer Entry Point
// ============================================================================

extern "C" int LLVMFuzzerTestOneInput(uint8_t const *data, std::size_t size) {
  // Initialize verbose flag from environment
  static bool initialized = false;
  if (!initialized) {
    char const *val = std::getenv("FUZZ_VERBOSE");
    g_verbose = (val != nullptr && (std::string_view{val} == "1" || std::string_view{val} == "true"));
    initialized = true;
  }

  ContractFuzzer fuzzer;
  fuzzer.run(data, size);

  return 0;
}

}  // namespace memgraph::planner::core::pattern::vm
