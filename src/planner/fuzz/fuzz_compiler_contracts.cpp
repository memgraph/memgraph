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
// violates invariants (e.g., writes to reserved register 0). The VM might
// not crash today, but such bugs indicate deeper problems. This fuzzer
// catches those structural violations that semantic testing might miss.
//
// Flow:
//   Fuzz input → generate Pattern ASTs → compile → verify contracts → abort if violated
//
// Contracts verified:
//
//   1. Register Allocation Contract
//      - eclass_regs[0] is never written by compiled code
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
//      - All BindSlotDedup/CheckSlot/MarkSeen/Yield.arg < num_slots()
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
//      - eclass_regs[0] is pre-defined as the input candidate
//
//   8. MarkSeen Ordering Contract
//      - MarkSeen/Yield only reference slots already bound via BindSlotDedup
//
//   9. binding_order Consistency Contract
//      - binding_order matches BindSlotDedup instructions in emission order
//
// Running:
//   ./build/src/planner/fuzz/fuzz_compiler_contracts -max_total_time=60
//
// On contract violation, the fuzzer aborts with detailed diagnostics.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <set>
#include <span>
#include <sstream>
#include <string>
#include <vector>

#include <fmt/format.h>

#include "fuzz_common.hpp"
#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/instruction.hpp"
#include "planner/pattern/vm/tracer.hpp"

namespace memgraph::planner::core::pattern::vm {

using fuzz::FuzzSymbol;
using fuzz::MultiPatternGenerator;
using fuzz::pattern_to_memgraph;
using fuzz::pattern_to_string;
using fuzz::PatternASTPtr;
using fuzz::PatternGenerator;

// ============================================================================
// Configuration
// ============================================================================

static bool g_verbose = false;

#define VERBOSE_OUT \
  if (g_verbose) std::cerr

// ============================================================================
// Contract Verifier
// ============================================================================

class CompiledPatternVerifier {
 public:
  explicit CompiledPatternVerifier(CompiledPattern<FuzzSymbol> const &cp) : cp_(cp) {}

  auto verify_all() -> std::vector<std::string> {
    std::vector<std::string> errors;
    verify_jump_targets(errors);
    verify_symbol_indices(errors);
    verify_slot_indices(errors);
    verify_iteration_pairing(errors);
    verify_code_structure(errors);
    verify_register_bounds(errors);
    verify_eclass_reg_zero_reserved(errors);
    verify_binding_order(errors);
    verify_register_liveness(errors);
    verify_mark_seen_ordering(errors);
    verify_binding_order_matches_code(errors);
    return errors;
  }

 private:
  // ===========================================================================
  // Contract 2: Jump Target Contract
  // ===========================================================================

  void verify_jump_targets(std::vector<std::string> &errors) {
    auto const &code = cp_.code();
    for (std::size_t i = 0; i < code.size(); ++i) {
      auto const &instr = code[i];
      if (uses_target(instr.op)) {
        if (instr.target >= code.size()) {
          errors.push_back(fmt::format("Jump target out of bounds: instr[{}] {} target={} >= code.size()={}",
                                       i,
                                       op_name(instr.op),
                                       instr.target,
                                       code.size()));
        }
        if (instr.target == 0xFFFF) {
          errors.push_back(fmt::format("Unpatched placeholder: instr[{}] {} has 0xFFFF target", i, op_name(instr.op)));
        }
      }
    }
  }

  // ===========================================================================
  // Contract 3: Symbol Table Contract
  // ===========================================================================

  void verify_symbol_indices(std::vector<std::string> &errors) {
    auto const &code = cp_.code();
    auto const &symbols = cp_.symbols();
    for (std::size_t i = 0; i < code.size(); ++i) {
      if (code[i].op == VMOp::CheckSymbol) {
        if (code[i].arg >= symbols.size()) {
          errors.push_back(fmt::format("Symbol index out of bounds: instr[{}] CheckSymbol arg={} >= symbols.size()={}",
                                       i,
                                       code[i].arg,
                                       symbols.size()));
        }
      }
    }
  }

  // ===========================================================================
  // Contract 4: Slot Binding Contract
  // ===========================================================================

  void verify_slot_indices(std::vector<std::string> &errors) {
    auto const &code = cp_.code();
    auto num_slots = cp_.num_slots();
    for (std::size_t i = 0; i < code.size(); ++i) {
      auto const &instr = code[i];
      bool uses_slot_arg = (instr.op == VMOp::BindSlotDedup || instr.op == VMOp::CheckSlot ||
                            instr.op == VMOp::MarkSeen || instr.op == VMOp::Yield);
      if (uses_slot_arg && num_slots > 0) {
        if (instr.arg >= num_slots) {
          errors.push_back(fmt::format("Slot index out of bounds: instr[{}] {} arg={} >= num_slots={}",
                                       i,
                                       op_name(instr.op),
                                       instr.arg,
                                       num_slots));
        }
      }
    }
  }

  // ===========================================================================
  // Contract 5: Iteration Pairing Contract
  // ===========================================================================

  void verify_iteration_pairing(std::vector<std::string> &errors) {
    auto const &code = cp_.code();

    // Track Iter instructions and their matching Next
    // Key: (dst register, iter type), Value: instruction index
    std::map<std::pair<uint8_t, VMOp>, std::size_t> pending_iters;

    auto iter_type_for_next = [](VMOp next_op) -> VMOp {
      switch (next_op) {
        case VMOp::NextENode:
          return VMOp::IterENodes;
        case VMOp::NextParent:
          return VMOp::IterParents;
        case VMOp::NextEClass:
          return VMOp::IterAllEClasses;
        default:
          return next_op;
      }
    };

    for (std::size_t i = 0; i < code.size(); ++i) {
      auto const &instr = code[i];
      switch (instr.op) {
        case VMOp::IterENodes:
        case VMOp::IterParents:
        case VMOp::IterAllEClasses: {
          auto key = std::make_pair(instr.dst, instr.op);
          pending_iters[key] = i;
          break;
        }
        case VMOp::NextENode:
        case VMOp::NextParent:
        case VMOp::NextEClass: {
          auto iter_op = iter_type_for_next(instr.op);
          auto key = std::make_pair(instr.dst, iter_op);
          if (!pending_iters.contains(key)) {
            errors.push_back(fmt::format(
                "{} without matching {}: instr[{}] dst={}", op_name(instr.op), op_name(iter_op), i, instr.dst));
          } else {
            pending_iters.erase(key);
          }
          break;
        }
        default:
          break;
      }
    }

    // Any unmatched Iters?
    for (auto const &[key, idx] : pending_iters) {
      errors.push_back(fmt::format("{} without matching Next: instr[{}] dst={}", op_name(key.second), idx, key.first));
    }
  }

  // ===========================================================================
  // Contract 6: Code Structure Contract
  // ===========================================================================

  void verify_code_structure(std::vector<std::string> &errors) {
    auto const &code = cp_.code();
    if (code.empty()) {
      errors.push_back("Code is empty (should have at least Halt)");
      return;
    }

    // Last instruction must be Halt
    if (code.back().op != VMOp::Halt) {
      errors.push_back(
          fmt::format("Last instruction is not Halt: {} at index {}", op_name(code.back().op), code.size() - 1));
    }

    // Count Yields
    std::size_t yield_count = 0;
    for (auto const &instr : code) {
      if (instr.op == VMOp::Yield) {
        ++yield_count;
      }
    }

    // Patterns with bindings should have at least one Yield
    // Patterns without bindings (e.g., pure wildcard) may have no Yield
    bool const has_bindings = !cp_.binding_order().empty();
    if (has_bindings && yield_count == 0) {
      errors.push_back("No Yield instruction in pattern with bindings");
    }
    if (!has_bindings && yield_count > 0) {
      errors.push_back("Yield instruction in pattern without bindings");
    }

    // Yield should be followed by Jump
    for (std::size_t i = 0; i + 1 < code.size(); ++i) {
      if (code[i].op == VMOp::Yield) {
        if (code[i + 1].op != VMOp::Jump) {
          errors.push_back(fmt::format("Yield at {} not followed by Jump (found {})", i, op_name(code[i + 1].op)));
        }
      }
    }
  }

  // ===========================================================================
  // Contract 1: Register Allocation Contract
  // ===========================================================================

  void verify_register_bounds(std::vector<std::string> &errors) {
    auto const &code = cp_.code();
    auto num_eclass = cp_.num_eclass_regs();
    auto num_enode = cp_.num_enode_regs();

    for (std::size_t i = 0; i < code.size(); ++i) {
      auto const &instr = code[i];

      // Check dst register bounds for ops that write to dst
      if (is_eclass_dst(instr.op)) {
        if (instr.dst >= num_eclass) {
          errors.push_back(fmt::format("eclass_reg dst out of bounds: instr[{}] {} dst={} >= num_eclass_regs={}",
                                       i,
                                       op_name(instr.op),
                                       instr.dst,
                                       num_eclass));
        }
      }
      if (is_enode_dst(instr.op)) {
        if (instr.dst >= num_enode) {
          errors.push_back(fmt::format("enode_reg dst out of bounds: instr[{}] {} dst={} >= num_enode_regs={}",
                                       i,
                                       op_name(instr.op),
                                       instr.dst,
                                       num_enode));
        }
      }

      // Check src register bounds for ops that read from src
      if (is_eclass_src(instr.op)) {
        if (instr.src >= num_eclass) {
          errors.push_back(fmt::format("eclass_reg src out of bounds: instr[{}] {} src={} >= num_eclass_regs={}",
                                       i,
                                       op_name(instr.op),
                                       instr.src,
                                       num_eclass));
        }
      }
      // Check dst register bounds for ops that read dst as a source (e.g., CheckEClassEq)
      if (reads_eclass_dst(instr.op)) {
        if (instr.dst >= num_eclass) {
          errors.push_back(fmt::format("eclass_reg dst (read) out of bounds: instr[{}] {} dst={} >= num_eclass_regs={}",
                                       i,
                                       op_name(instr.op),
                                       instr.dst,
                                       num_eclass));
        }
      }
      if (is_enode_src(instr.op)) {
        if (instr.src >= num_enode) {
          errors.push_back(fmt::format("enode_reg src out of bounds: instr[{}] {} src={} >= num_enode_regs={}",
                                       i,
                                       op_name(instr.op),
                                       instr.src,
                                       num_enode));
        }
      }
    }
  }

  void verify_eclass_reg_zero_reserved(std::vector<std::string> &errors) {
    auto const &code = cp_.code();
    for (std::size_t i = 0; i < code.size(); ++i) {
      auto const &instr = code[i];
      // eclass_regs[0] should only be read, never written by compiler-generated code
      if (is_eclass_dst(instr.op) && instr.dst == 0) {
        errors.push_back(fmt::format("Compiler writes to reserved eclass_regs[0]: instr[{}] {}", i, op_name(instr.op)));
      }
    }
  }

  void verify_binding_order(std::vector<std::string> &errors) {
    auto const &binding_order = cp_.binding_order();
    auto num_slots = cp_.num_slots();

    // binding_order should contain valid slot indices
    for (std::size_t i = 0; i < binding_order.size(); ++i) {
      if (value_of(binding_order[i]) >= num_slots) {
        errors.push_back(fmt::format("binding_order[{}]={} >= num_slots={}", i, binding_order[i], num_slots));
      }
    }

    // No duplicates in binding_order
    std::set<SlotIdx> seen;
    for (auto slot : binding_order) {
      if (seen.contains(slot)) {
        errors.push_back(fmt::format("Duplicate slot in binding_order: {}", slot));
      }
      seen.insert(slot);
    }

    // slot_to_order should be consistent with binding_order
    auto const &slot_to_order = cp_.slot_to_order();
    for (std::size_t i = 0; i < binding_order.size(); ++i) {
      auto slot = binding_order[i];
      if (value_of(slot) < slot_to_order.size()) {
        if (slot_to_order[value_of(slot)] != i) {
          errors.push_back(fmt::format(
              "slot_to_order[{}]={} but binding_order[{}]={}", slot, slot_to_order[value_of(slot)], i, slot));
        }
      }
    }
  }

  // ===========================================================================
  // Contract 7: Register Liveness Contract
  // ===========================================================================

  /// Verify that registers are defined before being read (in linear program order).
  /// Note: eclass_regs[0] is pre-defined as the input candidate.
  void verify_register_liveness(std::vector<std::string> &errors) {
    auto const &code = cp_.code();
    std::set<uint8_t> defined_eclass_regs;
    std::set<uint8_t> defined_enode_regs;

    // eclass_regs[0] is always defined (input candidate)
    defined_eclass_regs.insert(0);

    for (std::size_t i = 0; i < code.size(); ++i) {
      auto const &instr = code[i];

      // Check reads before marking writes (read-before-write in same instruction is still an error)

      // Check eclass src reads
      if (is_eclass_src(instr.op)) {
        if (!defined_eclass_regs.contains(instr.src)) {
          errors.push_back(
              fmt::format("Read of undefined eclass_reg: instr[{}] {} reads src={}", i, op_name(instr.op), instr.src));
        }
      }

      // Check eclass dst reads (CheckEClassEq reads both dst and src)
      if (reads_eclass_dst(instr.op)) {
        if (!defined_eclass_regs.contains(instr.dst)) {
          errors.push_back(
              fmt::format("Read of undefined eclass_reg: instr[{}] {} reads dst={}", i, op_name(instr.op), instr.dst));
        }
      }

      // Check enode src reads
      if (is_enode_src(instr.op)) {
        if (!defined_enode_regs.contains(instr.src)) {
          errors.push_back(
              fmt::format("Read of undefined enode_reg: instr[{}] {} reads src={}", i, op_name(instr.op), instr.src));
        }
      }

      // Mark writes
      if (is_eclass_dst(instr.op)) {
        defined_eclass_regs.insert(instr.dst);
      }
      if (is_enode_dst(instr.op)) {
        defined_enode_regs.insert(instr.dst);
      }
    }
  }

  // ===========================================================================
  // Contract 8: MarkSeen Ordering Contract
  // ===========================================================================

  /// Verify that MarkSeen/Yield only reference slots that have been bound via BindSlotDedup.
  void verify_mark_seen_ordering(std::vector<std::string> &errors) {
    auto const &code = cp_.code();
    std::set<uint8_t> bound_slots;

    for (std::size_t i = 0; i < code.size(); ++i) {
      auto const &instr = code[i];

      if (instr.op == VMOp::BindSlotDedup) {
        bound_slots.insert(instr.arg);
      } else if (instr.op == VMOp::MarkSeen) {
        if (!bound_slots.contains(instr.arg)) {
          errors.push_back(fmt::format("MarkSeen references slot {} before BindSlotDedup: instr[{}]", instr.arg, i));
        }
      } else if (instr.op == VMOp::Yield) {
        // Yield also marks a slot as seen (the last bound slot)
        if (!bound_slots.contains(instr.arg)) {
          errors.push_back(fmt::format("Yield references slot {} before BindSlotDedup: instr[{}]", instr.arg, i));
        }
      }
    }
  }

  // ===========================================================================
  // Contract 9: binding_order Matches Code Contract
  // ===========================================================================

  /// Verify that binding_order matches the BindSlotDedup instructions in emission order.
  void verify_binding_order_matches_code(std::vector<std::string> &errors) {
    auto const &code = cp_.code();
    auto const &binding_order = cp_.binding_order();

    // Extract slots from BindSlotDedup in order (first occurrence only)
    std::vector<SlotIdx> code_binding_order;
    std::set<uint8_t> seen_slots;

    for (auto const &instr : code) {
      if (instr.op == VMOp::BindSlotDedup) {
        if (!seen_slots.contains(instr.arg)) {
          code_binding_order.push_back(SlotIdx{instr.arg});
          seen_slots.insert(instr.arg);
        }
      }
    }

    // Compare with reported binding_order
    if (code_binding_order.size() != binding_order.size()) {
      errors.push_back(
          fmt::format("binding_order size mismatch: code has {} unique BindSlotDedup slots, "
                      "binding_order has {} entries",
                      code_binding_order.size(),
                      binding_order.size()));
      return;
    }

    for (std::size_t i = 0; i < binding_order.size(); ++i) {
      if (code_binding_order[i] != binding_order[i]) {
        errors.push_back(fmt::format("binding_order mismatch at index {}: code has slot {}, binding_order has slot {}",
                                     i,
                                     code_binding_order[i],
                                     binding_order[i]));
      }
    }
  }

  // ===========================================================================
  // Helper predicates
  // ===========================================================================

  static auto uses_target(VMOp op) -> bool {
    switch (op) {
      // Note: IterENodes has no target - e-classes always have at least one e-node
      case VMOp::NextENode:
      case VMOp::IterAllEClasses:
      case VMOp::NextEClass:
      case VMOp::IterParents:
      case VMOp::NextParent:
      case VMOp::CheckSymbol:
      case VMOp::CheckArity:
      case VMOp::BindSlotDedup:
      case VMOp::CheckSlot:
      case VMOp::CheckEClassEq:
      case VMOp::Jump:
        return true;
      default:
        return false;
    }
  }

  static auto is_eclass_dst(VMOp op) -> bool {
    return op == VMOp::LoadChild || op == VMOp::GetENodeEClass || op == VMOp::IterAllEClasses || op == VMOp::NextEClass;
  }

  static auto is_enode_dst(VMOp op) -> bool {
    return op == VMOp::IterENodes || op == VMOp::NextENode || op == VMOp::IterParents || op == VMOp::NextParent;
  }

  static auto is_eclass_src(VMOp op) -> bool {
    return op == VMOp::IterENodes || op == VMOp::IterParents || op == VMOp::BindSlotDedup || op == VMOp::CheckSlot ||
           op == VMOp::CheckEClassEq;
  }

  static auto is_enode_src(VMOp op) -> bool {
    return op == VMOp::LoadChild || op == VMOp::GetENodeEClass || op == VMOp::CheckSymbol || op == VMOp::CheckArity;
  }

  /// Ops that read eclass_regs[dst] (not write). CheckEClassEq compares dst vs src.
  static auto reads_eclass_dst(VMOp op) -> bool { return op == VMOp::CheckEClassEq; }

  CompiledPattern<FuzzSymbol> const &cp_;
};

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

    // Verify all contracts
    CompiledPatternVerifier verifier(compiled);
    auto errors = verifier.verify_all();

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
