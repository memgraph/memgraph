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

/// Shared bytecode invariant validators for pattern VM compiled bytecode.
///
/// Validators are parameterized on a Reporter policy so they work in both
/// gtest (EXPECT_*/ASSERT_*) and fuzzer (log + abort) contexts.
///
/// Reporter concept — methods take (condition args..., fmt::format_string, format_args...):
///   void expect_true(bool, fmt_str, args...)
///   void expect_eq(a, b, fmt_str, args...)
///   void expect_lt(a, b, fmt_str, args...)
///   void expect_ne(a, b, fmt_str, args...)
///   void expect_gt(a, b, fmt_str, args...)
///   void fail(fmt_str, args...)
///   bool assert_true(bool, fmt_str, args...)   // false = caller should return
///   bool assert_lt(a, b, fmt_str, args...)
///   bool assert_gt(a, b, fmt_str, args...)
///   bool assert_eq(a, b, fmt_str, args...)

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <vector>

#include <fmt/format.h>

#include "planner/pattern/vm/compiled_rule.hpp"
#include "planner/pattern/vm/instruction.hpp"
#include "planner/pattern/vm/tracer.hpp"

namespace memgraph::planner::core::pattern::vm {

// ============================================================================
// Helper predicates
// ============================================================================

constexpr auto is_next_op(VMOp op) -> bool {
  return op == VMOp::NextENode || op == VMOp::NextEClass || op == VMOp::NextSymbolEClass || op == VMOp::NextParent;
}

constexpr auto has_backtrack_target(VMOp op) -> bool {
  switch (op) {
    case VMOp::IterAllEClasses:
    case VMOp::IterSymbolEClasses:
    case VMOp::IterParents:
    case VMOp::NextENode:
    case VMOp::NextEClass:
    case VMOp::NextSymbolEClass:
    case VMOp::NextParent:
    case VMOp::CheckSymbol:
    case VMOp::CheckArity:
    case VMOp::BindSlot:
    case VMOp::CheckSlot:
    case VMOp::CheckEClassEq:
      return true;
    default:
      return false;
  }
}

constexpr auto is_valid_backtrack_target(VMOp op) -> bool {
  return is_next_op(op) || op == VMOp::MarkSeen || op == VMOp::Halt;
}

constexpr auto iter_to_next(VMOp op) -> VMOp {
  switch (op) {
    case VMOp::IterENodes:
      return VMOp::NextENode;
    case VMOp::IterAllEClasses:
      return VMOp::NextEClass;
    case VMOp::IterSymbolEClasses:
      return VMOp::NextSymbolEClass;
    case VMOp::IterParents:
      return VMOp::NextParent;
    default:
      return op;
  }
}

constexpr auto is_iter_op(VMOp op) -> bool {
  return op == VMOp::IterENodes || op == VMOp::IterAllEClasses || op == VMOp::IterSymbolEClasses ||
         op == VMOp::IterParents;
}

constexpr auto is_eclass_dst(VMOp op) -> bool {
  return op == VMOp::LoadChild || op == VMOp::GetENodeEClass || op == VMOp::IterAllEClasses || op == VMOp::NextEClass ||
         op == VMOp::IterSymbolEClasses || op == VMOp::NextSymbolEClass;
}

constexpr auto is_enode_dst(VMOp op) -> bool {
  return op == VMOp::IterENodes || op == VMOp::NextENode || op == VMOp::IterParents || op == VMOp::NextParent;
}

constexpr auto is_eclass_src(VMOp op) -> bool {
  return op == VMOp::IterENodes || op == VMOp::IterParents || op == VMOp::BindSlot || op == VMOp::CheckSlot ||
         op == VMOp::CheckEClassEq;
}

constexpr auto is_enode_src(VMOp op) -> bool {
  return op == VMOp::LoadChild || op == VMOp::GetENodeEClass || op == VMOp::CheckSymbol || op == VMOp::CheckArity;
}

constexpr auto reads_eclass_dst(VMOp op) -> bool { return op == VMOp::CheckEClassEq; }

// ============================================================================
// Register usage analysis
// ============================================================================

struct RegUsage {
  std::set<uint8_t> eclass_producers;
  std::set<uint8_t> enode_producers;
  std::set<uint8_t> eclass_consumers;
  std::set<uint8_t> enode_consumers;
};

inline auto collect_register_usage(std::span<Instruction const> code) -> RegUsage {
  auto usage = RegUsage{};
  for (auto const &instr : code) {
    switch (instr.op) {
      case VMOp::IterAllEClasses:
      case VMOp::NextEClass:
      case VMOp::IterSymbolEClasses:
      case VMOp::NextSymbolEClass:
      case VMOp::GetENodeEClass:
        usage.eclass_producers.insert(instr.dst);
        break;
      case VMOp::LoadChild:
        usage.eclass_producers.insert(instr.dst);
        usage.enode_consumers.insert(instr.src);
        break;
      case VMOp::CheckEClassEq:
        usage.eclass_consumers.insert(instr.dst);
        usage.eclass_consumers.insert(instr.src);
        break;
      case VMOp::IterENodes:
        usage.enode_producers.insert(instr.dst);
        usage.eclass_consumers.insert(instr.src);
        break;
      case VMOp::NextENode:
      case VMOp::NextParent:
        usage.enode_producers.insert(instr.dst);
        break;
      case VMOp::IterParents:
        usage.enode_producers.insert(instr.dst);
        usage.eclass_consumers.insert(instr.src);
        break;
      case VMOp::CheckSymbol:
      case VMOp::CheckArity:
        usage.enode_consumers.insert(instr.src);
        break;
      case VMOp::BindSlot:
      case VMOp::CheckSlot:
        usage.eclass_consumers.insert(instr.src);
        break;
      default:
        break;
    }
  }
  return usage;
}

// ============================================================================
// CompiledPatternInfo — lightweight metadata for validators
// ============================================================================

struct CompiledPatternInfo {
  std::size_t num_symbols = 0;
  std::size_t num_slots = 0;
  std::size_t num_eclass_regs = 0;
  std::size_t num_enode_regs = 0;
  std::span<SlotIdx const> binding_order;
  std::span<uint8_t const> slot_to_order;
};

// ============================================================================
// Phase 1: Structural preconditions (run first, later validators depend on these)
// ============================================================================

/// Non-empty bytecode ending with Halt.
template <typename Reporter>
void ValidateCodeStructure(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  if (!r.assert_true(!code.empty(), "Bytecode must not be empty")) return;
  r.expect_eq(
      code.back().op, VMOp::Halt, "Last instruction must be Halt, got {}\nBytecode:\n{}", op_name(code.back().op), bc);
}

/// All jump/backtrack targets in bounds, no unpatched 0xFFFF placeholders.
/// Must pass before any validator that indexes code[target].
template <typename Reporter>
void ValidateJumpTargetsInBounds(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (!has_backtrack_target(code[i].op) && code[i].op != VMOp::Jump) continue;
    r.expect_lt(code[i].target,
                code.size(),
                "Jump target out of bounds: instr[{}] {} target={} >= code.size()={}\nBytecode:\n{}",
                i,
                op_name(code[i].op),
                code[i].target,
                code.size(),
                bc);
    r.expect_ne(code[i].target,
                uint16_t{0xFFFF},
                "Unpatched placeholder: instr[{}] {} has 0xFFFF target\nBytecode:\n{}",
                i,
                op_name(code[i].op),
                bc);
  }
}

/// Symbol table, slot, and register index bounds.
template <typename Reporter>
void ValidateIndexBounds(Reporter &r, std::span<Instruction const> code, CompiledPatternInfo const &info,
                         std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    auto const &instr = code[i];

    // Symbol table bounds
    if (instr.op == VMOp::CheckSymbol || instr.op == VMOp::IterSymbolEClasses) {
      r.expect_lt(instr.arg,
                  info.num_symbols,
                  "{} at {} arg={} >= num_symbols={}\nBytecode:\n{}",
                  op_name(instr.op),
                  i,
                  instr.arg,
                  info.num_symbols,
                  bc);
    }

    // Slot bounds
    if (instr.op == VMOp::BindSlot || instr.op == VMOp::CheckSlot || instr.op == VMOp::MarkSeen ||
        instr.op == VMOp::Yield) {
      if (info.num_slots > 0) {
        r.expect_lt(instr.arg,
                    info.num_slots,
                    "{} at {} arg={} >= num_slots={}\nBytecode:\n{}",
                    op_name(instr.op),
                    i,
                    instr.arg,
                    info.num_slots,
                    bc);
      }
    }

    // Register bounds
    if (is_eclass_dst(instr.op))
      r.expect_lt(instr.dst,
                  info.num_eclass_regs,
                  "eclass_reg dst out of bounds: instr[{}] {} dst={} >= {}\nBytecode:\n{}",
                  i,
                  op_name(instr.op),
                  instr.dst,
                  info.num_eclass_regs,
                  bc);
    if (is_enode_dst(instr.op))
      r.expect_lt(instr.dst,
                  info.num_enode_regs,
                  "enode_reg dst out of bounds: instr[{}] {} dst={} >= {}\nBytecode:\n{}",
                  i,
                  op_name(instr.op),
                  instr.dst,
                  info.num_enode_regs,
                  bc);
    if (is_eclass_src(instr.op))
      r.expect_lt(instr.src,
                  info.num_eclass_regs,
                  "eclass_reg src out of bounds: instr[{}] {} src={} >= {}\nBytecode:\n{}",
                  i,
                  op_name(instr.op),
                  instr.src,
                  info.num_eclass_regs,
                  bc);
    if (reads_eclass_dst(instr.op))
      r.expect_lt(instr.dst,
                  info.num_eclass_regs,
                  "eclass_reg dst (read) out of bounds: instr[{}] {} dst={} >= {}\nBytecode:\n{}",
                  i,
                  op_name(instr.op),
                  instr.dst,
                  info.num_eclass_regs,
                  bc);
    if (is_enode_src(instr.op))
      r.expect_lt(instr.src,
                  info.num_enode_regs,
                  "enode_reg src out of bounds: instr[{}] {} src={} >= {}\nBytecode:\n{}",
                  i,
                  op_name(instr.op),
                  instr.src,
                  info.num_enode_regs,
                  bc);
  }
}

// ============================================================================
// Phase 2: Jump target semantics (safe to index code[target] after Phase 1)
// ============================================================================

/// Backtrack targets point to NextX, MarkSeen, or Halt.
template <typename Reporter>
void ValidateBacktracksToNextOrHalt(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (!has_backtrack_target(code[i].op)) continue;
    auto target = code[i].target;
    r.expect_true(is_valid_backtrack_target(code[target].op),
                  "Backtrack at {} ({}) targets {} ({}), expected NextX, MarkSeen, or Halt\nBytecode:\n{}",
                  i,
                  op_name(code[i].op),
                  target,
                  op_name(code[target].op),
                  bc);
  }
}

/// Jump semantics: backward→NextX/MarkSeen, forward→skips over NextX/MarkSeen.
template <typename Reporter>
void ValidateJumpTargetSemantics(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::Jump) continue;
    auto target = code[i].target;

    if (target <= i) {
      r.expect_true(is_next_op(code[target].op) || code[target].op == VMOp::MarkSeen,
                    "Backward Jump at {} targets {} ({}), expected NextX or MarkSeen\nBytecode:\n{}",
                    i,
                    target,
                    op_name(code[target].op),
                    bc);
    } else {
      bool skips_relevant = false;
      for (auto j = i + 1; j < target; ++j) {
        if (is_next_op(code[j].op) || code[j].op == VMOp::MarkSeen) {
          skips_relevant = true;
          break;
        }
      }
      r.expect_true(skips_relevant,
                    "Forward Jump at {} targets {} but does not skip over any NextX or MarkSeen\nBytecode:\n{}",
                    i,
                    target,
                    bc);
    }
  }
}

/// NextX nesting: inner loops backtrack earlier, outermost backtracks to Halt.
template <typename Reporter>
void ValidateNextBacktracksEarlierOrHalt(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (!is_next_op(code[i].op)) continue;
    auto target = code[i].target;
    auto target_op = code[target].op;
    if (target_op == VMOp::Halt) {
      r.expect_gt(target,
                  static_cast<uint16_t>(i),
                  "{} at {} backtracks to Halt at {} which should be after the NextX\nBytecode:\n{}",
                  op_name(code[i].op),
                  i,
                  target,
                  bc);
    } else {
      r.expect_lt(
          target,
          static_cast<uint16_t>(i),
          "{} at {} backtracks to {} at {} -- inner loops should backtrack to an earlier address\nBytecode:\n{}",
          op_name(code[i].op),
          i,
          op_name(target_op),
          target,
          bc);
    }
  }
}

/// BindSlot/CheckSlot never target MarkSeen (MarkSeen is for inner exhaustion, not binding failure).
template <typename Reporter>
void ValidateBindSlotCheckSlotSkipMarkSeen(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::BindSlot && code[i].op != VMOp::CheckSlot) continue;
    auto target = code[i].target;
    if (target < code.size() && code[target].op == VMOp::MarkSeen) {
      r.fail("{} at {} targets MarkSeen at {} -- should target NextX directly\nBytecode:\n{}",
             op_name(code[i].op),
             i,
             target,
             bc);
    }
  }
}

// ============================================================================
// Phase 3: Iteration structure
// ============================================================================

/// Every IterX has exactly one matching NextX with the same dst register, and vice versa.
template <typename Reporter>
void ValidateIterationPairing(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  struct IterInfo {
    VMOp expected_next;
    uint8_t dst;
    std::size_t pos;
  };

  std::vector<IterInfo> iters;
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (is_iter_op(code[i].op)) {
      iters.push_back({iter_to_next(code[i].op), code[i].dst, i});
    }
  }

  for (auto const &iter : iters) {
    int match_count = 0;
    for (std::size_t i = 0; i < code.size(); ++i) {
      if (code[i].op == iter.expected_next && code[i].dst == iter.dst) ++match_count;
    }
    r.expect_eq(match_count,
                1,
                "{} at {} (dst={}) has {} matching {} (expected 1)\nBytecode:\n{}",
                op_name(code[iter.pos].op),
                iter.pos,
                iter.dst,
                match_count,
                op_name(iter.expected_next),
                bc);
  }

  for (std::size_t i = 0; i < code.size(); ++i) {
    if (!is_next_op(code[i].op)) continue;
    bool has_iter = false;
    for (auto const &iter : iters) {
      if (iter.expected_next == code[i].op && iter.dst == code[i].dst) {
        has_iter = true;
        break;
      }
    }
    r.expect_true(
        has_iter, "{} at {} (dst={}) has no matching IterX\nBytecode:\n{}", op_name(code[i].op), i, code[i].dst, bc);
  }
}

/// IterX immediately followed by forward Jump (loop-entry skip pattern).
template <typename Reporter>
void ValidateIterFollowedByJump(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (!is_iter_op(code[i].op)) continue;
    if (!r.assert_lt(i + 1, code.size(), "{} at {} is the last instruction\nBytecode:\n{}", op_name(code[i].op), i, bc))
      return;
    r.expect_eq(code[i + 1].op,
                VMOp::Jump,
                "{} at {} should be immediately followed by Jump\nBytecode:\n{}",
                op_name(code[i].op),
                i,
                bc);
    if (code[i + 1].op == VMOp::Jump) {
      r.expect_gt(code[i + 1].target,
                  static_cast<uint16_t>(i + 1),
                  "Jump after {} at {} should be a forward jump\nBytecode:\n{}",
                  op_name(code[i].op),
                  i,
                  bc);
    }
  }
}

/// IterENodes.target must be 0 (e-classes always have >= 1 e-node).
template <typename Reporter>
void ValidateIterENodesTargetUnused(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op == VMOp::IterENodes) {
      r.expect_eq(code[i].target,
                  uint16_t{0},
                  "IterENodes at {} has non-zero target {} (target should be unused)\nBytecode:\n{}",
                  i,
                  code[i].target,
                  bc);
    }
  }
}

// ============================================================================
// Phase 4: Yield / binding structure
// ============================================================================

/// Exactly 1 Yield if any BindSlot exists, 0 otherwise. Yield followed by backward Jump.
template <typename Reporter>
void ValidateYieldStructure(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  bool has_bindings = false;
  int yield_count = 0;
  for (auto const &instr : code) {
    if (instr.op == VMOp::BindSlot) has_bindings = true;
    if (instr.op == VMOp::Yield) ++yield_count;
  }
  auto expected = has_bindings ? 1 : 0;
  r.expect_eq(yield_count,
              expected,
              "Expected {} Yield (has_bindings={}), found {}\nBytecode:\n{}",
              expected,
              has_bindings,
              yield_count,
              bc);

  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::Yield) continue;
    if (!r.assert_lt(
            i + 1, code.size(), "Yield at {} is the last instruction (expected Jump after)\nBytecode:\n{}", i, bc))
      return;
    r.expect_eq(code[i + 1].op,
                VMOp::Jump,
                "Yield at {} should be followed by Jump, got {}\nBytecode:\n{}",
                i,
                op_name(code[i + 1].op),
                bc);
    if (code[i + 1].op == VMOp::Jump) {
      r.expect_lt(code[i + 1].target,
                  static_cast<uint16_t>(i + 1),
                  "Jump after Yield at {} should be backward (target={})\nBytecode:\n{}",
                  i,
                  code[i + 1].target,
                  bc);
    }
  }
}

/// Slot usage: BindSlot before CheckSlot (linear order), slots contiguous [0,N).
/// Note: MarkSeen/Yield reference slots that are bound elsewhere in the loop body
/// (MarkSeen is emitted in the loop header, before the BindSlot in program order,
/// but only reached after BindSlot at runtime via loop-back). Their slot validity
/// is checked by ValidateBindingOrder via the binding_order metadata.
template <typename Reporter>
void ValidateSlotUsage(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  std::set<uint8_t> bound_slots;

  // CheckSlot must appear after BindSlot for that slot in linear program order
  for (std::size_t i = 0; i < code.size(); ++i) {
    auto const &instr = code[i];

    if (instr.op == VMOp::BindSlot) {
      bound_slots.insert(instr.arg);
    } else if (instr.op == VMOp::CheckSlot) {
      r.expect_true(bound_slots.contains(instr.arg),
                    "CheckSlot for slot[{}] at {} appears before any BindSlot for that slot\nBytecode:\n{}",
                    instr.arg,
                    i,
                    bc);
    }
  }

  // Slots should be contiguous [0, N)
  uint8_t expected = 0;
  for (auto slot : bound_slots) {
    r.expect_eq(slot, expected, "Slot indices have a gap at {}\nBytecode:\n{}", expected, bc);
    ++expected;
  }
}

// ============================================================================
// Phase 5: Instruction-pair invariants
// ============================================================================

/// CheckSymbol immediately followed by CheckArity with same src register.
template <typename Reporter>
void ValidateCheckSymbolArityPaired(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::CheckSymbol) continue;
    if (!r.assert_lt(i + 1, code.size(), "CheckSymbol at {} is the last instruction\nBytecode:\n{}", i, bc)) return;
    r.expect_eq(code[i + 1].op,
                VMOp::CheckArity,
                "CheckSymbol at {} should be followed by CheckArity, got {}\nBytecode:\n{}",
                i,
                op_name(code[i + 1].op),
                bc);
    if (code[i + 1].op == VMOp::CheckArity) {
      r.expect_eq(code[i].src,
                  code[i + 1].src,
                  "CheckSymbol at {} (src=rn{}) and CheckArity at {} (src=rn{}) should use same enode "
                  "register\nBytecode:\n{}",
                  i,
                  code[i].src,
                  i + 1,
                  code[i + 1].src,
                  bc);
    }
  }
}

/// CheckEClassEq: preceded by LoadChild, dst != src.
template <typename Reporter>
void ValidateCheckEClassEq(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::CheckEClassEq) continue;
    if (!r.assert_gt(i, std::size_t{0}, "CheckEClassEq cannot be first instruction\nBytecode:\n{}", bc)) return;
    r.expect_eq(code[i - 1].op,
                VMOp::LoadChild,
                "CheckEClassEq at {} should immediately follow LoadChild\nBytecode:\n{}",
                i,
                bc);
    r.expect_ne(
        code[i].dst, code[i].src, "CheckEClassEq at {} compares rc{} with itself\nBytecode:\n{}", i, code[i].dst, bc);
  }
}

/// GetENodeEClass immediately followed by BindSlot or IterParents.
template <typename Reporter>
void ValidateGetENodeEClassUsage(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::GetENodeEClass) continue;
    if (!r.assert_lt(i + 1, code.size(), "GetENodeEClass at {} is the last instruction\nBytecode:\n{}", i, bc)) return;
    auto next_op = code[i + 1].op;
    r.expect_true(next_op == VMOp::BindSlot || next_op == VMOp::IterParents,
                  "GetENodeEClass at {} followed by {}, expected BindSlot or IterParents\nBytecode:\n{}",
                  i,
                  op_name(next_op),
                  bc);
  }
}

/// MarkSeen must not be on IterENodes loops.
template <typename Reporter>
void ValidateMarkSeenNotOnENodeLoop(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::MarkSeen) continue;
    if (!r.assert_lt(i + 1, code.size(), "MarkSeen at {} is the last instruction\nBytecode:\n{}", i, bc)) return;

    auto next_op = code[i + 1].op;
    if (next_op == VMOp::NextENode) {
      r.fail("MarkSeen at {} is followed by NextENode at {} -- MarkSeen must not be on IterENodes loops\nBytecode:\n{}",
             i,
             i + 1,
             bc);
    }
    if (next_op == VMOp::Jump) {
      auto jump_target = code[i + 1].target;
      if (jump_target < code.size() && code[jump_target].op == VMOp::NextENode) {
        r.fail("MarkSeen at {} jumps to NextENode at {} -- MarkSeen must not be on IterENodes loops\nBytecode:\n{}",
               i,
               jump_target,
               bc);
      }
    }
  }
}

// ============================================================================
// Phase 6: Register invariants (contiguous, producer/consumer, liveness)
// ============================================================================

/// Registers: contiguous [0,N), every producer consumed, every consumer produced, defined before read.
template <typename Reporter>
void ValidateRegisters(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  auto usage = collect_register_usage(code);

  // Contiguous indices
  auto check_contiguous = [&](std::set<uint8_t> const &regs, char const *kind) {
    uint8_t expected = 0;
    for (auto reg : regs) {
      r.expect_eq(reg, expected, "{} register indices have a gap at {}\nBytecode:\n{}", kind, expected, bc);
      ++expected;
    }
  };
  check_contiguous(usage.eclass_producers, "EClass");
  check_contiguous(usage.enode_producers, "ENode");

  // Producer/consumer balance
  for (auto reg : usage.eclass_producers)
    r.expect_true(usage.eclass_consumers.contains(reg),
                  "EClass register rc{} is produced but never consumed\nBytecode:\n{}",
                  reg,
                  bc);
  for (auto reg : usage.eclass_consumers)
    r.expect_true(usage.eclass_producers.contains(reg),
                  "EClass register rc{} is consumed but never produced\nBytecode:\n{}",
                  reg,
                  bc);
  for (auto reg : usage.enode_producers)
    r.expect_true(usage.enode_consumers.contains(reg),
                  "ENode register rn{} is produced but never consumed\nBytecode:\n{}",
                  reg,
                  bc);
  for (auto reg : usage.enode_consumers)
    r.expect_true(usage.enode_producers.contains(reg),
                  "ENode register rn{} is consumed but never produced\nBytecode:\n{}",
                  reg,
                  bc);

  // Liveness: defined before read
  std::set<uint8_t> defined_eclass;
  std::set<uint8_t> defined_enode;

  for (std::size_t i = 0; i < code.size(); ++i) {
    auto const &instr = code[i];
    if (is_eclass_src(instr.op))
      r.expect_true(defined_eclass.contains(instr.src),
                    "Read of undefined eclass_reg: instr[{}] {} reads src={}\nBytecode:\n{}",
                    i,
                    op_name(instr.op),
                    instr.src,
                    bc);
    if (reads_eclass_dst(instr.op))
      r.expect_true(defined_eclass.contains(instr.dst),
                    "Read of undefined eclass_reg: instr[{}] {} reads dst={}\nBytecode:\n{}",
                    i,
                    op_name(instr.op),
                    instr.dst,
                    bc);
    if (is_enode_src(instr.op))
      r.expect_true(defined_enode.contains(instr.src),
                    "Read of undefined enode_reg: instr[{}] {} reads src={}\nBytecode:\n{}",
                    i,
                    op_name(instr.op),
                    instr.src,
                    bc);

    if (is_eclass_dst(instr.op)) defined_eclass.insert(instr.dst);
    if (is_enode_dst(instr.op)) defined_enode.insert(instr.dst);
  }
}

// ============================================================================
// Phase 7: Code layout invariants
// ============================================================================

/// Sibling LoadChild from the same enode register must not be separated by IterENodes.
template <typename Reporter>
void ValidateSiblingLoadsNotInNestedLoop(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  std::map<uint8_t, std::vector<std::size_t>> loads_by_src;
  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op == VMOp::LoadChild) loads_by_src[code[i].src].push_back(i);
  }

  for (auto const &[src_reg, positions] : loads_by_src) {
    if (positions.size() < 2) continue;
    for (auto j = positions.front() + 1; j < positions.back(); ++j) {
      if (code[j].op == VMOp::IterENodes) {
        r.fail(
            "LoadChild from rn{} at positions separated by IterENodes at {} -- sibling loads should "
            "not be inside nested loops\nBytecode:\n{}",
            src_reg,
            j,
            bc);
        break;
      }
    }
  }
}

/// Eclass-level parent joins must appear before the enode loop for that register.
template <typename Reporter>
void ValidateEclassJoinsBeforeEnodeLoop(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  std::set<uint8_t> eclass_level_regs;
  for (auto const &instr : code) {
    if (instr.op == VMOp::IterSymbolEClasses || instr.op == VMOp::IterAllEClasses) {
      eclass_level_regs.insert(instr.dst);
    }
  }

  for (auto reg : eclass_level_regs) {
    std::optional<std::size_t> first_iter_enodes;
    for (std::size_t i = 0; i < code.size(); ++i) {
      if (code[i].op == VMOp::IterENodes && code[i].src == reg) {
        first_iter_enodes = i;
        break;
      }
    }
    if (!first_iter_enodes) continue;

    for (std::size_t i = *first_iter_enodes + 1; i < code.size(); ++i) {
      if (code[i].op == VMOp::IterParents && code[i].src == reg) {
        auto bt = value_of(InstrAddr{code[i].target});
        if (bt < *first_iter_enodes) {
          r.fail(
              "IterParents at {} uses eclass register rc{} but appears after IterENodes at {} and "
              "backtracks before it -- eclass-level joins should be emitted before the enode loop\nBytecode:\n{}",
              i,
              reg,
              *first_iter_enodes,
              bc);
          return;
        }
      }
    }
  }
}

// ============================================================================
// Phase 8: Metadata consistency (binding_order / slot_to_order)
// ============================================================================

/// binding_order: valid indices, no duplicates, slot_to_order is inverse, matches BindSlot emission order.
template <typename Reporter>
void ValidateBindingOrder(Reporter &r, std::span<Instruction const> code, CompiledPatternInfo const &info,
                          std::string_view bc) {
  auto const &binding_order = info.binding_order;
  auto const &slot_to_order = info.slot_to_order;

  // Valid indices, no duplicates
  std::set<SlotIdx> seen;
  for (std::size_t i = 0; i < binding_order.size(); ++i) {
    r.expect_lt(value_of(binding_order[i]),
                info.num_slots,
                "binding_order[{}]={} >= num_slots={}\nBytecode:\n{}",
                i,
                binding_order[i],
                info.num_slots,
                bc);
    r.expect_true(
        !seen.contains(binding_order[i]), "Duplicate slot in binding_order: {}\nBytecode:\n{}", binding_order[i], bc);
    seen.insert(binding_order[i]);
  }

  // slot_to_order is inverse
  for (std::size_t i = 0; i < binding_order.size(); ++i) {
    auto slot = binding_order[i];
    if (value_of(slot) < slot_to_order.size()) {
      r.expect_eq(slot_to_order[value_of(slot)],
                  static_cast<uint8_t>(i),
                  "slot_to_order[{}]={} but binding_order[{}]={}\nBytecode:\n{}",
                  slot,
                  slot_to_order[value_of(slot)],
                  i,
                  slot,
                  bc);
    }
  }

  // Matches BindSlot emission order
  std::vector<SlotIdx> code_order;
  std::set<uint8_t> seen_in_code;
  for (auto const &instr : code) {
    if (instr.op == VMOp::BindSlot && !seen_in_code.contains(instr.arg)) {
      code_order.emplace_back(instr.arg);
      seen_in_code.insert(instr.arg);
    }
  }

  if (!r.assert_eq(code_order.size(),
                   binding_order.size(),
                   "binding_order size mismatch: code has {} unique BindSlot slots, binding_order has {} "
                   "entries\nBytecode:\n{}",
                   code_order.size(),
                   binding_order.size(),
                   bc))
    return;

  for (std::size_t i = 0; i < binding_order.size(); ++i)
    r.expect_eq(code_order[i],
                binding_order[i],
                "binding_order mismatch at index {}: code has slot {}, binding_order has slot {}\nBytecode:\n{}",
                i,
                code_order[i],
                binding_order[i],
                bc);
}

/// Yield backward Jump must not skip over BindSlot loops.
/// The Jump may target a NextX directly or a MarkSeen trampoline that precedes
/// a NextX. We resolve to the effective NextX, then check that every BindSlot
/// between it and Yield backtracks to an address <= the effective NextX.
/// A violation means an inner loop introduces new bindings that the Jump skips.
template <typename Reporter>
void ValidateYieldJumpCoversBindSlots(Reporter &r, std::span<Instruction const> code, std::string_view bc) {
  // Resolve a Yield Jump target to its effective NextX address.
  // The target may be a MarkSeen trampoline: MarkSeen → NextX or MarkSeen → Jump → NextX.
  auto resolve_target = [&](uint16_t target) -> uint16_t {
    if (target < code.size() && code[target].op == VMOp::MarkSeen) {
      auto next = target + 1;
      if (next < code.size()) {
        if (is_next_op(code[next].op)) return next;
        if (code[next].op == VMOp::Jump) return code[next].target;
      }
    }
    return target;
  };

  for (std::size_t i = 0; i < code.size(); ++i) {
    if (code[i].op != VMOp::Yield) continue;
    if (i + 1 >= code.size() || code[i + 1].op != VMOp::Jump) continue;

    auto jump_target = code[i + 1].target;
    // Only check backward jumps (the normal Yield loop-back)
    if (jump_target > i) continue;

    auto effective_target = resolve_target(jump_target);

    for (auto j = effective_target + 1; j < i; ++j) {
      if (code[j].op == VMOp::BindSlot) {
        r.expect_true(code[j].target <= effective_target,
                      "Yield backward Jump at {} targets {} (effective {}) but BindSlot at {} backtracks to {} -- "
                      "the Jump skips an inner loop that introduces new bindings\nBytecode:\n{}",
                      i + 1,
                      jump_target,
                      effective_target,
                      j,
                      code[j].target,
                      bc);
      }
    }
  }
}

// ============================================================================
// Aggregate validator
// ============================================================================

/// Run all structural invariant checks on a compiled pattern.
///
/// Phases are ordered so that later validators can rely on earlier preconditions:
///   1. Structural preconditions (non-empty, Halt, all indices in bounds)
///   2. Jump target semantics (safe to index code[target])
///   3. Iteration structure (pairing, loop entry)
///   4. Yield / binding structure
///   5. Instruction-pair invariants
///   6. Register invariants (contiguous, balanced, live)
///   7. Code layout invariants (sibling loads, eclass joins)
///   8. Metadata consistency (binding_order)
template <typename Reporter, typename Symbol>
void ValidateBytecodeInvariants(Reporter &r, CompiledPattern<Symbol> const &compiled) {
  auto code = compiled.code();
  auto bc = disassemble(code, compiled.symbols());
  auto info = CompiledPatternInfo{.num_symbols = compiled.symbols().size(),
                                  .num_slots = compiled.num_slots(),
                                  .num_eclass_regs = compiled.num_eclass_regs(),
                                  .num_enode_regs = compiled.num_enode_regs(),
                                  .binding_order = compiled.binding_order(),
                                  .slot_to_order = compiled.slot_to_order()};
  // Phase 1: structural preconditions
  ValidateCodeStructure(r, code, bc);
  ValidateJumpTargetsInBounds(r, code, bc);
  ValidateIndexBounds(r, code, info, bc);
  // Phase 2: jump target semantics
  ValidateBacktracksToNextOrHalt(r, code, bc);
  ValidateJumpTargetSemantics(r, code, bc);
  ValidateNextBacktracksEarlierOrHalt(r, code, bc);
  ValidateBindSlotCheckSlotSkipMarkSeen(r, code, bc);
  // Phase 3: iteration structure
  ValidateIterationPairing(r, code, bc);
  ValidateIterFollowedByJump(r, code, bc);
  ValidateIterENodesTargetUnused(r, code, bc);
  // Phase 4: yield / binding
  ValidateYieldStructure(r, code, bc);
  ValidateSlotUsage(r, code, bc);
  ValidateYieldJumpCoversBindSlots(r, code, bc);
  // Phase 5: instruction-pair invariants
  ValidateCheckSymbolArityPaired(r, code, bc);
  ValidateCheckEClassEq(r, code, bc);
  ValidateGetENodeEClassUsage(r, code, bc);
  ValidateMarkSeenNotOnENodeLoop(r, code, bc);
  // Phase 6: register invariants
  ValidateRegisters(r, code, bc);
  // Phase 7: code layout
  ValidateSiblingLoadsNotInNestedLoop(r, code, bc);
  ValidateEclassJoinsBeforeEnodeLoop(r, code, bc);
  // Phase 8: metadata consistency
  ValidateBindingOrder(r, code, info, bc);
}

}  // namespace memgraph::planner::core::pattern::vm
