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

import memgraph.planner.core.eids;

#include <cstddef>
#include <format>
#include <iostream>
#include <iterator>
#include <span>
#include <string>
#include <vector>

#include "planner/pattern/vm/instruction.hpp"

namespace memgraph::planner::core::pattern::vm {

/// Statistics collected during VM execution (for benchmarking/profiling)
struct VMStats {
  std::size_t instructions_executed{0};
  std::size_t iter_enode_calls{0};
  std::size_t iter_parent_calls{0};
  std::size_t iter_eclass_calls{0};
  std::size_t symbol_check_hits{0};
  std::size_t symbol_check_misses{0};
  std::size_t check_slot_hits{0};    // CheckSlot that passed
  std::size_t check_slot_misses{0};  // CheckSlot that failed
  std::size_t yields{0};

  void reset() { *this = VMStats{}; }

  [[nodiscard]] auto symbol_filter_rate() const -> double {
    auto total = symbol_check_hits + symbol_check_misses;
    return total > 0 ? static_cast<double>(symbol_check_misses) / static_cast<double>(total) : 0.0;
  }
};

/// Tracer interface for VM execution debugging.
///
/// All methods receive raw typed values — implementations decide how to
/// capture or format them.  The pc parameter is the program counter of the
/// instruction that triggered the event.
struct VMTracer {
  virtual ~VMTracer() = default;

  /// Called before executing each instruction
  virtual void on_instruction(std::size_t pc, Instruction const &instr) = 0;

  /// Called when iteration starts (IterENodes, IterParents, IterAllEClasses, IterSymbolEClasses)
  virtual void on_iter_start(std::size_t pc, std::size_t count) = 0;

  /// Called when iteration advances (NextENode, NextParent, NextEClass, NextSymbolEClass)
  virtual void on_iter_advance(std::size_t pc, std::size_t remaining) = 0;

  /// Called when a slot is bound
  virtual void on_bind(std::size_t pc, std::size_t slot, EClassId value) = 0;

  /// Called when a slot bind is rejected as duplicate (same value already seen)
  virtual void on_bind_duplicate(std::size_t pc, std::size_t slot, EClassId value) = 0;

  /// Called when CheckSymbol fails
  virtual void on_check_symbol_fail(std::size_t pc, ENodeId enode, std::size_t expected_sym_idx) = 0;

  /// Called when CheckArity fails
  virtual void on_check_arity_fail(std::size_t pc, ENodeId enode, std::size_t expected, std::size_t actual) = 0;

  /// Called when CheckSlot fails (shared variable mismatch)
  virtual void on_check_slot_fail(std::size_t pc, std::size_t slot, EClassId expected, EClassId actual) = 0;

  /// Called when CheckEClassEq fails
  virtual void on_check_eclass_eq_fail(std::size_t pc, EClassId lhs, EClassId rhs) = 0;

  /// Called when CheckSymbol passes
  virtual void on_check_symbol_pass(std::size_t pc, ENodeId enode, std::size_t sym_idx) = 0;

  /// Called when CheckArity passes
  virtual void on_check_arity_pass(std::size_t pc, ENodeId enode, std::size_t arity) = 0;

  /// Called when CheckSlot passes (shared variable matched)
  virtual void on_check_slot_pass(std::size_t pc, std::size_t slot, EClassId value) = 0;

  /// Called when CheckEClassEq passes
  virtual void on_check_eclass_eq_pass(std::size_t pc, EClassId value) = 0;

  /// Called when MarkSeen or Yield marks a slot as seen
  virtual void on_mark_seen(std::size_t pc, std::size_t slot, EClassId value) = 0;

  /// Called when a conditional check fails and execution backtracks to target
  virtual void on_backtrack(std::size_t pc, std::size_t target) = 0;

  /// Called when a match is yielded
  virtual void on_yield(std::size_t pc, std::span<EClassId const> slots) = 0;

  /// Called when execution halts
  virtual void on_halt(std::size_t pc, std::size_t total_instructions) = 0;
};

/// Null tracer - does nothing (for production use)
struct NullTracer final : VMTracer {
  void on_instruction(std::size_t, Instruction const &) override {}

  void on_iter_start(std::size_t, std::size_t) override {}

  void on_iter_advance(std::size_t, std::size_t) override {}

  void on_bind(std::size_t, std::size_t, EClassId) override {}

  void on_bind_duplicate(std::size_t, std::size_t, EClassId) override {}

  void on_check_symbol_fail(std::size_t, ENodeId, std::size_t) override {}

  void on_check_arity_fail(std::size_t, ENodeId, std::size_t, std::size_t) override {}

  void on_check_slot_fail(std::size_t, std::size_t, EClassId, EClassId) override {}

  void on_check_eclass_eq_fail(std::size_t, EClassId, EClassId) override {}

  void on_check_symbol_pass(std::size_t, ENodeId, std::size_t) override {}

  void on_check_arity_pass(std::size_t, ENodeId, std::size_t) override {}

  void on_check_slot_pass(std::size_t, std::size_t, EClassId) override {}

  void on_check_eclass_eq_pass(std::size_t, EClassId) override {}

  void on_mark_seen(std::size_t, std::size_t, EClassId) override {}

  void on_backtrack(std::size_t, std::size_t) override {}

  void on_yield(std::size_t, std::span<EClassId const>) override {}

  void on_halt(std::size_t, std::size_t) override {}
};

/// Recording tracer - captures execution trace for testing
struct RecordingTracer final : VMTracer {
  struct Event {
    enum class Type {
      Instruction,
      IterStart,
      IterAdvance,
      Bind,
      CheckPass,
      CheckFail,
      MarkSeen,
      Backtrack,
      Yield,
      Halt
    };
    Type type;
    std::size_t pc{0};
    std::string details;
  };

  std::vector<Event> events;

  void on_instruction(std::size_t pc, Instruction const &instr) override {
    events.push_back(
        {Event::Type::Instruction,
         pc,
         std::format(
             "{} dst={} src={} arg={} target={}", op_name(instr.op), instr.dst, instr.src, instr.arg, instr.target)});
  }

  void on_iter_start(std::size_t pc, std::size_t count) override {
    events.push_back({Event::Type::IterStart, pc, std::format("count={}", count)});
  }

  void on_iter_advance(std::size_t pc, std::size_t remaining) override {
    events.push_back({Event::Type::IterAdvance, pc, std::format("remaining={}", remaining)});
  }

  void on_bind(std::size_t pc, std::size_t slot, EClassId value) override {
    events.push_back({Event::Type::Bind, pc, std::format("slot={} value={}", slot, value)});
  }

  void on_bind_duplicate(std::size_t pc, std::size_t slot, EClassId value) override {
    events.push_back({Event::Type::CheckFail, pc, std::format("duplicate binding: slot={} value={}", slot, value)});
  }

  void on_check_symbol_fail(std::size_t pc, ENodeId enode, std::size_t expected_sym_idx) override {
    events.push_back({Event::Type::CheckFail,
                      pc,
                      std::format("symbol mismatch: enode={} expected_sym={}", enode, expected_sym_idx)});
  }

  void on_check_arity_fail(std::size_t pc, ENodeId enode, std::size_t expected, std::size_t actual) override {
    events.push_back({Event::Type::CheckFail,
                      pc,
                      std::format("arity mismatch: enode={} expected={} actual={}", enode, expected, actual)});
  }

  void on_check_slot_fail(std::size_t pc, std::size_t slot, EClassId expected, EClassId actual) override {
    events.push_back({Event::Type::CheckFail,
                      pc,
                      std::format("slot mismatch: slot={} expected={} actual={}", slot, expected, actual)});
  }

  void on_check_eclass_eq_fail(std::size_t pc, EClassId lhs, EClassId rhs) override {
    events.push_back({Event::Type::CheckFail, pc, std::format("eclass mismatch: lhs={} rhs={}", lhs, rhs)});
  }

  void on_check_symbol_pass(std::size_t pc, ENodeId enode, std::size_t sym_idx) override {
    events.push_back({Event::Type::CheckPass, pc, std::format("symbol match: enode={} sym={}", enode, sym_idx)});
  }

  void on_check_arity_pass(std::size_t pc, ENodeId enode, std::size_t arity) override {
    events.push_back({Event::Type::CheckPass, pc, std::format("arity match: enode={} arity={}", enode, arity)});
  }

  void on_check_slot_pass(std::size_t pc, std::size_t slot, EClassId value) override {
    events.push_back({Event::Type::CheckPass, pc, std::format("slot match: slot={} value={}", slot, value)});
  }

  void on_check_eclass_eq_pass(std::size_t pc, EClassId value) override {
    events.push_back({Event::Type::CheckPass, pc, std::format("eclass match: value={}", value)});
  }

  void on_mark_seen(std::size_t pc, std::size_t slot, EClassId value) override {
    events.push_back({Event::Type::MarkSeen, pc, std::format("slot={} value={}", slot, value)});
  }

  void on_backtrack(std::size_t pc, std::size_t target) override {
    events.push_back({Event::Type::Backtrack, pc, std::format("target={}", target)});
  }

  void on_yield(std::size_t pc, std::span<EClassId const> slots) override {
    std::string result = "slots=[";
    for (std::size_t i = 0; i < slots.size(); ++i) {
      if (i > 0) result += ", ";
      std::format_to(std::back_inserter(result), "{}", slots[i]);
    }
    result += ']';
    events.push_back({Event::Type::Yield, pc, std::move(result)});
  }

  void on_halt(std::size_t pc, std::size_t total_instructions) override {
    events.push_back({Event::Type::Halt, pc, std::format("total={}", total_instructions)});
  }

  void clear() { events.clear(); }

  /// Print trace to stream
  void print(std::ostream &os) const {
    for (auto const &e : events) {
      switch (e.type) {
        case Event::Type::Instruction:
          os << "[" << e.pc << "] " << e.details << "\n";
          break;
        case Event::Type::IterStart:
          os << "    -> iter_start " << e.details << "\n";
          break;
        case Event::Type::IterAdvance:
          os << "    -> iter_advance " << e.details << "\n";
          break;
        case Event::Type::Bind:
          os << "    -> bind " << e.details << "\n";
          break;
        case Event::Type::CheckPass:
          os << "    -> PASS " << e.details << "\n";
          break;
        case Event::Type::CheckFail:
          os << "    -> FAIL " << e.details << "\n";
          break;
        case Event::Type::MarkSeen:
          os << "    -> mark_seen " << e.details << "\n";
          break;
        case Event::Type::Backtrack:
          os << "    -> BACKTRACK " << e.details << "\n";
          break;
        case Event::Type::Yield:
          os << "    -> YIELD " << e.details << "\n";
          break;
        case Event::Type::Halt:
          os << "=== HALT " << e.details << " ===\n";
          break;
      }
    }
  }
};

/// Combined stats and tracing collector for VM execution (DevMode only)
///
/// Provides a single interface for both statistics collection and tracing.
/// Tracer is optional - if not set, only stats are collected.
struct VMCollector {
  VMStats stats;
  VMTracer *tracer{nullptr};

  void reset() { stats.reset(); }

  void set_tracer(VMTracer *t) { tracer = t; }

  void on_instruction(std::size_t pc, Instruction const &instr) {
    ++stats.instructions_executed;
    if (tracer) tracer->on_instruction(pc, instr);
  }

  void on_halt(std::size_t pc) {
    if (tracer) tracer->on_halt(pc, stats.instructions_executed);
  }

  void on_iter_enode_start(std::size_t pc, std::size_t count) {
    ++stats.iter_enode_calls;
    if (tracer) tracer->on_iter_start(pc, count);
  }

  void on_iter_parent_start(std::size_t pc, std::size_t count) {
    ++stats.iter_parent_calls;
    if (tracer) tracer->on_iter_start(pc, count);
  }

  void on_iter_all_eclasses_start(std::size_t pc, std::size_t count) {
    ++stats.iter_eclass_calls;
    if (tracer) tracer->on_iter_start(pc, count);
  }

  void on_iter_symbol_eclasses_start(std::size_t pc, std::size_t count) {
    ++stats.iter_eclass_calls;
    if (tracer) tracer->on_iter_start(pc, count);
  }

  void on_iter_advance(std::size_t pc, std::size_t remaining) {
    if (tracer) tracer->on_iter_advance(pc, remaining);
  }

  void on_check_symbol_hit(std::size_t pc, ENodeId enode, std::size_t sym_idx) {
    ++stats.symbol_check_hits;
    if (tracer) tracer->on_check_symbol_pass(pc, enode, sym_idx);
  }

  void on_check_symbol_miss(std::size_t pc, ENodeId enode, std::size_t expected_sym_idx) {
    ++stats.symbol_check_misses;
    if (tracer) tracer->on_check_symbol_fail(pc, enode, expected_sym_idx);
  }

  void on_check_arity_pass(std::size_t pc, ENodeId enode, std::size_t arity) {
    if (tracer) tracer->on_check_arity_pass(pc, enode, arity);
  }

  void on_check_arity_fail(std::size_t pc, ENodeId enode, std::size_t expected, std::size_t actual) {
    if (tracer) tracer->on_check_arity_fail(pc, enode, expected, actual);
  }

  void on_bind(std::size_t pc, std::size_t slot, EClassId eclass) {
    if (tracer) tracer->on_bind(pc, slot, eclass);
  }

  void on_bind_duplicate(std::size_t pc, std::size_t slot, EClassId eclass) {
    if (tracer) tracer->on_bind_duplicate(pc, slot, eclass);
  }

  void on_check_slot_hit(std::size_t pc, std::size_t slot, EClassId value) {
    ++stats.check_slot_hits;
    if (tracer) tracer->on_check_slot_pass(pc, slot, value);
  }

  void on_check_slot_miss(std::size_t pc, std::size_t slot, EClassId expected, EClassId actual) {
    ++stats.check_slot_misses;
    if (tracer) tracer->on_check_slot_fail(pc, slot, expected, actual);
  }

  void on_check_eclass_eq_pass(std::size_t pc, EClassId value) {
    if (tracer) tracer->on_check_eclass_eq_pass(pc, value);
  }

  void on_check_eclass_eq_fail(std::size_t pc, EClassId lhs, EClassId rhs) {
    if (tracer) tracer->on_check_eclass_eq_fail(pc, lhs, rhs);
  }

  void on_mark_seen(std::size_t pc, std::size_t slot, EClassId value) {
    if (tracer) tracer->on_mark_seen(pc, slot, value);
  }

  void on_backtrack(std::size_t pc, std::size_t target) {
    if (tracer) tracer->on_backtrack(pc, target);
  }

  void on_yield(std::size_t pc, std::span<EClassId const> slots) {
    ++stats.yields;
    if (tracer) tracer->on_yield(pc, slots);
  }
};

/// Disassemble bytecode to human-readable string
template <typename Symbol>
auto disassemble(std::span<Instruction const> code, std::span<Symbol const> symbols) -> std::string {
  std::string result;
  auto out = std::back_inserter(result);

  for (std::size_t i = 0; i < code.size(); ++i) {
    auto const &instr = code[i];
    std::format_to(out, "{}:\t{}", i, op_name(instr.op));

    switch (instr.op) {
      case VMOp::LoadChild:
        std::format_to(out, " rc{}, rn{}, {}", instr.dst, instr.src, instr.arg);
        break;

      case VMOp::GetENodeEClass:
        std::format_to(out, " rc{}, rn{}", instr.dst, instr.src);
        break;

      case VMOp::IterENodes:
        std::format_to(out, " rn{}, rc{}", instr.dst, instr.src);
        break;

      case VMOp::IterParents:
        std::format_to(out, " rn{}, rc{}, @{}", instr.dst, instr.src, instr.target);
        break;

      case VMOp::NextENode:
      case VMOp::NextParent:
        std::format_to(out, " rn{}, @{}", instr.dst, instr.target);
        break;

      case VMOp::NextEClass:
        std::format_to(out, " rc{}, @{}", instr.dst, instr.target);
        break;

      case VMOp::IterAllEClasses:
        std::format_to(out, " rc{}, @{}", instr.dst, instr.target);
        break;

      case VMOp::IterSymbolEClasses:
        if (instr.arg < symbols.size()) {
          std::format_to(out, " rc{}, {} (sym[{}]), @{}", instr.dst, symbols[instr.arg], instr.arg, instr.target);
        } else {
          std::format_to(out, " rc{}, sym[{}], @{}", instr.dst, instr.arg, instr.target);
        }
        break;

      case VMOp::NextSymbolEClass:
        std::format_to(out, " rc{}, @{}", instr.dst, instr.target);
        break;

      case VMOp::CheckSymbol:
        if (instr.arg < symbols.size()) {
          std::format_to(out, " rn{}, {} (sym[{}]), @{}", instr.src, symbols[instr.arg], instr.arg, instr.target);
        } else {
          std::format_to(out, " rn{}, sym[{}], @{}", instr.src, instr.arg, instr.target);
        }
        break;

      case VMOp::CheckArity:
        std::format_to(out, " rn{}, {}, @{}", instr.src, instr.arg, instr.target);
        break;

      case VMOp::BindSlot:
        std::format_to(out, " slot[{}], rc{}, @{}", instr.arg, instr.src, instr.target);
        break;

      case VMOp::CheckSlot:
        std::format_to(out, " slot[{}], rc{}, @{}", instr.arg, instr.src, instr.target);
        break;

      case VMOp::CheckEClassEq:
        std::format_to(out, " rc{}, rc{}, @{}", instr.dst, instr.src, instr.target);
        break;

      case VMOp::MarkSeen:
        std::format_to(out, " slot[{}]", instr.arg);
        break;

      case VMOp::Jump:
        std::format_to(out, " @{}", instr.target);
        break;

      case VMOp::Yield:
        // Yield marks the last slot as seen, then emits all bindings
        std::format_to(out, " (mark slot[{}])", instr.arg);
        break;

      case VMOp::Halt:
        // No operands
        break;
    }
    result += '\n';
  }
  return result;
}

}  // namespace memgraph::planner::core::pattern::vm
