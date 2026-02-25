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
#include <iostream>
#include <span>
#include <sstream>
#include <string>
#include <vector>

#include "planner/pattern/vm/instruction.hpp"

namespace memgraph::planner::core::vm {

/// Statistics collected during VM execution (for benchmarking/profiling)
struct VMStats {
  std::size_t instructions_executed{0};
  std::size_t iter_enode_calls{0};
  std::size_t iter_parent_calls{0};
  std::size_t parent_symbol_hits{0};    // Parents that matched symbol filter
  std::size_t parent_symbol_misses{0};  // Parents that failed symbol filter
  std::size_t check_slot_hits{0};       // CheckSlot that passed
  std::size_t check_slot_misses{0};     // CheckSlot that failed
  std::size_t yields{0};

  void reset() { *this = VMStats{}; }

  [[nodiscard]] auto parent_filter_rate() const -> double {
    auto total = parent_symbol_hits + parent_symbol_misses;
    return total > 0 ? static_cast<double>(parent_symbol_misses) / static_cast<double>(total) : 0.0;
  }
};

/// Tracer interface for VM execution debugging
struct VMTracer {
  virtual ~VMTracer() = default;

  /// Called before executing each instruction
  virtual void on_instruction(std::size_t pc, Instruction const &instr) = 0;

  /// Called when iteration starts (IterENodes, IterParents)
  virtual void on_iter_start(std::size_t pc, std::size_t count) = 0;

  /// Called when iteration advances (NextENode, NextParent)
  virtual void on_iter_advance(std::size_t pc, std::size_t remaining) = 0;

  /// Called when a slot is bound
  virtual void on_bind(std::size_t slot, EClassId value) = 0;

  /// Called when a check fails (CheckSymbol, CheckArity, CheckSlot)
  virtual void on_check_fail(std::size_t pc, std::string_view reason) = 0;

  /// Called when a match is yielded
  virtual void on_yield(std::span<EClassId const> slots) = 0;

  /// Called when execution halts
  virtual void on_halt(std::size_t total_instructions) = 0;
};

/// Null tracer - does nothing (for production use)
struct NullTracer final : VMTracer {
  void on_instruction(std::size_t, Instruction const &) override {}

  void on_iter_start(std::size_t, std::size_t) override {}

  void on_iter_advance(std::size_t, std::size_t) override {}

  void on_bind(std::size_t, EClassId) override {}

  void on_check_fail(std::size_t, std::string_view) override {}

  void on_yield(std::span<EClassId const>) override {}

  void on_halt(std::size_t) override {}
};

/// Recording tracer - captures execution trace for testing
struct RecordingTracer final : VMTracer {
  struct Event {
    enum class Type { Instruction, IterStart, IterAdvance, Bind, CheckFail, Yield, Halt };
    Type type;
    std::size_t pc{0};
    std::string details;
  };

  std::vector<Event> events;

  void on_instruction(std::size_t pc, Instruction const &instr) override {
    std::ostringstream ss;
    ss << op_name(instr.op) << " dst=" << static_cast<int>(instr.dst) << " src=" << static_cast<int>(instr.src)
       << " arg=" << static_cast<int>(instr.arg) << " target=" << instr.target;
    events.push_back({Event::Type::Instruction, pc, ss.str()});
  }

  void on_iter_start(std::size_t pc, std::size_t count) override {
    events.push_back({Event::Type::IterStart, pc, "count=" + std::to_string(count)});
  }

  void on_iter_advance(std::size_t pc, std::size_t remaining) override {
    events.push_back({Event::Type::IterAdvance, pc, "remaining=" + std::to_string(remaining)});
  }

  void on_bind(std::size_t slot, EClassId value) override {
    events.push_back(
        {Event::Type::Bind, 0, "slot=" + std::to_string(slot) + " value=" + std::to_string(value.value_of())});
  }

  void on_check_fail(std::size_t pc, std::string_view reason) override {
    events.push_back({Event::Type::CheckFail, pc, std::string(reason)});
  }

  void on_yield(std::span<EClassId const> slots) override {
    std::ostringstream ss;
    ss << "slots=[";
    for (std::size_t i = 0; i < slots.size(); ++i) {
      if (i > 0) ss << ", ";
      ss << slots[i].value_of();
    }
    ss << "]";
    events.push_back({Event::Type::Yield, 0, ss.str()});
  }

  void on_halt(std::size_t total_instructions) override {
    events.push_back({Event::Type::Halt, 0, "total=" + std::to_string(total_instructions)});
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
        case Event::Type::CheckFail:
          os << "    -> FAIL " << e.details << "\n";
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

  void on_halt() {
    if (tracer) tracer->on_halt(stats.instructions_executed);
  }

  void on_iter_enode_start(std::size_t pc, std::size_t count) {
    ++stats.iter_enode_calls;
    if (tracer) tracer->on_iter_start(pc, count);
  }

  void on_iter_parent_start(std::size_t pc, std::size_t count) {
    ++stats.iter_parent_calls;
    if (tracer) tracer->on_iter_start(pc, count);
  }

  void on_iter_advance(std::size_t pc, std::size_t remaining) {
    if (tracer) tracer->on_iter_advance(pc, remaining);
  }

  void on_check_symbol_hit() { ++stats.parent_symbol_hits; }

  void on_check_symbol_miss(std::size_t pc) {
    ++stats.parent_symbol_misses;
    if (tracer) tracer->on_check_fail(pc, "symbol mismatch");
  }

  void on_check_arity_fail(std::size_t pc) {
    if (tracer) tracer->on_check_fail(pc, "arity mismatch");
  }

  void on_bind(std::size_t slot, EClassId eclass) {
    if (tracer) tracer->on_bind(slot, eclass);
  }

  void on_bind_duplicate(std::size_t pc) {
    if (tracer) tracer->on_check_fail(pc, "duplicate binding");
  }

  void on_check_slot_hit() { ++stats.check_slot_hits; }

  void on_check_slot_miss(std::size_t pc) {
    ++stats.check_slot_misses;
    if (tracer) tracer->on_check_fail(pc, "slot mismatch");
  }

  void on_yield(std::span<EClassId const> slots) {
    ++stats.yields;
    if (tracer) tracer->on_yield(slots);
  }
};

/// Disassemble bytecode to human-readable string
template <typename Symbol>
auto disassemble(std::span<Instruction const> code, std::span<Symbol const> symbols) -> std::string {
  std::ostringstream ss;
  for (std::size_t i = 0; i < code.size(); ++i) {
    auto const &instr = code[i];
    ss << i << ":\t" << op_name(instr.op);

    switch (instr.op) {
      case VMOp::LoadChild:
        ss << " r" << static_cast<int>(instr.dst) << ", r" << static_cast<int>(instr.src) << ", "
           << static_cast<int>(instr.arg);
        break;

      case VMOp::GetENodeEClass:
        ss << " r" << static_cast<int>(instr.dst) << ", r" << static_cast<int>(instr.src);
        break;

      case VMOp::IterENodes:
      case VMOp::IterParents:
        ss << " r" << static_cast<int>(instr.dst) << ", r" << static_cast<int>(instr.src) << ", @" << instr.target;
        break;

      case VMOp::NextENode:
      case VMOp::NextParent:
      case VMOp::NextEClass:
        ss << " r" << static_cast<int>(instr.dst) << ", @" << instr.target;
        break;

      case VMOp::IterAllEClasses:
        ss << " r" << static_cast<int>(instr.dst) << ", @" << instr.target;
        break;

      case VMOp::CheckSymbol:
        ss << " r" << static_cast<int>(instr.src);
        if (instr.arg < symbols.size()) {
          ss << ", sym[" << static_cast<int>(instr.arg) << "]";
        }
        ss << ", @" << instr.target;
        break;

      case VMOp::CheckArity:
        ss << " r" << static_cast<int>(instr.src) << ", " << static_cast<int>(instr.arg) << ", @" << instr.target;
        break;

      case VMOp::BindSlotDedup:
        ss << " slot[" << static_cast<int>(instr.arg) << "], r" << static_cast<int>(instr.src) << ", @" << instr.target;
        break;

      case VMOp::CheckSlot:
        ss << " slot[" << static_cast<int>(instr.arg) << "], r" << static_cast<int>(instr.src) << ", @" << instr.target;
        break;

      case VMOp::MarkSeen:
        ss << " slot[" << static_cast<int>(instr.arg) << "]";
        break;

      case VMOp::Jump:
        ss << " @" << instr.target;
        break;

      case VMOp::Yield:
        // Yield marks slot as seen before emitting (special MarkSeen)
        ss << " slot[" << static_cast<int>(instr.arg) << "]";
        break;

      case VMOp::Halt:
        // No operands
        break;
    }
    ss << "\n";
  }
  return ss.str();
}

}  // namespace memgraph::planner::core::vm
