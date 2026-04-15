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

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "planner/pattern/match.hpp"
#include "planner/pattern/vm/instruction.hpp"
#include "planner/pattern/vm/state.hpp"
#include "planner/pattern/vm/types.hpp"

namespace memgraph::planner::core::pattern::vm {

/// Non-templated base class for CompiledMatcher.
///
/// Contains all Symbol-independent state and accessors. This reduces template
/// instantiation overhead and allows code that only needs bytecode/register
/// metadata to work with a non-templated interface.
///
/// @see CompiledMatcher for the full templated interface
class CompiledMatcherBase {
 public:
  CompiledMatcherBase() = default;

  CompiledMatcherBase(std::vector<Instruction> code, std::size_t num_eclass_regs, std::size_t num_enode_regs,
                      std::vector<SlotIdx> binding_order, VarSlotMap var_slots);

  [[nodiscard]] auto code() const -> std::span<Instruction const> { return code_; }

  [[nodiscard]] auto num_slots() const -> std::size_t { return var_slots_.size(); }

  [[nodiscard]] auto num_eclass_regs() const -> std::size_t { return num_eclass_regs_; }

  [[nodiscard]] auto num_enode_regs() const -> std::size_t { return num_enode_regs_; }

  /// The order in which slots are bound during pattern matching.
  /// For pattern A(?x, B(?y, ?z)) compiled as B-first, this might be [1, 2, 0]
  /// meaning ?y (slot 1) is bound first, then ?z (slot 2), then ?x (slot 0).
  [[nodiscard]] auto binding_order() const -> std::span<SlotIdx const> { return binding_order_; }

  /// Inverse mapping: slot index -> order position.
  /// For binding_order [1, 2, 0], slot_to_order returns [2, 0, 1].
  /// Used by deduplication: when slot s changes, clear seen sets for all slots
  /// at positions > slot_to_order[s] in binding_order.
  [[nodiscard]] auto slot_to_order() const -> std::span<uint8_t const> { return slot_to_order_; }

  /// Get VMState configuration for this pattern
  [[nodiscard]] auto state_config() const -> VMStateConfig;

  /// Variable to slot index mapping for binding lookup
  [[nodiscard]] auto var_slots() const -> VarSlotMap const & { return var_slots_; }

 protected:
  std::vector<Instruction> code_{Instruction::halt()};
  std::size_t num_eclass_regs_ = 0;     // Registers holding e-class IDs
  std::size_t num_enode_regs_ = 0;      // Registers holding e-node IDs (and iteration state)
  std::vector<SlotIdx> binding_order_;  // Order in which slots are bound during matching
  std::vector<uint8_t> slot_to_order_;  // Inverse: slot index -> order position (O(n) vs O(n²))
  VarSlotMap var_slots_;                // Variable to slot index mapping
};

/// Compiled pattern ready for VM execution.
///
/// Contains the bytecode, symbol table, and metadata needed by VMExecutor.
/// Produced by PatternsCompiler, consumed by VMExecutor.
///
/// ## Contract
///
/// CompiledMatcher encapsulates the contract between PatternsCompiler and VMExecutor:
///
/// - code(): The bytecode sequence; all jump targets are valid
/// - num_slots(): Number of result slots (variables)
/// - num_eclass_regs(): E-class registers needed (≥1, reg 0 is input)
/// - num_enode_regs(): E-node registers needed
/// - symbols(): Symbol table for CheckSymbol/IterSymbolEClasses instructions
/// - binding_order(): Order slots are bound (for deduplication)
/// - slot_to_order(): Inverse mapping for efficient clearing
///
/// @tparam Symbol The symbol type (must match PatternsCompiler and VMExecutor)
///
/// @see PatternsCompiler for bytecode generation
/// @see VMExecutor for bytecode execution
/// @see Instruction for bytecode format
template <typename Symbol>
class CompiledMatcher : public CompiledMatcherBase {
 public:
  /// Default constructor creates a no-op pattern that produces no matches.
  CompiledMatcher() = default;

  CompiledMatcher(std::vector<Instruction> code, std::size_t num_eclass_regs, std::size_t num_enode_regs,
                  std::vector<Symbol> symbols, std::vector<SlotIdx> binding_order, VarSlotMap var_slots);

  [[nodiscard]] auto symbols() const -> std::span<Symbol const> { return symbols_; }

 private:
  std::vector<Symbol> symbols_;  // Symbol table for CheckSymbol/IterSymbolEClasses (deduplicated by compiler)
};

template <typename Symbol>
CompiledMatcher<Symbol>::CompiledMatcher(std::vector<Instruction> code, std::size_t num_eclass_regs,
                                         std::size_t num_enode_regs, std::vector<Symbol> symbols,
                                         std::vector<SlotIdx> binding_order, VarSlotMap var_slots)
    : CompiledMatcherBase(std::move(code), num_eclass_regs, num_enode_regs, std::move(binding_order),
                          std::move(var_slots)),
      symbols_(std::move(symbols)) {}

}  // namespace memgraph::planner::core::pattern::vm
