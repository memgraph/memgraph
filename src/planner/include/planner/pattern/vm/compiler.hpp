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

#include <optional>
#include <span>
#include <variant>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/pattern/pattern.hpp"
#include "planner/pattern/vm/compiled_pattern.hpp"
#include "planner/pattern/vm/instruction.hpp"
#include "planner/pattern/vm/types.hpp"

namespace memgraph::planner::core::pattern::vm {

/// Non-templated base class for PatternCompiler.
///
/// Contains Symbol-independent state and helper methods for bytecode generation.
/// This reduces template instantiation overhead by moving common logic to a
/// single compilation unit.
///
/// @see PatternCompiler for the full templated interface
class PatternCompilerBase {
 protected:
  PatternCompilerBase() = default;

  /// Reset all compiler state for a fresh compilation
  void reset();

  /// Emit standard iteration loop: IterX, Jump, NextX, patch-jump.
  /// Returns the loop position (where NextX is).
  auto emit_iter_loop(Instruction iter_instr, Instruction next_instr) -> InstrAddr;

  /// Emit a BindSlotDedup instruction with backtrack target for early duplicate detection.
  /// All bind_slot emissions should go through this to ensure binding order is tracked.
  void emit_bind_slot(SlotIdx slot, EClassReg eclass_reg, InstrAddr backtrack);

  /// Allocate an e-class register (for LoadChild, GetENodeEClass, IterAllEClasses destinations)
  auto alloc_eclass_reg() -> EClassReg;

  /// Allocate an e-node register (for IterENodes, IterParents destinations)
  auto alloc_enode_reg() -> ENodeReg;

  /// Get slot index for a pattern variable
  [[nodiscard]] auto get_slot(PatternVar var) const -> SlotIdx;

  /// Emit variable binding (check if seen, else bind with dedup)
  void emit_var_binding(PatternVar var, EClassReg eclass_reg, InstrAddr backtrack);

  static constexpr InstrAddr kHaltPlaceholder{0xFFFF};

  std::vector<Instruction> code_;
  boost::unordered_flat_set<PatternVar> seen_vars_;
  boost::unordered_flat_map<PatternVar, SlotIdx> slot_map_;
  boost::unordered_flat_map<PatternVar, EClassReg> var_to_reg_;  // Maps vars to eclass registers
  std::vector<SlotIdx> binding_order_;                           // Order in which slots are bound
  EClassReg next_eclass_reg_{1};                                 // eclass_regs[0] reserved for input
  ENodeReg next_enode_reg_{0};
};

// =============================================================================
// PatternCompiler Contract Documentation
// =============================================================================
//
// PatternCompiler transforms high-level Pattern<Symbol> into executable bytecode.
// It is the PRODUCER in the VM contract; VMExecutor is the CONSUMER.
//
// ## Contractual Guarantees (what PatternCompiler promises to VMExecutor)
//
// ### 1. Register Allocation Contract
//
//   - eclass_regs[0] is ALWAYS reserved for the input candidate e-class
//   - eclass_regs[1..num_eclass_regs-1] are allocated sequentially
//   - enode_regs[0..num_enode_regs-1] are allocated sequentially
//   - No register index exceeds 255 (uint8_t limit)
//   - CompiledPattern::num_eclass_regs() returns exact count needed
//   - CompiledPattern::num_enode_regs() returns exact count needed
//
// ### 2. Jump Target Contract
//
//   - ALL jump targets are valid instruction indices in [0, code.size())
//   - 0xFFFF (kHaltPlaceholder) is used during compilation, patched before return
//   - Backtrack targets always point to NextX or earlier instructions
//   - No forward jumps beyond the Halt instruction
//
// ### 3. Symbol Table Contract
//
//   - CheckSymbol.arg is an index into CompiledPattern::symbols()
//   - Symbol indices are in [0, symbols.size())
//   - Symbols are deduplicated (same symbol -> same index)
//   - symbols() returns the exact symbol table used by all CheckSymbol ops
//
// ### 4. Slot Binding Contract
//
//   - slots[i] maps to pattern variables per CompiledPattern::var_slots()
//   - num_slots() equals the number of distinct variables across all patterns
//   - binding_order() returns the order slots are bound during execution
//   - slot_to_order() maps slot index to its position in binding_order (for clearing)
//   - BindSlotDedup always uses slot indices in [0, num_slots())
//
// ### 5. Iteration Pairing Contract
//
//   - Every IterENodes(dst, ...) has a corresponding NextENode(dst, ...)
//   - Every IterParents(dst, ...) has a corresponding NextParent(dst, ...)
//   - Every IterAllEClasses(dst, ...) has a corresponding NextEClass(dst, ...)
//   - The dst register uniquely identifies the iteration state
//
// ### 6. Code Structure Contract
//
//   - Code begins with iteration setup for root pattern
//   - Code ends with: Yield, Jump(innermost_loop), Halt
//   - Halt is always the last instruction
//   - At least one Yield instruction exists (unless pattern cannot match)
//
// ## Join Strategies
//
//   1. Parent Traversal (depth 1): When patterns share a variable at root's child
//      - IterParents from bound variable, CheckSymbol, verify child
//      - O(parents) per candidate
//
//   2. Parent Chain (depth N): When shared variable is deeply nested
//      - Walk upward through N levels of IterParents
//      - O(parents^N) per candidate
//
//   3. Cartesian Product: When no shared variable
//      - IterAllEClasses × IterENodes
//      - O(eclasses × enodes) - expensive, used as last resort
//
// =============================================================================

/// Compiles patterns into VM bytecode for efficient e-graph matching.
///
/// Handles both single patterns and multi-pattern joins with automatic
/// join order computation based on shared variables.
///
/// For multi-pattern joins, uses parent traversal when a shared variable
/// allows efficient joining, otherwise falls back to Cartesian product.
///
/// ## Contract
///
/// PatternCompiler guarantees that the produced CompiledPattern satisfies all
/// contracts expected by VMExecutor. See instruction.hpp for the full contract
/// specification.
///
/// ## Example
///
/// Single pattern `Neg(Neg(?x))` compiles to:
/// ```
/// 0:  IterENodes r1, r0, @halt     ; iterate e-nodes in input e-class
/// 1:  Jump @3
/// 2:  NextENode r1, @halt          ; advance, or jump to halt
/// 3:  CheckSymbol r1, Neg, @2      ; wrong symbol -> try next
/// 4:  CheckArity r1, 1, @2         ; wrong arity -> try next
/// 5:  LoadChild r2, r1, 0          ; load child e-class
/// 6:  IterENodes r3, r2, @2        ; iterate inner Neg
/// ...
/// N:  Yield
/// N+1: Jump @innermost             ; try more combinations
/// halt:
/// N+2: Halt
/// ```
///
/// @tparam Symbol The symbol type used in patterns (e.g., Op enum)
///
/// @see VMOp for opcode definitions
/// @see Instruction for bytecode format
/// @see VMExecutor for bytecode execution
/// @see CompiledPattern for the compiled bytecode container
template <typename Symbol>
class PatternCompiler : protected PatternCompilerBase {
 public:
  /// Compile a single pattern into bytecode.
  /// @return Compiled pattern or nullopt if pattern exceeds register limit
  auto compile(Pattern<Symbol> const &pattern) -> std::optional<CompiledPattern<Symbol>> {
    return compile_patterns(std::span<Pattern<Symbol> const>(&pattern, 1));
  }

  /// Compile multiple patterns into fused bytecode with automatic join order.
  /// Analyzes shared variables to determine optimal anchor and join order.
  /// Empty pattern set returns empty pattern (matches nothing).
  /// @return Compiled pattern or nullopt if patterns exceed register limit
  auto compile(std::span<Pattern<Symbol> const> patterns) -> std::optional<CompiledPattern<Symbol>> {
    if (patterns.empty()) return CompiledPattern<Symbol>{};
    return compile_patterns(patterns);
  }

 private:
  // ============================================================================
  // Types
  // ============================================================================

  /// Represents a path from root to a node in a pattern tree.
  /// Each entry is (symbol, child_index) describing how to descend.
  struct PatternPath {
    std::vector<std::pair<SymbolWithChildren<Symbol>, std::size_t>> steps;
    PatternVar shared_var;
  };

  // ============================================================================
  // Core compilation
  // ============================================================================

  auto compile_patterns(std::span<Pattern<Symbol> const> patterns) -> std::optional<CompiledPattern<Symbol>>;

  void reset();

  // ============================================================================
  // Join order computation
  // ============================================================================

  /// Compute optimal join order. Returns (anchor_index, ordered_join_indices).
  /// For single pattern, returns (0, {}).
  static auto compute_join_order(std::span<Pattern<Symbol> const> patterns)
      -> std::pair<std::size_t, std::vector<std::size_t>>;

  void build_slot_map(std::span<Pattern<Symbol> const> patterns);

  // ============================================================================
  // Pattern emission (anchor)
  // ============================================================================

  auto emit_pattern(Pattern<Symbol> const &pattern, EClassReg eclass_reg, InstrAddr backtrack) -> InstrAddr;

  auto emit_node(Pattern<Symbol> const &pattern, PatternNodeId node_id, EClassReg eclass_reg, InstrAddr backtrack)
      -> InstrAddr;

  auto emit_symbol_node(Pattern<Symbol> const &pattern, PatternNodeId node_id, SymbolWithChildren<Symbol> const &sym,
                        EClassReg eclass_reg, InstrAddr backtrack) -> InstrAddr;

  // ============================================================================
  // Joined pattern emission
  // ============================================================================

  auto emit_joined_pattern(Pattern<Symbol> const &pattern, InstrAddr anchor_backtrack) -> InstrAddr;

  auto emit_joined_variable_root(Pattern<Symbol> const &pattern, InstrAddr backtrack) -> InstrAddr;

  auto emit_joined_cartesian(Pattern<Symbol> const &pattern, SymbolWithChildren<Symbol> const &sym, InstrAddr backtrack)
      -> InstrAddr;

  auto emit_joined_with_parent_traversal(Pattern<Symbol> const &pattern, SymbolWithChildren<Symbol> const &sym,
                                         PatternVar shared_var, std::size_t shared_idx, EClassReg shared_reg,
                                         InstrAddr backtrack) -> InstrAddr;

  /// Emit parent chain traversal for deeply nested shared variables.
  /// For pattern (F (F (F (F ?v0)))), traverses upward from ?v0 through 4 levels of F parents.
  /// This is O(parents^depth) per match, much better than O(n) Cartesian product.
  auto emit_joined_with_parent_chain(Pattern<Symbol> const &pattern, PatternPath const &path, EClassReg shared_reg,
                                     InstrAddr backtrack) -> InstrAddr;

  auto emit_joined_child(Pattern<Symbol> const &pattern, PatternNodeId node_id, EClassReg eclass_reg,
                         InstrAddr backtrack) -> InstrAddr;

  // ============================================================================
  // Helpers
  // ============================================================================

  /// Find the path from root to a shared variable in the pattern tree.
  /// Returns nullopt if no shared variable is found or pattern has variable/wildcard root.
  [[nodiscard]] auto find_path_to_shared_var(Pattern<Symbol> const &pattern) const -> std::optional<PatternPath>;

  [[nodiscard]] auto find_path_recursive(Pattern<Symbol> const &pattern, PatternNodeId node_id, PatternPath &path) const
      -> bool;

  /// Check if a pattern is redundant in a JOIN context.
  /// Returns true only if the pattern is a pure variable root (no symbol structure)
  /// AND that variable is already bound.
  ///
  /// For example, in "F(?v0) JOIN ?v0", the second pattern is redundant.
  /// But in "F(?v0) JOIN F2(?v0)", we still need to verify F2 nodes exist.
  [[nodiscard]] auto is_redundant_joined_pattern(Pattern<Symbol> const &pattern) const -> bool;

  auto get_symbol_index(Symbol const &sym) -> uint8_t;

  std::vector<Symbol> symbols_;  // Symbol table (Symbol-dependent)
};

template <typename Symbol>
auto PatternCompiler<Symbol>::compile_patterns(std::span<Pattern<Symbol> const> patterns)
    -> std::optional<CompiledPattern<Symbol>> {
  reset();

  // Compute join order (trivial for single pattern)
  auto [anchor_idx, join_order] = compute_join_order(patterns);
  auto const &anchor = patterns[anchor_idx];

  // Build unified slot map
  build_slot_map(patterns);

  // Get entry symbol for index lookup
  std::optional<Symbol> entry_symbol;
  if (auto const *sym = std::get_if<SymbolWithChildren<Symbol>>(&anchor[anchor.root()])) {
    entry_symbol = sym->sym;
  }

  // Emit anchor pattern
  auto anchor_innermost = emit_pattern(anchor, EClassReg{0}, kHaltPlaceholder);

  // Emit joined patterns
  InstrAddr innermost = anchor_innermost;
  for (auto idx : join_order) {
    innermost = emit_joined_pattern(patterns[idx], innermost);
  }

  // Emit yield and continue loop
  // Yield marks the last bound slot as seen (implicit MarkSeen)
  auto last_slot = binding_order_.empty() ? SlotIdx{0} : binding_order_.back();
  code_.push_back(Instruction::yield(last_slot));
  code_.push_back(Instruction::jmp(innermost));

  // Patch halt placeholders and add halt
  auto halt_pos = InstrAddr{static_cast<uint16_t>(code_.size())};
  for (auto &instr : code_) {
    if (instr.target == value_of(kHaltPlaceholder)) instr.target = value_of(halt_pos);
  }
  code_.push_back(Instruction::halt());

  // Convert strong types to underlying types for CompiledPattern
  std::vector<uint8_t> binding_order_raw;
  binding_order_raw.reserve(binding_order_.size());
  for (auto slot : binding_order_) {
    binding_order_raw.push_back(value_of(slot));
  }

  VarSlotMap var_slots_raw;
  for (auto const &[var, slot] : slot_map_) {
    var_slots_raw[var] = value_of(slot);
  }

  return CompiledPattern<Symbol>(std::move(code_),
                                 value_of(next_eclass_reg_),
                                 value_of(next_enode_reg_),
                                 std::move(symbols_),
                                 entry_symbol,
                                 std::move(binding_order_raw),
                                 std::move(var_slots_raw));
}

template <typename Symbol>
void PatternCompiler<Symbol>::reset() {
  PatternCompilerBase::reset();
  symbols_.clear();
}

template <typename Symbol>
auto PatternCompiler<Symbol>::compute_join_order(std::span<Pattern<Symbol> const> patterns)
    -> std::pair<std::size_t, std::vector<std::size_t>> {
  auto const n = patterns.size();
  if (n == 1) return {0, {}};

  // Collect variables per pattern
  std::vector<boost::unordered_flat_set<PatternVar>> pattern_vars(n);
  for (std::size_t i = 0; i < n; ++i) {
    for (auto const &[var, _] : patterns[i].var_slots()) {
      pattern_vars[i].insert(var);
    }
  }

  // Find pattern with most variables (likely best anchor)
  std::size_t anchor = 0;
  std::size_t max_vars = pattern_vars[0].size();
  for (std::size_t i = 1; i < n; ++i) {
    if (pattern_vars[i].size() > max_vars) {
      max_vars = pattern_vars[i].size();
      anchor = i;
    }
  }

  // Greedy join order: pick pattern sharing most vars with already-joined set
  boost::unordered_flat_set<std::size_t> remaining;
  for (std::size_t i = 0; i < n; ++i) {
    if (i != anchor) remaining.insert(i);
  }

  boost::unordered_flat_set<PatternVar> joined_vars = pattern_vars[anchor];
  std::vector<std::size_t> order;
  order.reserve(n - 1);

  while (!remaining.empty()) {
    std::size_t best = *remaining.begin();
    std::size_t best_shared = 0;

    for (auto idx : remaining) {
      std::size_t shared = 0;
      for (auto const &var : pattern_vars[idx]) {
        if (joined_vars.contains(var)) ++shared;
      }
      if (shared > best_shared) {
        best_shared = shared;
        best = idx;
      }
    }

    order.push_back(best);
    remaining.erase(best);
    for (auto const &var : pattern_vars[best]) {
      joined_vars.insert(var);
    }
  }

  return {anchor, order};
}

template <typename Symbol>
void PatternCompiler<Symbol>::build_slot_map(std::span<Pattern<Symbol> const> patterns) {
  // For single patterns, use the pattern's existing slot assignments directly
  // to ensure consistency with MatcherIndex's variable ordering.
  // For multi-pattern joins, we need to merge slot maps carefully.
  if (patterns.size() == 1) {
    for (auto const &[var, slot] : patterns[0].var_slots()) {
      slot_map_[var] = SlotIdx{slot};
    }
  } else {
    // Multi-pattern: assign slots sequentially, but respect each pattern's
    // internal ordering by iterating in slot order
    for (auto const &pattern : patterns) {
      // Collect vars sorted by their slot index to ensure deterministic order
      std::vector<std::pair<PatternVar, uint8_t>> vars(pattern.var_slots().begin(), pattern.var_slots().end());
      std::sort(vars.begin(), vars.end(), [](auto const &a, auto const &b) { return a.second < b.second; });

      for (auto const &[var, _] : vars) {
        if (!slot_map_.contains(var)) {
          slot_map_[var] = SlotIdx{static_cast<uint8_t>(slot_map_.size())};
        }
      }
    }
  }
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_pattern(Pattern<Symbol> const &pattern, EClassReg eclass_reg, InstrAddr backtrack)
    -> InstrAddr {
  return emit_node(pattern, pattern.root(), eclass_reg, backtrack);
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_node(Pattern<Symbol> const &pattern, PatternNodeId node_id, EClassReg eclass_reg,
                                        InstrAddr backtrack) -> InstrAddr {
  auto const &node = pattern[node_id];
  InstrAddr innermost = backtrack;

  std::visit(
      [&](auto const &n) {
        using T = std::decay_t<decltype(n)>;
        if constexpr (std::is_same_v<T, Wildcard>) {
          // Wildcard matches anything
        } else if constexpr (std::is_same_v<T, PatternVar>) {
          emit_var_binding(n, eclass_reg, backtrack);
          var_to_reg_[n] = eclass_reg;
        } else if constexpr (std::is_same_v<T, SymbolWithChildren<Symbol>>) {
          innermost = emit_symbol_node(pattern, node_id, n, eclass_reg, backtrack);
        }
      },
      node);

  return innermost;
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_symbol_node(Pattern<Symbol> const &pattern, PatternNodeId node_id,
                                               SymbolWithChildren<Symbol> const &sym, EClassReg eclass_reg,
                                               InstrAddr backtrack) -> InstrAddr {
  // Bind this node BEFORE iteration if it has a binding.
  // The e-class is the same for all e-nodes in the iteration, so we bind once.
  // Backtrack goes to outer loop (not the e-node iteration we're about to create).
  if (auto binding = pattern.binding_for(node_id)) {
    emit_var_binding(*binding, eclass_reg, backtrack);
  }

  auto sym_idx = get_symbol_index(sym.sym);
  auto enode_reg = alloc_enode_reg();  // IterENodes produces e-node

  // Emit iteration loop
  auto loop_pos = emit_iter_loop(Instruction::iter_enodes(enode_reg, eclass_reg, backtrack),
                                 Instruction::next_enode(enode_reg, backtrack));

  // Check symbol and arity
  code_.push_back(Instruction::check_symbol(enode_reg, sym_idx, loop_pos));
  code_.push_back(Instruction::check_arity(enode_reg, static_cast<uint8_t>(sym.children.size()), loop_pos));

  // For leaf symbols (no children), existence check is sufficient - after matching
  // one e-node, backtrack to parent instead of trying more e-nodes in this e-class.
  // This prevents duplicate matches when an e-class has multiple e-nodes with same symbol.
  if (sym.children.empty()) {
    return backtrack;
  }

  // Process children - each child should backtrack to the innermost loop so far
  // (not the parent's loop), so that we try all combinations of nested e-nodes
  InstrAddr innermost = loop_pos;
  for (std::size_t i = 0; i < sym.children.size(); ++i) {
    auto child_reg = alloc_eclass_reg();  // LoadChild produces e-class
    code_.push_back(Instruction::load_child(child_reg, enode_reg, static_cast<uint8_t>(i)));
    innermost = emit_node(pattern, sym.children[i], child_reg, innermost);
  }

  return innermost;
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_joined_pattern(Pattern<Symbol> const &pattern, InstrAddr anchor_backtrack)
    -> InstrAddr {
  // Optimization: skip pure variable patterns that introduce no new bindings
  // e.g., in "?v0 JOIN ?v0", the second ?v0 is redundant
  // But "F(?v0) JOIN F2(?v0)" still needs to verify F2 exists
  if (is_redundant_joined_pattern(pattern)) {
    return anchor_backtrack;
  }

  auto const &root_node = pattern[pattern.root()];
  auto const *sym = std::get_if<SymbolWithChildren<Symbol>>(&root_node);

  if (!sym) {
    // Variable/wildcard root - iterate all e-classes
    return emit_joined_variable_root(pattern, anchor_backtrack);
  }

  // Find shared variable child for parent traversal (depth 1)
  for (std::size_t i = 0; i < sym->children.size(); ++i) {
    if (auto const *var = std::get_if<PatternVar>(&pattern[sym->children[i]])) {
      if (auto it = var_to_reg_.find(*var); it != var_to_reg_.end()) {
        return emit_joined_with_parent_traversal(pattern, *sym, *var, i, it->second, anchor_backtrack);
      }
    }
  }

  // Check for deep shared variable - traverse upward through parent chain
  // This handles patterns like (F (F (F (F ?v0)))) where ?v0 is nested deep
  if (auto path = find_path_to_shared_var(pattern)) {
    auto it = var_to_reg_.find(path->shared_var);
    if (it != var_to_reg_.end()) {
      return emit_joined_with_parent_chain(pattern, *path, it->second, anchor_backtrack);
    }
  }

  // No shared variable - Cartesian product
  return emit_joined_cartesian(pattern, *sym, anchor_backtrack);
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_joined_variable_root(Pattern<Symbol> const &pattern, InstrAddr backtrack)
    -> InstrAddr {
  auto eclass_reg = alloc_eclass_reg();  // IterAllEClasses produces e-class
  auto loop_pos = emit_iter_loop(Instruction::iter_all_eclasses(eclass_reg, backtrack),
                                 Instruction::next_eclass(eclass_reg, backtrack));

  // Handle root binding
  if (auto const *var = std::get_if<PatternVar>(&pattern[pattern.root()])) {
    emit_var_binding(*var, eclass_reg, loop_pos);
    var_to_reg_[*var] = eclass_reg;
  }
  if (auto binding = pattern.binding_for(pattern.root())) {
    emit_var_binding(*binding, eclass_reg, loop_pos);
  }

  return loop_pos;
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_joined_cartesian(Pattern<Symbol> const &pattern,
                                                    SymbolWithChildren<Symbol> const &sym, InstrAddr backtrack)
    -> InstrAddr {
  auto eclass_reg = alloc_eclass_reg();  // IterAllEClasses produces e-class
  auto enode_reg = alloc_enode_reg();    // IterENodes produces e-node

  // Outer loop: all e-classes
  auto eclass_loop = emit_iter_loop(Instruction::iter_all_eclasses(eclass_reg, backtrack),
                                    Instruction::next_eclass(eclass_reg, backtrack));

  // Bind root BEFORE e-node iteration if needed.
  // Each e-class is different; if duplicate, backtrack to e-class loop.
  if (auto binding = pattern.binding_for(pattern.root())) {
    emit_bind_slot(get_slot(*binding), eclass_reg, eclass_loop);
  }

  // Inner loop: e-nodes in each e-class
  auto enode_loop = emit_iter_loop(Instruction::iter_enodes(enode_reg, eclass_reg, eclass_loop),
                                   Instruction::next_enode(enode_reg, eclass_loop));

  // Check symbol and arity
  auto sym_idx = get_symbol_index(sym.sym);
  code_.push_back(Instruction::check_symbol(enode_reg, sym_idx, enode_loop));
  code_.push_back(Instruction::check_arity(enode_reg, static_cast<uint8_t>(sym.children.size()), enode_loop));

  // Process children
  InstrAddr innermost = enode_loop;
  for (std::size_t i = 0; i < sym.children.size(); ++i) {
    auto child_reg = alloc_eclass_reg();  // LoadChild produces e-class
    code_.push_back(Instruction::load_child(child_reg, enode_reg, static_cast<uint8_t>(i)));
    innermost = emit_joined_child(pattern, sym.children[i], child_reg, innermost);
  }

  return innermost;
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_joined_with_parent_traversal(Pattern<Symbol> const &pattern,
                                                                SymbolWithChildren<Symbol> const &sym,
                                                                PatternVar shared_var, std::size_t shared_idx,
                                                                EClassReg shared_reg, InstrAddr backtrack)
    -> InstrAddr {
  auto parent_reg = alloc_enode_reg();  // IterParents produces e-node
  auto sym_idx = get_symbol_index(sym.sym);

  // Iterate all parents with lazy symbol checking (avoids buffer allocation)
  auto loop_pos = emit_iter_loop(Instruction::iter_parents(parent_reg, shared_reg, backtrack),
                                 Instruction::next_parent(parent_reg, backtrack));

  // Check symbol and arity (backtrack to loop on mismatch)
  code_.push_back(Instruction::check_symbol(parent_reg, sym_idx, loop_pos));
  code_.push_back(Instruction::check_arity(parent_reg, static_cast<uint8_t>(sym.children.size()), loop_pos));

  // Bind root BEFORE processing children if needed.
  // Each parent could be in a different e-class, backtrack to parent loop if duplicate.
  if (auto binding = pattern.binding_for(pattern.root())) {
    auto eclass_reg = alloc_eclass_reg();  // GetENodeEClass produces e-class
    code_.push_back(Instruction::get_enode_eclass(eclass_reg, parent_reg));
    emit_bind_slot(get_slot(*binding), eclass_reg, loop_pos);
  }

  // Process children
  InstrAddr innermost = loop_pos;
  for (std::size_t i = 0; i < sym.children.size(); ++i) {
    auto child_reg = alloc_eclass_reg();  // LoadChild produces e-class
    code_.push_back(Instruction::load_child(child_reg, parent_reg, static_cast<uint8_t>(i)));

    if (i == shared_idx) {
      // Verify shared variable matches
      code_.push_back(Instruction::check_slot(get_slot(shared_var), child_reg, innermost));
    } else {
      innermost = emit_joined_child(pattern, sym.children[i], child_reg, innermost);
    }
  }

  return innermost;
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_joined_with_parent_chain(Pattern<Symbol> const &pattern, PatternPath const &path,
                                                            EClassReg shared_reg, InstrAddr backtrack) -> InstrAddr {
  // Current e-class register starts at the shared variable
  EClassReg current_eclass_reg = shared_reg;
  InstrAddr innermost = backtrack;

  // Traverse path in reverse (from shared var up to root)
  // Each step: iterate parents with expected symbol, verify child index
  for (auto it = path.steps.rbegin(); it != path.steps.rend(); ++it) {
    auto const &[sym, child_idx] = *it;
    auto parent_reg = alloc_enode_reg();
    auto sym_idx = get_symbol_index(sym.sym);

    // Iterate parents of current e-class
    auto loop_pos = emit_iter_loop(Instruction::iter_parents(parent_reg, current_eclass_reg, innermost),
                                   Instruction::next_parent(parent_reg, innermost));

    // Check symbol and arity
    code_.push_back(Instruction::check_symbol(parent_reg, sym_idx, loop_pos));
    code_.push_back(Instruction::check_arity(parent_reg, static_cast<uint8_t>(sym.children.size()), loop_pos));

    // Verify the shared variable is at the expected child index
    auto verify_child_reg = alloc_eclass_reg();
    code_.push_back(Instruction::load_child(verify_child_reg, parent_reg, static_cast<uint8_t>(child_idx)));
    code_.push_back(Instruction::check_eclass_eq(verify_child_reg, current_eclass_reg, loop_pos));

    // Process non-shared children (bind new variables, verify structure)
    // sym.children contains the PatternNodeIds for each child position
    //
    // IMPORTANT: Sibling children at this level should backtrack to THIS level's loop (loop_pos),
    // not the previous level's loop (innermost). When a sibling structure check fails,
    // we want to try the next parent at THIS level, not skip to the parent level above.
    InstrAddr sibling_innermost = loop_pos;
    for (std::size_t i = 0; i < sym.children.size(); ++i) {
      if (i == child_idx) continue;  // Skip the child we're traversing through

      auto child_reg = alloc_eclass_reg();
      code_.push_back(Instruction::load_child(child_reg, parent_reg, static_cast<uint8_t>(i)));

      // Verify the sibling child's structure using emit_joined_child
      // This handles variables (checking slot bindings), symbols (verifying structure), etc.
      sibling_innermost = emit_joined_child(pattern, sym.children[i], child_reg, sibling_innermost);
    }

    // Move up: get the e-class of this parent for next iteration
    auto next_eclass_reg = alloc_eclass_reg();
    code_.push_back(Instruction::get_enode_eclass(next_eclass_reg, parent_reg));
    current_eclass_reg = next_eclass_reg;
    innermost = loop_pos;
  }

  // Bind root if needed
  if (auto binding = pattern.binding_for(pattern.root())) {
    emit_bind_slot(get_slot(*binding), current_eclass_reg, innermost);
  }

  return innermost;
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_joined_child(Pattern<Symbol> const &pattern, PatternNodeId node_id,
                                                EClassReg eclass_reg, InstrAddr backtrack) -> InstrAddr {
  auto const &node = pattern[node_id];
  InstrAddr innermost = backtrack;

  std::visit(
      [&](auto const &n) {
        using T = std::decay_t<decltype(n)>;
        if constexpr (std::is_same_v<T, Wildcard>) {
          // Wildcard matches anything
        } else if constexpr (std::is_same_v<T, PatternVar>) {
          emit_var_binding(n, eclass_reg, backtrack);
          var_to_reg_[n] = eclass_reg;
        } else if constexpr (std::is_same_v<T, SymbolWithChildren<Symbol>>) {
          // Bind this node BEFORE iteration if it has a binding.
          // The e-class is the same for all e-nodes, so we bind once.
          if (auto binding = pattern.binding_for(node_id)) {
            emit_bind_slot(get_slot(*binding), eclass_reg, backtrack);
          }

          auto sym_idx = get_symbol_index(n.sym);
          auto enode_reg = alloc_enode_reg();  // IterENodes produces e-node

          auto loop_pos = emit_iter_loop(Instruction::iter_enodes(enode_reg, eclass_reg, backtrack),
                                         Instruction::next_enode(enode_reg, backtrack));

          code_.push_back(Instruction::check_symbol(enode_reg, sym_idx, loop_pos));
          code_.push_back(Instruction::check_arity(enode_reg, static_cast<uint8_t>(n.children.size()), loop_pos));

          // For leaf symbols, backtrack to parent after match (existence check only)
          if (n.children.empty()) {
            innermost = backtrack;
          } else {
            innermost = loop_pos;
            for (std::size_t i = 0; i < n.children.size(); ++i) {
              auto child_reg = alloc_eclass_reg();  // LoadChild produces e-class
              code_.push_back(Instruction::load_child(child_reg, enode_reg, static_cast<uint8_t>(i)));
              innermost = emit_joined_child(pattern, n.children[i], child_reg, innermost);
            }
          }
        }
      },
      node);

  return innermost;
}

template <typename Symbol>
auto PatternCompiler<Symbol>::find_path_to_shared_var(Pattern<Symbol> const &pattern) const
    -> std::optional<PatternPath> {
  auto const &root_node = pattern[pattern.root()];
  auto const *root_sym = std::get_if<SymbolWithChildren<Symbol>>(&root_node);
  if (!root_sym) return std::nullopt;

  PatternPath path;
  return find_path_recursive(pattern, pattern.root(), path) ? std::optional{path} : std::nullopt;
}

template <typename Symbol>
auto PatternCompiler<Symbol>::find_path_recursive(Pattern<Symbol> const &pattern, PatternNodeId node_id,
                                                  PatternPath &path) const -> bool {
  auto const &node = pattern[node_id];

  // Check if this is a shared variable
  if (auto const *var = std::get_if<PatternVar>(&node)) {
    if (var_to_reg_.contains(*var)) {
      path.shared_var = *var;
      return true;
    }
  }

  // Recurse into symbol children
  if (auto const *sym = std::get_if<SymbolWithChildren<Symbol>>(&node)) {
    for (std::size_t i = 0; i < sym->children.size(); ++i) {
      path.steps.emplace_back(*sym, i);
      if (find_path_recursive(pattern, sym->children[i], path)) {
        return true;
      }
      path.steps.pop_back();
    }
  }

  return false;
}

template <typename Symbol>
auto PatternCompiler<Symbol>::is_redundant_joined_pattern(Pattern<Symbol> const &pattern) const -> bool {
  // Only skip if the pattern is just a single variable or wildcard at the root
  // (no symbol structure to verify)
  auto const &root_node = pattern[pattern.root()];

  // Check if root is a symbol - if so, we need to verify it exists
  if (std::holds_alternative<SymbolWithChildren<Symbol>>(root_node)) {
    return false;  // Has symbol structure, must verify
  }

  // Root is a variable or wildcard - check if all variables are already bound
  for (auto const &[var, _] : pattern.var_slots()) {
    if (!seen_vars_.contains(var)) {
      return false;  // This variable is new
    }
  }
  return true;  // Pure variable/wildcard with all variables already bound
}

template <typename Symbol>
auto PatternCompiler<Symbol>::get_symbol_index(Symbol const &sym) -> uint8_t {
  for (std::size_t i = 0; i < symbols_.size(); ++i) {
    if (symbols_[i] == sym) return static_cast<uint8_t>(i);
  }
  symbols_.push_back(sym);
  return static_cast<uint8_t>(symbols_.size() - 1);
}

}  // namespace memgraph::planner::core::pattern::vm
