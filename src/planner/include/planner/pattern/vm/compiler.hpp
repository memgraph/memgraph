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
#include <variant>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/pattern/pattern.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/pattern/vm/instruction.hpp"

namespace memgraph::planner::core::vm {

/// Compiles patterns into VM bytecode for efficient e-graph matching.
///
/// Handles both single patterns and multi-pattern joins with automatic
/// join order computation based on shared variables.
///
/// For multi-pattern joins, uses parent traversal when a shared variable
/// allows efficient joining, otherwise falls back to Cartesian product.
///
/// Example single pattern `Neg(Neg(?x))`:
/// ```
/// 0:  IterENodes r1, r0, @halt     ; iterate e-nodes in input e-class
/// 1:  Jump @3
/// 2:  NextENode r1, @halt          ; advance, or jump to halt
/// 3:  CheckSymbol r1, Neg, @2      ; wrong symbol -> try next
/// 4:  CheckArity r1, 1, @2         ; wrong arity -> try next
/// 5:  LoadChild r2, r1, 0          ; load child
/// 6:  IterENodes r3, r2, @2        ; iterate inner Neg
/// ...
/// N:  Yield
/// N+1: Jump @innermost             ; try more combinations
/// halt:
/// N+2: Halt
/// ```
template <typename Symbol>
class PatternCompiler {
 public:
  /// Compile a single pattern into bytecode.
  /// @return Compiled bytecode or nullopt if pattern exceeds register limit
  auto compile(Pattern<Symbol> const &pattern) -> std::optional<CompiledPattern<Symbol>> {
    std::array<Pattern<Symbol> const *, 1> arr = {&pattern};
    return compile_patterns(arr);
  }

  /// Compile multiple patterns into fused bytecode with automatic join order.
  /// Analyzes shared variables to determine optimal anchor and join order.
  /// @return Compiled bytecode or nullopt if patterns exceed register limit
  auto compile(std::span<Pattern<Symbol> const> patterns) -> std::optional<CompiledPattern<Symbol>> {
    if (patterns.empty()) return std::nullopt;
    if (patterns.size() == 1) return compile(patterns[0]);

    // Build array of pointers for compile_patterns
    std::vector<Pattern<Symbol> const *> ptrs;
    ptrs.reserve(patterns.size());
    for (auto const &p : patterns) ptrs.push_back(&p);
    return compile_patterns(ptrs);
  }

  /// Get the slot map from the last successful compilation.
  [[nodiscard]] auto slot_map() const -> boost::unordered_flat_map<PatternVar, std::size_t> const & {
    return slot_map_;
  }

 private:
  // ============================================================================
  // Core compilation
  // ============================================================================

  auto compile_patterns(std::span<Pattern<Symbol> const *> patterns) -> std::optional<CompiledPattern<Symbol>> {
    reset();

    // Compute join order (trivial for single pattern)
    auto [anchor_idx, join_order] = compute_join_order(patterns);
    auto const &anchor = *patterns[anchor_idx];

    // Build unified slot map
    build_slot_map(patterns);

    // Get entry symbol for index lookup
    std::optional<Symbol> entry_symbol;
    if (auto const *sym = std::get_if<SymbolWithChildren<Symbol>>(&anchor[anchor.root()])) {
      entry_symbol = sym->sym;
    }

    // Emit anchor pattern
    auto anchor_innermost = emit_pattern(anchor, 0, kHaltPlaceholder);

    // Emit joined patterns
    uint16_t innermost = anchor_innermost;
    for (auto idx : join_order) {
      innermost = emit_joined_pattern(*patterns[idx], innermost);
    }

    // Emit yield and continue loop
    code_.push_back(Instruction::yield());
    code_.push_back(Instruction::jmp(innermost));

    // Patch halt placeholders and add halt
    auto halt_pos = static_cast<uint16_t>(code_.size());
    for (auto &instr : code_) {
      if (instr.target == kHaltPlaceholder) instr.target = halt_pos;
    }
    code_.push_back(Instruction::halt());

    return CompiledPattern<Symbol>(std::move(code_), slot_map_.size(), next_reg_, std::move(symbols_), entry_symbol);
  }

  void reset() {
    code_.clear();
    symbols_.clear();
    seen_vars_.clear();
    var_to_reg_.clear();
    slot_map_.clear();
    next_reg_ = 1;
  }

  // ============================================================================
  // Join order computation
  // ============================================================================

  /// Compute optimal join order. Returns (anchor_index, ordered_join_indices).
  /// For single pattern, returns (0, {}).
  static auto compute_join_order(std::span<Pattern<Symbol> const *> patterns)
      -> std::pair<std::size_t, std::vector<std::size_t>> {
    auto const n = patterns.size();
    if (n == 1) return {0, {}};

    // Collect variables per pattern
    std::vector<boost::unordered_flat_set<PatternVar>> pattern_vars(n);
    for (std::size_t i = 0; i < n; ++i) {
      for (auto const &[var, _] : patterns[i]->var_slots()) {
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

  void build_slot_map(std::span<Pattern<Symbol> const *> patterns) {
    for (auto const *pattern : patterns) {
      for (auto const &[var, _] : pattern->var_slots()) {
        if (!slot_map_.contains(var)) {
          slot_map_[var] = slot_map_.size();
        }
      }
    }
  }

  // ============================================================================
  // Iteration loop helper
  // ============================================================================

  /// Emit standard iteration loop: IterX, Jump, NextX, patch-jump.
  /// Returns the loop position (where NextX is).
  auto emit_iter_loop(Instruction iter_instr, Instruction next_instr) -> uint16_t {
    code_.push_back(iter_instr);
    auto jump_pos = static_cast<uint16_t>(code_.size());
    code_.push_back(Instruction::jmp(0));  // placeholder
    auto loop_pos = static_cast<uint16_t>(code_.size());
    code_.push_back(next_instr);
    code_[jump_pos].target = static_cast<uint16_t>(code_.size());
    return loop_pos;
  }

  // ============================================================================
  // Pattern emission (anchor)
  // ============================================================================

  auto emit_pattern(Pattern<Symbol> const &pattern, uint8_t eclass_reg, uint16_t backtrack) -> uint16_t {
    return emit_node(pattern, pattern.root(), eclass_reg, backtrack);
  }

  auto emit_node(Pattern<Symbol> const &pattern, PatternNodeId node_id, uint8_t eclass_reg, uint16_t backtrack)
      -> uint16_t {
    auto const &node = pattern[node_id];
    uint16_t innermost = backtrack;

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

  auto emit_symbol_node(Pattern<Symbol> const &pattern, PatternNodeId node_id, SymbolWithChildren<Symbol> const &sym,
                        uint8_t eclass_reg, uint16_t backtrack) -> uint16_t {
    auto sym_idx = get_symbol_index(sym.sym);
    auto enode_reg = alloc_reg();

    // Emit iteration loop
    auto loop_pos = emit_iter_loop(Instruction::iter_enodes(enode_reg, eclass_reg, backtrack),
                                   Instruction::next_enode(enode_reg, backtrack));

    // Check symbol and arity
    code_.push_back(Instruction::check_symbol(enode_reg, sym_idx, loop_pos));
    code_.push_back(Instruction::check_arity(enode_reg, static_cast<uint8_t>(sym.children.size()), loop_pos));

    // Bind this node if it has a binding
    if (auto binding = pattern.binding_for(node_id)) {
      emit_var_binding(*binding, eclass_reg, loop_pos);
    }

    // Process children
    uint16_t innermost = loop_pos;
    for (std::size_t i = 0; i < sym.children.size(); ++i) {
      auto child_reg = alloc_reg();
      code_.push_back(Instruction::load_child(child_reg, enode_reg, static_cast<uint8_t>(i)));
      innermost = emit_node(pattern, sym.children[i], child_reg, loop_pos);
    }

    return innermost;
  }

  // ============================================================================
  // Joined pattern emission
  // ============================================================================

  auto emit_joined_pattern(Pattern<Symbol> const &pattern, uint16_t anchor_backtrack) -> uint16_t {
    auto const &root_node = pattern[pattern.root()];
    auto const *sym = std::get_if<SymbolWithChildren<Symbol>>(&root_node);

    if (!sym) {
      // Variable/wildcard root - iterate all e-classes
      return emit_joined_variable_root(pattern, anchor_backtrack);
    }

    // Find shared variable child for parent traversal
    for (std::size_t i = 0; i < sym->children.size(); ++i) {
      if (auto const *var = std::get_if<PatternVar>(&pattern[sym->children[i]])) {
        if (auto it = var_to_reg_.find(*var); it != var_to_reg_.end()) {
          return emit_joined_with_parent_traversal(pattern, *sym, *var, i, it->second, anchor_backtrack);
        }
      }
    }

    // No shared variable - Cartesian product
    return emit_joined_cartesian(pattern, *sym, anchor_backtrack);
  }

  auto emit_joined_variable_root(Pattern<Symbol> const &pattern, uint16_t backtrack) -> uint16_t {
    auto eclass_reg = alloc_reg();
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

  auto emit_joined_cartesian(Pattern<Symbol> const &pattern, SymbolWithChildren<Symbol> const &sym, uint16_t backtrack)
      -> uint16_t {
    auto eclass_reg = alloc_reg();
    auto enode_reg = alloc_reg();

    // Outer loop: all e-classes
    auto eclass_loop = emit_iter_loop(Instruction::iter_all_eclasses(eclass_reg, backtrack),
                                      Instruction::next_eclass(eclass_reg, backtrack));

    // Inner loop: e-nodes in each e-class
    auto enode_loop = emit_iter_loop(Instruction::iter_enodes(enode_reg, eclass_reg, eclass_loop),
                                     Instruction::next_enode(enode_reg, eclass_loop));

    // Check symbol and arity
    auto sym_idx = get_symbol_index(sym.sym);
    code_.push_back(Instruction::check_symbol(enode_reg, sym_idx, enode_loop));
    code_.push_back(Instruction::check_arity(enode_reg, static_cast<uint8_t>(sym.children.size()), enode_loop));

    // Process children
    uint16_t innermost = enode_loop;
    for (std::size_t i = 0; i < sym.children.size(); ++i) {
      auto child_reg = alloc_reg();
      code_.push_back(Instruction::load_child(child_reg, enode_reg, static_cast<uint8_t>(i)));
      innermost = emit_joined_child(pattern, sym.children[i], child_reg, innermost);
    }

    // Bind root if needed
    if (auto binding = pattern.binding_for(pattern.root())) {
      code_.push_back(Instruction::bind_slot(get_slot(*binding), eclass_reg));
    }

    return innermost;
  }

  auto emit_joined_with_parent_traversal(Pattern<Symbol> const &pattern, SymbolWithChildren<Symbol> const &sym,
                                         PatternVar shared_var, std::size_t shared_idx, uint8_t shared_reg,
                                         uint16_t backtrack) -> uint16_t {
    auto parent_reg = alloc_reg();
    auto sym_idx = get_symbol_index(sym.sym);

    // Iterate parents with this symbol
    auto loop_pos = emit_iter_loop(Instruction::iter_parents_sym(parent_reg, shared_reg, sym_idx, backtrack),
                                   Instruction::next_parent(parent_reg, backtrack));

    // Check arity
    code_.push_back(Instruction::check_arity(parent_reg, static_cast<uint8_t>(sym.children.size()), loop_pos));

    // Process children
    uint16_t innermost = loop_pos;
    for (std::size_t i = 0; i < sym.children.size(); ++i) {
      auto child_reg = alloc_reg();
      code_.push_back(Instruction::load_child(child_reg, parent_reg, static_cast<uint8_t>(i)));

      if (i == shared_idx) {
        // Verify shared variable matches
        code_.push_back(Instruction::check_slot(get_slot(shared_var), child_reg, innermost));
      } else {
        innermost = emit_joined_child(pattern, sym.children[i], child_reg, innermost);
      }
    }

    // Bind root if needed
    if (auto binding = pattern.binding_for(pattern.root())) {
      auto eclass_reg = alloc_reg();
      code_.push_back(Instruction::get_enode_eclass(eclass_reg, parent_reg));
      code_.push_back(Instruction::bind_slot(get_slot(*binding), eclass_reg));
    }

    return innermost;
  }

  auto emit_joined_child(Pattern<Symbol> const &pattern, PatternNodeId node_id, uint8_t eclass_reg, uint16_t backtrack)
      -> uint16_t {
    auto const &node = pattern[node_id];
    uint16_t innermost = backtrack;

    std::visit(
        [&](auto const &n) {
          using T = std::decay_t<decltype(n)>;
          if constexpr (std::is_same_v<T, Wildcard>) {
            // Wildcard matches anything
          } else if constexpr (std::is_same_v<T, PatternVar>) {
            emit_var_binding(n, eclass_reg, backtrack);
            var_to_reg_[n] = eclass_reg;
          } else if constexpr (std::is_same_v<T, SymbolWithChildren<Symbol>>) {
            auto sym_idx = get_symbol_index(n.sym);
            auto enode_reg = alloc_reg();

            auto loop_pos = emit_iter_loop(Instruction::iter_enodes(enode_reg, eclass_reg, backtrack),
                                           Instruction::next_enode(enode_reg, backtrack));

            code_.push_back(Instruction::check_symbol(enode_reg, sym_idx, loop_pos));
            code_.push_back(Instruction::check_arity(enode_reg, static_cast<uint8_t>(n.children.size()), loop_pos));

            if (auto binding = pattern.binding_for(node_id)) {
              code_.push_back(Instruction::bind_slot(get_slot(*binding), eclass_reg));
            }

            innermost = loop_pos;
            for (std::size_t i = 0; i < n.children.size(); ++i) {
              auto child_reg = alloc_reg();
              code_.push_back(Instruction::load_child(child_reg, enode_reg, static_cast<uint8_t>(i)));
              innermost = emit_joined_child(pattern, n.children[i], child_reg, innermost);
            }
          }
        },
        node);

    return innermost;
  }

  // ============================================================================
  // Variable binding
  // ============================================================================

  void emit_var_binding(PatternVar var, uint8_t eclass_reg, uint16_t backtrack) {
    auto slot = get_slot(var);
    if (seen_vars_.contains(var)) {
      code_.push_back(Instruction::check_slot(slot, eclass_reg, backtrack));
    } else {
      seen_vars_.insert(var);
      code_.push_back(Instruction::bind_slot(slot, eclass_reg));
    }
  }

  // ============================================================================
  // Helpers
  // ============================================================================

  auto get_slot(PatternVar var) const -> uint8_t {
    auto it = slot_map_.find(var);
    return it != slot_map_.end() ? static_cast<uint8_t>(it->second) : 0;
  }

  auto get_symbol_index(Symbol const &sym) -> uint8_t {
    for (std::size_t i = 0; i < symbols_.size(); ++i) {
      if (symbols_[i] == sym) return static_cast<uint8_t>(i);
    }
    symbols_.push_back(sym);
    return static_cast<uint8_t>(symbols_.size() - 1);
  }

  auto alloc_reg() -> uint8_t {
    // Register indices are uint8_t in instructions, so max 256 registers
    // This is effectively unlimited for practical patterns (supports ~125 nesting levels)
    return next_reg_++;
  }

  static constexpr uint16_t kHaltPlaceholder = 0xFFFF;

  std::vector<Instruction> code_;
  std::vector<Symbol> symbols_;
  boost::unordered_flat_set<PatternVar> seen_vars_;
  boost::unordered_flat_map<PatternVar, std::size_t> slot_map_;
  boost::unordered_flat_map<PatternVar, uint8_t> var_to_reg_;
  uint8_t next_reg_{1};
};

// Keep old name as alias for backward compatibility in tests
template <typename Symbol>
using PatternsCompiler = PatternCompiler<Symbol>;

}  // namespace memgraph::planner::core::vm
