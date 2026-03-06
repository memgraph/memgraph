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

#include <cassert>
#include <span>
#include <type_traits>
#include <vector>

#include "planner/pattern/match_index.hpp"
#include "planner/pattern/match_storage.hpp"
#include "planner/pattern/vm/compiled_pattern.hpp"
#include "planner/pattern/vm/tracer.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::pattern::vm {

// =============================================================================
// VMExecutor Contract Documentation
// =============================================================================
//
// VMExecutor interprets CompiledPattern bytecode against an e-graph.
// It is the CONSUMER in the VM contract; PatternCompiler is the PRODUCER.
//
// ## Contractual Expectations (what VMExecutor requires from PatternCompiler)
//
// ### 1. Well-Formed Bytecode
//
//   - All jump targets are valid instruction indices
//   - Register indices don't exceed num_*_regs - 1
//   - Symbol indices don't exceed symbols.size() - 1
//   - Slot indices don't exceed num_slots - 1
//
// ### 2. Iteration Consistency
//
//   - IterX/NextX pairs use the same dst register
//   - Iteration state is managed per-register by VMState
//   - Exhausted iterators trigger backtrack via jump target
//
// ### 3. Canonical E-Class IDs
//
//   - Input candidates MUST be canonical (egraph.find(id) == id)
//   - VMExecutor canonicalizes via LoadChild and GetENodeEClass
//   - slots[] always contain canonical IDs after BindSlot
//
// ## Execution Model
//
// For each candidate e-class:
//   1. Set eclass_regs[0] = candidate, pc = 0
//   2. Fetch instruction at pc
//   3. Dispatch to handler based on opcode
//   4. Handler may: advance pc, jump to target, or halt
//   5. Repeat until Halt
//
// ## Deduplication Protocol
//
// Prevents duplicate match tuples via seen_per_slot sets:
//
//   1. BindSlot checks if value is in seen_per_slot[slot]
//   2. If seen, backtrack (don't explore same value twice)
//   3. Yield/MarkSeen adds value to seen_per_slot[slot]
//   4. When slot i changes, clear seen_per_slot for slots bound after i in binding_order
//
// This ensures each unique binding tuple is yielded exactly once.
//
// =============================================================================

/// VM executor for pattern matching.
///
/// Executes compiled patterns against an e-graph, collecting matches.
/// This is the CONSUMER of the VM contract defined in instruction.hpp.
///
/// ## Contract Expectations
///
/// VMExecutor expects CompiledPattern to satisfy:
///
/// 1. **Valid Bytecode**: All instructions are well-formed per VMOp semantics
/// 2. **Valid Targets**: Jump targets are in [0, code.size())
/// 3. **Valid Indices**: Register, symbol, slot indices within bounds
/// 4. **Paired Iterations**: IterX/NextX use matching dst registers
///
/// ## Execution Guarantees
///
/// VMExecutor provides:
///
/// 1. **Canonical Results**: All slot values are canonical e-class IDs
/// 2. **No Duplicates**: Each unique binding tuple yielded exactly once
/// 3. **Complete Enumeration**: All valid matches are found
/// 4. **Correct Backtracking**: Failed checks backtrack to correct loop
///
/// ## Usage
///
/// ```cpp
/// PatternCompiler<Op> compiler;
/// auto compiled = compiler.compile(pattern);
/// if (!compiled) { /* pattern too complex */ }
///
/// VMExecutor<Op, Analysis> executor(egraph);
/// EMatchContext ctx;
/// std::vector<PatternMatch> matches;
/// executor.execute(*compiled, matcher, ctx, matches);
/// ```
///
/// @tparam Symbol The symbol type used in patterns
/// @tparam Analysis The e-graph analysis type
/// @tparam DevMode Enable stats collection and tracing (for debugging/profiling)
///
/// @see PatternCompiler for bytecode generation
/// @see CompiledPattern for the bytecode container
/// @see VMOp for opcode definitions
/// @see Instruction for bytecode format
template <typename Symbol, typename Analysis, bool DevMode = false>
class VMExecutor {
 public:
  using EGraphType = EGraph<Symbol, Analysis>;

  explicit VMExecutor(EGraphType const &egraph) : egraph_(&egraph) {}

  VMExecutor(EGraphType const &egraph, VMTracer *tracer)
    requires(DevMode)
      : egraph_(&egraph) {
    collector_.set_tracer(tracer);
  }

  /// Execute compiled pattern with automatic candidate lookup via MatcherIndex.
  ///
  /// Uses the pattern's entry_symbol() to look up candidates from the matcher's
  /// symbol index. If the pattern has a variable/wildcard root, falls back to
  /// iterating all e-classes.
  ///
  /// ## Contract
  ///
  /// @pre pattern was produced by PatternCompiler (satisfies all bytecode contracts)
  /// @pre matcher's index is up-to-date with the e-graph
  /// @pre arena is valid for storing match bindings
  ///
  /// @post results contains all unique matches found
  /// @post Stale index entries (merged e-classes) are handled via canonicalization
  ///
  /// @param pattern The compiled bytecode to execute
  /// @param index MatcherIndex with symbol index for candidate lookup
  /// @param arena MatchArena for storing match bindings
  /// @param results Output vector for matches (appended, not cleared)
  void execute(CompiledPattern<Symbol> const &pattern, MatcherIndex<Symbol, Analysis> &index, MatchArena &arena,
               std::vector<PatternMatch> &results);

  /// Get execution stats (only available when DevMode=true)
  [[nodiscard]] auto stats() const -> VMStats const &
    requires(DevMode)
  {
    return collector_.stats;
  }

  /// Set tracer for debugging (only available when DevMode=true)
  void set_tracer(VMTracer *tracer)
    requires(DevMode)
  {
    collector_.set_tracer(tracer);
  }

 private:
  /// Execute compiled pattern with explicit candidate list (internal implementation).
  ///
  /// @pre candidates contains ONLY canonical e-class IDs (egraph.find(id) == id)
  void execute_impl(CompiledPattern<Symbol> const &pattern, std::span<EClassId const> candidates, MatchArena &arena,
                    std::vector<PatternMatch> &results);

  void run_until_halt(MatchArena &arena, std::vector<PatternMatch> &results);

  // Instruction is 6 bytes - pass by value for efficiency (fits in register)
  [[gnu::always_inline]] void exec_load_child(Instruction instr) {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    // Future optimization: LOAD_CHILDREN instruction to batch get_enode calls
    state_.eclass_regs[instr.dst] = egraph_->find(enode.children()[instr.arg]);
  }

  [[gnu::always_inline]] void exec_get_enode_eclass(Instruction instr) {
    auto enode_id = state_.enode_regs[instr.src];
    state_.eclass_regs[instr.dst] = egraph_->find(enode_id);
  }

  [[gnu::always_inline]] void exec_iter_enodes(Instruction instr);

  /// Common helper for advancing span-based iterators (ENodesIter, AllEClassesIter)
  template <typename Iter, typename IdType>
  [[nodiscard]] [[gnu::always_inline]] auto advance_span_iter(Iter &iter, IdType &out_reg) -> bool;

  [[nodiscard]] [[gnu::always_inline]] auto exec_next_enode(Instruction instr) -> bool {
    return advance_span_iter(state_.get_enodes_iter(instr.dst), state_.enode_regs[instr.dst]);
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_iter_all_eclasses(Instruction instr) -> bool;

  [[nodiscard]] [[gnu::always_inline]] auto exec_next_eclass(Instruction instr) -> bool {
    return advance_span_iter(state_.get_eclasses_iter(instr.dst), state_.eclass_regs[instr.dst]);
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_iter_parents(Instruction instr) -> bool;

  /// Advance index-based parent iteration (for IterParents)
  [[nodiscard]] [[gnu::always_inline]] auto exec_next_parent(Instruction instr) -> bool;

  [[nodiscard]] [[gnu::always_inline]] auto exec_check_symbol(Instruction instr) -> bool;

  [[nodiscard]] [[gnu::always_inline]] auto exec_check_arity(Instruction instr) -> bool;

  [[nodiscard]] [[gnu::always_inline]] auto exec_bind_slot(Instruction instr) -> bool;

  [[nodiscard]] [[gnu::always_inline]] auto exec_check_slot(Instruction instr) -> bool;

  [[nodiscard]] [[gnu::always_inline]] auto exec_check_eclass_eq(Instruction instr) -> bool;

  [[gnu::always_inline]] void exec_yield(Instruction instr, MatchArena &arena, std::vector<PatternMatch> &results);

  [[gnu::always_inline]] void exec_mark_seen(Instruction instr) { state_.mark_seen(instr.arg); }

  EGraphType const *egraph_;
  std::span<Instruction const> code_;
  std::span<Symbol const> symbols_;
  VMState state_;
  std::span<EClassId const> all_eclasses_;   // Span into e-graph's canonical e-class list (for IterAllEClasses)
  std::vector<EClassId> candidates_buffer_;  // Buffer for automatic candidate lookup

  // Dev mode only: combined stats and tracing (zero overhead when DevMode=false)
  struct NoDevState {};

  [[no_unique_address]] std::conditional_t<DevMode, VMCollector, NoDevState> collector_;
};

template <typename Symbol, typename Analysis, bool DevMode>
void VMExecutor<Symbol, Analysis, DevMode>::execute(CompiledPattern<Symbol> const &pattern,
                                                    MatcherIndex<Symbol, Analysis> &index, MatchArena &arena,
                                                    std::vector<PatternMatch> &results) {
  // Get candidates based on pattern's entry symbol
  candidates_buffer_.clear();
  if (auto entry_sym = pattern.entry_symbol()) {
    index.candidates_for_symbol(*entry_sym, candidates_buffer_);
  } else {
    // Root is variable/wildcard - get all canonical e-classes
    index.all_candidates(candidates_buffer_);
  }

  execute_impl(pattern, candidates_buffer_, arena, results);
}

template <typename Symbol, typename Analysis, bool DevMode>
void VMExecutor<Symbol, Analysis, DevMode>::execute_impl(CompiledPattern<Symbol> const &pattern,
                                                         std::span<EClassId const> candidates, MatchArena &arena,
                                                         std::vector<PatternMatch> &results) {
  // Pattern with no slots has nothing to bind - skip execution
  if (pattern.num_slots() == 0) return;

  state_.reset(pattern.state_config());
  if constexpr (DevMode) {
    collector_.reset();
  }
  code_ = pattern.code();
  symbols_ = pattern.symbols();

  // E-graph maintains canonical e-class list directly - used by IterAllEClasses
  all_eclasses_ = egraph_->canonical_eclass_ids();

  for (auto candidate : candidates) {
    assert(egraph_->find(candidate) == candidate && "candidates must be canonical");
    state_.eclass_regs[0] = candidate;
    state_.pc = 0;

    run_until_halt(arena, results);
  }
}

template <typename Symbol, typename Analysis, bool DevMode>
void VMExecutor<Symbol, Analysis, DevMode>::run_until_halt(MatchArena &arena, std::vector<PatternMatch> &results) {
  // Dispatch table - must match VMOp enum order exactly
  static constexpr void *dispatch_table[] = {
      &&op_LoadChild,
      &&op_GetENodeEClass,
      &&op_IterENodes,
      &&op_NextENode,
      &&op_IterAllEClasses,
      &&op_NextEClass,
      &&op_IterParents,
      &&op_NextParent,
      &&op_CheckSymbol,
      &&op_CheckArity,
      &&op_BindSlot,
      &&op_CheckSlot,
      &&op_CheckEClassEq,
      &&op_MarkSeen,
      &&op_Jump,
      &&op_Yield,
      &&op_Halt,
  };

  // Cache current instruction - fetched once per dispatch, used by all handlers
  Instruction instr;

  // clang-format off
#define DISPATCH()                                                      \
  do {                                                                  \
    if (state_.pc >= code_.size()) return; /*DMG_ASSERT*/               \
    instr = code_[state_.pc];                                           \
    if constexpr (DevMode) {                                            \
      collector_.on_instruction(state_.pc, instr);                      \
    }                                                                   \
    goto *dispatch_table[static_cast<uint8_t>(instr.op)];               \
  } while (0)

#define NEXT() do { ++state_.pc; DISPATCH(); } while (0)
#define JUMP(target) do { state_.pc = (target); DISPATCH(); } while (0)
#define NEXT_OR_JUMP(condition) \
  do { if (condition) { NEXT(); } else { JUMP(instr.target); } } while (0)
  // clang-format on

  DISPATCH();

op_LoadChild:
  exec_load_child(instr);
  NEXT();

op_GetENodeEClass:
  exec_get_enode_eclass(instr);
  NEXT();

op_IterENodes:
  exec_iter_enodes(instr);
  NEXT();

op_NextENode:
  NEXT_OR_JUMP(exec_next_enode(instr));

op_IterAllEClasses:
  NEXT_OR_JUMP(exec_iter_all_eclasses(instr));

op_NextEClass:
  NEXT_OR_JUMP(exec_next_eclass(instr));

op_IterParents:
  NEXT_OR_JUMP(exec_iter_parents(instr));

op_NextParent:
  NEXT_OR_JUMP(exec_next_parent(instr));

op_CheckSymbol:
  NEXT_OR_JUMP(exec_check_symbol(instr));

op_CheckArity:
  NEXT_OR_JUMP(exec_check_arity(instr));

op_BindSlot:
  NEXT_OR_JUMP(exec_bind_slot(instr));

op_CheckSlot:
  NEXT_OR_JUMP(exec_check_slot(instr));

op_CheckEClassEq:
  NEXT_OR_JUMP(exec_check_eclass_eq(instr));

op_MarkSeen:
  exec_mark_seen(instr);
  NEXT();

op_Jump:
  JUMP(instr.target);

op_Yield:
  exec_yield(instr, arena, results);
  NEXT();

op_Halt:
  if constexpr (DevMode) {
    collector_.on_halt();
  }
  return;

#undef DISPATCH
#undef NEXT
#undef JUMP
#undef NEXT_OR_JUMP
}

template <typename Symbol, typename Analysis, bool DevMode>
void VMExecutor<Symbol, Analysis, DevMode>::exec_iter_enodes(Instruction instr) {
  auto eclass_id = state_.eclass_regs[instr.src];
  auto nodes = egraph_->eclass(eclass_id).nodes();

  if constexpr (DevMode) {
    collector_.on_iter_enode_start(state_.pc, nodes.size());
  }

  // Note: No empty check - e-classes always have at least one e-node
  state_.start_enode_iter(instr.dst, nodes);
  state_.enode_regs[instr.dst] = nodes[0];
}

template <typename Symbol, typename Analysis, bool DevMode>
template <typename Iter, typename IdType>
auto VMExecutor<Symbol, Analysis, DevMode>::advance_span_iter(Iter &iter, IdType &out_reg) -> bool {
  iter.advance();
  if (iter.exhausted()) {
    if constexpr (DevMode) {
      collector_.on_iter_advance(state_.pc, 0);
    }
    return false;
  }
  if constexpr (DevMode) {
    collector_.on_iter_advance(state_.pc, iter.remaining());
  }
  out_reg = iter.current();
  return true;
}

template <typename Symbol, typename Analysis, bool DevMode>
auto VMExecutor<Symbol, Analysis, DevMode>::exec_iter_all_eclasses(Instruction instr) -> bool {
  // E-graph maintains canonical e-class list - set in execute_impl()
  if constexpr (DevMode) {
    collector_.on_iter_all_eclasses_start(state_.pc, all_eclasses_.size());
  }

  if (all_eclasses_.empty()) [[unlikely]] {
    return false;
  }

  state_.start_all_eclasses_iter(instr.dst, all_eclasses_);
  state_.eclass_regs[instr.dst] = all_eclasses_[0];
  return true;
}

template <typename Symbol, typename Analysis, bool DevMode>
auto VMExecutor<Symbol, Analysis, DevMode>::exec_iter_parents(Instruction instr) -> bool {
  auto eclass_id = state_.eclass_regs[instr.src];
  auto const &parents = egraph_->eclass(eclass_id).parents();

  if constexpr (DevMode) {
    collector_.on_iter_parent_start(state_.pc, parents.size());
  }

  if (parents.empty()) {
    return false;
  }

  state_.start_parent_iter(instr.dst, eclass_id, parents.size());
  state_.enode_regs[instr.dst] = *parents.begin();
  return true;
}

template <typename Symbol, typename Analysis, bool DevMode>
auto VMExecutor<Symbol, Analysis, DevMode>::exec_next_parent(Instruction instr) -> bool {
  auto &iter = state_.get_parents_iter(instr.dst);
  iter.advance();
  if (iter.exhausted()) {
    if constexpr (DevMode) {
      collector_.on_iter_advance(state_.pc, 0);
    }
    return false;
  }
  if constexpr (DevMode) {
    collector_.on_iter_advance(state_.pc, iter.remaining());
  }
  // Index-based iteration requires looking up the e-class again
  auto pit = egraph_->eclass(iter.eclass).parents().begin();
  std::advance(pit, static_cast<std::ptrdiff_t>(iter.index()));
  state_.enode_regs[instr.dst] = *pit;
  return true;
}

template <typename Symbol, typename Analysis, bool DevMode>
auto VMExecutor<Symbol, Analysis, DevMode>::exec_check_symbol(Instruction instr) -> bool {
  auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
  if (enode.symbol() != symbols_[instr.arg]) {
    if constexpr (DevMode) {
      collector_.on_check_symbol_miss(state_.pc);
    }
    return false;
  }
  if constexpr (DevMode) {
    collector_.on_check_symbol_hit();
  }
  return true;
}

template <typename Symbol, typename Analysis, bool DevMode>
auto VMExecutor<Symbol, Analysis, DevMode>::exec_check_arity(Instruction instr) -> bool {
  auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
  if (enode.arity() != instr.arg) {
    if constexpr (DevMode) {
      collector_.on_check_arity_fail(state_.pc);
    }
    return false;
  }
  return true;
}

template <typename Symbol, typename Analysis, bool DevMode>
auto VMExecutor<Symbol, Analysis, DevMode>::exec_bind_slot(Instruction instr) -> bool {
  auto eclass = state_.eclass_regs[instr.src];
  bool is_new = state_.try_bind(instr.arg, eclass);
  if constexpr (DevMode) {
    collector_.on_bind(instr.arg, eclass);
    if (!is_new) {
      collector_.on_bind_duplicate(state_.pc);
    }
  }
  return is_new;
}

template <typename Symbol, typename Analysis, bool DevMode>
auto VMExecutor<Symbol, Analysis, DevMode>::exec_check_slot(Instruction instr) -> bool {
  // Both values are already canonical:
  // - slots[] was set by BindSlot from canonical eclass_regs
  // - eclass_regs[] was set by LoadChild which canonicalizes
  if (state_.slots[instr.arg] != state_.eclass_regs[instr.src]) {
    if constexpr (DevMode) {
      collector_.on_check_slot_miss(state_.pc);
    }
    return false;
  }
  if constexpr (DevMode) {
    collector_.on_check_slot_hit();
  }
  return true;
}

template <typename Symbol, typename Analysis, bool DevMode>
auto VMExecutor<Symbol, Analysis, DevMode>::exec_check_eclass_eq(Instruction instr) -> bool {
  // Compare two e-class registers directly (both already canonical)
  if (state_.eclass_regs[instr.dst] != state_.eclass_regs[instr.src]) {
    if constexpr (DevMode) {
      collector_.on_check_eclass_eq_fail(state_.pc);
    }
    return false;
  }
  return true;
}

template <typename Symbol, typename Analysis, bool DevMode>
void VMExecutor<Symbol, Analysis, DevMode>::exec_yield(Instruction instr, MatchArena &arena,
                                                       std::vector<PatternMatch> &results) {
  // Mark last slot as seen (Yield is a special MarkSeen + emit)
  state_.mark_seen(instr.arg);

  if constexpr (DevMode) {
    collector_.on_yield(state_.slots);
  }
  results.push_back(arena.intern(state_.slots));
}

}  // namespace memgraph::planner::core::pattern::vm
