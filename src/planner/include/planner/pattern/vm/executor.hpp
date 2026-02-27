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

#include <algorithm>
#include <optional>
#include <span>
#include <type_traits>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "planner/egraph/egraph.hpp"
#include "planner/pattern/match.hpp"
#include "planner/pattern/matcher.hpp"
#include "planner/pattern/vm/instruction.hpp"
#include "planner/pattern/vm/state.hpp"
#include "planner/pattern/vm/tracer.hpp"

namespace memgraph::planner::core::vm {

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
//   - slots[] always contain canonical IDs after BindSlotDedup
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
//   1. BindSlotDedup checks if value is in seen_per_slot[slot]
//   2. If seen, backtrack (don't explore same value twice)
//   3. Yield/MarkSeen adds value to seen_per_slot[slot]
//   4. When slot i changes, clear seen_per_slot for slots bound after i in binding_order
//
// This ensures each unique binding tuple is yielded exactly once.
//
// =============================================================================

/// Compiled pattern ready for VM execution.
///
/// Contains the bytecode, symbol table, and metadata needed by VMExecutor.
/// Produced by PatternCompiler, consumed by VMExecutor.
///
/// ## Contract
///
/// CompiledPattern encapsulates the contract between PatternCompiler and VMExecutor:
///
/// - code(): The bytecode sequence; all jump targets are valid
/// - num_slots(): Number of result slots (variables)
/// - num_eclass_regs(): E-class registers needed (≥1, reg 0 is input)
/// - num_enode_regs(): E-node registers needed
/// - symbols(): Symbol table for CheckSymbol instructions
/// - entry_symbol(): Root symbol for candidate lookup (nullopt if variable root)
/// - binding_order(): Order slots are bound (for deduplication)
/// - slot_to_order(): Inverse mapping for efficient clearing
///
/// @tparam Symbol The symbol type (must match PatternCompiler and VMExecutor)
///
/// @see PatternCompiler for bytecode generation
/// @see VMExecutor for bytecode execution
/// @see Instruction for bytecode format
template <typename Symbol>
class CompiledPattern {
 public:
  CompiledPattern(std::vector<Instruction> code, std::size_t num_slots, std::size_t num_eclass_regs,
                  std::size_t num_enode_regs, std::vector<Symbol> symbols, std::optional<Symbol> entry_symbol,
                  std::vector<uint8_t> binding_order)
      : code_(std::move(code)),
        num_slots_(num_slots),
        num_eclass_regs_(num_eclass_regs),
        num_enode_regs_(num_enode_regs),
        symbols_(std::move(symbols)),
        entry_symbol_(std::move(entry_symbol)),
        binding_order_(std::move(binding_order)) {
    // Compute inverse mapping: slot -> order position
    // For binding_order [1, 2, 0], slot_to_order_ becomes [2, 0, 1]
    // meaning slot 0 is bound at position 2, slot 1 at position 0, slot 2 at position 1
    slot_to_order_.resize(num_slots_);
    for (std::size_t order_idx = 0; order_idx < binding_order_.size(); ++order_idx) {
      slot_to_order_[binding_order_[order_idx]] = static_cast<uint8_t>(order_idx);
    }
  }

  [[nodiscard]] auto code() const -> std::span<Instruction const> { return code_; }

  [[nodiscard]] auto num_slots() const -> std::size_t { return num_slots_; }

  [[nodiscard]] auto num_eclass_regs() const -> std::size_t { return num_eclass_regs_; }

  [[nodiscard]] auto num_enode_regs() const -> std::size_t { return num_enode_regs_; }

  [[nodiscard]] auto symbols() const -> std::span<Symbol const> { return symbols_; }

  [[nodiscard]] auto entry_symbol() const -> std::optional<Symbol> const & { return entry_symbol_; }

  /// The order in which slots are bound during pattern matching.
  /// For pattern A(?x, B(?y, ?z)) compiled as B-first, this might be [1, 2, 0]
  /// meaning ?y (slot 1) is bound first, then ?z (slot 2), then ?x (slot 0).
  [[nodiscard]] auto binding_order() const -> std::span<uint8_t const> { return binding_order_; }

  /// Inverse mapping: slot index -> order position.
  /// For binding_order [1, 2, 0], slot_to_order returns [2, 0, 1].
  /// Used by deduplication: when slot s changes, clear seen sets for all slots
  /// at positions > slot_to_order[s] in binding_order.
  [[nodiscard]] auto slot_to_order() const -> std::span<uint8_t const> { return slot_to_order_; }

  /// Get VMState configuration for this pattern
  [[nodiscard]] auto state_config() const -> VMStateConfig {
    return {.num_eclass_regs = num_eclass_regs_,
            .num_enode_regs = num_enode_regs_,
            .binding_order = binding_order_,
            .slot_to_order = slot_to_order_};
  }

 private:
  std::vector<Instruction> code_;
  std::size_t num_slots_;
  std::size_t num_eclass_regs_;         // Registers holding e-class IDs
  std::size_t num_enode_regs_;          // Registers holding e-node IDs (and iteration state)
  std::vector<Symbol> symbols_;         // Symbol table for CheckSymbol (deduplicated by compiler)
  std::optional<Symbol> entry_symbol_;  // For index-based candidate lookup
  std::vector<uint8_t> binding_order_;  // Order in which slots are bound during matching
  std::vector<uint8_t> slot_to_order_;  // Inverse: slot index -> order position (O(n) vs O(n²))
};

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

  explicit VMExecutor(EGraphType const &egraph, VMTracer *tracer = nullptr) : egraph_(&egraph) {
    if constexpr (DevMode) {
      collector_.set_tracer(tracer);
    }
  }

  /// Execute compiled pattern with automatic candidate lookup via EMatcher.
  ///
  /// Uses the pattern's entry_symbol() to look up candidates from the matcher's
  /// symbol index. If the pattern has a variable/wildcard root, falls back to
  /// iterating all e-classes.
  ///
  /// ## Contract
  ///
  /// @pre pattern was produced by PatternCompiler (satisfies all bytecode contracts)
  /// @pre matcher's index is up-to-date with the e-graph
  /// @pre ctx.arena() is valid for storing match bindings
  ///
  /// @post results contains all unique matches found
  /// @post Stale index entries (merged e-classes) are handled via canonicalization
  ///
  /// @param pattern The compiled bytecode to execute
  /// @param matcher EMatcher with symbol index for candidate lookup
  /// @param ctx Match context with arena for storing bindings
  /// @param results Output vector for matches (appended, not cleared)
  void execute(CompiledPattern<Symbol> const &pattern, EMatcher<Symbol, Analysis> &matcher, EMatchContext &ctx,
               std::vector<PatternMatch> &results) {
    // Get candidates based on pattern's entry symbol
    candidates_buffer_.clear();
    if (auto entry_sym = pattern.entry_symbol()) {
      matcher.candidates_for_symbol(*entry_sym, candidates_buffer_);
    } else {
      // Root is variable/wildcard - get all e-classes
      matcher.all_candidates(candidates_buffer_);
    }

    // Canonicalize and deduplicate candidates.
    // The matcher index may have stale entries pointing to merged-away e-classes.
    // We must canonicalize before passing to the VM, which assumes canonical IDs.
    for (auto &cand : candidates_buffer_) {
      cand = egraph_->find(cand);
    }
    // Remove duplicates (stale entries that now point to the same canonical e-class)
    std::sort(candidates_buffer_.begin(), candidates_buffer_.end());
    candidates_buffer_.erase(std::unique(candidates_buffer_.begin(), candidates_buffer_.end()),
                             candidates_buffer_.end());

    execute_impl(pattern, candidates_buffer_, ctx, results);
  }

  /// Get execution stats (only meaningful when DevMode=true)
  [[nodiscard]] auto stats() const -> VMStats const & {
    static_assert(DevMode, "stats() requires DevMode=true");
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
  void execute_impl(CompiledPattern<Symbol> const &pattern, std::span<EClassId const> candidates, EMatchContext &ctx,
                    std::vector<PatternMatch> &results) {
    state_.reset(pattern.state_config());
    if constexpr (DevMode) {
      collector_.reset();
    }
    code_ = pattern.code();
    symbols_ = pattern.symbols();

    // E-graph maintains canonical e-class list directly - used by IterAllEClasses
    all_eclasses_ = egraph_->canonical_eclass_ids();

    for (auto candidate : candidates) {
      DMG_ASSERT(egraph_->find(candidate) == candidate, "candidates must be canonical");
      state_.eclass_regs[0] = candidate;
      state_.pc = 0;

      run_until_halt(ctx, results);
    }
  }

  void run_until_halt(EMatchContext &ctx, std::vector<PatternMatch> &results) {
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
        &&op_BindSlotDedup,
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
    NEXT_OR_JUMP(exec_iter_enodes(instr));

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

  op_BindSlotDedup:
    NEXT_OR_JUMP(exec_bind_slot_dedup(instr));

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
    exec_yield(instr, ctx, results);
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

  [[nodiscard]] [[gnu::always_inline]] auto exec_iter_enodes(Instruction instr) -> bool {
    auto eclass_id = state_.eclass_regs[instr.src];
    auto nodes = egraph_->eclass(eclass_id).nodes();

    if constexpr (DevMode) {
      collector_.on_iter_enode_start(state_.pc, nodes.size());
    }

    if (nodes.empty()) {
      return false;
    }

    state_.start_enode_iter(instr.dst, nodes);
    state_.enode_regs[instr.dst] = nodes[0];
    return true;
  }

  /// Common helper for advancing span-based iterators (ENodesIter, AllEClassesIter)
  template <typename Iter, typename IdType>
  [[nodiscard]] [[gnu::always_inline]] auto advance_span_iter(Iter &iter, IdType &out_reg) -> bool {
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

  [[nodiscard]] [[gnu::always_inline]] auto exec_next_enode(Instruction instr) -> bool {
    return advance_span_iter(state_.get_enodes_iter(instr.dst), state_.enode_regs[instr.dst]);
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_iter_all_eclasses(Instruction instr) -> bool {
    // E-graph maintains canonical e-class list - set in execute_impl()
    if (all_eclasses_.empty()) [[unlikely]] {
      return false;
    }

    state_.start_all_eclasses_iter(instr.dst, all_eclasses_);
    state_.eclass_regs[instr.dst] = all_eclasses_[0];
    return true;
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_next_eclass(Instruction instr) -> bool {
    return advance_span_iter(state_.get_eclasses_iter(instr.dst), state_.eclass_regs[instr.dst]);
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_iter_parents(Instruction instr) -> bool {
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

  /// Advance index-based parent iteration (for IterParents)
  [[nodiscard]] [[gnu::always_inline]] auto exec_next_parent(Instruction instr) -> bool {
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

  [[nodiscard]] [[gnu::always_inline]] auto exec_check_symbol(Instruction instr) -> bool {
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

  [[nodiscard]] [[gnu::always_inline]] auto exec_check_arity(Instruction instr) -> bool {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    if (enode.arity() != instr.arg) {
      if constexpr (DevMode) {
        collector_.on_check_arity_fail(state_.pc);
      }
      return false;
    }
    return true;
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_bind_slot_dedup(Instruction instr) -> bool {
    auto eclass = state_.eclass_regs[instr.src];
    bool is_new = state_.try_bind_dedup(instr.arg, eclass);
    if constexpr (DevMode) {
      collector_.on_bind(instr.arg, eclass);
      if (!is_new) {
        collector_.on_bind_duplicate(state_.pc);
      }
    }
    return is_new;
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_check_slot(Instruction instr) -> bool {
    // Both values are already canonical:
    // - slots[] was set by BindSlotDedup from canonical eclass_regs
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

  [[nodiscard]] [[gnu::always_inline]] auto exec_check_eclass_eq(Instruction instr) -> bool {
    // Compare two e-class registers directly (both already canonical)
    return state_.eclass_regs[instr.dst] == state_.eclass_regs[instr.src];
  }

  [[gnu::always_inline]] void exec_yield(Instruction instr, EMatchContext &ctx, std::vector<PatternMatch> &results) {
    // Mark last slot as seen (Yield is a special MarkSeen + emit)
    state_.mark_seen(instr.arg);

    if constexpr (DevMode) {
      collector_.on_yield(state_.slots);
    }
    results.push_back(ctx.arena().intern(state_.slots));
  }

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

// Backwards compatibility alias - VMExecutorVerify is now just VMExecutor
template <typename Symbol, typename Analysis, bool DevMode = false>
using VMExecutorVerify = VMExecutor<Symbol, Analysis, DevMode>;

}  // namespace memgraph::planner::core::vm
