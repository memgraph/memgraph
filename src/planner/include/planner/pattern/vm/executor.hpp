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

/// Compiled pattern ready for VM execution
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
    // Precompute slots_bound_after for each slot (used by deduplication)
    // slots_bound_after[i] contains the slots that are bound AFTER slot i in binding order
    slots_bound_after_.resize(num_slots_);
    for (std::size_t order_idx = 0; order_idx < binding_order_.size(); ++order_idx) {
      auto slot = binding_order_[order_idx];
      // All slots after this one in binding order should be cleared when this slot changes
      for (std::size_t j = order_idx + 1; j < binding_order_.size(); ++j) {
        slots_bound_after_[slot].push_back(binding_order_[j]);
      }
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

  /// For each slot, the list of slots that are bound AFTER it in binding order.
  /// Used by deduplication: when slot i changes, clear seen_per_slot[j] for each j in slots_bound_after[i].
  [[nodiscard]] auto slots_bound_after(std::size_t slot) const -> std::span<uint8_t const> {
    return slots_bound_after_[slot];
  }

 private:
  std::vector<Instruction> code_;
  std::size_t num_slots_;
  std::size_t num_eclass_regs_;                          // Registers holding e-class IDs
  std::size_t num_enode_regs_;                           // Registers holding e-node IDs (and iteration state)
  std::vector<Symbol> symbols_;                          // Symbol table for CheckSymbol (deduplicated by compiler)
  std::optional<Symbol> entry_symbol_;                   // For index-based candidate lookup
  std::vector<uint8_t> binding_order_;                   // Order in which slots are bound during matching
  std::vector<std::vector<uint8_t>> slots_bound_after_;  // Precomputed: slots to clear when slot i changes
};

/// VM executor for pattern matching
///
/// Executes compiled patterns against an e-graph, collecting matches.
///
/// @tparam DevMode Enable stats collection and tracing (for debugging/profiling)
template <typename Symbol, typename Analysis, bool DevMode = false>
class VMExecutor {
 public:
  using EGraphType = EGraph<Symbol, Analysis>;

  explicit VMExecutor(EGraphType const &egraph, VMTracer *tracer = nullptr) : egraph_(&egraph) {
    if constexpr (DevMode) {
      tracer_ = tracer ? tracer : &null_tracer_;
    }
  }

  /// Execute compiled pattern, collecting matches.
  /// @pre candidates must contain canonical e-class IDs (use egraph.find() if unsure)
  void execute(CompiledPattern<Symbol> const &pattern, std::span<EClassId const> candidates, EMatchContext &ctx,
               std::vector<PatternMatch> &results) {
    state_.reset(
        pattern.num_slots(), pattern.num_eclass_regs(), pattern.num_enode_regs(), [&pattern](std::size_t slot) {
          return pattern.slots_bound_after(slot);
        });
    if constexpr (DevMode) {
      stats_.reset();
    }
    code_ = pattern.code();
    symbols_ = pattern.symbols();

    // Pre-cache all e-classes for IterAllEClasses instruction.
    // Future optimization: only do this if pattern contains IterAllEClasses.
    all_eclasses_buffer_.clear();
    for (auto const &[id, _] : egraph_->canonical_classes()) {
      all_eclasses_buffer_.push_back(id);
    }

    for (auto candidate : candidates) {
      DMG_ASSERT(egraph_->find(candidate) == candidate, "candidates must be canonical");
      state_.eclass_regs[0] = candidate;
      state_.pc = 0;

      run_until_halt(ctx, results);
    }
  }

  /// Execute compiled pattern with automatic candidate lookup via EMatcher
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

    execute(pattern, candidates_buffer_, ctx, results);
  }

  /// Get execution stats (only meaningful when DevMode=true)
  [[nodiscard]] auto stats() const -> VMStats const & {
    static_assert(DevMode, "stats() requires DevMode=true");
    return stats_;
  }

  /// Set tracer for debugging (only available when DevMode=true)
  void set_tracer(VMTracer *tracer)
    requires(DevMode)
  {
    tracer_ = tracer ? tracer : &null_tracer_;
  }

 private:
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
        &&op_MarkSeen,
        &&op_Jump,
        &&op_Yield,
        &&op_Halt,
    };

    // clang-format off
#define DISPATCH()                                                      \
  do {                                                                  \
    if (state_.pc >= code_.size()) return; /*DMG_ASSERT*/               \
    if constexpr (DevMode) {                                    \
      tracer_->on_instruction(state_.pc, code_[state_.pc]);             \
      ++stats_.instructions_executed;                                   \
    }                                                                   \
    goto *dispatch_table[static_cast<uint8_t>(code_[state_.pc].op)];    \
  } while (0)

#define NEXT() do { ++state_.pc; DISPATCH(); } while (0)
#define JUMP(target) do { state_.pc = (target); DISPATCH(); } while (0)
    // clang-format on

    DISPATCH();

  op_LoadChild:
    exec_load_child(code_[state_.pc]);
    NEXT();

  op_GetENodeEClass:
    exec_get_enode_eclass(code_[state_.pc]);
    NEXT();

  op_IterENodes:
    if (exec_iter_enodes(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_NextENode:
    if (exec_next_enode(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_IterAllEClasses:
    if (exec_iter_all_eclasses(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_NextEClass:
    // TODO: can we have a better macro that will NEXT_OR_JUMP
    if (exec_next_eclass(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_IterParents:
    if (exec_iter_parents(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_NextParent:
    if (exec_next_parent(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_CheckSymbol:
    if (exec_check_symbol(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_CheckArity:
    if (exec_check_arity(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_BindSlotDedup:
    if (exec_bind_slot_dedup(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_CheckSlot:
    if (exec_check_slot(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_MarkSeen:
    exec_mark_seen(code_[state_.pc]);
    NEXT();

  op_Jump:
    JUMP(code_[state_.pc].target);

  op_Yield:
    exec_yield(code_[state_.pc], ctx, results);
    NEXT();

  op_Halt:
    if constexpr (DevMode) {
      tracer_->on_halt(stats_.instructions_executed);
    }
    return;

#undef DISPATCH
#undef NEXT
#undef JUMP
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
    if constexpr (DevMode) {
      ++stats_.iter_enode_calls;
    }
    auto eclass_id = state_.eclass_regs[instr.src];
    auto nodes = egraph_->eclass(eclass_id).nodes();

    if constexpr (DevMode) {
      tracer_->on_iter_start(state_.pc, nodes.size());
    }

    if (nodes.empty()) {
      return false;
    }

    state_.start_enode_iter(instr.dst, nodes);
    state_.enode_regs[instr.dst] = nodes[0];
    return true;
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_next_enode(Instruction instr) -> bool {
    auto &iter = state_.get_enodes_iter(instr.dst);
    iter.advance();
    if (iter.exhausted()) {
      if constexpr (DevMode) {
        tracer_->on_iter_advance(state_.pc, 0);
      }
      return false;
    }

    if constexpr (DevMode) {
      tracer_->on_iter_advance(state_.pc, iter.remaining());
    }
    state_.enode_regs[instr.dst] = iter.current();
    return true;
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_iter_all_eclasses(Instruction instr) -> bool {
    // Buffer is pre-cached in execute() - no need to rebuild here
    if (all_eclasses_buffer_.empty()) [[unlikely]] {
      return false;
    }

    state_.start_all_eclasses_iter(instr.dst, all_eclasses_buffer_);
    state_.eclass_regs[instr.dst] = all_eclasses_buffer_[0];
    return true;
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_next_eclass(Instruction instr) -> bool {
    // TODO: This is a common pattern for all iterators, can we have a common helper?
    //       get iter X, advance, exhausted, set X with current
    auto &iter = state_.get_eclasses_iter(instr.dst);
    iter.advance();
    if (iter.exhausted()) {
      return false;
    }

    state_.eclass_regs[instr.dst] = iter.current();
    return true;
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_iter_parents(Instruction instr) -> bool {
    if constexpr (DevMode) {
      ++stats_.iter_parent_calls;
    }
    auto eclass_id = state_.eclass_regs[instr.src];
    auto const &parents = egraph_->eclass(eclass_id).parents();

    if constexpr (DevMode) {
      tracer_->on_iter_start(state_.pc, parents.size());
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
        tracer_->on_iter_advance(state_.pc, 0);
      }
      return false;
    }
    if constexpr (DevMode) {
      tracer_->on_iter_advance(state_.pc, iter.remaining());
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
        ++stats_.parent_symbol_misses;
        tracer_->on_check_fail(state_.pc, "symbol mismatch");
      }
      return false;
    }
    if constexpr (DevMode) {
      ++stats_.parent_symbol_hits;
    }
    return true;
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_check_arity(Instruction instr) -> bool {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    if (enode.arity() != instr.arg) {
      if constexpr (DevMode) {
        tracer_->on_check_fail(state_.pc, "arity mismatch");
      }
      return false;
    }
    return true;
  }

  [[nodiscard]] [[gnu::always_inline]] auto exec_bind_slot_dedup(Instruction instr) -> bool {
    auto eclass = state_.eclass_regs[instr.src];
    bool is_new = state_.try_bind_dedup(instr.arg, eclass);
    if constexpr (DevMode) {
      tracer_->on_bind(instr.arg, eclass);
      if (!is_new) {
        tracer_->on_check_fail(state_.pc, "duplicate binding");
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
        ++stats_.check_slot_misses;
        tracer_->on_check_fail(state_.pc, "slot mismatch");
      }
      return false;
    }
    if constexpr (DevMode) {
      ++stats_.check_slot_hits;
    }
    return true;
  }

  [[gnu::always_inline]] void exec_yield(Instruction instr, EMatchContext &ctx, std::vector<PatternMatch> &results) {
    // Mark last slot as seen (Yield is a special MarkSeen + emit)
    state_.mark_seen(instr.arg);

    if constexpr (DevMode) {
      ++stats_.yields;
      tracer_->on_yield(state_.slots);
    }
    results.push_back(ctx.arena().intern(state_.slots));
  }

  [[gnu::always_inline]] void exec_mark_seen(Instruction instr) { state_.mark_seen(instr.arg); }

  EGraphType const *egraph_;
  std::span<Instruction const> code_;
  std::span<Symbol const> symbols_;
  VMState state_;
  std::vector<EClassId> all_eclasses_buffer_;  // Buffer for IterAllEClasses (pre-cached in execute())
  std::vector<EClassId> candidates_buffer_;    // Buffer for automatic candidate lookup

  // Dev mode only: stats and tracing (zero overhead when DevMode=false)
  struct NoDevState {};

  [[no_unique_address]] std::conditional_t<DevMode, VMStats, NoDevState> stats_;
  [[no_unique_address]] std::conditional_t<DevMode, NullTracer, NoDevState> null_tracer_;
  [[no_unique_address]] std::conditional_t<DevMode, VMTracer *, NoDevState> tracer_;
};

// Backwards compatibility alias - VMExecutorVerify is now just VMExecutor
template <typename Symbol, typename Analysis, bool DevMode = false>
using VMExecutorVerify = VMExecutor<Symbol, Analysis, DevMode>;

}  // namespace memgraph::planner::core::vm
