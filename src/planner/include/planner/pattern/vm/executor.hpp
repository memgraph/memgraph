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

#include <optional>
#include <span>
#include <type_traits>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "planner/egraph/egraph.hpp"
#include "planner/pattern/match.hpp"
#include "planner/pattern/matcher.hpp"
#include "planner/pattern/vm/instruction.hpp"
#include "planner/pattern/vm/parent_index.hpp"
#include "planner/pattern/vm/state.hpp"
#include "planner/pattern/vm/tracer.hpp"

namespace memgraph::planner::core::vm {

/// Compiled pattern ready for VM execution
template <typename Symbol>
class CompiledPattern {
 public:
  CompiledPattern(std::vector<Instruction> code, std::size_t num_slots, std::size_t num_registers,
                  std::vector<Symbol> symbols, std::optional<Symbol> entry_symbol)
      : code_(std::move(code)),
        num_slots_(num_slots),
        num_registers_(num_registers),
        symbols_(std::move(symbols)),
        entry_symbol_(std::move(entry_symbol)) {}

  [[nodiscard]] auto code() const -> std::span<Instruction const> { return code_; }

  [[nodiscard]] auto num_slots() const -> std::size_t { return num_slots_; }

  [[nodiscard]] auto num_registers() const -> std::size_t { return num_registers_; }

  [[nodiscard]] auto symbols() const -> std::span<Symbol const> { return symbols_; }

  [[nodiscard]] auto entry_symbol() const -> std::optional<Symbol> const & { return entry_symbol_; }

 private:
  std::vector<Instruction> code_;
  std::size_t num_slots_;
  std::size_t num_registers_;
  std::vector<Symbol> symbols_;         // Symbol table for CheckSymbol/IterParentsSym
  std::optional<Symbol> entry_symbol_;  // For index-based candidate lookup
};

/// VM executor for pattern matching - "verify" mode
///
/// This executor always verifies child consistency when traversing parents.
/// Safe but slower - does not require a clean parent index.
template <typename Symbol, typename Analysis, typename Tracer = NullTracer>
class VMExecutorVerify {
 public:
  using EGraphType = EGraph<Symbol, Analysis>;

  explicit VMExecutorVerify(EGraphType const &egraph, Tracer *tracer = nullptr) : egraph_(&egraph) {
    if (tracer) {
      tracer_ = tracer;
    } else {
      tracer_ = &null_tracer_;
    }
  }

  /// Execute compiled pattern, collecting matches
  void execute(CompiledPattern<Symbol> const &pattern, std::span<EClassId const> candidates, EMatchContext &ctx,
               std::vector<PatternMatch> &results) {
    state_.reset(pattern.num_slots(), pattern.num_registers());
    stats_.reset();
    code_ = pattern.code();
    symbols_ = pattern.symbols();

    for (auto candidate : candidates) {
      auto canonical = egraph_->find(candidate);
      state_.eclass_regs[0] = canonical;
      state_.pc = 0;
      state_.iter_order.clear();
      for (auto &iter : state_.iter_by_reg) {
        iter.reset();
      }
      state_.bound.reset();

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

    execute(pattern, candidates_buffer_, ctx, results);
  }

  [[nodiscard]] auto stats() const -> VMStats const & { return stats_; }

  /// Set tracer for debugging
  void set_tracer(Tracer *tracer) { tracer_ = tracer ? tracer : &null_tracer_; }

 private:
  // Compile-time constant for whether tracing is enabled
  static constexpr bool kTracingEnabled = !std::is_same_v<Tracer, NullTracer>;

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
        &&op_IterParentsSym,
        &&op_NextParent,
        &&op_CheckSymbol,
        &&op_CheckArity,
        &&op_BindSlot,
        &&op_CheckSlot,
        &&op_BindOrCheck,
        &&op_Jump,
        &&op_Yield,
        &&op_Halt,
    };

    // clang-format off
#define DISPATCH()                                                      \
  do {                                                                  \
    if (state_.pc >= code_.size()) return;                              \
    if constexpr (kTracingEnabled) {                                    \
      tracer_->on_instruction(state_.pc, code_[state_.pc]);             \
    }                                                                   \
    ++stats_.instructions_executed;                                     \
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

  op_IterParentsSym:
    // In verify mode, fall back to regular parent iteration
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

  op_BindSlot:
    exec_bind_slot(code_[state_.pc]);
    NEXT();

  op_CheckSlot:
    if (exec_check_slot(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_BindOrCheck:
    if (exec_bind_or_check(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_Jump:
    JUMP(code_[state_.pc].target);

  op_Yield:
    exec_yield(ctx, results);
    NEXT();

  op_Halt:
    if constexpr (kTracingEnabled) {
      tracer_->on_halt(stats_.instructions_executed);
    }
    return;

#undef DISPATCH
#undef NEXT
#undef JUMP
  }

  void exec_load_child(Instruction const &instr) {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    state_.eclass_regs[instr.dst] = egraph_->find(enode.children()[instr.arg]);
  }

  void exec_get_enode_eclass(Instruction const &instr) {
    auto enode_id = state_.enode_regs[instr.src];
    state_.eclass_regs[instr.dst] = egraph_->find(enode_id);
  }

  [[nodiscard]] auto exec_iter_enodes(Instruction const &instr) -> bool {
    ++stats_.iter_enode_calls;
    auto eclass_id = state_.eclass_regs[instr.src];
    auto const &eclass = egraph_->eclass(eclass_id);
    auto nodes = eclass.nodes();

    if constexpr (kTracingEnabled) {
      tracer_->on_iter_start(state_.pc, nodes.size());
    }

    if (nodes.empty()) {
      return false;
    }

    state_.start_enode_iter(instr.dst, nodes);
    state_.enode_regs[instr.dst] = nodes[0];
    return true;
  }

  [[nodiscard]] auto exec_next_enode(Instruction const &instr) -> bool {
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::ENodes) {
      if constexpr (kTracingEnabled) {
        tracer_->on_iter_advance(state_.pc, 0);
      }
      return false;
    }

    iter.advance();
    if (iter.exhausted()) {
      if constexpr (kTracingEnabled) {
        tracer_->on_iter_advance(state_.pc, 0);
      }
      state_.deactivate_iter_and_nested(instr.dst);
      return false;
    }

    if constexpr (kTracingEnabled) {
      tracer_->on_iter_advance(state_.pc, iter.remaining());
    }
    state_.enode_regs[instr.dst] = iter.current();
    return true;
  }

  [[nodiscard]] auto exec_iter_all_eclasses(Instruction const &instr) -> bool {
    all_eclasses_buffer_.clear();
    for (auto const &[id, _] : egraph_->canonical_classes()) {
      all_eclasses_buffer_.push_back(id);
    }

    if (all_eclasses_buffer_.empty()) {
      return false;
    }

    state_.start_all_eclasses_iter(instr.dst, all_eclasses_buffer_);
    state_.eclass_regs[instr.dst] = all_eclasses_buffer_[0];
    return true;
  }

  [[nodiscard]] auto exec_next_eclass(Instruction const &instr) -> bool {
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::AllEClasses) {
      return false;
    }

    iter.advance();
    if (iter.exhausted()) {
      state_.deactivate_iter_and_nested(instr.dst);
      return false;
    }

    state_.eclass_regs[instr.dst] = iter.current_eclass();
    return true;
  }

  [[nodiscard]] auto exec_iter_parents(Instruction const &instr) -> bool {
    ++stats_.iter_parent_calls;
    auto eclass_id = state_.eclass_regs[instr.src];
    auto const &eclass = egraph_->eclass(eclass_id);
    auto const &parents = eclass.parents();

    if constexpr (kTracingEnabled) {
      tracer_->on_iter_start(state_.pc, parents.size());
    }

    if (parents.empty()) {
      return false;
    }

    state_.start_parent_iter(instr.dst, eclass_id, parents.size());
    state_.enode_regs[instr.dst] = *parents.begin();
    return true;
  }

  [[nodiscard]] auto exec_next_parent(Instruction const &instr) -> bool {
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::Parents && iter.kind != IterState::Kind::ParentsFiltered) {
      if constexpr (kTracingEnabled) {
        tracer_->on_iter_advance(state_.pc, 0);
      }
      return false;
    }

    iter.advance();
    if (iter.exhausted()) {
      if constexpr (kTracingEnabled) {
        tracer_->on_iter_advance(state_.pc, 0);
      }
      state_.deactivate_iter_and_nested(instr.dst);
      return false;
    }

    if constexpr (kTracingEnabled) {
      tracer_->on_iter_advance(state_.pc, iter.remaining());
    }

    if (iter.kind == IterState::Kind::ParentsFiltered) {
      state_.enode_regs[instr.dst] = iter.nodes_span[iter.current_idx];
    } else {
      auto const &eclass = egraph_->eclass(iter.parent_eclass);
      auto const &parents = eclass.parents();
      auto pit = parents.begin();
      std::advance(pit, static_cast<std::ptrdiff_t>(iter.current_idx));
      state_.enode_regs[instr.dst] = *pit;
    }
    return true;
  }

  [[nodiscard]] auto exec_check_symbol(Instruction const &instr) -> bool {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    if (enode.symbol() != symbols_[instr.arg]) {
      ++stats_.parent_symbol_misses;
      if constexpr (kTracingEnabled) {
        tracer_->on_check_fail(state_.pc, "symbol mismatch");
      }
      return false;
    }
    ++stats_.parent_symbol_hits;
    return true;
  }

  [[nodiscard]] auto exec_check_arity(Instruction const &instr) -> bool {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    if (enode.arity() != instr.arg) {
      if constexpr (kTracingEnabled) {
        tracer_->on_check_fail(state_.pc, "arity mismatch");
      }
      return false;
    }
    return true;
  }

  void exec_bind_slot(Instruction const &instr) {
    state_.bind(instr.arg, state_.eclass_regs[instr.src]);
    if constexpr (kTracingEnabled) {
      tracer_->on_bind(instr.arg, state_.eclass_regs[instr.src]);
    }
  }

  [[nodiscard]] auto exec_check_slot(Instruction const &instr) -> bool {
    auto expected = egraph_->find(state_.slots[instr.arg]);
    auto actual = egraph_->find(state_.eclass_regs[instr.src]);
    if (expected != actual) {
      ++stats_.check_slot_misses;
      if constexpr (kTracingEnabled) {
        tracer_->on_check_fail(state_.pc, "slot mismatch");
      }
      return false;
    }
    ++stats_.check_slot_hits;
    return true;
  }

  [[nodiscard]] auto exec_bind_or_check(Instruction const &instr) -> bool {
    if (!state_.is_bound(instr.arg)) {
      state_.bind(instr.arg, state_.eclass_regs[instr.src]);
      if constexpr (kTracingEnabled) {
        tracer_->on_bind(instr.arg, state_.eclass_regs[instr.src]);
      }
      return true;
    }
    return exec_check_slot(instr);
  }

  void exec_yield(EMatchContext &ctx, std::vector<PatternMatch> &results) {
    ++stats_.yields;
    if constexpr (kTracingEnabled) {
      tracer_->on_yield(state_.slots);
    }
    results.push_back(ctx.arena().intern(state_.slots));
    // Clear bound flags to allow finding different variable bindings in subsequent matches
    state_.bound.reset();
  }

  EGraphType const *egraph_;
  std::span<Instruction const> code_;
  std::span<Symbol const> symbols_;
  VMState state_;
  VMStats stats_;
  Tracer null_tracer_;
  Tracer *tracer_;
  std::vector<EClassId> all_eclasses_buffer_;  // Buffer for IterAllEClasses
  std::vector<EClassId> candidates_buffer_;    // Buffer for automatic candidate lookup
};

/// VM executor for pattern matching - "clean" mode
///
/// This executor uses ParentSymbolIndex for efficient parent traversal.
/// Requires the index to be kept clean (rebuilt after e-graph modifications).
/// Faster but requires index maintenance.
template <typename Symbol, typename Analysis>
class VMExecutorClean {
 public:
  using EGraphType = EGraph<Symbol, Analysis>;
  using ParentIndexType = ParentSymbolIndex<Symbol, Analysis>;

  VMExecutorClean(EGraphType const &egraph, ParentIndexType const &parent_index)
      : egraph_(&egraph), parent_index_(&parent_index) {}

  void execute(CompiledPattern<Symbol> const &pattern, std::span<EClassId const> candidates, EMatchContext &ctx,
               std::vector<PatternMatch> &results) {
    state_.reset(pattern.num_slots(), pattern.num_registers());
    stats_.reset();
    code_ = pattern.code();
    symbols_ = pattern.symbols();

    for (auto candidate : candidates) {
      auto canonical = egraph_->find(candidate);
      state_.eclass_regs[0] = canonical;
      state_.pc = 0;
      state_.iter_order.clear();
      for (auto &iter : state_.iter_by_reg) {
        iter.reset();
      }
      state_.bound.reset();
      filtered_parents_ = {};

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

    execute(pattern, candidates_buffer_, ctx, results);
  }

  [[nodiscard]] auto stats() const -> VMStats const & { return stats_; }

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
        &&op_IterParentsSym,
        &&op_NextParent,
        &&op_CheckSymbol,
        &&op_CheckArity,
        &&op_BindSlot,
        &&op_CheckSlot,
        &&op_BindOrCheck,
        &&op_Jump,
        &&op_Yield,
        &&op_Halt,
    };

    // clang-format off
#define DISPATCH()                                                      \
  do {                                                                  \
    if (state_.pc >= code_.size()) return;                              \
    ++stats_.instructions_executed;                                     \
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

  op_IterParentsSym:
    if (exec_iter_parents_sym(code_[state_.pc])) {
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

  op_BindSlot:
    exec_bind_slot(code_[state_.pc]);
    NEXT();

  op_CheckSlot:
    if (exec_check_slot(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_BindOrCheck:
    if (exec_bind_or_check(code_[state_.pc])) {
      NEXT();
    } else {
      JUMP(code_[state_.pc].target);
    }

  op_Jump:
    JUMP(code_[state_.pc].target);

  op_Yield:
    exec_yield(ctx, results);
    NEXT();

  op_Halt:
    return;

#undef DISPATCH
#undef NEXT
#undef JUMP
  }

  void exec_load_child(Instruction const &instr) {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    state_.eclass_regs[instr.dst] = egraph_->find(enode.children()[instr.arg]);
  }

  void exec_get_enode_eclass(Instruction const &instr) {
    auto enode_id = state_.enode_regs[instr.src];
    state_.eclass_regs[instr.dst] = egraph_->find(enode_id);
  }

  [[nodiscard]] auto exec_iter_enodes(Instruction const &instr) -> bool {
    ++stats_.iter_enode_calls;
    auto eclass_id = state_.eclass_regs[instr.src];
    auto const &eclass = egraph_->eclass(eclass_id);
    auto nodes = eclass.nodes();

    if (nodes.empty()) {
      return false;
    }

    state_.start_enode_iter(instr.dst, nodes);
    state_.enode_regs[instr.dst] = nodes[0];
    return true;
  }

  [[nodiscard]] auto exec_next_enode(Instruction const &instr) -> bool {
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::ENodes) {
      return false;
    }

    iter.advance();
    if (iter.exhausted()) {
      state_.deactivate_iter_and_nested(instr.dst);
      return false;
    }

    state_.enode_regs[instr.dst] = iter.current();
    return true;
  }

  [[nodiscard]] auto exec_iter_all_eclasses(Instruction const &instr) -> bool {
    all_eclasses_buffer_.clear();
    for (auto const &[id, _] : egraph_->canonical_classes()) {
      all_eclasses_buffer_.push_back(id);
    }

    if (all_eclasses_buffer_.empty()) {
      return false;
    }

    state_.start_all_eclasses_iter(instr.dst, all_eclasses_buffer_);
    state_.eclass_regs[instr.dst] = all_eclasses_buffer_[0];
    return true;
  }

  [[nodiscard]] auto exec_next_eclass(Instruction const &instr) -> bool {
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::AllEClasses) {
      return false;
    }

    iter.advance();
    if (iter.exhausted()) {
      state_.deactivate_iter_and_nested(instr.dst);
      return false;
    }

    state_.eclass_regs[instr.dst] = iter.current_eclass();
    return true;
  }

  [[nodiscard]] auto exec_iter_parents(Instruction const &instr) -> bool {
    ++stats_.iter_parent_calls;
    auto eclass_id = state_.eclass_regs[instr.src];
    auto const &eclass = egraph_->eclass(eclass_id);
    auto const &parents = eclass.parents();

    if (parents.empty()) {
      return false;
    }

    state_.start_parent_iter(instr.dst, eclass_id, parents.size());
    state_.enode_regs[instr.dst] = *parents.begin();
    return true;
  }

  [[nodiscard]] auto exec_iter_parents_sym(Instruction const &instr) -> bool {
    ++stats_.iter_parent_calls;
    auto eclass_id = state_.eclass_regs[instr.src];
    auto sym = symbols_[instr.arg];

    auto parents = parent_index_->parents_with_symbol(eclass_id, sym);

    if (parents.empty()) {
      return false;
    }

    filtered_parents_ = parents;
    state_.start_filtered_parent_iter(instr.dst, parents);
    state_.enode_regs[instr.dst] = parents[0];

    stats_.parent_symbol_hits += parents.size();
    return true;
  }

  [[nodiscard]] auto exec_next_parent(Instruction const &instr) -> bool {
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::Parents && iter.kind != IterState::Kind::ParentsFiltered) {
      return false;
    }

    iter.advance();
    if (iter.exhausted()) {
      state_.deactivate_iter_and_nested(instr.dst);
      filtered_parents_ = {};
      return false;
    }

    if (iter.kind == IterState::Kind::ParentsFiltered) {
      state_.enode_regs[instr.dst] = iter.current();
    } else {
      auto const &eclass = egraph_->eclass(iter.parent_eclass);
      auto const &parents = eclass.parents();
      auto pit = parents.begin();
      std::advance(pit, static_cast<std::ptrdiff_t>(iter.current_idx));
      state_.enode_regs[instr.dst] = *pit;
    }
    return true;
  }

  [[nodiscard]] auto exec_check_symbol(Instruction const &instr) -> bool {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    if (enode.symbol() != symbols_[instr.arg]) {
      ++stats_.parent_symbol_misses;
      return false;
    }
    ++stats_.parent_symbol_hits;
    return true;
  }

  [[nodiscard]] auto exec_check_arity(Instruction const &instr) -> bool {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    if (enode.arity() != instr.arg) {
      return false;
    }
    return true;
  }

  void exec_bind_slot(Instruction const &instr) { state_.bind(instr.arg, state_.eclass_regs[instr.src]); }

  [[nodiscard]] auto exec_check_slot(Instruction const &instr) -> bool {
    auto expected = egraph_->find(state_.slots[instr.arg]);
    auto actual = egraph_->find(state_.eclass_regs[instr.src]);
    if (expected != actual) {
      ++stats_.check_slot_misses;
      return false;
    }
    ++stats_.check_slot_hits;
    return true;
  }

  [[nodiscard]] auto exec_bind_or_check(Instruction const &instr) -> bool {
    if (!state_.is_bound(instr.arg)) {
      state_.bind(instr.arg, state_.eclass_regs[instr.src]);
      return true;
    }
    return exec_check_slot(instr);
  }

  void exec_yield(EMatchContext &ctx, std::vector<PatternMatch> &results) {
    ++stats_.yields;
    results.push_back(ctx.arena().intern(state_.slots));
    // Clear bound flags to allow finding different variable bindings in subsequent matches
    state_.bound.reset();
  }

  EGraphType const *egraph_;
  ParentIndexType const *parent_index_;
  std::span<Instruction const> code_;
  std::span<Symbol const> symbols_;
  std::span<ENodeId const> filtered_parents_;  // Current filtered parent list
  VMState state_;
  VMStats stats_;
  std::vector<EClassId> all_eclasses_buffer_;  // Buffer for IterAllEClasses
  std::vector<EClassId> candidates_buffer_;    // Buffer for automatic candidate lookup
};

}  // namespace memgraph::planner::core::vm
