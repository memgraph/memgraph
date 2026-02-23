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
#include "planner/pattern/vm/instruction.hpp"
#include "planner/pattern/vm/parent_index.hpp"
#include "planner/pattern/vm/state.hpp"
#include "planner/pattern/vm/tracer.hpp"

namespace memgraph::planner::core::vm {

/// Compiled pattern ready for VM execution
template <typename Symbol>
class CompiledPattern {
 public:
  CompiledPattern(std::vector<Instruction> code, std::size_t num_slots, std::vector<Symbol> symbols,
                  std::optional<Symbol> entry_symbol)
      : code_(std::move(code)),
        num_slots_(num_slots),
        symbols_(std::move(symbols)),
        entry_symbol_(std::move(entry_symbol)) {}

  [[nodiscard]] auto code() const -> std::span<Instruction const> { return code_; }

  [[nodiscard]] auto num_slots() const -> std::size_t { return num_slots_; }

  [[nodiscard]] auto symbols() const -> std::span<Symbol const> { return symbols_; }

  [[nodiscard]] auto entry_symbol() const -> std::optional<Symbol> const & { return entry_symbol_; }

 private:
  std::vector<Instruction> code_;
  std::size_t num_slots_;
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
    state_.reset(pattern.num_slots());
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

  [[nodiscard]] auto stats() const -> VMStats const & { return stats_; }

  /// Set tracer for debugging
  void set_tracer(Tracer *tracer) { tracer_ = tracer ? tracer : &null_tracer_; }

 private:
  // Compile-time constant for whether tracing is enabled
  static constexpr bool kTracingEnabled = !std::is_same_v<Tracer, NullTracer>;

  void run_until_halt(EMatchContext &ctx, std::vector<PatternMatch> &results) {
    while (state_.pc < code_.size()) {
      auto const &instr = code_[state_.pc];
      if constexpr (kTracingEnabled) {
        tracer_->on_instruction(state_.pc, instr);
      }
      ++stats_.instructions_executed;
      jumped_ = false;

      switch (instr.op) {
        case VMOp::LoadChild:
          exec_load_child(instr);
          break;
        case VMOp::GetENodeEClass:
          exec_get_enode_eclass(instr);
          break;
        case VMOp::IterENodes:
          exec_iter_enodes(instr);
          break;
        case VMOp::NextENode:
          exec_next_enode(instr);
          break;
        case VMOp::IterAllEClasses:
          exec_iter_all_eclasses(instr);
          break;
        case VMOp::NextEClass:
          exec_next_eclass(instr);
          break;
        case VMOp::IterParents:
          exec_iter_parents(instr);
          break;
        case VMOp::IterParentsSym:
          // In verify mode, fall back to regular parent iteration
          // The bytecode should include CheckSymbol after this
          exec_iter_parents(instr);
          break;
        case VMOp::NextParent:
          exec_next_parent(instr);
          break;
        case VMOp::CheckSymbol:
          exec_check_symbol(instr);
          break;
        case VMOp::CheckArity:
          exec_check_arity(instr);
          break;
        case VMOp::BindSlot:
          exec_bind_slot(instr);
          break;
        case VMOp::CheckSlot:
          exec_check_slot(instr);
          break;
        case VMOp::BindOrCheck:
          exec_bind_or_check(instr);
          break;
        case VMOp::Jump:
          state_.pc = instr.target;
          jumped_ = true;
          break;
        case VMOp::Yield:
          exec_yield(ctx, results);
          break;
        case VMOp::Halt:
          if constexpr (kTracingEnabled) {
            tracer_->on_halt(stats_.instructions_executed);
          }
          return;
      }
      if (!jumped_) {
        ++state_.pc;
      }
    }
  }

  void exec_load_child(Instruction const &instr) {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    state_.eclass_regs[instr.dst] = egraph_->find(enode.children()[instr.arg]);
  }

  void exec_get_enode_eclass(Instruction const &instr) {
    auto enode_id = state_.enode_regs[instr.src];
    state_.eclass_regs[instr.dst] = egraph_->find(enode_id);
  }

  void exec_iter_enodes(Instruction const &instr) {
    ++stats_.iter_enode_calls;
    auto eclass_id = state_.eclass_regs[instr.src];
    auto const &eclass = egraph_->eclass(eclass_id);
    auto nodes = eclass.nodes();

    if constexpr (kTracingEnabled) {
      tracer_->on_iter_start(state_.pc, nodes.size());
    }

    if (nodes.empty()) {
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    // Store span directly in register-indexed state
    state_.start_enode_iter(instr.dst, nodes);
    state_.enode_regs[instr.dst] = nodes[0];
  }

  void exec_next_enode(Instruction const &instr) {
    // O(1) lookup by register
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::ENodes) {
      // No active iteration for this register
      if constexpr (kTracingEnabled) {
        tracer_->on_iter_advance(state_.pc, 0);
      }
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    iter.advance();
    if (iter.exhausted()) {
      if constexpr (kTracingEnabled) {
        tracer_->on_iter_advance(state_.pc, 0);
      }
      // Deactivate this and all nested iterations
      state_.deactivate_iter_and_nested(instr.dst);
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    if constexpr (kTracingEnabled) {
      tracer_->on_iter_advance(state_.pc, iter.remaining());
    }
    // Direct access to cached span - no e-graph lookup needed
    state_.enode_regs[instr.dst] = iter.current();
  }

  void exec_iter_all_eclasses(Instruction const &instr) {
    // Build list of all canonical e-classes
    all_eclasses_buffer_.clear();
    for (auto const &[id, _] : egraph_->canonical_classes()) {
      all_eclasses_buffer_.push_back(id);
    }

    if (all_eclasses_buffer_.empty()) {
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    // Store span in register-indexed state
    state_.start_all_eclasses_iter(instr.dst, all_eclasses_buffer_);
    state_.eclass_regs[instr.dst] = all_eclasses_buffer_[0];
  }

  void exec_next_eclass(Instruction const &instr) {
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::AllEClasses) {
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    iter.advance();
    if (iter.exhausted()) {
      state_.deactivate_iter_and_nested(instr.dst);
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    state_.eclass_regs[instr.dst] = iter.current_eclass();
  }

  void exec_iter_parents(Instruction const &instr) {
    ++stats_.iter_parent_calls;
    auto eclass_id = state_.eclass_regs[instr.src];
    auto const &eclass = egraph_->eclass(eclass_id);
    auto const &parents = eclass.parents();

    if constexpr (kTracingEnabled) {
      tracer_->on_iter_start(state_.pc, parents.size());
    }

    if (parents.empty()) {
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    // Store e-class ID for index-based parent lookup
    state_.start_parent_iter(instr.dst, eclass_id, parents.size());
    state_.enode_regs[instr.dst] = *parents.begin();
  }

  void exec_next_parent(Instruction const &instr) {
    // O(1) lookup by register
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::Parents && iter.kind != IterState::Kind::ParentsFiltered) {
      // No active parent iteration for this register
      if constexpr (kTracingEnabled) {
        tracer_->on_iter_advance(state_.pc, 0);
      }
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    iter.advance();
    if (iter.exhausted()) {
      if constexpr (kTracingEnabled) {
        tracer_->on_iter_advance(state_.pc, 0);
      }
      // Deactivate this and all nested iterations
      state_.deactivate_iter_and_nested(instr.dst);
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    if constexpr (kTracingEnabled) {
      tracer_->on_iter_advance(state_.pc, iter.remaining());
    }

    // For filtered parents, use span; for regular parents, use index lookup
    if (iter.kind == IterState::Kind::ParentsFiltered) {
      state_.enode_regs[instr.dst] = iter.nodes_span[iter.current_idx];
    } else {
      // Index-based lookup - need to iterate through the set
      auto const &eclass = egraph_->eclass(iter.parent_eclass);
      auto const &parents = eclass.parents();
      auto pit = parents.begin();
      std::advance(pit, static_cast<std::ptrdiff_t>(iter.current_idx));
      state_.enode_regs[instr.dst] = *pit;
    }
  }

  void exec_check_symbol(Instruction const &instr) {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    if (enode.symbol() != symbols_[instr.arg]) {
      ++stats_.parent_symbol_misses;
      if constexpr (kTracingEnabled) {
        tracer_->on_check_fail(state_.pc, "symbol mismatch");
      }
      state_.pc = instr.target;
      jumped_ = true;
    } else {
      ++stats_.parent_symbol_hits;
    }
  }

  void exec_check_arity(Instruction const &instr) {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    if (enode.arity() != instr.arg) {
      if constexpr (kTracingEnabled) {
        tracer_->on_check_fail(state_.pc, "arity mismatch");
      }
      state_.pc = instr.target;
      jumped_ = true;
    }
  }

  void exec_bind_slot(Instruction const &instr) {
    state_.bind(instr.arg, state_.eclass_regs[instr.src]);
    if constexpr (kTracingEnabled) {
      tracer_->on_bind(instr.arg, state_.eclass_regs[instr.src]);
    }
  }

  void exec_check_slot(Instruction const &instr) {
    auto expected = egraph_->find(state_.slots[instr.arg]);
    auto actual = egraph_->find(state_.eclass_regs[instr.src]);
    if (expected != actual) {
      ++stats_.check_slot_misses;
      if constexpr (kTracingEnabled) {
        tracer_->on_check_fail(state_.pc, "slot mismatch");
      }
      state_.pc = instr.target;
      jumped_ = true;
    } else {
      ++stats_.check_slot_hits;
    }
  }

  void exec_bind_or_check(Instruction const &instr) {
    if (!state_.is_bound(instr.arg)) {
      state_.bind(instr.arg, state_.eclass_regs[instr.src]);
      if constexpr (kTracingEnabled) {
        tracer_->on_bind(instr.arg, state_.eclass_regs[instr.src]);
      }
    } else {
      exec_check_slot(instr);
    }
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
  bool jumped_{false};
  std::vector<EClassId> all_eclasses_buffer_;  // Buffer for IterAllEClasses
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
    state_.reset(pattern.num_slots());
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

  [[nodiscard]] auto stats() const -> VMStats const & { return stats_; }

 private:
  void run_until_halt(EMatchContext &ctx, std::vector<PatternMatch> &results) {
    while (state_.pc < code_.size()) {
      auto const &instr = code_[state_.pc];
      ++stats_.instructions_executed;
      jumped_ = false;

      switch (instr.op) {
        case VMOp::LoadChild:
          exec_load_child(instr);
          break;
        case VMOp::GetENodeEClass:
          exec_get_enode_eclass(instr);
          break;
        case VMOp::IterENodes:
          exec_iter_enodes(instr);
          break;
        case VMOp::NextENode:
          exec_next_enode(instr);
          break;
        case VMOp::IterAllEClasses:
          exec_iter_all_eclasses(instr);
          break;
        case VMOp::NextEClass:
          exec_next_eclass(instr);
          break;
        case VMOp::IterParents:
          exec_iter_parents(instr);
          break;
        case VMOp::IterParentsSym:
          // Use the symbol index for direct lookup
          exec_iter_parents_sym(instr);
          break;
        case VMOp::NextParent:
          exec_next_parent(instr);
          break;
        case VMOp::CheckSymbol:
          exec_check_symbol(instr);
          break;
        case VMOp::CheckArity:
          exec_check_arity(instr);
          break;
        case VMOp::BindSlot:
          exec_bind_slot(instr);
          break;
        case VMOp::CheckSlot:
          exec_check_slot(instr);
          break;
        case VMOp::BindOrCheck:
          exec_bind_or_check(instr);
          break;
        case VMOp::Jump:
          state_.pc = instr.target;
          jumped_ = true;
          break;
        case VMOp::Yield:
          exec_yield(ctx, results);
          break;
        case VMOp::Halt:
          return;
      }
      if (!jumped_) {
        ++state_.pc;
      }
    }
  }

  void exec_load_child(Instruction const &instr) {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    state_.eclass_regs[instr.dst] = egraph_->find(enode.children()[instr.arg]);
  }

  void exec_get_enode_eclass(Instruction const &instr) {
    auto enode_id = state_.enode_regs[instr.src];
    state_.eclass_regs[instr.dst] = egraph_->find(enode_id);
  }

  void exec_iter_enodes(Instruction const &instr) {
    ++stats_.iter_enode_calls;
    auto eclass_id = state_.eclass_regs[instr.src];
    auto const &eclass = egraph_->eclass(eclass_id);
    auto nodes = eclass.nodes();

    if (nodes.empty()) {
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    state_.start_enode_iter(instr.dst, nodes);
    state_.enode_regs[instr.dst] = nodes[0];
  }

  void exec_next_enode(Instruction const &instr) {
    // O(1) lookup by register
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::ENodes) {
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    iter.advance();
    if (iter.exhausted()) {
      state_.deactivate_iter_and_nested(instr.dst);
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    state_.enode_regs[instr.dst] = iter.current();
  }

  void exec_iter_all_eclasses(Instruction const &instr) {
    // Build list of all canonical e-classes
    all_eclasses_buffer_.clear();
    for (auto const &[id, _] : egraph_->canonical_classes()) {
      all_eclasses_buffer_.push_back(id);
    }

    if (all_eclasses_buffer_.empty()) {
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    state_.start_all_eclasses_iter(instr.dst, all_eclasses_buffer_);
    state_.eclass_regs[instr.dst] = all_eclasses_buffer_[0];
  }

  void exec_next_eclass(Instruction const &instr) {
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::AllEClasses) {
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    iter.advance();
    if (iter.exhausted()) {
      state_.deactivate_iter_and_nested(instr.dst);
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    state_.eclass_regs[instr.dst] = iter.current_eclass();
  }

  void exec_iter_parents(Instruction const &instr) {
    ++stats_.iter_parent_calls;
    auto eclass_id = state_.eclass_regs[instr.src];
    auto const &eclass = egraph_->eclass(eclass_id);
    auto const &parents = eclass.parents();

    if (parents.empty()) {
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    // Store e-class ID for index-based parent lookup
    state_.start_parent_iter(instr.dst, eclass_id, parents.size());
    state_.enode_regs[instr.dst] = *parents.begin();
  }

  void exec_iter_parents_sym(Instruction const &instr) {
    ++stats_.iter_parent_calls;
    auto eclass_id = state_.eclass_regs[instr.src];
    auto sym = symbols_[instr.arg];

    // Use the symbol index for O(1) lookup
    auto parents = parent_index_->parents_with_symbol(eclass_id, sym);

    if (parents.empty()) {
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    // Store the filtered parents for iteration (span into index, valid until rebuild)
    filtered_parents_ = parents;
    state_.start_filtered_parent_iter(instr.dst, parents);
    state_.enode_regs[instr.dst] = parents[0];

    // Track that all these parents match the symbol
    stats_.parent_symbol_hits += parents.size();
  }

  void exec_next_parent(Instruction const &instr) {
    // O(1) lookup by register
    auto &iter = state_.get_iter(instr.dst);

    if (iter.kind != IterState::Kind::Parents && iter.kind != IterState::Kind::ParentsFiltered) {
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    iter.advance();
    if (iter.exhausted()) {
      state_.deactivate_iter_and_nested(instr.dst);
      filtered_parents_ = {};
      state_.pc = instr.target;
      jumped_ = true;
      return;
    }

    // For filtered parents, use span; for regular parents, use index lookup
    if (iter.kind == IterState::Kind::ParentsFiltered) {
      state_.enode_regs[instr.dst] = iter.current();
    } else {
      // Index-based lookup - need to iterate through the set
      auto const &eclass = egraph_->eclass(iter.parent_eclass);
      auto const &parents = eclass.parents();
      auto pit = parents.begin();
      std::advance(pit, static_cast<std::ptrdiff_t>(iter.current_idx));
      state_.enode_regs[instr.dst] = *pit;
    }
  }

  void exec_check_symbol(Instruction const &instr) {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    if (enode.symbol() != symbols_[instr.arg]) {
      ++stats_.parent_symbol_misses;
      state_.pc = instr.target;
      jumped_ = true;
    } else {
      ++stats_.parent_symbol_hits;
    }
  }

  void exec_check_arity(Instruction const &instr) {
    auto const &enode = egraph_->get_enode(state_.enode_regs[instr.src]);
    if (enode.arity() != instr.arg) {
      state_.pc = instr.target;
      jumped_ = true;
    }
  }

  void exec_bind_slot(Instruction const &instr) { state_.bind(instr.arg, state_.eclass_regs[instr.src]); }

  void exec_check_slot(Instruction const &instr) {
    auto expected = egraph_->find(state_.slots[instr.arg]);
    auto actual = egraph_->find(state_.eclass_regs[instr.src]);
    if (expected != actual) {
      ++stats_.check_slot_misses;
      state_.pc = instr.target;
      jumped_ = true;
    } else {
      ++stats_.check_slot_hits;
    }
  }

  void exec_bind_or_check(Instruction const &instr) {
    if (!state_.is_bound(instr.arg)) {
      state_.bind(instr.arg, state_.eclass_regs[instr.src]);
    } else {
      exec_check_slot(instr);
    }
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
  bool jumped_{false};
  std::vector<EClassId> all_eclasses_buffer_;  // Buffer for IterAllEClasses
};

}  // namespace memgraph::planner::core::vm
