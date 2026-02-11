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
#include <ranges>
#include <span>
#include <stdexcept>
#include <variant>
#include <vector>

#include <boost/container/small_vector.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/pattern/pattern.hpp"
#include "planner/pattern/vm/compiled_pattern.hpp"
#include "planner/pattern/vm/instruction.hpp"
#include "planner/pattern/vm/types.hpp"
#include "utils/variant_helpers.hpp"

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

  // ============================================================================
  // Symbol-independent types
  // ============================================================================

  /// Represents a step in the path from root toward the entry node.
  struct PathStep {
    PatternNodeId node_id;  // Symbol node ID
    std::size_t child_idx;  // Which child leads to next step (or to entry node)
  };

  using PathSteps = std::vector<PathStep>;

  /// How the entry node connects to prior patterns or binds a new variable.
  struct JoinVar {
    PatternVar var;  // Shared variable — enters via existing register
  };

  struct EclassBinding {
    PatternVar var;  // New variable bound at eclass level during iteration
  };

  using EntryBinding = std::variant<std::monostate, JoinVar, EclassBinding>;

  /// Entry decision for a pattern, computed once at planning time.
  /// Determines where to start walking and what Phase 1 binds.
  struct EntryDecision {
    PatternNodeId node;                  // Node to start walking from
    std::vector<PathStep> path_to_root;  // Path from entry UP to root (empty if entry IS root)
    EntryBinding binding;                // How this entry connects (join, new binding, or neither)
  };

  /// A pattern with its pre-computed entry decision.
  struct PlannedPattern {
    std::size_t pattern_idx;
    EntryDecision entry;
  };

  /// A node in the emission plan: anchor pattern + patterns hoisted into its eclass phase.
  struct EmitNode {
    PlannedPattern anchor;
    boost::container::small_vector<PlannedPattern, 4> hoisted;
  };

  /// Pre-computed emission plan: determines join order, entry points, and hoisting tree.
  struct EmitPlan {
    std::vector<EmitNode> chain;
  };

  /// Result of emitting eclass-level iteration + optional root binding.
  struct EClassSetup {
    EClassReg eclass_reg;
    InstrAddr exhaust;  // eclass-level exhaustion backtrack (MarkSeen if present, else loop)
  };

  /// Result of emitting symbol structure (iteration, checks, children).
  struct SymbolStructure {
    InstrAddr innermost;         // Deepest child backtrack (for exhaustive enum), or backtrack for leaf
    InstrAddr parent_traversal;  // Backtrack for joined parent traversal: loop_pos for non-leaf, backtrack for leaf
  };

  /// Noop entry: wildcard or already-bound var — no iteration established.
  struct NoopEntry {};

  /// Active entry: eclass register established, ready for structural matching.
  struct MatchEntry {
    PatternNodeId start_node;                // Where in the pattern to begin structural matching
    std::span<PathStep const> path_to_root;  // Borrows from EmitPlan (stable lifetime)
    EClassReg eclass_reg;
    std::optional<InstrAddr> backtrack_override;  // Fixed for join entries; nullopt → use exhaust_bt
  };

  using EClassEntry = std::variant<NoopEntry, MatchEntry>;

  /// Result of eclass-level entry phase: the entry + exhaust backtrack to thread through hoisting.
  struct EClassPhase {
    EClassEntry entry;
    InstrAddr exhaust_bt;
  };

  // ============================================================================
  // Symbol-independent methods
  // ============================================================================

  /// Result of an iteration loop emission.
  struct IterLoopAddrs {
    InstrAddr exhaust;  ///< Where inner exhaustion backtracks (MarkSeen if present, else NextX)
    InstrAddr loop;     ///< NextX address (BindSlot failure backtracks here directly)
  };

  /// Emit iteration loop: IterX, Jump, [MarkSeen,] NextX, patch-jump.
  auto emit_iter_loop(Instruction iter_instr, Instruction next_instr, std::optional<SlotIdx> mark_slot = std::nullopt)
      -> IterLoopAddrs;

  /// Allocate an e-class register
  auto alloc_eclass_reg() -> EClassReg;

  /// Allocate an e-node register
  auto alloc_enode_reg() -> ENodeReg;

  /// Get slot index for a pattern variable
  [[nodiscard]] auto get_slot(PatternVar var) const -> SlotIdx;

  /// Emit an instruction and return its address
  auto emit(Instruction instr) -> InstrAddr;

  /// Get the address where the next instruction will be emitted
  [[nodiscard]] auto current_addr() const -> InstrAddr;

  /// Patch a jump target at the given address
  void patch_target(InstrAddr addr, InstrAddr target);

  /// Emit variable binding (check if seen, else bind with dedup).
  auto emit_var_binding(PatternVar var, EClassReg eclass_reg, InstrAddr backtrack) -> InstrAddr;

  /// Returns slot for MarkSeen if var is not the last unbound slot (Yield handles the last).
  [[nodiscard]] auto last_unbound_slot(PatternVar var) const -> std::optional<SlotIdx> {
    if (seen_vars_.size() + 1 == slot_map_.size()) return std::nullopt;
    return get_slot(var);
  }

  /// Emit IterAllEClasses loop + binding for an unbound variable root.
  auto emit_var_eclass_iter(PatternVar var, InstrAddr backtrack) -> EClassSetup;

  /// Pre-computed join plan: order and reverse dependencies (all in pattern-index space).
  struct JoinPlan {
    std::vector<std::size_t> order;  // Pattern indices in join order
    boost::unordered_flat_map<std::size_t, std::size_t>
        latest_dep;  // Pattern index → latest earlier pattern index sharing vars (absent = independent)
  };

  /// Compute join order, reverse dependencies, and index mapping.
  static auto compute_join_plan(std::span<boost::unordered_flat_set<PatternVar> const> pat_vars) -> JoinPlan;

  static constexpr InstrAddr kHaltPlaceholder{0xFFFF};

  std::vector<Instruction> code_;
  boost::unordered_flat_set<PatternVar> seen_vars_;
  VarSlotMap slot_map_;
  boost::unordered_flat_map<PatternVar, EClassReg> var_to_reg_;  // Maps vars to eclass registers
  std::vector<SlotIdx> binding_order_;                           // Order in which slots are bound
  uint8_t next_eclass_reg_{0};
  uint8_t next_enode_reg_{0};
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
//   - eclass_regs[0..num_eclass_regs-1] are allocated sequentially
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
//   - BindSlot always uses slot indices in [0, num_slots())
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
//   - Code begins with iteration setup for root pattern (if any)
//   - For patterns with bindings: ends with Yield, Jump(innermost_loop), Halt
//   - For patterns without bindings (e.g., pure wildcard): just Halt
//   - Halt is always the last instruction
//
// ## Join Strategies
//
//   1. Parent Traversal: When patterns share a variable
//      - Finds the DEEPEST shared variable (deeper = fewer parent traversals)
//      - Walk upward through parent chain via IterParents
//      - O(parents^depth) per candidate, minimized by choosing deepest
//
//   2. Cartesian Product: When no shared variable
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
/// 0:  IterENodes r1, r0            ; iterate e-nodes in input e-class (no backtrack - always ≥1)
/// 1:  Jump @3
/// 2:  NextENode r1, @halt          ; advance, or jump to halt
/// 3:  CheckSymbol r1, Neg, @2      ; wrong symbol -> try next
/// 4:  CheckArity r1, 1, @2         ; wrong arity -> try next
/// 5:  LoadChild r2, r1, 0          ; load child e-class
/// 6:  IterENodes r3, r2            ; iterate inner Neg (no backtrack)
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
  /// Compile multiple patterns into fused bytecode with automatic join order.
  /// Analyzes shared variables to determine optimal anchor and join order.
  /// Empty pattern set returns empty pattern (matches nothing).
  auto compile(std::span<Pattern<Symbol> const> patterns) -> CompiledPattern<Symbol> {
    return compile_patterns(patterns);
  }

  template <typename... Patterns>
    requires(std::same_as<std::remove_cvref_t<Patterns>, Pattern<Symbol>> && ...)
  auto compile(Patterns &&...patterns) {
    auto pattern_array = std::array{std::forward<Patterns>(patterns)...};
    return compile(pattern_array);
  }

 private:
  // ============================================================================
  // Compilation
  // ============================================================================

  auto compile_patterns(std::span<Pattern<Symbol> const> patterns) -> CompiledPattern<Symbol>;
  void reset();
  void build_slot_map(std::span<Pattern<Symbol> const> patterns);

  // ============================================================================
  // Planning (static, no codegen state)
  // ============================================================================

  /// Compute the full emission plan: join order, entry decisions, and hoisting tree.
  static auto compute_emit_plan(std::span<Pattern<Symbol> const> patterns) -> EmitPlan;

  /// Compute entry decision for a pattern given the set of already-bound variables.
  static auto compute_entry(Pattern<Symbol> const &pat, boost::unordered_flat_set<PatternVar> const &bound)
      -> EntryDecision;

  // ============================================================================
  // Emission: top-level
  // ============================================================================

  /// Emit all patterns according to the pre-computed emission plan.
  auto emit_patterns(std::span<Pattern<Symbol> const> patterns, EmitPlan const &plan) -> InstrAddr;

  /// Emit eclass-level entry using pre-computed entry decision.
  auto emit_eclass_entry(Pattern<Symbol> const &pattern, EntryDecision const &decision, InstrAddr backtrack)
      -> EClassPhase;

  /// Emit structural matching: walk down from entry, then up via parent traversal.
  auto emit_structure_match(Pattern<Symbol> const &pattern, MatchEntry const &entry, InstrAddr exhaust_bt) -> InstrAddr;

  // ============================================================================
  // Emission: node-level
  // ============================================================================

  auto emit_node(Pattern<Symbol> const &pattern, PatternNodeId node_id, EClassReg eclass_reg, InstrAddr backtrack)
      -> InstrAddr;

  auto emit_symbol_node(Pattern<Symbol> const &pattern, PatternNodeId node_id, SymbolWithChildren<Symbol> const &sym,
                        EClassReg eclass_reg, InstrAddr backtrack,
                        std::optional<InstrAddr> bind_backtrack = std::nullopt) -> InstrAddr;

  auto emit_symbol_eclass_iter(SymbolWithChildren<Symbol> const &sym, std::optional<PatternVar> binding,
                               InstrAddr backtrack) -> EClassSetup;

  auto emit_symbol_structure(Pattern<Symbol> const &pattern, SymbolWithChildren<Symbol> const &sym,
                             EClassReg eclass_reg, InstrAddr backtrack) -> SymbolStructure;

  /// All LoadChild instructions are emitted before any nested iteration loops,
  /// ensuring sibling loads don't end up inside a child's IterENodes loop.
  auto emit_children(Pattern<Symbol> const &pattern, SymbolWithChildren<Symbol> const &sym, ENodeReg enode_reg,
                     InstrAddr backtrack, std::optional<std::size_t> skip_child_idx = std::nullopt) -> InstrAddr;

  // ============================================================================
  // Emission: utilities
  // ============================================================================

  auto get_symbol_index(Symbol const &sym) -> uint8_t;

  std::vector<Symbol> symbols_;  // Symbol table (Symbol-dependent)
};

template <typename Symbol>
auto PatternCompiler<Symbol>::compile_patterns(std::span<Pattern<Symbol> const> patterns) -> CompiledPattern<Symbol> {
  if (patterns.empty()) {
    return CompiledPattern<Symbol>{};
  }

  // Filter out patterns that contribute no bindings or checks (e.g., pure wildcards,
  // symbols with only wildcard children). Such patterns cannot affect match results.
  auto effective = std::vector<Pattern<Symbol>>{};
  effective.reserve(patterns.size());
  std::ranges::copy_if(
      patterns, std::back_inserter(effective), [](auto const &pat) { return !pat.var_slots().empty(); });
  if (effective.empty()) {
    return CompiledPattern<Symbol>{};
  }

  reset();

  // Build unified slot map
  build_slot_map(effective);

  // Compute emission plan (join order + hoisting tree)
  auto const plan = compute_emit_plan(effective);

  auto innermost = emit_patterns(effective, plan);

  // Emit yield and continue loop
  emit(Instruction::yield(binding_order_.back()));
  emit(Instruction::jmp(innermost));

  // Emit halt and patch placeholders
  auto halt_pos = emit(Instruction::halt());
  for (auto &instr : code_) {
    if (instr.target == value_of(kHaltPlaceholder)) instr.target = value_of(halt_pos);
  }

  return CompiledPattern<Symbol>(std::move(code_),
                                 next_eclass_reg_,
                                 next_enode_reg_,
                                 std::move(symbols_),
                                 std::move(binding_order_),
                                 std::move(slot_map_));
}

template <typename Symbol>
void PatternCompiler<Symbol>::reset() {
  PatternCompilerBase::reset();
  symbols_.clear();
}

template <typename Symbol>
void PatternCompiler<Symbol>::build_slot_map(std::span<Pattern<Symbol> const> patterns) {
  // For single patterns, use the pattern's existing slot assignments directly
  // to ensure consistency with MatcherIndex's variable ordering.
  // For multi-pattern joins, we need to merge slot maps carefully.
  if (patterns.size() == 1) {
    slot_map_ = patterns[0].var_slots();
  } else {
    // Validate: no PatternVar can have symbol bindings in multiple patterns
    // e.g., pattern1: ?x=A(), pattern2: ?x=B() is invalid
    boost::unordered_flat_set<PatternVar> vars_with_symbol_bindings;
    for (auto const &pattern : patterns) {
      for (auto const &[_, var] : pattern.bindings()) {
        if (vars_with_symbol_bindings.contains(var)) {
          throw std::invalid_argument("PatternVar cannot have symbol bindings in multiple patterns");
        }
        vars_with_symbol_bindings.insert(var);
      }
    }

    // Multi-pattern: assign slots sequentially, deduplicating shared variables
    for (auto const &pattern : patterns) {
      for (auto const &[var, _] : pattern.var_slots()) {
        if (!slot_map_.contains(var)) {
          slot_map_[var] = SlotIdx{static_cast<uint8_t>(slot_map_.size())};
        }
      }
    }
  }
}

// ============================================================================
// Planning
// ============================================================================

// ----------------------------------------------------------------------------
// Compute entry decision: unified search for a pattern's entry point.
//
// Determines where to start walking and what Phase 1 binds. A single tree
// walk using the bound-var set (no codegen state needed).
//
//   Priority 1: Deepest shared var → enters via existing register → no new binding
//   Priority 2: Deepest non-root symbol → IterSymbolEClasses → walk up via parents
//   Priority 3: Root fallback → depends on root node type
// ----------------------------------------------------------------------------

template <typename Symbol>
auto PatternCompiler<Symbol>::compute_entry(Pattern<Symbol> const &pat,
                                            boost::unordered_flat_set<PatternVar> const &bound) -> EntryDecision {
  struct Candidate {
    std::size_t depth;
    PatternNodeId node;
    PathSteps ancestors;
  };

  std::optional<Candidate> symbol;
  std::optional<std::pair<Candidate, PatternVar>> shared;

  auto search = [&](auto &self, PatternNodeId node_id, PathSteps &steps) -> void {
    auto try_shared = [&](PatternVar var) {
      if (bound.contains(var) && (!shared || steps.size() > shared->first.depth)) {
        shared = {{steps.size(), node_id, steps}, var};
      }
    };

    std::visit(utils::Overloaded{
                   [&](PatternVar const &var) { try_shared(var); },
                   [&](SymbolWithChildren<Symbol> const &sym) {
                     if (!symbol || steps.size() > symbol->depth) {
                       symbol = {steps.size(), node_id, steps};
                     }
                     if (auto binding = pat.binding_for(node_id)) {
                       try_shared(*binding);
                     }
                     for (std::size_t i = 0; i < sym.children.size(); ++i) {
                       steps.emplace_back(node_id, i);
                       self(self, sym.children[i], steps);
                       steps.pop_back();
                     }
                   },
                   [](Wildcard) {},
               },
               pat[node_id]);
  };

  PathSteps steps;
  search(search, pat.root(), steps);

  // Priority 1: deepest shared var → enter via existing register, no new binding.
  if (shared) {
    auto &[cand, var] = *shared;
    return EntryDecision{cand.node, std::move(cand.ancestors), JoinVar{var}};
  }

  // Priority 2: deepest symbol → enter via IterSymbolEClasses, walk up via parents if non-root.
  if (symbol) {
    auto binding = pat.binding_for(symbol->node);
    return EntryDecision{symbol->node,
                         std::move(symbol->ancestors),
                         binding ? EntryBinding{EclassBinding{*binding}} : EntryBinding{std::monostate{}}};
  }

  // Fallback: root variable (patterns with no symbols).
  auto root_binding =
      std::visit(utils::Overloaded{
                     [&](PatternVar const &var) -> EntryBinding {
                       return bound.contains(var) ? EntryBinding{std::monostate{}} : EntryBinding{EclassBinding{var}};
                     },
                     [](auto const &) -> EntryBinding { return std::monostate{}; },
                 },
                 pat[pat.root()]);

  return EntryDecision{pat.root(), {}, root_binding};
}

template <typename Symbol>
auto PatternCompiler<Symbol>::compute_emit_plan(std::span<Pattern<Symbol> const> patterns) -> EmitPlan {
  std::vector<boost::unordered_flat_set<PatternVar>> pat_vars(patterns.size());
  for (std::size_t i = 0; i < patterns.size(); ++i) {
    auto keys = patterns[i].var_slots() | std::views::keys;
    pat_vars[i].insert(keys.begin(), keys.end());
  }

  auto const join = compute_join_plan(pat_vars);

  // Build emission plan: walk the join order, hoisting independent patterns into anchor eclass phases.
  auto bound = boost::unordered_flat_set<PatternVar>{};
  auto pending = boost::container::small_vector<std::size_t, 8>{join.order.begin(), join.order.end()};
  auto placed = boost::unordered_flat_set<std::size_t>{};

  auto plan_pattern = [&](std::size_t idx) -> PlannedPattern {
    auto entry = compute_entry(patterns[idx], bound);
    if (auto *eb = std::get_if<EclassBinding>(&entry.binding)) {
      bound.insert(eb->var);
    }
    return {idx, std::move(entry)};
  };

  auto plan = EmitPlan{};
  while (!pending.empty()) {
    auto const anchor = pending.front();
    placed.insert(anchor);
    auto node = EmitNode{plan_pattern(anchor), {}};

    auto deferred = boost::container::small_vector<std::size_t, 8>{};
    for (std::size_t k = 1; k < pending.size(); ++k) {
      auto const cand = pending[k];
      auto dep_it = join.latest_dep.find(cand);
      bool deps_resolved = (dep_it == join.latest_dep.end()) || placed.contains(dep_it->second);
      bool vars_available = std::ranges::none_of(
          pat_vars[cand], [&](auto const &v) { return pat_vars[anchor].contains(v) && !bound.contains(v); });

      if (deps_resolved && vars_available) {
        node.hoisted.push_back(plan_pattern(cand));
        bound.insert(pat_vars[cand].begin(), pat_vars[cand].end());
        placed.insert(cand);
      } else {
        deferred.push_back(cand);
      }
    }

    bound.insert(pat_vars[anchor].begin(), pat_vars[anchor].end());
    plan.chain.push_back(std::move(node));
    pending = std::move(deferred);
  }
  return plan;
}

// ============================================================================
// Emission: top-level
// ============================================================================

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_patterns(std::span<Pattern<Symbol> const> patterns, EmitPlan const &plan)
    -> InstrAddr {
  auto resolve = [&](Pattern<Symbol> const &p, EClassPhase const &phase) {
    return std::visit(utils::Overloaded{
                          [&](NoopEntry) { return phase.exhaust_bt; },
                          [&](MatchEntry const &e) { return emit_structure_match(p, e, phase.exhaust_bt); },
                      },
                      phase.entry);
  };

  InstrAddr innermost = kHaltPlaceholder;
  for (auto const &node : plan.chain) {
    auto const &pat = patterns[node.anchor.pattern_idx];
    auto [entry, exhaust_bt] = emit_eclass_entry(pat, node.anchor.entry, innermost);

    // Emit hoisted patterns into eclass phase.
    for (auto const &hoist : node.hoisted) {
      auto const &hpat = patterns[hoist.pattern_idx];
      exhaust_bt = resolve(hpat, emit_eclass_entry(hpat, hoist.entry, exhaust_bt));
    }

    innermost = resolve(pat, {entry, exhaust_bt});
  }
  return innermost;
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_eclass_entry(Pattern<Symbol> const &pattern, EntryDecision const &decision,
                                                InstrAddr backtrack) -> EClassPhase {
  auto noop = [&] { return EClassPhase{NoopEntry{}, backtrack}; };
  auto make = [&](EClassSetup const &ec) {
    return EClassPhase{MatchEntry{decision.node, decision.path_to_root, ec.eclass_reg, std::nullopt}, ec.exhaust};
  };

  return std::visit(
      utils::Overloaded{
          // JoinVar: enter via existing register — pattern node irrelevant.
          [&](JoinVar const &jv, auto const &) {
            auto it = var_to_reg_.find(jv.var);
            assert(it != var_to_reg_.end());
            return EClassPhase{MatchEntry{decision.node, decision.path_to_root, it->second, backtrack}, backtrack};
          },
          // EclassBinding + var: iterate all eclasses for the new variable.
          [&](EclassBinding const &eb, PatternVar const &) { return make(emit_var_eclass_iter(eb.var, backtrack)); },
          // EclassBinding + symbol: iterate symbol eclasses with binding.
          [&](EclassBinding const &eb, SymbolWithChildren<Symbol> const &sym) {
            return make(emit_symbol_eclass_iter(sym, eb.var, backtrack));
          },
          // No binding + symbol root: iterate eclasses without binding.
          [&](std::monostate, SymbolWithChildren<Symbol> const &sym) {
            return make(emit_symbol_eclass_iter(sym, std::nullopt, backtrack));
          },
          // Everything else: noop (wildcard, already-bound var).
          [&](auto const &, auto const &) { return noop(); },
      },
      decision.binding,
      pattern[decision.node]);
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_structure_match(Pattern<Symbol> const &pattern, MatchEntry const &entry,
                                                   InstrAddr exhaust_bt) -> InstrAddr {
  auto eclass_reg = entry.eclass_reg;
  auto structure_bt = entry.backtrack_override.value_or(exhaust_bt);

  // ── Walk down from entry (verify structure, process children) ──────────

  auto innermost =
      std::visit(utils::Overloaded{
                     [&](SymbolWithChildren<Symbol> const &sym) {
                       auto structure = emit_symbol_structure(pattern, sym, eclass_reg, structure_bt);
                       return entry.path_to_root.empty() ? structure.innermost : structure.parent_traversal;
                     },
                     [&](auto const &) { return exhaust_bt; },  // Non-symbol entry: no structure to walk
                 },
                 pattern[entry.start_node]);

  // ── Walk up from entry to root via parent traversal ────────────────────

  if (entry.path_to_root.empty()) {
    return innermost;
  }

  auto const vars_before_parents = seen_vars_.size();
  EClassReg current_eclass_reg = eclass_reg;
  std::optional<EClassReg> verify_child_reg;

  for (auto const &[step_idx, step] : entry.path_to_root | std::views::reverse | std::views::enumerate) {
    auto const &step_sym = std::get<SymbolWithChildren<Symbol>>(pattern[step.node_id]);
    auto child_idx = step.child_idx;
    auto parent_reg = alloc_enode_reg();
    auto sym_idx = get_symbol_index(step_sym.sym);

    // Iterate parents of current e-class.
    auto loop_pos = emit_iter_loop(Instruction::iter_parents(parent_reg, current_eclass_reg, innermost),
                                   Instruction::next_parent(parent_reg, innermost))
                        .loop;

    // Check symbol and arity.
    emit(Instruction::check_symbol(parent_reg, sym_idx, loop_pos));
    emit(Instruction::check_arity(parent_reg, static_cast<uint8_t>(step_sym.children.size()), loop_pos));

    // Verify the shared variable is at the expected child index (only needed when arity > 1).
    if (step_sym.children.size() > 1) {
      if (!verify_child_reg) {
        verify_child_reg = alloc_eclass_reg();
      }
      emit(Instruction::load_child(*verify_child_reg, parent_reg, static_cast<uint8_t>(child_idx)));
      emit(Instruction::check_eclass_eq(*verify_child_reg, current_eclass_reg, loop_pos));
    }

    // Process sibling children (backtrack to this level's loop, not previous level).
    emit_children(pattern, step_sym, parent_reg, loop_pos, child_idx);

    bool is_outermost_step = (step_idx + 1 == entry.path_to_root.size());
    auto binding = pattern.binding_for(step.node_id);

    // Move up: get e-class of this parent for next step or binding.
    if (binding || !is_outermost_step) {
      auto next_eclass_reg = alloc_eclass_reg();
      emit(Instruction::get_enode_eclass(next_eclass_reg, parent_reg));
      current_eclass_reg = next_eclass_reg;
    }

    // Bind this node if it has a binding.
    if (binding) {
      auto slot = get_slot(*binding);
      emit_var_binding(*binding, current_eclass_reg, loop_pos);
      // MarkSeen trampoline: when next step exhausts, mark this slot before
      // backtracking to this step's NextParent.
      auto skip_addr = emit(Instruction::jmp(InstrAddr{0}));
      auto mark_addr = emit(Instruction::mark_seen(slot));
      emit(Instruction::jmp(loop_pos));
      patch_target(skip_addr, current_addr());
      innermost = mark_addr;
    } else {
      innermost = loop_pos;
    }
  }

  // If no new bindings were added during parent traversal, the output tuple
  // is fully determined before the parent chain. Multiple parent paths from the
  // same eclass would yield duplicates — skip them and advance to the next eclass.
  // For shared-var entries exhaust_bt == backtrack, so this is a no-op.
  if (seen_vars_.size() == vars_before_parents) {
    return exhaust_bt;
  }

  return innermost;
}

// ============================================================================
// Emission: node-level
// ============================================================================

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_node(Pattern<Symbol> const &pattern, PatternNodeId node_id, EClassReg eclass_reg,
                                        InstrAddr backtrack) -> InstrAddr {
  return std::visit(utils::Overloaded{
                        [&](Wildcard) { return backtrack; },
                        [&](PatternVar const &var) { return emit_var_binding(var, eclass_reg, backtrack); },
                        [&](SymbolWithChildren<Symbol> const &sym) {
                          return emit_symbol_node(pattern, node_id, sym, eclass_reg, backtrack);
                        },
                    },
                    pattern[node_id]);
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_symbol_node(Pattern<Symbol> const &pattern, PatternNodeId node_id,
                                               SymbolWithChildren<Symbol> const &sym, EClassReg eclass_reg,
                                               InstrAddr backtrack, std::optional<InstrAddr> bind_backtrack)
    -> InstrAddr {
  // Bind this node BEFORE iteration if it has a binding.
  // The e-class is the same for all e-nodes in the iteration, so we bind once.
  // BindSlot backtracks directly to outer NextX (skip MarkSeen - value already seen).
  // Inner exhaustion backtracks through MarkSeen (the original backtrack).
  if (auto binding = pattern.binding_for(node_id)) {
    emit_var_binding(*binding, eclass_reg, bind_backtrack.value_or(backtrack));
  }

  return emit_symbol_structure(pattern, sym, eclass_reg, backtrack).innermost;
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_symbol_eclass_iter(SymbolWithChildren<Symbol> const &sym,
                                                      std::optional<PatternVar> binding, InstrAddr backtrack)
    -> EClassSetup {
  auto sym_idx = get_symbol_index(sym.sym);
  auto eclass_reg = alloc_eclass_reg();
  auto mark = binding.transform([this](auto var) { return get_slot(var); });
  auto [exhaust, loop] = emit_iter_loop(Instruction::iter_symbol_eclasses(eclass_reg, sym_idx, backtrack),
                                        Instruction::next_symbol_eclass(eclass_reg, backtrack),
                                        mark);

  if (binding) {
    emit_var_binding(*binding, eclass_reg, loop);
  }

  return {eclass_reg, exhaust};
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_symbol_structure(Pattern<Symbol> const &pattern,
                                                    SymbolWithChildren<Symbol> const &sym, EClassReg eclass_reg,
                                                    InstrAddr backtrack) -> SymbolStructure {
  auto sym_idx = get_symbol_index(sym.sym);
  auto enode_reg = alloc_enode_reg();

  auto loop_pos =
      emit_iter_loop(Instruction::iter_enodes(enode_reg, eclass_reg), Instruction::next_enode(enode_reg, backtrack))
          .loop;

  emit(Instruction::check_symbol(enode_reg, sym_idx, loop_pos));
  emit(Instruction::check_arity(enode_reg, static_cast<uint8_t>(sym.children.size()), loop_pos));

  // For leaf symbols (no children), existence check is sufficient - after matching
  // one e-node, backtrack to parent instead of trying more e-nodes in this e-class.
  // This prevents duplicate matches when an e-class has multiple e-nodes with same symbol.
  if (sym.children.empty()) {
    return {backtrack, backtrack};
  }

  auto innermost = emit_children(pattern, sym, enode_reg, loop_pos);
  return {innermost, loop_pos};
}

template <typename Symbol>
auto PatternCompiler<Symbol>::emit_children(Pattern<Symbol> const &pattern, SymbolWithChildren<Symbol> const &sym,
                                            ENodeReg enode_reg, InstrAddr backtrack,
                                            std::optional<std::size_t> skip_child_idx) -> InstrAddr {
  // Phase 1: LoadChild for all non-wildcard children. Non-nesting nodes
  // (PatternVar) are processed immediately via emit_node. Symbol children
  // are deferred to phase 2 so their IterENodes loops don't swallow siblings.
  struct DeferredChild {
    PatternNodeId node_id;
    EClassReg child_reg;
  };

  // Stack-allocated for typical arities (≤4 children). Heap fallback for larger.
  boost::container::small_vector<DeferredChild, 4> deferred;
  InstrAddr innermost = backtrack;
  for (std::size_t i = 0; i < sym.children.size(); ++i) {
    if (skip_child_idx && i == *skip_child_idx) continue;
    auto const child_id = sym.children[i];
    std::visit(utils::Overloaded{
                   [](Wildcard) {},  // No register needed for wildcards
                   [&](PatternVar const &) {
                     // Non-nesting: LoadChild + process immediately at enode level.
                     auto child_reg = alloc_eclass_reg();
                     emit(Instruction::load_child(child_reg, enode_reg, static_cast<uint8_t>(i)));
                     innermost = emit_node(pattern, child_id, child_reg, innermost);
                   },
                   [&](SymbolWithChildren<Symbol> const &) {
                     // Nesting: LoadChild now, defer IterENodes to phase 2.
                     auto child_reg = alloc_eclass_reg();
                     emit(Instruction::load_child(child_reg, enode_reg, static_cast<uint8_t>(i)));
                     deferred.push_back({child_id, child_reg});
                   },
               },
               pattern[child_id]);
  }

  // Phase 2: Process deferred symbol children (creates nested IterENodes loops).
  for (auto const &[node_id, child_reg] : deferred) {
    innermost = emit_node(pattern, node_id, child_reg, innermost);
  }
  return innermost;
}

// ============================================================================
// Emission: utilities
// ============================================================================

template <typename Symbol>
auto PatternCompiler<Symbol>::get_symbol_index(Symbol const &sym) -> uint8_t {
  // TODO: maybe a strong type (so we know uint8_t is in relation to symbols_)
  //       better name than `get_symbol_index`
  //       Not a linear search
  for (std::size_t i = 0; i < symbols_.size(); ++i) {
    if (symbols_[i] == sym) return static_cast<uint8_t>(i);
  }
  symbols_.push_back(sym);
  return static_cast<uint8_t>(symbols_.size() - 1);
}

}  // namespace memgraph::planner::core::pattern::vm
