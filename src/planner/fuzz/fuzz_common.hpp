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

// ============================================================================
// Common Infrastructure for Fuzz Testing
// ============================================================================
//
// This header provides shared types and utilities for:
//   - fuzz_pattern_vm.cpp: Pattern matching fuzzer (VM vs egglog)
//   - fuzz_egraph.cpp: E-graph operations fuzzer
//   - corpus_benchmark.cpp: Corpus benchmark tool
//   - corpus_profile.cpp: Corpus profiling tool
// ============================================================================

#include <algorithm>
#include <cstdint>
#include <format>
#include <functional>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include "planner/pattern/pattern.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::fuzz {

using pattern::Pattern;
using pattern::PatternNodeId;
using pattern::PatternVar;

// ============================================================================
// Fuzz Symbols
// ============================================================================
//
// Simple symbol set for fuzz testing:
//   - Leaf symbols (A-E): 0-ary nodes with a disambiguator
//   - Compound symbols (F, G, H): 1-ary nodes
//   - Binary operators (Plus, Mul): 2-ary nodes
//   - Ternary operator: 3-ary node

enum class FuzzSymbol : uint8_t {
  // Leaf symbols (0-4)
  A = 0,
  B = 1,
  C = 2,
  D = 3,
  E = 4,
  // Compound symbols (10-15)
  F = 10,
  G = 11,
  H = 12,
  Plus = 13,
  Mul = 14,
  Ternary = 15,
};

/// Empty analysis for fuzz testing (no analysis data needed)
struct FuzzAnalysis {};

// ============================================================================
// Symbol Utilities
// ============================================================================

inline auto symbol_name(FuzzSymbol sym) -> std::string_view {
  switch (sym) {
    case FuzzSymbol::A:
      return "A";
    case FuzzSymbol::B:
      return "B";
    case FuzzSymbol::C:
      return "C";
    case FuzzSymbol::D:
      return "D";
    case FuzzSymbol::E:
      return "E";
    case FuzzSymbol::F:
      return "F";
    case FuzzSymbol::G:
      return "G";
    case FuzzSymbol::H:
      return "H";
    case FuzzSymbol::Plus:
      return "Plus";
    case FuzzSymbol::Mul:
      return "Mul";
    case FuzzSymbol::Ternary:
      return "Ternary";
    default:
      return "Unknown";
  }
}

inline auto is_leaf_symbol(FuzzSymbol sym) -> bool { return static_cast<uint8_t>(sym) < 10; }

}  // namespace memgraph::planner::core::fuzz

/// std::formatter for FuzzSymbol (required by tracer disassembly)
template <>
struct std::formatter<memgraph::planner::core::fuzz::FuzzSymbol> : std::formatter<std::string_view> {
  auto format(memgraph::planner::core::fuzz::FuzzSymbol sym, std::format_context &ctx) const {
    return std::formatter<std::string_view>::format(memgraph::planner::core::fuzz::symbol_name(sym), ctx);
  }
};

namespace memgraph::planner::core::fuzz {

/// Dump e-graph structure showing e-nodes per e-class
template <typename Analysis>
void dump_egraph(EGraph<FuzzSymbol, Analysis> const &egraph, std::ostream &out) {
  out << "E-graph dump (" << egraph.num_classes() << " classes, " << egraph.num_nodes() << " nodes):\n";
  for (auto const &[eclass_id, eclass] : egraph.canonical_classes()) {
    out << "  E-class " << eclass_id << " (" << eclass.size() << " nodes):\n";
    for (auto enode_id : eclass.nodes()) {
      auto const &enode = egraph.get_enode(enode_id);
      out << "    " << enode_id << ": " << symbol_name(enode.symbol());
      if (enode.arity() > 0) {
        out << "(";
        for (std::size_t i = 0; i < enode.arity(); ++i) {
          if (i > 0) out << ", ";
          out << enode.children()[i];
        }
        out << ")";
      } else if (enode.disambiguator() != 0) {
        out << "[D" << enode.disambiguator() << "]";
      }
      out << "\n";
    }
  }
}

// ============================================================================
// Pattern AST
// ============================================================================
//
// A simple AST representation for patterns, used to generate both:
//   - Memgraph Pattern<FuzzSymbol> objects
//   - Egglog pattern strings (for oracle verification)

struct PatternAST;
using PatternASTPtr = std::shared_ptr<PatternAST>;

struct PatternAST {
  struct Variable {
    uint8_t id;
  };

  struct SymbolNode {
    FuzzSymbol sym;
    std::vector<PatternASTPtr> children;
  };

  std::variant<Variable, SymbolNode> node;

  static auto make_var(uint8_t id) -> PatternASTPtr {
    auto p = std::make_shared<PatternAST>();
    p->node = Variable{id};
    return p;
  }

  static auto make_sym(FuzzSymbol sym, std::vector<PatternASTPtr> children = {}) -> PatternASTPtr {
    auto p = std::make_shared<PatternAST>();
    p->node = SymbolNode{sym, std::move(children)};
    return p;
  }
};

// ============================================================================
// Pattern Conversion Functions
// ============================================================================

/// Convert pattern AST to memgraph Pattern<FuzzSymbol>
inline auto pattern_to_memgraph(PatternASTPtr const &ast) -> Pattern<FuzzSymbol> {
  auto builder = Pattern<FuzzSymbol>::Builder{};

  std::function<PatternNodeId(PatternASTPtr const &)> build = [&](PatternASTPtr const &p) -> PatternNodeId {
    return std::visit(
        [&](auto const &n) -> PatternNodeId {
          using T = std::decay_t<decltype(n)>;
          if constexpr (std::is_same_v<T, PatternAST::Variable>) {
            return builder.var(PatternVar{n.id});
          } else {
            auto const &sym_node = n;
            if (sym_node.children.empty()) {
              // Leaf node - match any disambiguator
              return builder.sym(sym_node.sym);
            } else {
              utils::small_vector<PatternNodeId> child_ids;
              for (auto const &child : sym_node.children) {
                child_ids.push_back(build(child));
              }
              return builder.sym(sym_node.sym, std::move(child_ids));
            }
          }
        },
        p->node);
  };

  build(ast);
  return std::move(builder).build();
}

/// Convert pattern AST to human-readable string
inline auto pattern_to_string(PatternASTPtr const &ast) -> std::string {
  return std::visit(
      [&](auto const &n) -> std::string {
        using T = std::decay_t<decltype(n)>;
        if constexpr (std::is_same_v<T, PatternAST::Variable>) {
          return fmt::format("?v{}", n.id);
        } else {
          auto const &sym_node = n;
          std::string result = fmt::format("({}", symbol_name(sym_node.sym));
          for (auto const &child : sym_node.children) {
            result += " " + pattern_to_string(child);
          }
          result += ")";
          return result;
        }
      },
      ast->node);
}

/// Convert pattern AST to egglog pattern string.
/// leaf_counter is incremented for each leaf symbol encountered so that every
/// leaf occurrence gets a unique disambiguator variable (?d0, ?d1, ...) rather
/// than sharing a single ?d.
inline auto pattern_to_egglog(PatternASTPtr const &ast, std::vector<std::string> &out_vars, int &leaf_counter)
    -> std::string {
  return std::visit(
      [&](auto const &n) -> std::string {
        using T = std::decay_t<decltype(n)>;
        if constexpr (std::is_same_v<T, PatternAST::Variable>) {
          auto var_name = fmt::format("?v{}", n.id);
          // Track unique vars
          if (std::ranges::find(out_vars, var_name) == out_vars.end()) {
            out_vars.push_back(var_name);
          }
          return var_name;
        } else {
          auto const &sym_node = n;
          std::string result = fmt::format("({}", symbol_name(sym_node.sym));
          if (is_leaf_symbol(sym_node.sym)) {
            // Each leaf occurrence gets its own disambiguator variable
            auto d_var = fmt::format("?d{}", leaf_counter++);
            result += " " + d_var;
            out_vars.push_back(d_var);
          } else {
            for (auto const &child : sym_node.children) {
              result += " " + pattern_to_egglog(child, out_vars, leaf_counter);
            }
          }
          result += ")";
          return result;
        }
      },
      ast->node);
}

// ============================================================================
// Pattern Generator
// ============================================================================
//
// Generates random pattern ASTs from fuzz input data.
// Supports various pattern shapes including:
//   - Simple variables
//   - Leaf symbol patterns
//   - Unary/binary/ternary compound patterns
//   - Repeated variable patterns (same var at multiple positions)
//   - Nested patterns with shared variables

class PatternGenerator {
 public:
  /// Set the maximum recursion depth allowed for generated patterns (2–6).
  /// Called once per top-level generate() call; the value is consumed from the
  /// fuzz stream so the fuzzer can explore different depths.
  void set_max_depth(uint8_t raw) {
    // Map raw byte to range [2, 6]
    max_depth_ = 2 + (raw % 5);
  }

  auto generate(uint8_t const *data, size_t &pos, size_t size, int depth = 0) -> PatternASTPtr {
    if (pos >= size || depth > max_depth_) {
      // Generate a simple variable
      return PatternAST::make_var(next_var_id_++);
    }

    uint8_t type = data[pos++] % 10;

    switch (type) {
      case 0:
      case 1: {
        // Variable
        uint8_t var_id;
        if (pos < size && !used_vars_.empty() && (data[pos] % 3) == 0) {
          // Reuse existing variable (repeated var pattern)
          var_id = used_vars_[data[pos++] % used_vars_.size()];
        } else {
          var_id = next_var_id_++;
          used_vars_.push_back(var_id);
        }
        return PatternAST::make_var(var_id);
      }

      case 2: {
        // Leaf symbol pattern (A, B, C, D, E)
        if (pos >= size) return PatternAST::make_var(next_var_id_++);
        auto sym = static_cast<FuzzSymbol>(data[pos++] % 5);
        return PatternAST::make_sym(sym);
      }

      case 3: {
        // Unary pattern: F(?x), G(?x), H(?x)
        if (pos >= size) return PatternAST::make_var(next_var_id_++);
        auto sym = static_cast<FuzzSymbol>(10 + (data[pos++] % 3));  // F, G, H
        auto child = generate(data, pos, size, depth + 1);
        return PatternAST::make_sym(sym, {child});
      }

      case 4: {
        // Binary pattern: Plus(?x, ?y), Mul(?x, ?y)
        if (pos >= size) return PatternAST::make_var(next_var_id_++);
        auto sym = static_cast<FuzzSymbol>(13 + (data[pos++] % 2));  // Plus, Mul
        auto left = generate(data, pos, size, depth + 1);
        auto right = generate(data, pos, size, depth + 1);
        return PatternAST::make_sym(sym, {left, right});
      }

      case 5: {
        // Repeated variable pattern: F(?x), where ?x already used
        if (used_vars_.empty()) {
          used_vars_.push_back(next_var_id_++);
        }
        auto var_id = used_vars_[0];  // Reuse first var
        if (pos >= size) return PatternAST::make_var(var_id);
        auto sym = static_cast<FuzzSymbol>(10 + (data[pos++] % 3));
        return PatternAST::make_sym(sym, {PatternAST::make_var(var_id)});
      }

      case 6: {
        // Ternary pattern: Ternary(?x, ?y, ?z)
        auto c0 = generate(data, pos, size, depth + 1);
        auto c1 = generate(data, pos, size, depth + 1);
        auto c2 = generate(data, pos, size, depth + 1);
        return PatternAST::make_sym(FuzzSymbol::Ternary, {c0, c1, c2});
      }

      case 7: {
        // Nested repeated variable: Plus(?x, Plus(?x, ?y)) or Mul(?x, Mul(?x, ?y))
        // The same variable appears at two different depths — tests equality constraints
        // across nested structures.
        if (pos >= size) return PatternAST::make_var(next_var_id_++);
        auto sym = static_cast<FuzzSymbol>(13 + (data[pos++] % 2));  // Plus or Mul
        uint8_t shared_var = next_var_id_++;
        used_vars_.push_back(shared_var);
        auto inner_right = generate(data, pos, size, depth + 2);
        auto inner = PatternAST::make_sym(sym, {PatternAST::make_var(shared_var), inner_right});
        return PatternAST::make_sym(sym, {PatternAST::make_var(shared_var), inner});
      }

      case 8: {
        // Same leaf in both children: Plus(A, A) or Mul(B, B)
        // Tests matching when both children must be the same leaf symbol.
        if (pos >= size) return PatternAST::make_var(next_var_id_++);
        auto outer_sym = static_cast<FuzzSymbol>(13 + (data[pos++] % 2));  // Plus or Mul
        if (pos >= size) return PatternAST::make_var(next_var_id_++);
        auto leaf_sym = static_cast<FuzzSymbol>(data[pos++] % 5);  // A-E
        return PatternAST::make_sym(outer_sym, {PatternAST::make_sym(leaf_sym), PatternAST::make_sym(leaf_sym)});
      }

      case 9: {
        // DAG pattern: Plus(F(?x), G(?x)) — same variable in disjoint subtrees
        // Tests that the matcher correctly handles shared variables across branches.
        if (used_vars_.empty()) {
          used_vars_.push_back(next_var_id_++);
        }
        auto shared_var = used_vars_[0];
        if (pos >= size) return PatternAST::make_var(shared_var);
        auto outer_sym = static_cast<FuzzSymbol>(13 + (data[pos++] % 2));  // Plus or Mul
        // Left branch: F/G/H(?x)
        auto left_wrapper = static_cast<FuzzSymbol>(10 + ((pos < size ? data[pos++] : 0) % 3));
        // Right branch: F/G/H(?x) (possibly same, possibly different wrapper)
        auto right_wrapper = static_cast<FuzzSymbol>(10 + ((pos < size ? data[pos++] : 0) % 3));
        auto left = PatternAST::make_sym(left_wrapper, {PatternAST::make_var(shared_var)});
        auto right = PatternAST::make_sym(right_wrapper, {PatternAST::make_var(shared_var)});
        return PatternAST::make_sym(outer_sym, {left, right});
      }

      default:
        return PatternAST::make_var(next_var_id_++);
    }
  }

  void reset() {
    next_var_id_ = 0;
    used_vars_.clear();
    max_depth_ = 3;  // restored to default on each reset
  }

  /// Get used vars for sharing across patterns
  [[nodiscard]] auto get_used_vars() const -> std::vector<uint8_t> const & { return used_vars_; }

  /// Force a specific set of vars as "already used" for shared variable generation
  void set_used_vars(std::vector<uint8_t> vars) { used_vars_ = std::move(vars); }

 private:
  uint8_t next_var_id_ = 0;
  int max_depth_ = 3;
  std::vector<uint8_t> used_vars_;
};

// ============================================================================
// Multi-Pattern Generator
// ============================================================================
//
// Generates 1-3 patterns that may share variables across them.
// Used for testing multi-pattern joins in the matcher.

class MultiPatternGenerator {
 public:
  struct MultiPatternResult {
    std::vector<PatternASTPtr> patterns;
    std::set<uint8_t> shared_vars;  // Variables that appear in multiple patterns
  };

  /// Generate 1-3 patterns with shared variables
  auto generate(uint8_t const *data, size_t &pos, size_t size) -> MultiPatternResult {
    MultiPatternResult result;
    if (pos >= size) return result;

    // Determine number of patterns (1-3)
    uint8_t num_patterns = 1 + (data[pos++] % 3);

    // Set fuzz-driven max depth (2–6) for all patterns in this batch
    if (pos < size) {
      pattern_gen_.set_max_depth(data[pos++]);
    }

    // Generate first pattern
    pattern_gen_.reset();
    result.patterns.push_back(pattern_gen_.generate(data, pos, size));
    auto shared_vars = pattern_gen_.get_used_vars();

    // Generate remaining patterns, sharing some variables from previous patterns
    for (uint8_t i = 1; i < num_patterns && pos < size; ++i) {
      pattern_gen_.reset();

      // Force some vars to be shared (use vars from previous patterns)
      if (!shared_vars.empty()) {
        // Pick 1-2 vars to share
        uint8_t num_shared = 1 + (pos < size ? data[pos++] % 2 : 0);
        std::vector<uint8_t> to_share;
        for (uint8_t j = 0; j < num_shared && j < shared_vars.size(); ++j) {
          to_share.push_back(shared_vars[j % shared_vars.size()]);
        }
        pattern_gen_.set_used_vars(to_share);

        // Track which vars are actually shared
        for (auto v : to_share) {
          result.shared_vars.insert(v);
        }
      }

      result.patterns.push_back(pattern_gen_.generate(data, pos, size));

      // Update shared_vars pool with new vars
      for (auto v : pattern_gen_.get_used_vars()) {
        if (std::ranges::find(shared_vars, v) == shared_vars.end()) {
          shared_vars.push_back(v);
        }
      }
    }

    return result;
  }

 private:
  PatternGenerator pattern_gen_;
};

}  // namespace memgraph::planner::core::fuzz
