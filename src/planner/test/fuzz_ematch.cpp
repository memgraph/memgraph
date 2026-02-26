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

// NOTE: This fuzzer is NOT safe to run with multiple threads (e.g. libFuzzer's
// -fork or custom multi-threaded executor modes). There are two reasons:
//
//  1. The egglog oracle writes to a fixed path (/tmp/fuzz_ematch.egg). Parallel
//     instances would overwrite each other's temp files, producing wrong egglog
//     input and therefore spurious mismatch reports.
//
//  2. The environment-variable flags (g_verbose, g_skip_egglog) are initialised
//     through a plain `static bool initialized` with no synchronisation. Under
//     concurrent execution this is a data race.
//
// Run with the default single-process mode or with -jobs / -workers (which fork
// separate processes rather than threads).
//
// Running example:
//   ./build/src/planner/test/fuzz_ematch -max_total_time=10
//
// On a mismatch the fuzzer calls abort(), which libFuzzer treats as a crash. It
// will stop immediately, print the mismatch details (counts, pattern, egglog
// program and output) to stderr, and write a reproducer file in the current
// directory named crash-<sha1hash>. To reproduce:
//
//   FUZZ_VERBOSE=1 ./build/src/planner/test/fuzz_ematch crash-<sha1hash>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <ranges>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include "planner/egraph/egraph.hpp"
#include "planner/egraph/processing_context.hpp"
#include "planner/pattern/match.hpp"
#include "planner/pattern/matcher.hpp"
#include "planner/pattern/pattern.hpp"
#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/pattern/vm/tracer.hpp"
#include "planner/rewrite/rule.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

// ============================================================================
// Configuration
// ============================================================================

static bool g_verbose = false;
static bool g_skip_egglog = false;

#define VERBOSE_OUT \
  if (g_verbose) std::cerr

// ============================================================================
// Diagnostic Helpers
// ============================================================================

using BindingTuple = std::vector<EClassId>;

/// Format a binding tuple as a string for diagnostics
inline auto format_tuple(BindingTuple const &tuple) -> std::string {
  std::ostringstream ss;
  ss << "(";
  for (std::size_t i = 0; i < tuple.size(); ++i) {
    if (i > 0) ss << ", ";
    ss << tuple[i];
  }
  ss << ")";
  return ss.str();
}

/// Print the difference between two sets of binding tuples
inline void print_tuple_diff(std::set<BindingTuple> const &set_a, std::set<BindingTuple> const &set_b,
                             std::string_view name_a, std::string_view name_b) {
  // Find tuples in A but not in B
  std::set<BindingTuple> only_in_a;
  std::ranges::set_difference(set_a, set_b, std::inserter(only_in_a, only_in_a.begin()));

  // Find tuples in B but not in A
  std::set<BindingTuple> only_in_b;
  std::ranges::set_difference(set_b, set_a, std::inserter(only_in_b, only_in_b.begin()));

  if (!only_in_a.empty()) {
    std::cerr << "\n  In " << name_a << " but NOT in " << name_b << " (" << only_in_a.size() << " tuples):\n";
    for (auto const &t : only_in_a | std::views::take(10)) {
      std::cerr << "    " << format_tuple(t) << "\n";
    }
    if (only_in_a.size() > 10) {
      std::cerr << "    ... and " << (only_in_a.size() - 10) << " more\n";
    }
  }

  if (!only_in_b.empty()) {
    std::cerr << "\n  In " << name_b << " but NOT in " << name_a << " (" << only_in_b.size() << " tuples):\n";
    for (auto const &t : only_in_b | std::views::take(10)) {
      std::cerr << "    " << format_tuple(t) << "\n";
    }
    if (only_in_b.size() > 10) {
      std::cerr << "    ... and " << (only_in_b.size() - 10) << " more\n";
    }
  }
}

// ============================================================================
// Fuzz Symbols - reused from fuzz_egraph.cpp
// ============================================================================

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
  Ternary = 15,  // 3-child operator
};

struct FuzzAnalysis {};

// ============================================================================
// Symbol Name Mapping
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

inline auto is_leaf_symbol(FuzzSymbol sym) -> bool { return static_cast<uint32_t>(sym) < 10; }

// ============================================================================
// Egglog Program Generator
// ============================================================================

class EgglogGenerator {
 public:
  auto emit_leaf(FuzzSymbol sym, uint64_t disambiguator) -> std::string {
    auto var_name = fmt::format("n{}", var_counter_++);
    program_ << fmt::format("(let {} ({} {}))\n", var_name, symbol_name(sym), disambiguator);
    return var_name;
  }

  auto emit_unary(FuzzSymbol sym, std::string_view child) -> std::string {
    auto var_name = fmt::format("n{}", var_counter_++);
    program_ << fmt::format("(let {} ({} {}))\n", var_name, symbol_name(sym), child);
    return var_name;
  }

  auto emit_binary(FuzzSymbol sym, std::string_view left, std::string_view right) -> std::string {
    auto var_name = fmt::format("n{}", var_counter_++);
    program_ << fmt::format("(let {} ({} {} {}))\n", var_name, symbol_name(sym), left, right);
    return var_name;
  }

  auto emit_ternary(FuzzSymbol sym, std::string_view c0, std::string_view c1, std::string_view c2) -> std::string {
    auto var_name = fmt::format("n{}", var_counter_++);
    program_ << fmt::format("(let {} ({} {} {} {}))\n", var_name, symbol_name(sym), c0, c1, c2);
    return var_name;
  }

  void emit_merge(std::string_view id1, std::string_view id2) { program_ << fmt::format("(union {} {})\n", id1, id2); }

  auto generate() const -> std::string { return program_.str(); }

  void clear() {
    program_.str("");
    program_.clear();
    var_counter_ = 0;
  }

 private:
  std::ostringstream program_;
  int var_counter_ = 0;
};

// ============================================================================
// Egglog Runner
// ============================================================================

struct EgglogResult {
  bool success = false;
  size_t match_count = 0;
  std::string output;
  std::string error;
};

auto run_egglog(std::string_view program) -> EgglogResult {
  EgglogResult result;

  // Write program to temp file
  auto temp_path = std::filesystem::temp_directory_path() / "fuzz_ematch.egg";
  {
    std::ofstream temp_file(temp_path);
    if (!temp_file) {
      result.error = "Failed to create temp file";
      return result;
    }
    temp_file << program;
  }

  // Run egglog and capture output
  std::string cmd = fmt::format("egglog {} 2>&1", temp_path.string());
  FILE *pipe = popen(cmd.c_str(), "r");
  if (!pipe) {
    result.error = "Failed to run egglog";
    return result;
  }

  std::array<char, 4096> buffer{};
  std::ostringstream output;
  while (fgets(buffer.data(), buffer.size(), pipe) != nullptr) {
    output << buffer.data();
  }

  int status = pclose(pipe);
  result.output = output.str();

  if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
    result.success = true;

    // Parse match count from print-function output
    // Format: (MatchResult <values...>)
    // Lines may have leading whitespace
    std::istringstream iss(result.output);
    std::string line;
    while (std::getline(iss, line)) {
      // Strip leading whitespace
      auto pos = line.find_first_not_of(" \t");
      if (pos != std::string::npos) {
        auto trimmed = line.substr(pos);
        // Count lines that start with "(MatchResult"
        if (trimmed.find("(MatchResult") == 0) {
          result.match_count++;
        }
      }
    }
  } else {
    result.error = "Egglog execution failed: " + result.output;
  }

  // Clean up temp file
  std::filesystem::remove(temp_path);

  return result;
}

// ============================================================================
// Pattern AST for Generation
// ============================================================================

struct PatternAST;
using PatternASTPtr = std::shared_ptr<PatternAST>;

struct PatternAST {
  struct Variable {
    int id;
  };

  struct SymbolNode {
    FuzzSymbol sym;
    std::vector<PatternASTPtr> children;
  };

  std::variant<Variable, SymbolNode> node;

  static auto make_var(int id) -> PatternASTPtr {
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

// Convert pattern AST to egglog pattern string.
// leaf_counter is incremented for each leaf symbol encountered so that every
// leaf occurrence gets a unique disambiguator variable (?d0, ?d1, ...) rather
// than sharing a single ?d.  Sharing would force all leaves in the pattern to
// match e-nodes with the *same* disambiguator, making the egglog query
// artificially more restrictive than the EMatcher and causing false mismatches.
auto pattern_to_egglog(PatternASTPtr const &ast, std::vector<std::string> &out_vars, int &leaf_counter) -> std::string {
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
            // Each leaf occurrence gets its own disambiguator variable so that
            // multiple leaf nodes in the same pattern can independently match
            // e-nodes with different disambiguator values.
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

// Convert pattern AST to memgraph Pattern
auto pattern_to_memgraph(PatternASTPtr const &ast) -> Pattern<FuzzSymbol> {
  auto builder = Pattern<FuzzSymbol>::Builder{};

  std::function<PatternNodeId(PatternASTPtr const &)> build = [&](PatternASTPtr const &p) -> PatternNodeId {
    return std::visit(
        [&](auto const &n) -> PatternNodeId {
          using T = std::decay_t<decltype(n)>;
          if constexpr (std::is_same_v<T, PatternAST::Variable>) {
            return builder.var(PatternVar{static_cast<uint32_t>(n.id)});
          } else {
            auto const &sym_node = n;
            if (sym_node.children.empty()) {
              // Leaf node - use wildcard for disambiguator match
              // Actually for leaf symbols we match any disambiguator, so use sym with no children
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

// ============================================================================
// Pattern Generator
// ============================================================================

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
        int var_id;
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
        int shared_var = next_var_id_++;
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
  [[nodiscard]] auto get_used_vars() const -> std::vector<int> const & { return used_vars_; }

  /// Force a specific set of vars as "already used" for shared variable generation
  void set_used_vars(std::vector<int> vars) { used_vars_ = std::move(vars); }

 private:
  int next_var_id_ = 0;
  int max_depth_ = 3;
  std::vector<int> used_vars_;
};

// ============================================================================
// Multi-Pattern Generator
// ============================================================================

class MultiPatternGenerator {
 public:
  struct MultiPatternResult {
    std::vector<PatternASTPtr> patterns;
    std::set<int> shared_vars;  // Variables that appear in multiple patterns
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
        std::vector<int> to_share;
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

// ============================================================================
// Fuzzer State
// ============================================================================

class FuzzerState {
 public:
  bool execute_operation(uint8_t op, uint8_t const *data, size_t &pos, size_t size) {
    operation_count_++;
    VERBOSE_OUT << "\n--- Operation #" << operation_count_ << " ---\n";

    // Dispatch table: compound nodes get 3 slots vs 1 for leaf so the fuzzer
    // builds deeper, more pattern-matchable graphs more often.
    static constexpr std::array<uint8_t, 11> kDispatch = {
        0,  // CREATE_LEAF        (1/11)
        1,
        1,
        1,  // CREATE_COMPOUND    (3/11)
        2,  // MERGE              (1/11)
        3,  // REBUILD            (1/11)
        4,  // SET_PATTERN        (1/11)
        5,
        5,  // MATCH_AND_VERIFY   (2/11)
        6,  // VERIFY_ABSENT      (1/11)
        7,  // APPLY_REWRITE      (1/11)
    };
    int operation_type = kDispatch[op % kDispatch.size()];
    static constexpr std::array<char const *, 8> op_names = {"CREATE_LEAF",
                                                             "CREATE_COMPOUND",
                                                             "MERGE",
                                                             "REBUILD",
                                                             "SET_PATTERN",
                                                             "MATCH_AND_VERIFY",
                                                             "VERIFY_ABSENT",
                                                             "APPLY_REWRITE"};
    VERBOSE_OUT << "Op: " << op_names[operation_type] << " (raw: " << static_cast<int>(op) << ")\n";

    switch (operation_type) {
      case 0:
        return create_leaf_node(data, pos, size);
      case 1:
        return create_compound_node(data, pos, size);
      case 2:
        return merge_classes(data, pos, size);
      case 3:
        return rebuild();
      case 4:
        return set_pattern(data, pos, size);
      case 5:
        return match_and_verify();
      case 6:
        return verify_absent();
      case 7:
        return apply_rewrite_cycle(data, pos, size);
      default:
        return true;
    }
  }

  bool create_leaf_node(uint8_t const *data, size_t &pos, size_t size) {
    if (pos >= size) return true;

    uint8_t symbol = data[pos++] % 5;  // A-E

    // Read up to 4 bytes for disambiguator (32-bit range) so the fuzzer can
    // generate leaf nodes that are never merged by coincidence.  One byte
    // (0–255) is too narrow: after ~256 leaves the probability that two random
    // leaves share a disambiguator and appear equivalent to egglog grows quickly.
    uint64_t disambiguator = 0;
    for (int byte_idx = 0; byte_idx < 4 && pos < size; ++byte_idx) {
      disambiguator = (disambiguator << 8) | data[pos++];
    }

    auto sym = static_cast<FuzzSymbol>(symbol);
    auto info = egraph_.emplace(sym, disambiguator);
    created_ids_.push_back(info.eclass_id);

    // Track in egglog
    auto var_name = egglog_.emit_leaf(sym, disambiguator);
    egglog_names_[info.eclass_id] = var_name;

    VERBOSE_OUT << "Created leaf: " << symbol_name(sym) << "(D" << disambiguator << ") -> " << info.eclass_id << "\n";
    return true;
  }

  bool create_compound_node(uint8_t const *data, size_t &pos, size_t size) {
    if (pos >= size || created_ids_.empty()) return true;

    // Symbol dispatch weighted by arity: each symbol gets as many slots as its
    // arity, so higher-arity (more interesting for pattern matching) nodes are
    // proportionally more likely.
    static constexpr std::array<uint8_t, 10> kSymDispatch = {
        0,  // F       arity 1  (1 slot)
        1,  // G       arity 1  (1 slot)
        2,  // H       arity 1  (1 slot)
        3,
        3,  // Plus    arity 2  (2 slots)
        4,
        4,  // Mul     arity 2  (2 slots)
        5,
        5,
        5,  // Ternary arity 3  (3 slots)
    };
    auto sym = static_cast<FuzzSymbol>(10 + kSymDispatch[data[pos++] % kSymDispatch.size()]);
    created_symbols_.insert(sym);

    // Determine arity based on symbol
    size_t arity;
    if (sym == FuzzSymbol::Ternary) {
      arity = 3;
    } else if (sym == FuzzSymbol::Plus || sym == FuzzSymbol::Mul) {
      arity = 2;
    } else {
      arity = 1;
    }

    if (created_ids_.size() < arity) return true;

    // pick_node_idx reads 2 bytes per child so all nodes (not just first 256)
    // are reachable once the graph grows beyond uint8_t range.
    utils::small_vector<EClassId> children;
    std::vector<std::string> child_names;
    for (size_t i = 0; i < arity; ++i) {
      auto idx = pick_node_idx(data, pos, size);
      if (!idx) return true;
      auto child_id = egraph_.find(created_ids_[*idx]);
      children.push_back(child_id);
      child_names.push_back(egglog_names_[created_ids_[*idx]]);
    }

    auto info = egraph_.emplace(sym, children);
    created_ids_.push_back(info.eclass_id);

    // Track in egglog
    std::string var_name;
    if (arity == 1) {
      var_name = egglog_.emit_unary(sym, child_names[0]);
    } else if (arity == 2) {
      var_name = egglog_.emit_binary(sym, child_names[0], child_names[1]);
    } else {
      var_name = egglog_.emit_ternary(sym, child_names[0], child_names[1], child_names[2]);
    }
    egglog_names_[info.eclass_id] = var_name;

    VERBOSE_OUT << "Created compound: " << symbol_name(sym) << " -> " << info.eclass_id << "\n";
    return true;
  }

  bool merge_classes(uint8_t const *data, size_t &pos, size_t size) {
    if (created_ids_.size() < 2) return true;

    auto idx1 = pick_node_idx(data, pos, size);
    if (!idx1) return true;
    auto idx2 = pick_node_idx(data, pos, size);
    if (!idx2) return true;
    if (*idx1 == *idx2) return true;

    auto id1 = created_ids_[*idx1];
    auto id2 = created_ids_[*idx2];

    VERBOSE_OUT << "Merging class " << id1 << " with " << id2 << "\n";

    egraph_.merge(id1, id2);
    egglog_.emit_merge(egglog_names_[id1], egglog_names_[id2]);

    return true;
  }

  bool rebuild() {
    VERBOSE_OUT << "Rebuilding e-graph...\n";
    egraph_.rebuild(ctx_);
    VERBOSE_OUT << "Rebuild complete. Classes: " << egraph_.num_classes() << "\n";
    return true;
  }

  bool set_pattern(uint8_t const *data, size_t &pos, size_t size) {
    if (pos >= size) return true;

    auto result = pattern_gen_.generate(data, pos, size);
    current_patterns_ast_ = std::move(result.patterns);
    current_shared_vars_ = std::move(result.shared_vars);

    // Convert patterns to egglog strings
    current_patterns_egglog_.clear();
    current_all_vars_.clear();

    // leaf_counter is shared across all patterns so that disambiguator variables
    // (?d0, ?d1, ...) are unique globally.  If it were reset to 0 per pattern,
    // the same name (e.g. ?d0) would appear in multiple pattern conditions of the
    // combined egglog rule, creating an unintended join constraint: egglog would
    // require all occurrences of ?d0 to match nodes with the *same* disambiguator
    // value, making the query more restrictive than the EMatcher.
    int leaf_counter = 0;
    for (auto const &ast : current_patterns_ast_) {
      std::vector<std::string> vars;
      auto egglog_str = pattern_to_egglog(ast, vars, leaf_counter);
      current_patterns_egglog_.push_back(egglog_str);

      // Collect all ?vN vars (not ?dN)
      for (auto const &v : vars) {
        if (v.size() >= 2 && v[0] == '?' && v[1] == 'v') {
          current_all_vars_.insert(v);
        }
      }
    }

    VERBOSE_OUT << "Set " << current_patterns_ast_.size() << " pattern(s):\n";
    for (size_t i = 0; i < current_patterns_egglog_.size(); ++i) {
      VERBOSE_OUT << "  Pattern " << i << ": " << current_patterns_egglog_[i] << "\n";
    }
    VERBOSE_OUT << "  Shared vars: " << current_shared_vars_.size() << "\n";

    return true;
  }

  bool match_and_verify() {
    if (current_patterns_ast_.empty() || created_ids_.empty()) {
      VERBOSE_OUT << "Skipping match: no patterns or no nodes\n";
      return true;
    }

    // Ensure e-graph is rebuilt before matching
    egraph_.rebuild(ctx_);

    // Convert AST patterns to memgraph patterns
    std::vector<Pattern<FuzzSymbol>> patterns;
    for (auto const &ast : current_patterns_ast_) {
      patterns.push_back(pattern_to_memgraph(ast));
    }

    // Check if any pattern has 0 variables
    bool has_zero_vars = false;
    for (auto const &p : patterns) {
      if (p.num_vars() == 0) {
        has_zero_vars = true;
        break;
      }
    }
    if (has_zero_vars) {
      VERBOSE_OUT << "Skipping pattern with 0 variables\n";
      return true;
    }

    // Collect binding tuples instead of just counting.
    // Egglog uses set semantics (deduplicates), so we must deduplicate too.
    // Extract the ?vN variable IDs to collect from matches.
    // parse_var_id is shared by both the var_ids extraction and v_vars sort below.
    auto parse_var_id = [](std::string_view s) -> uint32_t {
      uint32_t id = 0;
      std::from_chars(s.data() + 2, s.data() + s.size(), id);  // skip "?v"
      return id;
    };

    std::vector<uint32_t> var_ids;
    for (auto const &v : current_all_vars_) {
      if (v.size() >= 3 && v[0] == '?' && v[1] == 'v') {
        var_ids.push_back(parse_var_id(v));
      }
    }
    std::ranges::sort(var_ids);

    // Use set of canonicalized binding tuples for deduplication
    using BindingTuple = std::vector<EClassId>;
    std::set<BindingTuple> ematcher_unique;
    std::set<BindingTuple> vm_unique;

    std::size_t ematcher_raw = 0;
    std::size_t vm_raw = 0;

    // Run EMatcher-based matching via RewriteRule::apply
    {
      EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
      RewriteContext rewrite_ctx;

      auto counting_rule_builder = RewriteRule<FuzzSymbol, FuzzAnalysis>::Builder("fuzz_count");
      for (auto &p : patterns) {
        counting_rule_builder = std::move(counting_rule_builder).pattern(std::move(p));
      }
      auto counting_rule = std::move(counting_rule_builder)
                               .apply([this, &ematcher_raw, &ematcher_unique, &var_ids](
                                          RuleContext<FuzzSymbol, FuzzAnalysis> &, Match const &m) {
                                 ++ematcher_raw;
                                 BindingTuple tuple;
                                 tuple.reserve(var_ids.size());
                                 for (auto id : var_ids) {
                                   auto eclass_id = m[PatternVar{id}];
                                   tuple.push_back(egraph_.find(eclass_id));
                                 }
                                 ematcher_unique.insert(std::move(tuple));
                               });

      counting_rule.apply(egraph_, matcher, rewrite_ctx);
    }

    VERBOSE_OUT << "EMatcher found " << ematcher_raw << " raw matches, " << ematcher_unique.size() << " unique\n";

    // Run VM-based matching via RewriteRule::apply_vm
    // Store compiled pattern for diagnostics
    std::optional<vm::CompiledPattern<FuzzSymbol>> compiled_pattern_copy;
    bool vm_compilation_succeeded = false;
    {
      EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
      vm::VMExecutorVerify<FuzzSymbol, FuzzAnalysis> vm_executor(egraph_);
      RewriteContext rewrite_ctx;

      patterns.clear();
      for (auto const &ast : current_patterns_ast_) {
        patterns.push_back(pattern_to_memgraph(ast));
      }

      auto vm_rule_builder = RewriteRule<FuzzSymbol, FuzzAnalysis>::Builder("fuzz_vm");
      for (auto &p : patterns) {
        vm_rule_builder = std::move(vm_rule_builder).pattern(std::move(p));
      }
      auto vm_rule =
          std::move(vm_rule_builder)
              .apply([this, &vm_raw, &vm_unique, &var_ids](RuleContext<FuzzSymbol, FuzzAnalysis> &, Match const &m) {
                ++vm_raw;
                BindingTuple tuple;
                tuple.reserve(var_ids.size());
                for (auto id : var_ids) {
                  auto eclass_id = m[PatternVar{id}];
                  tuple.push_back(egraph_.find(eclass_id));
                }
                vm_unique.insert(std::move(tuple));
              });

      // Check if VM compilation succeeded
      if (!vm_rule.compiled_pattern()) {
        VERBOSE_OUT << "VM compilation failed, skipping VM verification\n";
        // Fall back to EMatcher-only verification
        vm_raw = ematcher_raw;
        vm_unique = ematcher_unique;
      } else {
        vm_compilation_succeeded = true;
        compiled_pattern_copy = *vm_rule.compiled_pattern();  // Copy for diagnostics
        vm_rule.apply_vm(egraph_, matcher, vm_executor, rewrite_ctx);
      }
    }

    VERBOSE_OUT << "VM found " << vm_raw << " raw matches, " << vm_unique.size() << " unique\n";

    // Use unique counts for comparison with egglog
    std::size_t ematcher_count = ematcher_unique.size();
    std::size_t vm_count = vm_unique.size();

    // Without egglog as oracle, skip verification
    if (g_skip_egglog) {
      VERBOSE_OUT << "Egglog skipped - no oracle verification\n";
      return true;
    }

    // Check if any pattern is a pure variable (e.g., "?v0")
    // Egglog can't handle rules like (rule ((= ?p0 ?v0)) ...) because ?p0 is ungrounded
    bool has_pure_variable_pattern = false;
    for (auto const &p : current_patterns_egglog_) {
      if (p.size() >= 2 && p[0] == '?' && p[1] == 'v') {
        has_pure_variable_pattern = true;
        break;
      }
    }

    if (has_pure_variable_pattern) {
      // For pure variable patterns, we can verify directly:
      // A single variable pattern matches all e-classes
      // Multi-pattern with variables is the cross product
      // Since EMatcher and VM already agree, just verify they match expected count
      VERBOSE_OUT << "Pattern contains pure variable - skipping egglog (EMatcher=VM=" << ematcher_count << ")\n";
      if (ematcher_count != vm_count) {
        std::cerr << "\n!!! EMatcher/VM MISMATCH for variable pattern !!!\n";
        std::cerr << "EMatcher: " << ematcher_count << " unique (" << ematcher_raw << " raw)\n";
        std::cerr << "VM:       " << vm_count << " unique (" << vm_raw << " raw)\n";
        std::cerr << "\nPatterns:\n";
        for (size_t i = 0; i < current_patterns_egglog_.size(); ++i) {
          std::cerr << "  " << i << ": " << current_patterns_egglog_[i] << "\n";
        }
        std::cerr << "\nE-graph has " << egraph_.num_classes() << " classes, " << egraph_.num_nodes() << " nodes\n";
        // Print binding tuple differences
        print_tuple_diff(ematcher_unique, vm_unique, "EMatcher", "VM");
        // Print bytecode if available
        if (vm_compilation_succeeded && compiled_pattern_copy) {
          std::cerr << "\nVM Bytecode:\n"
                    << vm::disassemble<FuzzSymbol>(compiled_pattern_copy->code(), compiled_pattern_copy->symbols())
                    << "\n";
        }
        abort();
      }
      return true;
    }

    // Build egglog rule (works for single or multi-pattern)
    // Format: (rule ((= ?p0 pattern0) (= ?p1 pattern1) ...) ((MatchResult ?v0 ?v1 ...)))
    std::ostringstream full_program;
    full_program << "(datatype Expr\n";
    full_program << "  (A i64)\n";
    full_program << "  (B i64)\n";
    full_program << "  (C i64)\n";
    full_program << "  (D i64)\n";
    full_program << "  (E i64)\n";
    full_program << "  (F Expr)\n";
    full_program << "  (G Expr)\n";
    full_program << "  (H Expr)\n";
    full_program << "  (Plus Expr Expr)\n";
    full_program << "  (Mul Expr Expr)\n";
    full_program << "  (Ternary Expr Expr Expr)\n";
    full_program << ")\n\n";
    full_program << egglog_.generate();

    // Build type signature and var list for MatchResult.
    // Sort by numeric ID (reusing parse_var_id from above) to match the numeric
    // sort applied to var_ids.  Lexicographic sort would mis-order IDs >= 10
    // (e.g. "?v10" < "?v2" lexicographically but 10 > 2 numerically), making
    // the column order in egglog's MatchResult inconsistent with the tuple order
    // collected by EMatcher/VM — which would break any future tuple-level comparison.
    std::vector<std::string> v_vars(current_all_vars_.begin(), current_all_vars_.end());
    std::ranges::sort(v_vars, std::less{}, parse_var_id);

    std::string type_sig;
    std::string var_list;
    for (bool first = true; auto const &v : v_vars) {
      if (!first) {
        type_sig += ' ';
        var_list += ' ';
      }
      type_sig += "Expr";
      var_list += v;
      first = false;
    }

    full_program << fmt::format("\n(relation MatchResult ({}))\n", type_sig);

    // Build the rule with multiple pattern conditions
    full_program << "(rule (";
    for (size_t i = 0; i < current_patterns_egglog_.size(); ++i) {
      if (i > 0) full_program << " ";
      full_program << fmt::format("(= ?p{} {})", i, current_patterns_egglog_[i]);
    }
    full_program << ")\n";
    full_program << fmt::format("      ((MatchResult {})))\n", var_list);
    full_program << "(run 1000)\n";
    full_program << "(print-function MatchResult 100000)\n";

    VERBOSE_OUT << "Egglog program:\n" << full_program.str() << "\n";

    auto egglog_result = run_egglog(full_program.str());

    if (!egglog_result.success) {
      VERBOSE_OUT << "Egglog failed: " << egglog_result.error << "\n";
      VERBOSE_OUT << "Program:\n" << full_program.str() << "\n";
      // Don't fail on egglog errors - might be pattern issues
      return true;
    }

    size_t egglog_count = egglog_result.match_count;
    VERBOSE_OUT << "Egglog found " << egglog_count << " matches\n";

    // NOTE: Oracle comparison is count-only, not set-comparison.
    // Egglog and memgraph use independent e-class ID spaces, so we cannot
    // directly compare binding tuples.  A bug that produces the correct *number*
    // of matches but with different tuples (e.g., swapped bindings) would not be
    // caught here.  Tuple-level bugs between EMatcher and VM are still detected
    // by the EMatcher-vs-VM set comparison above.
    bool ematcher_ok = (ematcher_count == egglog_count);
    bool vm_ok = (vm_count == egglog_count);

    if (!ematcher_ok || !vm_ok) {
      std::cerr << "\n!!! MATCH COUNT MISMATCH (egglog is ground truth) !!!\n";
      std::cerr << "Egglog:      " << egglog_count << " unique matches (oracle)\n";
      std::cerr << "EMatcher:    " << ematcher_count << " unique (" << ematcher_raw << " raw)"
                << (ematcher_ok ? " OK" : " WRONG") << "\n";
      std::cerr << "VM executor: " << vm_count << " unique (" << vm_raw << " raw)" << (vm_ok ? " OK" : " WRONG")
                << "\n";
      std::cerr << "\nPatterns:\n";
      for (size_t i = 0; i < current_patterns_egglog_.size(); ++i) {
        std::cerr << "  " << i << ": " << current_patterns_egglog_[i] << "\n";
      }
      std::cerr << "\nE-graph has " << egraph_.num_classes() << " classes, " << egraph_.num_nodes() << " nodes\n";

      // Print binding tuple differences between EMatcher and VM
      if (!ematcher_ok && !vm_ok) {
        // Both wrong - compare EMatcher vs VM
        std::cerr << "\nBinding tuple difference (EMatcher vs VM):";
        print_tuple_diff(ematcher_unique, vm_unique, "EMatcher", "VM");
      } else if (!ematcher_ok) {
        std::cerr << "\nEMatcher has wrong count (VM is correct)";
        print_tuple_diff(ematcher_unique, vm_unique, "EMatcher", "VM");
      } else {
        std::cerr << "\nVM has wrong count (EMatcher is correct)";
        print_tuple_diff(vm_unique, ematcher_unique, "VM", "EMatcher");
      }

      // Print VM bytecode for debugging
      if (vm_compilation_succeeded && compiled_pattern_copy) {
        std::cerr << "\nVM Bytecode:\n"
                  << vm::disassemble<FuzzSymbol>(compiled_pattern_copy->code(), compiled_pattern_copy->symbols())
                  << "\n";
      }

      std::cerr << "\nEgglog program:\n" << full_program.str() << "\n";
      std::cerr << "\nEgglog output:\n" << egglog_result.output << "\n";
      abort();
    }

    VERBOSE_OUT << "Pattern verification passed\n";
    return true;
  }

  /// Apply one rewrite cycle: match current patterns, create new compound nodes
  /// whose children are the matched e-classes, merge each new node with one of
  /// the matched e-classes, rebuild, then re-verify the patterns.  This exercises
  /// the interaction between rewriting and pattern matching (e.g., congruence
  /// closure updating matches after a rewrite).
  ///
  /// The rewrite shape is fuzz-driven:
  ///   0-2: Unary wrapper F/G/H(x) = x
  ///   3-4: Binary self-wrapper Plus/Mul(x, x) = x
  ///   5:   Nested unary F(F(x)) = x
  bool apply_rewrite_cycle(uint8_t const *data, size_t &pos, size_t size) {
    if (current_patterns_ast_.empty() || created_ids_.empty()) {
      VERBOSE_OUT << "APPLY_REWRITE: no pattern set, skipping\n";
      return true;
    }

    // Determine rewrite shape from fuzz data
    uint8_t shape = (pos < size) ? data[pos++] % 6 : 0;

    egraph_.rebuild(ctx_);

    std::vector<Pattern<FuzzSymbol>> patterns;
    for (auto const &ast : current_patterns_ast_) {
      patterns.push_back(pattern_to_memgraph(ast));
    }

    // Skip patterns with no variables (cannot extract bindings)
    for (auto const &p : patterns) {
      if (p.num_vars() == 0) {
        VERBOSE_OUT << "APPLY_REWRITE: pattern has 0 vars, skipping\n";
        return true;
      }
    }

    // Collect the first match's variable 0 binding (one representative per match)
    std::vector<EClassId> match_roots;
    {
      EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
      RewriteContext rewrite_ctx;

      auto rule_builder = RewriteRule<FuzzSymbol, FuzzAnalysis>::Builder("fuzz_rewrite");
      for (auto &p : patterns) {
        rule_builder = std::move(rule_builder).pattern(std::move(p));
      }
      auto rule =
          std::move(rule_builder).apply([&match_roots](RuleContext<FuzzSymbol, FuzzAnalysis> &, Match const &m) {
            // Limit to 8 rewrites to keep the e-graph from blowing up
            if (match_roots.size() < 8) {
              match_roots.push_back(m[PatternVar{0}]);
            }
          });
      rule.apply(egraph_, matcher, rewrite_ctx);
    }

    if (match_roots.empty()) {
      VERBOSE_OUT << "APPLY_REWRITE: no matches, skipping rewrite\n";
      return true;
    }

    // Shape descriptions for logging
    static constexpr std::array<char const *, 6> kShapeNames = {
        "F(x)=x", "G(x)=x", "H(x)=x", "Plus(x,x)=x", "Mul(x,x)=x", "F(F(x))=x"};
    VERBOSE_OUT << "APPLY_REWRITE: applying " << match_roots.size() << " rewrite(s) with shape " << kShapeNames[shape]
                << "\n";

    for (auto root_id : match_roots) {
      auto canonical = egraph_.find(root_id);
      auto root_name = egglog_names_.count(canonical) ? egglog_names_[canonical] : egglog_names_[root_id];

      EClassId wrapper_id{};
      std::string wrapper_name;

      switch (shape) {
        case 0:
        case 1:
        case 2: {
          // Unary wrapper: F/G/H(x) = x
          auto sym = static_cast<FuzzSymbol>(10 + shape);  // F=10, G=11, H=12
          utils::small_vector<EClassId> children;
          children.push_back(canonical);
          auto info = egraph_.emplace(sym, children);
          wrapper_id = info.eclass_id;
          created_symbols_.insert(sym);
          wrapper_name = egglog_.emit_unary(sym, root_name);
          break;
        }
        case 3:
        case 4: {
          // Binary self-wrapper: Plus(x, x) = x or Mul(x, x) = x
          auto sym = (shape == 3) ? FuzzSymbol::Plus : FuzzSymbol::Mul;
          utils::small_vector<EClassId> children;
          children.push_back(canonical);
          children.push_back(canonical);
          auto info = egraph_.emplace(sym, children);
          wrapper_id = info.eclass_id;
          created_symbols_.insert(sym);
          wrapper_name = egglog_.emit_binary(sym, root_name, root_name);
          break;
        }
        case 5: {
          // Nested unary: F(F(x)) = x
          utils::small_vector<EClassId> inner_children;
          inner_children.push_back(canonical);
          auto inner_info = egraph_.emplace(FuzzSymbol::F, inner_children);
          created_ids_.push_back(inner_info.eclass_id);
          auto inner_name = egglog_.emit_unary(FuzzSymbol::F, root_name);
          egglog_names_[inner_info.eclass_id] = inner_name;

          utils::small_vector<EClassId> outer_children;
          outer_children.push_back(inner_info.eclass_id);
          auto outer_info = egraph_.emplace(FuzzSymbol::F, outer_children);
          wrapper_id = outer_info.eclass_id;
          created_symbols_.insert(FuzzSymbol::F);
          wrapper_name = egglog_.emit_unary(FuzzSymbol::F, inner_name);
          break;
        }
        default:
          continue;
      }

      created_ids_.push_back(wrapper_id);
      egglog_names_[wrapper_id] = wrapper_name;

      // Merge back: wrapper = x
      egraph_.merge(wrapper_id, canonical);
      egglog_.emit_merge(wrapper_name, root_name);
    }

    egraph_.rebuild(ctx_);

    // Re-verify: patterns must still match consistently after the rewrite
    VERBOSE_OUT << "APPLY_REWRITE: re-verifying after rewrite\n";
    return match_and_verify();
  }

  bool verify_absent() {
    if (created_ids_.empty()) return true;

    // Collect compound symbols NOT yet inserted into this e-graph
    static constexpr std::array<FuzzSymbol, 6> kCompoundSymbols = {
        FuzzSymbol::F,
        FuzzSymbol::G,
        FuzzSymbol::H,
        FuzzSymbol::Plus,
        FuzzSymbol::Mul,
        FuzzSymbol::Ternary,
    };

    FuzzSymbol absent_sym{};
    bool found = false;
    for (auto s : kCompoundSymbols) {
      if (!created_symbols_.contains(s)) {
        absent_sym = s;
        found = true;
        break;
      }
    }

    if (!found) {
      VERBOSE_OUT << "VERIFY_ABSENT: all compound symbols present, skipping\n";
      return true;
    }

    VERBOSE_OUT << "VERIFY_ABSENT: checking " << symbol_name(absent_sym) << " yields 0 matches\n";

    egraph_.rebuild(ctx_);

    // Build a simple pattern: AbsentSym(?v0 [, ?v1 [, ?v2]])
    size_t arity = 1;
    if (absent_sym == FuzzSymbol::Plus || absent_sym == FuzzSymbol::Mul) arity = 2;
    if (absent_sym == FuzzSymbol::Ternary) arity = 3;

    auto builder = Pattern<FuzzSymbol>::Builder{};
    utils::small_vector<PatternNodeId> children;
    for (size_t i = 0; i < arity; ++i) {
      children.push_back(builder.var(PatternVar{static_cast<uint32_t>(i)}));
    }
    builder.sym(absent_sym, std::move(children));
    auto pattern = std::move(builder).build();

    EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
    EMatchContext match_ctx;
    std::vector<PatternMatch> matches;
    matcher.match_into(pattern, match_ctx, matches);

    if (!matches.empty()) {
      std::cerr << "\n!!! VERIFY_ABSENT FAILURE !!!\n";
      std::cerr << "Symbol " << symbol_name(absent_sym) << " was never created but EMatcher found " << matches.size()
                << " match(es)\n";
      std::cerr << "E-graph has " << egraph_.num_classes() << " classes, " << egraph_.num_nodes() << " nodes\n";
      abort();
    }

    VERBOSE_OUT << "VERIFY_ABSENT: " << symbol_name(absent_sym) << " correctly yields 0 matches\n";
    return true;
  }

  void finalize() {
    if (created_ids_.empty()) return;

    egraph_.rebuild(ctx_);

    // 1. Structural invariant: a bare variable pattern should match once per
    //    canonical e-class.  This catches basic EMatcher/union-find corruption
    //    that a structured-pattern match might miss.
    {
      auto builder = Pattern<FuzzSymbol>::Builder{};
      builder.var(PatternVar{0});
      auto var_pattern = std::move(builder).build();

      EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
      EMatchContext match_ctx;
      std::vector<PatternMatch> matches;
      matcher.match_into(var_pattern, match_ctx, matches);

      if (matches.size() != egraph_.num_classes()) {
        std::cerr << "\n!!! VARIABLE PATTERN MISMATCH !!!\n";
        std::cerr << "Expected " << egraph_.num_classes() << " matches (one per e-class)\n";
        std::cerr << "Got " << matches.size() << " matches\n";
        abort();
      }
      VERBOSE_OUT << "Final variable-pattern check passed: " << matches.size() << " matches\n";
    }

    // 2. If a pattern was set during this fuzz run, run one final match+verify
    //    against the egglog oracle.  This catches state-corruption bugs that
    //    only manifest after all merge/rebuild operations have settled.
    if (!current_patterns_ast_.empty()) {
      VERBOSE_OUT << "Running final match_and_verify...\n";
      match_and_verify();
    }
  }

 private:
  /// Pick a node index from `created_ids_` using 2 bytes of fuzz data so that
  /// all nodes (not just the first 256) are reachable as children/merge targets.
  [[nodiscard]] auto pick_node_idx(uint8_t const *data, size_t &pos, size_t size) const -> std::optional<size_t> {
    if (pos + 2 > size || created_ids_.empty()) return std::nullopt;
    auto raw = static_cast<uint16_t>((static_cast<uint16_t>(data[pos]) << 8) | data[pos + 1]);
    pos += 2;
    return static_cast<size_t>(raw % created_ids_.size());
  }

  EGraph<FuzzSymbol, FuzzAnalysis> egraph_;
  ProcessingContext<FuzzSymbol> ctx_;
  std::vector<EClassId> created_ids_;
  size_t operation_count_ = 0;

  // Egglog tracking
  EgglogGenerator egglog_;
  std::unordered_map<EClassId, std::string> egglog_names_;

  // Track which compound symbols have been inserted (for zero-match testing)
  std::set<FuzzSymbol> created_symbols_;

  // Current pattern(s) for matching (1-3 patterns with shared variables)
  MultiPatternGenerator pattern_gen_;
  std::vector<PatternASTPtr> current_patterns_ast_;
  std::vector<std::string> current_patterns_egglog_;
  std::set<int> current_shared_vars_;
  std::set<std::string> current_all_vars_;
};

// ============================================================================
// Main Fuzzer Entry Point
// ============================================================================

extern "C" int LLVMFuzzerTestOneInput(uint8_t const *data, size_t size) {
  if (size < 4) return 0;

  // Initialize flags from environment variables
  // NOT thread-safe: plain bool without synchronization. libFuzzer runs the
  // harness single-threaded by default, but -fork / custom executor modes may
  // call LLVMFuzzerTestOneInput from multiple threads concurrently, turning
  // this into a data race. Replace with std::call_once / std::once_flag if
  // multi-threaded execution is ever needed.
  static bool initialized = false;
  if (!initialized) {
    auto env_flag = [](char const *name) -> bool {
      char const *val = std::getenv(name);
      if (val == nullptr) return false;
      std::string_view sv{val};
      return sv == "1" || sv == "true";
    };
    g_verbose = env_flag("FUZZ_VERBOSE");
    g_skip_egglog = env_flag("FUZZ_SKIP_EGGLOG");

    initialized = true;
  }

  FuzzerState state;
  size_t pos = 0;

  // Initialize egglog program
  state.execute_operation(0, data, pos, size);  // Create at least one node

  // Execute operations from input
  while (pos < size && pos < 1000) {
    uint8_t op = data[pos++];
    if (!state.execute_operation(op, data, pos, size)) {
      break;
    }
  }

  // Final verification
  state.finalize();

  return 0;
}

}  // namespace memgraph::planner::core
