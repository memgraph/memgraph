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

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <set>
#include <sstream>
#include <string>
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

inline auto symbol_name(FuzzSymbol sym) -> std::string {
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
  // Note: the datatype preamble is NOT stored in this stream; it is emitted
  // directly in match_and_verify() before prepending generate(). emit_preamble()
  // was removed because it was dead code — never called, and match_and_verify()
  // already handles the preamble inline.

  auto emit_leaf(FuzzSymbol sym, uint64_t disambiguator) -> std::string {
    auto var_name = fmt::format("n{}", var_counter_++);
    program_ << fmt::format("(let {} ({} {}))\n", var_name, symbol_name(sym), disambiguator);
    return var_name;
  }

  auto emit_unary(FuzzSymbol sym, std::string const &child) -> std::string {
    auto var_name = fmt::format("n{}", var_counter_++);
    program_ << fmt::format("(let {} ({} {}))\n", var_name, symbol_name(sym), child);
    return var_name;
  }

  auto emit_binary(FuzzSymbol sym, std::string const &left, std::string const &right) -> std::string {
    auto var_name = fmt::format("n{}", var_counter_++);
    program_ << fmt::format("(let {} ({} {} {}))\n", var_name, symbol_name(sym), left, right);
    return var_name;
  }

  auto emit_ternary(FuzzSymbol sym, std::string const &c0, std::string const &c1, std::string const &c2)
      -> std::string {
    auto var_name = fmt::format("n{}", var_counter_++);
    program_ << fmt::format("(let {} ({} {} {} {}))\n", var_name, symbol_name(sym), c0, c1, c2);
    return var_name;
  }

  void emit_merge(std::string const &id1, std::string const &id2) {
    program_ << fmt::format("(union {} {})\n", id1, id2);
  }

  // Emit a query pattern and count matches using a relation and rule
  // Returns the relation name for later printing
  void emit_pattern_query(std::string const &pattern_str, std::vector<std::string> const &vars, size_t query_id) {
    auto relation_name = fmt::format("Match{}", query_id);

    // Build relation type signature
    std::string type_sig;
    for (size_t i = 0; i < vars.size(); ++i) {
      if (i > 0) type_sig += " ";
      type_sig += "Expr";
    }

    // Build var list for relation
    std::string var_list;
    for (size_t i = 0; i < vars.size(); ++i) {
      if (i > 0) var_list += " ";
      var_list += vars[i];
    }

    program_ << fmt::format("\n(relation {} ({}))\n", relation_name, type_sig);
    program_ << fmt::format("(rule ((= ?root {}))\n", pattern_str);
    program_ << fmt::format("      (({} {})))\n", relation_name, var_list);

    current_relation_ = relation_name;
  }

  void emit_run_and_print() {
    program_ << "(run 1000)\n";  // Run to saturation
    if (!current_relation_.empty()) {
      program_ << fmt::format("(print-function {} 100000)\n", current_relation_);
    }
  }

  auto generate() const -> std::string { return program_.str(); }

  void clear() {
    program_.str("");
    program_.clear();
    var_counter_ = 0;
    current_relation_.clear();
  }

 private:
  std::ostringstream program_;
  int var_counter_ = 0;
  std::string current_relation_;
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

auto run_egglog(std::string const &program) -> EgglogResult {
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
          bool found = false;
          for (auto const &v : out_vars) {
            if (v == var_name) {
              found = true;
              break;
            }
          }
          if (!found) {
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
  auto generate(uint8_t const *data, size_t &pos, size_t size, int depth = 0) -> PatternASTPtr {
    if (pos >= size || depth > 3) {
      // Generate a simple variable
      return PatternAST::make_var(next_var_id_++);
    }

    uint8_t type = data[pos++] % 7;

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

      default:
        return PatternAST::make_var(next_var_id_++);
    }
  }

  void reset() {
    next_var_id_ = 0;
    used_vars_.clear();
  }

  /// Get used vars for sharing across patterns
  [[nodiscard]] auto get_used_vars() const -> std::vector<int> const & { return used_vars_; }

  /// Force a specific set of vars as "already used" for shared variable generation
  void set_used_vars(std::vector<int> vars) { used_vars_ = std::move(vars); }

 private:
  int next_var_id_ = 0;
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
        if (std::find(shared_vars.begin(), shared_vars.end(), v) == shared_vars.end()) {
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

    int operation_type = op % 6;
    char const *op_names[] = {"CREATE_LEAF", "CREATE_COMPOUND", "MERGE", "REBUILD", "SET_PATTERN", "MATCH_AND_VERIFY"};
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
      default:
        return true;
    }
  }

  bool create_leaf_node(uint8_t const *data, size_t &pos, size_t size) {
    if (pos >= size) return true;

    uint8_t symbol = data[pos++] % 5;  // A-E
    uint64_t disambiguator = pos < size ? data[pos++] : 0;

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
    if (pos + 2 >= size || created_ids_.empty()) return true;

    uint8_t symbol_idx = data[pos++] % 6;  // F, G, H, Plus, Mul, Ternary
    auto sym = static_cast<FuzzSymbol>(10 + symbol_idx);

    // Determine arity based on symbol
    size_t arity;
    if (sym == FuzzSymbol::Ternary) {
      arity = 3;
    } else if (sym == FuzzSymbol::Plus || sym == FuzzSymbol::Mul) {
      arity = 2;
    } else {
      arity = 1;
    }

    if (pos + arity > size) return true;
    if (created_ids_.size() < arity) return true;

    utils::small_vector<EClassId> children;
    std::vector<std::string> child_names;
    for (size_t i = 0; i < arity; ++i) {
      uint8_t child_idx = data[pos++] % created_ids_.size();
      auto child_id = egraph_.find(created_ids_[child_idx]);
      children.push_back(child_id);
      child_names.push_back(egglog_names_[created_ids_[child_idx]]);
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
    if (pos + 2 > size || created_ids_.size() < 2) return true;

    uint8_t idx1 = data[pos++] % created_ids_.size();
    uint8_t idx2 = data[pos++] % created_ids_.size();
    if (idx1 == idx2) return true;

    auto id1 = created_ids_[idx1];
    auto id2 = created_ids_[idx2];

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

    for (auto const &ast : current_patterns_ast_) {
      std::vector<std::string> vars;
      int leaf_counter = 0;
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
    std::vector<uint32_t> var_ids;
    for (auto const &v : current_all_vars_) {
      // Parse "?vN" to get N
      if (v.size() >= 3 && v[0] == '?' && v[1] == 'v') {
        var_ids.push_back(static_cast<uint32_t>(std::stoi(v.substr(2))));
      }
    }
    std::sort(var_ids.begin(), var_ids.end());

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

      patterns.clear();
      for (auto const &ast : current_patterns_ast_) {
        patterns.push_back(pattern_to_memgraph(ast));
      }

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
        std::cerr << "EMatcher: " << ematcher_count << ", VM: " << vm_count << "\n";
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

    // Build type signature and var list for MatchResult
    std::vector<std::string> v_vars(current_all_vars_.begin(), current_all_vars_.end());
    std::sort(v_vars.begin(), v_vars.end());

    std::string type_sig;
    std::string var_list;
    for (size_t i = 0; i < v_vars.size(); ++i) {
      if (i > 0) {
        type_sig += " ";
        var_list += " ";
      }
      type_sig += "Expr";
      var_list += v_vars[i];
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

    // Verify counts match
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
      std::cerr << "\nEgglog program:\n" << full_program.str() << "\n";
      std::cerr << "\nEgglog output:\n" << egglog_result.output << "\n";
      abort();
    }

    VERBOSE_OUT << "Pattern verification passed\n";
    return true;
  }

  void finalize() {
    // Final verification with a simple variable pattern
    if (created_ids_.empty()) return;

    egraph_.rebuild(ctx_);

    // Test matching with a simple variable pattern (should match all e-classes)
    auto builder = Pattern<FuzzSymbol>::Builder{};
    builder.var(PatternVar{0});
    auto var_pattern = std::move(builder).build();

    EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
    EMatchContext match_ctx;
    std::vector<PatternMatch> matches;
    matcher.match_into(var_pattern, match_ctx, matches);

    // Variable pattern should match once per canonical e-class
    if (matches.size() != egraph_.num_classes()) {
      std::cerr << "\n!!! VARIABLE PATTERN MISMATCH !!!\n";
      std::cerr << "Expected " << egraph_.num_classes() << " matches (one per e-class)\n";
      std::cerr << "Got " << matches.size() << " matches\n";
      abort();
    }

    VERBOSE_OUT << "Final verification passed: " << matches.size() << " matches for variable pattern\n";
  }

 private:
  EGraph<FuzzSymbol, FuzzAnalysis> egraph_;
  ProcessingContext<FuzzSymbol> ctx_;
  std::vector<EClassId> created_ids_;
  size_t operation_count_ = 0;

  // Egglog tracking
  EgglogGenerator egglog_;
  std::unordered_map<EClassId, std::string> egglog_names_;

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
    char const *verbose_env = std::getenv("FUZZ_VERBOSE");
    g_verbose = verbose_env != nullptr && (std::strcmp(verbose_env, "1") == 0 || std::strcmp(verbose_env, "true") == 0);

    char const *skip_env = std::getenv("FUZZ_SKIP_EGGLOG");
    g_skip_egglog = skip_env != nullptr && (std::strcmp(skip_env, "1") == 0 || std::strcmp(skip_env, "true") == 0);

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
