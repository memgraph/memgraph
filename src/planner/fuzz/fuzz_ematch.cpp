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
#include <vector>

#include <fmt/format.h>

#include "fuzz_common.hpp"
#include "planner/egraph/processing_context.hpp"
#include "planner/pattern/match.hpp"
#include "planner/pattern/matcher.hpp"
#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/pattern/vm/tracer.hpp"
#include "planner/rewrite/rule.hpp"

namespace memgraph::planner::core {

// Import fuzz types into this namespace
using fuzz::dump_egraph;
using fuzz::FuzzAnalysis;
using fuzz::FuzzSymbol;
using fuzz::is_leaf_symbol;
using fuzz::MultiPatternGenerator;
using fuzz::pattern_to_egglog;
using fuzz::pattern_to_memgraph;
using fuzz::PatternAST;
using fuzz::PatternASTPtr;
using fuzz::PatternGenerator;
using fuzz::symbol_name;

// ============================================================================
// Configuration
// ============================================================================

static bool g_verbose = false;
static bool g_skip_egglog = false;

// Maximum number of variables in a pattern before skipping.
// Patterns with too many unique variables can cause exponential match counts.
// 81 variables in a 4-level Ternary pattern creates ~billions of matches.
static constexpr std::size_t kMaxPatternVariables = 20;

#define VERBOSE_OUT \
  if (g_verbose) std::cerr

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
// Fuzzer State
// ============================================================================

class FuzzerState {
 public:
  bool execute_operation(uint8_t op, uint8_t const *data, size_t &pos, size_t size) {
    operation_count_++;
    VERBOSE_OUT << "\n--- Operation #" << operation_count_ << " ---\n";

    // Dispatch table: compound nodes get 3 slots vs 1 for leaf so the fuzzer
    // builds deeper, more pattern-matchable graphs more often.
    // MATCH_AND_VERIFY is not dispatched randomly - it runs once at the end in finalize().
    static constexpr std::array<uint8_t, 9> kDispatch = {
        0,  // CREATE_LEAF        (1/9)
        1,
        1,
        1,  // CREATE_COMPOUND    (3/9)
        2,  // MERGE              (1/9)
        3,  // REBUILD            (1/9)
        4,  // SET_PATTERN        (1/9)
        6,  // VERIFY_ABSENT      (1/9)
        7,  // APPLY_REWRITE      (1/9)
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

    // Check pattern complexity
    for (auto const &p : patterns) {
      if (p.num_vars() == 0) {
        VERBOSE_OUT << "Skipping pattern with 0 variables\n";
        return true;
      }
      if (p.num_vars() > kMaxPatternVariables) {
        VERBOSE_OUT << "Skipping pattern with " << p.num_vars() << " variables (max " << kMaxPatternVariables << ")\n";
        return true;
      }
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
    std::set<BindingTuple> vm_unique;

    std::size_t vm_raw = 0;

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
        return true;
      }
      vm_compilation_succeeded = true;
      compiled_pattern_copy = *vm_rule.compiled_pattern();  // Copy for diagnostics
      vm_rule.apply_vm(egraph_, matcher, vm_executor, rewrite_ctx);
    }

    VERBOSE_OUT << "VM found " << vm_raw << " raw matches, " << vm_unique.size() << " unique\n";

    // Use unique count for comparison with egglog
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
      // For pure variable patterns, we skip egglog verification.
      VERBOSE_OUT << "Pattern contains pure variable - skipping egglog (VM=" << vm_count << ")\n";
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
    // caught here.
    bool vm_ok = (vm_count == egglog_count);

    if (!vm_ok) {
      std::cerr << "\n!!! VM MISMATCH (egglog is ground truth) !!!\n";
      std::cerr << "Egglog:      " << egglog_count << " unique matches (oracle)\n";
      std::cerr << "VM executor: " << vm_count << " unique (" << vm_raw << " raw) WRONG\n";
      std::cerr << "\nPatterns:\n";
      for (size_t i = 0; i < current_patterns_egglog_.size(); ++i) {
        std::cerr << "  " << i << ": " << current_patterns_egglog_[i] << "\n";
      }

      std::cerr << "\n";
      dump_egraph(egraph_, std::cerr);

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
      vm::VMExecutorVerify<FuzzSymbol, FuzzAnalysis> vm_executor(egraph_);
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
      rule.apply_vm(egraph_, matcher, vm_executor, rewrite_ctx);
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

    std::size_t match_count = 0;
    {
      EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
      vm::VMExecutorVerify<FuzzSymbol, FuzzAnalysis> vm_executor(egraph_);
      RewriteContext rewrite_ctx;

      auto rule = RewriteRule<FuzzSymbol, FuzzAnalysis>::Builder("fuzz_absent")
                      .pattern(std::move(pattern))
                      .apply([&match_count](RuleContext<FuzzSymbol, FuzzAnalysis> &, Match const &) { ++match_count; });
      rule.apply_vm(egraph_, matcher, vm_executor, rewrite_ctx);
    }

    if (match_count > 0) {
      std::cerr << "\n!!! VERIFY_ABSENT FAILURE !!!\n";
      std::cerr << "Symbol " << symbol_name(absent_sym) << " was never created but VM found " << match_count
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

    // Run final match+verify against the egglog oracle.
    // This catches state-corruption bugs that only manifest after all
    // merge/rebuild operations have settled.
    if (current_patterns_ast_.empty()) {
      VERBOSE_OUT << "No pattern set, skipping match_and_verify\n";
      return;
    }
    VERBOSE_OUT << "Running final match_and_verify...\n";
    match_and_verify();
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
