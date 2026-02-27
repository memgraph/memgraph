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

// Benchmark tool for EMatcher vs VM pattern matching.
//
// Usage:
//   ./bench_ematch <corpus_dir> [--filter-matches N] [--top N] [--perf-record]
//
// This tool:
// 1. Reads fuzzer corpus files to build e-graphs and patterns
// 2. Benchmarks EMatcher vs VM execution time
// 3. Reports statistics and identifies interesting cases for profiling

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include "planner/egraph/egraph.hpp"
#include "planner/egraph/processing_context.hpp"
#include "planner/pattern/match.hpp"
#include "planner/pattern/matcher.hpp"
#include "planner/pattern/pattern.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/rewrite/rule.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

// ============================================================================
// Fuzz Symbols - same as fuzz_ematch.cpp
// ============================================================================

enum class FuzzSymbol : uint8_t {
  A = 0,
  B = 1,
  C = 2,
  D = 3,
  E = 4,
  F = 10,
  G = 11,
  H = 12,
  Plus = 13,
  Mul = 14,
  Ternary = 15,
};

struct FuzzAnalysis {};

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
// Pattern AST (same as fuzz_ematch.cpp)
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

auto pattern_to_string(PatternASTPtr const &ast) -> std::string {
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

// ============================================================================
// Pattern Generator (same as fuzz_ematch.cpp)
// ============================================================================

class PatternGenerator {
 public:
  auto generate(uint8_t const *data, size_t &pos, size_t size, int depth = 0) -> PatternASTPtr {
    if (pos >= size || depth > 3) {
      return PatternAST::make_var(next_var_id_++);
    }

    uint8_t type = data[pos++] % 7;

    switch (type) {
      case 0:
      case 1: {
        int var_id;
        if (pos < size && !used_vars_.empty() && (data[pos] % 3) == 0) {
          var_id = used_vars_[data[pos++] % used_vars_.size()];
        } else {
          var_id = next_var_id_++;
          used_vars_.push_back(var_id);
        }
        return PatternAST::make_var(var_id);
      }

      case 2: {
        if (pos >= size) return PatternAST::make_var(next_var_id_++);
        auto sym = static_cast<FuzzSymbol>(data[pos++] % 5);
        return PatternAST::make_sym(sym);
      }

      case 3: {
        if (pos >= size) return PatternAST::make_var(next_var_id_++);
        auto sym = static_cast<FuzzSymbol>(10 + (data[pos++] % 3));
        auto child = generate(data, pos, size, depth + 1);
        return PatternAST::make_sym(sym, {child});
      }

      case 4: {
        if (pos >= size) return PatternAST::make_var(next_var_id_++);
        auto sym = static_cast<FuzzSymbol>(13 + (data[pos++] % 2));
        auto left = generate(data, pos, size, depth + 1);
        auto right = generate(data, pos, size, depth + 1);
        return PatternAST::make_sym(sym, {left, right});
      }

      case 5: {
        if (used_vars_.empty()) {
          used_vars_.push_back(next_var_id_++);
        }
        auto var_id = used_vars_[0];
        if (pos >= size) return PatternAST::make_var(var_id);
        auto sym = static_cast<FuzzSymbol>(10 + (data[pos++] % 3));
        return PatternAST::make_sym(sym, {PatternAST::make_var(var_id)});
      }

      case 6: {
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

  [[nodiscard]] auto get_used_vars() const -> std::vector<int> const & { return used_vars_; }

  void set_used_vars(std::vector<int> vars) { used_vars_ = std::move(vars); }

 private:
  int next_var_id_ = 0;
  std::vector<int> used_vars_;
};

class MultiPatternGenerator {
 public:
  struct MultiPatternResult {
    std::vector<PatternASTPtr> patterns;
    std::set<int> shared_vars;
  };

  auto generate(uint8_t const *data, size_t &pos, size_t size) -> MultiPatternResult {
    MultiPatternResult result;
    if (pos >= size) return result;

    uint8_t num_patterns = 1 + (data[pos++] % 3);

    pattern_gen_.reset();
    result.patterns.push_back(pattern_gen_.generate(data, pos, size));
    auto shared_vars = pattern_gen_.get_used_vars();

    for (uint8_t i = 1; i < num_patterns && pos < size; ++i) {
      pattern_gen_.reset();

      if (!shared_vars.empty()) {
        uint8_t num_shared = 1 + (pos < size ? data[pos++] % 2 : 0);
        std::vector<int> to_share;
        for (uint8_t j = 0; j < num_shared && j < shared_vars.size(); ++j) {
          to_share.push_back(shared_vars[j % shared_vars.size()]);
        }
        pattern_gen_.set_used_vars(to_share);

        for (auto v : to_share) {
          result.shared_vars.insert(v);
        }
      }

      result.patterns.push_back(pattern_gen_.generate(data, pos, size));

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
// Benchmark Result
// ============================================================================

struct BenchResult {
  std::string filename;
  std::string pattern_str;
  std::size_t egraph_classes;
  std::size_t egraph_nodes;
  std::size_t num_patterns;
  std::size_t match_count;
  double ematcher_time_us;
  double vm_time_us;
  double speedup;
  bool vm_compiled;
};

// ============================================================================
// Benchmark Runner
// ============================================================================

class BenchRunner {
 public:
  auto run_file(std::filesystem::path const &path) -> std::optional<BenchResult> {
    std::ifstream file(path, std::ios::binary);
    if (!file) return std::nullopt;

    std::vector<uint8_t> data((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    if (data.size() < 4) return std::nullopt;

    return run_data(data.data(), data.size(), path.filename().string());
  }

  auto run_data(uint8_t const *data, size_t size, std::string const &name) -> std::optional<BenchResult> {
    // Reset state
    egraph_ = EGraph<FuzzSymbol, FuzzAnalysis>{};
    created_ids_.clear();
    current_patterns_ast_.clear();

    size_t pos = 0;

    // Execute operations to build e-graph and pattern
    execute_operation(0, data, pos, size);  // Create at least one node

    while (pos < size && pos < 1000) {
      uint8_t op = data[pos++];
      execute_operation(op, data, pos, size);
    }

    // Finalize and benchmark
    return benchmark_current_state(name);
  }

 private:
  void execute_operation(uint8_t op, uint8_t const *data, size_t &pos, size_t size) {
    int operation_type = op % 6;

    switch (operation_type) {
      case 0:
        create_leaf_node(data, pos, size);
        break;
      case 1:
        create_compound_node(data, pos, size);
        break;
      case 2:
        merge_classes(data, pos, size);
        break;
      case 3:
        rebuild();
        break;
      case 4:
        set_pattern(data, pos, size);
        break;
      case 5:
        // Skip match_and_verify - we'll benchmark at the end
        break;
    }
  }

  void create_leaf_node(uint8_t const *data, size_t &pos, size_t size) {
    if (pos >= size) return;
    uint8_t symbol = data[pos++] % 5;
    uint64_t disambiguator = pos < size ? data[pos++] : 0;
    auto sym = static_cast<FuzzSymbol>(symbol);
    auto info = egraph_.emplace(sym, disambiguator);
    created_ids_.push_back(info.eclass_id);
  }

  void create_compound_node(uint8_t const *data, size_t &pos, size_t size) {
    if (pos + 2 >= size || created_ids_.empty()) return;

    uint8_t symbol_idx = data[pos++] % 6;
    auto sym = static_cast<FuzzSymbol>(10 + symbol_idx);

    size_t arity;
    if (sym == FuzzSymbol::Ternary) {
      arity = 3;
    } else if (sym == FuzzSymbol::Plus || sym == FuzzSymbol::Mul) {
      arity = 2;
    } else {
      arity = 1;
    }

    if (pos + arity > size) return;
    if (created_ids_.size() < arity) return;

    utils::small_vector<EClassId> children;
    for (size_t i = 0; i < arity; ++i) {
      uint8_t child_idx = data[pos++] % created_ids_.size();
      auto child_id = egraph_.find(created_ids_[child_idx]);
      children.push_back(child_id);
    }

    auto info = egraph_.emplace(sym, children);
    created_ids_.push_back(info.eclass_id);
  }

  void merge_classes(uint8_t const *data, size_t &pos, size_t size) {
    if (pos + 2 > size || created_ids_.size() < 2) return;

    uint8_t idx1 = data[pos++] % created_ids_.size();
    uint8_t idx2 = data[pos++] % created_ids_.size();
    if (idx1 == idx2) return;

    egraph_.merge(created_ids_[idx1], created_ids_[idx2]);
  }

  void rebuild() { egraph_.rebuild(ctx_); }

  void set_pattern(uint8_t const *data, size_t &pos, size_t size) {
    if (pos >= size) return;
    auto result = pattern_gen_.generate(data, pos, size);
    current_patterns_ast_ = std::move(result.patterns);
  }

  auto benchmark_current_state(std::string const &name) -> std::optional<BenchResult> {
    if (current_patterns_ast_.empty() || created_ids_.empty()) {
      return std::nullopt;
    }

    egraph_.rebuild(ctx_);

    // Convert patterns
    std::vector<Pattern<FuzzSymbol>> patterns;
    for (auto const &ast : current_patterns_ast_) {
      patterns.push_back(pattern_to_memgraph(ast));
    }

    // Check for zero-variable patterns
    for (auto const &p : patterns) {
      if (p.num_vars() == 0) {
        return std::nullopt;
      }
    }

    BenchResult result;
    result.filename = name;
    result.egraph_classes = egraph_.num_classes();
    result.egraph_nodes = egraph_.num_nodes();
    result.num_patterns = patterns.size();
    result.pattern_str = "";
    for (size_t i = 0; i < current_patterns_ast_.size(); ++i) {
      if (i > 0) result.pattern_str += " JOIN ";
      result.pattern_str += pattern_to_string(current_patterns_ast_[i]);
    }

    // Benchmark EMatcher
    constexpr int kWarmupRuns = 3;
    constexpr int kBenchRuns = 10;

    std::size_t match_count = 0;

    // Warmup
    for (int i = 0; i < kWarmupRuns; ++i) {
      EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
      RewriteContext rewrite_ctx;

      std::vector<Pattern<FuzzSymbol>> patterns_copy;
      for (auto const &ast : current_patterns_ast_) {
        patterns_copy.push_back(pattern_to_memgraph(ast));
      }

      auto rule_builder = RewriteRule<FuzzSymbol, FuzzAnalysis>::Builder("bench");
      for (auto &p : patterns_copy) {
        rule_builder = std::move(rule_builder).pattern(std::move(p));
      }
      auto rule = std::move(rule_builder).apply([&match_count](RuleContext<FuzzSymbol, FuzzAnalysis> &, Match const &) {
        ++match_count;
      });
      match_count = 0;
      rule.apply(egraph_, matcher, rewrite_ctx);
    }

    result.match_count = match_count;

    // Benchmark EMatcher
    auto ematcher_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < kBenchRuns; ++i) {
      EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
      RewriteContext rewrite_ctx;

      std::vector<Pattern<FuzzSymbol>> patterns_copy;
      for (auto const &ast : current_patterns_ast_) {
        patterns_copy.push_back(pattern_to_memgraph(ast));
      }

      auto rule_builder = RewriteRule<FuzzSymbol, FuzzAnalysis>::Builder("bench");
      for (auto &p : patterns_copy) {
        rule_builder = std::move(rule_builder).pattern(std::move(p));
      }
      std::size_t count = 0;
      auto rule =
          std::move(rule_builder).apply([&count](RuleContext<FuzzSymbol, FuzzAnalysis> &, Match const &) { ++count; });
      rule.apply(egraph_, matcher, rewrite_ctx);
    }
    auto ematcher_end = std::chrono::high_resolution_clock::now();
    result.ematcher_time_us =
        std::chrono::duration_cast<std::chrono::microseconds>(ematcher_end - ematcher_start).count() /
        static_cast<double>(kBenchRuns);

    // Benchmark VM
    result.vm_compiled = false;
    result.vm_time_us = result.ematcher_time_us;  // Default to same if compilation fails

    {
      EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
      vm::VMExecutorVerify<FuzzSymbol, FuzzAnalysis> vm_executor(egraph_);
      RewriteContext rewrite_ctx;

      std::vector<Pattern<FuzzSymbol>> patterns_copy;
      for (auto const &ast : current_patterns_ast_) {
        patterns_copy.push_back(pattern_to_memgraph(ast));
      }

      auto rule_builder = RewriteRule<FuzzSymbol, FuzzAnalysis>::Builder("bench_vm");
      for (auto &p : patterns_copy) {
        rule_builder = std::move(rule_builder).pattern(std::move(p));
      }
      std::size_t count = 0;
      auto rule =
          std::move(rule_builder).apply([&count](RuleContext<FuzzSymbol, FuzzAnalysis> &, Match const &) { ++count; });

      if (rule.compiled_pattern()) {
        result.vm_compiled = true;

        // Warmup
        for (int i = 0; i < kWarmupRuns; ++i) {
          count = 0;
          rule.apply_vm(egraph_, matcher, vm_executor, rewrite_ctx);
        }

        // Benchmark
        auto vm_start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < kBenchRuns; ++i) {
          count = 0;
          rule.apply_vm(egraph_, matcher, vm_executor, rewrite_ctx);
        }
        auto vm_end = std::chrono::high_resolution_clock::now();
        result.vm_time_us = std::chrono::duration_cast<std::chrono::microseconds>(vm_end - vm_start).count() /
                            static_cast<double>(kBenchRuns);
      }
    }

    result.speedup = result.ematcher_time_us / result.vm_time_us;

    return result;
  }

  EGraph<FuzzSymbol, FuzzAnalysis> egraph_;
  ProcessingContext<FuzzSymbol> ctx_;
  std::vector<EClassId> created_ids_;
  MultiPatternGenerator pattern_gen_;
  std::vector<PatternASTPtr> current_patterns_ast_;
};

}  // namespace memgraph::planner::core

int main(int argc, char *argv[]) {
  using namespace memgraph::planner::core;

  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <corpus_dir> [--filter-matches N] [--top N]\n";
    return 1;
  }

  std::filesystem::path corpus_dir = argv[1];
  std::size_t min_matches = 0;
  std::size_t top_n = 0;

  for (int i = 2; i < argc; ++i) {
    if (std::strcmp(argv[i], "--filter-matches") == 0 && i + 1 < argc) {
      min_matches = std::stoull(argv[++i]);
    } else if (std::strcmp(argv[i], "--top") == 0 && i + 1 < argc) {
      top_n = std::stoull(argv[++i]);
    }
  }

  std::vector<BenchResult> results;
  BenchRunner runner;

  std::size_t processed = 0;
  std::size_t skipped = 0;

  for (auto const &entry : std::filesystem::directory_iterator(corpus_dir)) {
    if (!entry.is_regular_file()) continue;

    auto result = runner.run_file(entry.path());
    if (result) {
      if (result->match_count >= min_matches) {
        results.push_back(*result);
      }
      ++processed;
    } else {
      ++skipped;
    }

    if ((processed + skipped) % 100 == 0) {
      std::cerr << "\rProcessed " << processed << " files, skipped " << skipped << "..." << std::flush;
    }
  }
  std::cerr << "\rProcessed " << processed << " files, skipped " << skipped << "          \n";

  // Sort by speedup (most interesting cases first)
  std::sort(results.begin(), results.end(), [](auto const &a, auto const &b) {
    return a.ematcher_time_us > b.ematcher_time_us;
  });

  if (top_n > 0 && top_n < results.size()) {
    results.resize(top_n);
  }

  // Print results
  std::cout << "\n=== Benchmark Results ===\n\n";
  std::cout << std::setw(40) << std::left << "Pattern" << std::setw(8) << "Classes" << std::setw(8) << "Nodes"
            << std::setw(10) << "Matches" << std::setw(12) << "EMatcher(us)" << std::setw(12) << "VM(us)"
            << std::setw(10) << "Speedup"
            << "\n";
  std::cout << std::string(100, '-') << "\n";

  double total_ematcher = 0;
  double total_vm = 0;
  std::size_t vm_compiled_count = 0;

  for (auto const &r : results) {
    std::string pattern_short = r.pattern_str.substr(0, 37);
    if (r.pattern_str.length() > 37) pattern_short += "...";

    std::cout << std::setw(40) << std::left << pattern_short << std::setw(8) << r.egraph_classes << std::setw(8)
              << r.egraph_nodes << std::setw(10) << r.match_count << std::setw(12) << std::fixed << std::setprecision(1)
              << r.ematcher_time_us << std::setw(12) << r.vm_time_us << std::setw(10) << std::setprecision(2)
              << r.speedup << (r.vm_compiled ? "" : " (no VM)") << "\n";

    total_ematcher += r.ematcher_time_us;
    total_vm += r.vm_time_us;
    if (r.vm_compiled) ++vm_compiled_count;
  }

  std::cout << std::string(100, '-') << "\n";
  std::cout << "\nSummary:\n";
  std::cout << "  Total cases: " << results.size() << "\n";
  std::cout << "  VM compiled: " << vm_compiled_count << " (" << (100.0 * vm_compiled_count / results.size()) << "%)\n";
  std::cout << "  Total EMatcher time: " << std::fixed << std::setprecision(1) << total_ematcher << " us\n";
  std::cout << "  Total VM time: " << total_vm << " us\n";
  std::cout << "  Overall speedup: " << std::setprecision(2) << (total_ematcher / total_vm) << "x\n";

  // Print top cases for profiling
  std::cout << "\n=== Top cases for profiling (by EMatcher time) ===\n";
  for (std::size_t i = 0; i < std::min(results.size(), std::size_t{5}); ++i) {
    std::cout << "\n" << (i + 1) << ". " << results[i].filename << "\n";
    std::cout << "   Pattern: " << results[i].pattern_str << "\n";
    std::cout << "   E-graph: " << results[i].egraph_classes << " classes, " << results[i].egraph_nodes << " nodes\n";
    std::cout << "   Matches: " << results[i].match_count << "\n";
    std::cout << "   Time: EMatcher=" << results[i].ematcher_time_us << "us, VM=" << results[i].vm_time_us << "us\n";
  }

  return 0;
}
