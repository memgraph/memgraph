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
//   ./corpus_benchmark <corpus_dir> [--filter-matches N] [--top N] [--perf-record]
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
#include <string>
#include <vector>

#include <fmt/format.h>

#include "fuzz_common.hpp"
#include "planner/egraph/processing_context.hpp"
#include "planner/pattern/match.hpp"
#include "planner/pattern/matcher.hpp"
#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/rewrite/rule.hpp"

namespace memgraph::planner::core {

// Import fuzz types into this namespace
using fuzz::FuzzAnalysis;
using fuzz::FuzzSymbol;
using fuzz::MultiPatternGenerator;
using fuzz::pattern_to_memgraph;
using fuzz::pattern_to_string;
using fuzz::PatternAST;
using fuzz::PatternASTPtr;

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

  // Print top cases for profiling (by EMatcher time)
  std::cout << "\n=== Top cases for profiling (by EMatcher time) ===\n";
  for (std::size_t i = 0; i < std::min(results.size(), std::size_t{5}); ++i) {
    std::cout << "\n" << (i + 1) << ". " << results[i].filename << "\n";
    std::cout << "   Pattern: " << results[i].pattern_str << "\n";
    std::cout << "   E-graph: " << results[i].egraph_classes << " classes, " << results[i].egraph_nodes << " nodes\n";
    std::cout << "   Matches: " << results[i].match_count << "\n";
    std::cout << "   Time: EMatcher=" << results[i].ematcher_time_us << "us, VM=" << results[i].vm_time_us << "us\n";
  }

  // Print top cases for profiling (by VM time)
  auto results_by_vm = results;
  std::sort(results_by_vm.begin(), results_by_vm.end(), [](auto const &a, auto const &b) {
    return a.vm_time_us > b.vm_time_us;
  });

  std::cout << "\n=== Top cases for profiling (by VM time) ===\n";
  for (std::size_t i = 0; i < std::min(results_by_vm.size(), std::size_t{5}); ++i) {
    std::cout << "\n" << (i + 1) << ". " << results_by_vm[i].filename << "\n";
    std::cout << "   Pattern: " << results_by_vm[i].pattern_str << "\n";
    std::cout << "   E-graph: " << results_by_vm[i].egraph_classes << " classes, " << results_by_vm[i].egraph_nodes
              << " nodes\n";
    std::cout << "   Matches: " << results_by_vm[i].match_count << "\n";
    std::cout << "   Time: EMatcher=" << results_by_vm[i].ematcher_time_us << "us, VM=" << results_by_vm[i].vm_time_us
              << "us\n";
  }

  return 0;
}
