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

// Profiling tool for VM pattern matching.
//
// Usage:
//   ./corpus_profile <corpus_file> [--iterations N]
//
// Run with perf:
//   perf record -g ./corpus_profile <corpus_file> --iterations 100000
//   perf report

#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
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

// Profile runner
class ProfileRunner {
 public:
  bool setup(uint8_t const *data, size_t size) {
    egraph_ = EGraph<FuzzSymbol, FuzzAnalysis>{};
    created_ids_.clear();

    size_t pos = 0;
    execute_operation(0, data, pos, size);

    while (pos < size && pos < 1000) {
      uint8_t op = data[pos++];
      execute_operation(op, data, pos, size);
    }

    if (current_patterns_ast_.empty() || created_ids_.empty()) return false;

    egraph_.rebuild(ctx_);

    patterns_.clear();
    for (auto const &ast : current_patterns_ast_) {
      patterns_.push_back(pattern_to_memgraph(ast));
    }

    for (auto const &p : patterns_) {
      if (p.num_vars() == 0) return false;
    }

    // Pre-compile for VM
    std::vector<Pattern<FuzzSymbol>> patterns_copy;
    for (auto const &ast : current_patterns_ast_) {
      patterns_copy.push_back(pattern_to_memgraph(ast));
    }

    auto rule_builder = RewriteRule<FuzzSymbol, FuzzAnalysis>::Builder("profile_vm");
    for (auto &p : patterns_copy) {
      rule_builder = std::move(rule_builder).pattern(std::move(p));
    }
    vm_rule_ = std::make_unique<RewriteRule<FuzzSymbol, FuzzAnalysis>>(
        std::move(rule_builder).apply([this](RuleContext<FuzzSymbol, FuzzAnalysis> &, Match const &) {
          ++match_count_;
        }));

    if (!vm_rule_->compiled_pattern()) return false;

    // Print info
    std::cerr << "E-graph: " << egraph_.num_classes() << " classes, " << egraph_.num_nodes() << " nodes\n";
    std::cerr << "Patterns:\n";
    for (size_t i = 0; i < current_patterns_ast_.size(); ++i) {
      std::cerr << "  " << (i + 1) << ": " << pattern_to_string(current_patterns_ast_[i]) << "\n";
    }

    return true;
  }

  void run_vm(int iterations) {
    EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
    vm::VMExecutorVerify<FuzzSymbol, FuzzAnalysis> vm_executor(egraph_);
    RewriteContext rewrite_ctx;

    for (int i = 0; i < iterations; ++i) {
      match_count_ = 0;
      vm_rule_->apply_vm(egraph_, matcher, vm_executor, rewrite_ctx);
    }
  }

  [[nodiscard]] auto match_count() const -> std::size_t { return match_count_; }

 private:
  void execute_operation(uint8_t op, uint8_t const *data, size_t &pos, size_t size) {
    switch (op % 6) {
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
    size_t arity = (sym == FuzzSymbol::Ternary) ? 3 : ((sym == FuzzSymbol::Plus || sym == FuzzSymbol::Mul) ? 2 : 1);
    if (pos + arity > size || created_ids_.size() < arity) return;

    utils::small_vector<EClassId> children;
    for (size_t i = 0; i < arity; ++i) {
      uint8_t child_idx = data[pos++] % created_ids_.size();
      children.push_back(egraph_.find(created_ids_[child_idx]));
    }
    auto info = egraph_.emplace(sym, children);
    created_ids_.push_back(info.eclass_id);
  }

  void merge_classes(uint8_t const *data, size_t &pos, size_t size) {
    if (pos + 2 > size || created_ids_.size() < 2) return;
    uint8_t idx1 = data[pos++] % created_ids_.size();
    uint8_t idx2 = data[pos++] % created_ids_.size();
    if (idx1 != idx2) egraph_.merge(created_ids_[idx1], created_ids_[idx2]);
  }

  void rebuild() { egraph_.rebuild(ctx_); }

  void set_pattern(uint8_t const *data, size_t &pos, size_t size) {
    if (pos >= size) return;
    auto result = pattern_gen_.generate(data, pos, size);
    current_patterns_ast_ = std::move(result.patterns);
  }

  EGraph<FuzzSymbol, FuzzAnalysis> egraph_;
  ProcessingContext<FuzzSymbol> ctx_;
  std::vector<EClassId> created_ids_;
  MultiPatternGenerator pattern_gen_;
  std::vector<PatternASTPtr> current_patterns_ast_;
  std::vector<Pattern<FuzzSymbol>> patterns_;
  std::unique_ptr<RewriteRule<FuzzSymbol, FuzzAnalysis>> vm_rule_;
  std::size_t match_count_ = 0;
};

}  // namespace memgraph::planner::core

int main(int argc, char *argv[]) {
  using namespace memgraph::planner::core;

  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <corpus_file> [--iterations N]\n";
    return 1;
  }

  std::string filename = argv[1];
  int iterations = 10000;

  for (int i = 2; i < argc; ++i) {
    if (std::strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
      iterations = std::stoi(argv[++i]);
    }
  }

  std::ifstream file(filename, std::ios::binary);
  if (!file) {
    std::cerr << "Failed to open " << filename << "\n";
    return 1;
  }

  std::vector<uint8_t> data((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

  ProfileRunner runner;
  if (!runner.setup(data.data(), data.size())) {
    std::cerr << "Failed to setup profiling (no valid pattern or e-graph)\n";
    return 1;
  }

  std::cerr << "Running " << iterations << " iterations\n";

  auto start = std::chrono::high_resolution_clock::now();

  std::cerr << "Running VM...\n";
  runner.run_vm(iterations);

  auto end = std::chrono::high_resolution_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  std::cerr << "Done in " << duration_ms << "ms\n";
  std::cerr << "Matches per iteration: " << runner.match_count() << "\n";

  return 0;
}
