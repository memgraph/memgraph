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

// Profiling tool for EMatcher vs VM pattern matching.
//
// Usage:
//   ./profile_ematch <corpus_file> [--iterations N] [--mode ematcher|vm|both]
//
// Run with perf:
//   perf record -g ./profile_ematch <corpus_file> --iterations 100000 --mode vm
//   perf report

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
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
#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "planner/rewrite/rule.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

// Fuzz symbols (same as fuzz_ematch.cpp)
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

// Pattern AST
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

// Pattern generators
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
        if (used_vars_.empty()) used_vars_.push_back(next_var_id_++);
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
        for (auto v : to_share) result.shared_vars.insert(v);
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

  void run_ematcher(int iterations) {
    for (int i = 0; i < iterations; ++i) {
      EMatcher<FuzzSymbol, FuzzAnalysis> matcher(egraph_);
      RewriteContext rewrite_ctx;

      std::vector<Pattern<FuzzSymbol>> patterns_copy;
      for (auto const &ast : current_patterns_ast_) {
        patterns_copy.push_back(pattern_to_memgraph(ast));
      }

      auto rule_builder = RewriteRule<FuzzSymbol, FuzzAnalysis>::Builder("profile");
      for (auto &p : patterns_copy) {
        rule_builder = std::move(rule_builder).pattern(std::move(p));
      }
      match_count_ = 0;
      auto rule = std::move(rule_builder).apply([this](RuleContext<FuzzSymbol, FuzzAnalysis> &, Match const &) {
        ++match_count_;
      });
      rule.apply(egraph_, matcher, rewrite_ctx);
    }
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
    std::cerr << "Usage: " << argv[0] << " <corpus_file> [--iterations N] [--mode ematcher|vm|both]\n";
    return 1;
  }

  std::string filename = argv[1];
  int iterations = 10000;
  std::string mode = "both";

  for (int i = 2; i < argc; ++i) {
    if (std::strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
      iterations = std::stoi(argv[++i]);
    } else if (std::strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
      mode = argv[++i];
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

  std::cerr << "Running " << iterations << " iterations, mode: " << mode << "\n";

  auto start = std::chrono::high_resolution_clock::now();

  if (mode == "ematcher" || mode == "both") {
    std::cerr << "Running EMatcher...\n";
    runner.run_ematcher(iterations);
  }

  if (mode == "vm" || mode == "both") {
    std::cerr << "Running VM...\n";
    runner.run_vm(iterations);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  std::cerr << "Done in " << duration_ms << "ms\n";
  std::cerr << "Matches per iteration: " << runner.match_count() << "\n";

  return 0;
}
