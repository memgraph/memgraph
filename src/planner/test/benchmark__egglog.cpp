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

#include <sys/wait.h>
#include <unistd.h>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "test_egraph_fixture.hpp"
#include "test_patterns.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

using namespace test;

namespace {

auto is_truthy_env(char const *name) -> bool {
  if (auto const *v = std::getenv(name); v != nullptr) {
    auto const value = std::string_view(v);
    return value == "1" || value == "true" || value == "TRUE" || value == "yes" || value == "YES";
  }
  return false;
}

auto env_to_size_t(char const *name, std::size_t default_value) -> std::size_t {
  if (auto const *v = std::getenv(name); v != nullptr) {
    try {
      return static_cast<std::size_t>(std::stoull(v));
    } catch (...) {
      return default_value;
    }
  }
  return default_value;
}

auto run_cmd_capture(std::string const &cmd) -> std::optional<std::string> {
  std::array<char, 4096> buffer{};
  std::ostringstream out;
  FILE *pipe = popen(cmd.c_str(), "r");
  if (!pipe) return std::nullopt;
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    out << buffer.data();
  }
  auto const status = pclose(pipe);
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) return std::nullopt;
  return out.str();
}

auto run_cmd_nocapture(std::string const &cmd) -> bool {
  auto const status = std::system(cmd.c_str());
  return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

auto egglog_exists() -> bool { return run_cmd_capture("command -v egglog >/dev/null 2>&1 && echo ok").has_value(); }

struct QueryDef {
  std::string name;
  TestPattern pattern;
  std::string egglog_pattern;
  std::vector<std::string> egglog_vars;
};

struct EgglogRunResult {
  bool success{false};
  std::unordered_map<std::string, std::size_t> match_counts;
  std::string output;
};

struct RustEgglogBenchResult {
  bool success{false};
  double query_only_ms{0.0};
  double end_to_end_ms{0.0};
  std::vector<std::size_t> match_counts;
  std::string raw_output;
};

auto count_relation_rows(std::string const &output, std::string const &relation_name) -> std::size_t {
  auto const needle = "(" + relation_name;
  std::size_t count = 0;
  std::istringstream iss(output);
  std::string line;
  while (std::getline(iss, line)) {
    auto const first = line.find_first_not_of(" \t");
    if (first == std::string::npos) continue;
    if (line.substr(first).rfind(needle, 0) == 0) {
      ++count;
    }
  }
  return count;
}

class EgglogProgramBuilder {
 public:
  auto add_leaf(std::string sym, std::uint64_t disambiguator) -> std::string {
    auto const name = next_name();
    lines_.emplace_back("(let " + name + " (" + std::move(sym) + " " + std::to_string(disambiguator) + "))");
    return name;
  }

  auto add_node(std::string sym, std::vector<std::string> children) -> std::string {
    auto const name = next_name();
    std::string expr = "(let " + name + " (" + std::move(sym);
    for (auto const &child : children) {
      expr += " " + child;
    }
    expr += "))";
    lines_.emplace_back(std::move(expr));
    return name;
  }

  void add_union(std::string const &a, std::string const &b) { lines_.emplace_back("(union " + a + " " + b + ")"); }

  auto build_multi_query_program(std::span<QueryDef const> queries, std::span<std::string const> relation_names) const
      -> std::string {
    std::ostringstream out;
    emit_preamble(out);

    for (std::size_t qidx = 0; qidx < queries.size(); ++qidx) {
      auto const &query = queries[qidx];
      auto const &relation_name = relation_names[qidx];

      out << "(relation " << relation_name << " (";
      for (std::size_t i = 0; i < query.egglog_vars.size(); ++i) {
        if (i > 0) out << " ";
        out << "Expr";
      }
      out << "))\n";

      out << "(rule ((= ?root " << query.egglog_pattern << ")) ((" << relation_name;
      for (auto const &v : query.egglog_vars) {
        out << " " << v;
      }
      out << ")))\n";
    }
    out << "(run 1000)\n";
    for (auto const &relation_name : relation_names) {
      out << "(print-function " << relation_name << " 1000000)\n";
    }
    return out.str();
  }

  auto build_baseline_program() const -> std::string {
    std::ostringstream out;
    emit_preamble(out);
    // Baseline includes parsing/loading of datatype + all facts, but no query work.
    out << "(run 0)\n";
    return out.str();
  }

  void clear() {
    lines_.clear();
    counter_ = 0;
  }

 private:
  void emit_preamble(std::ostringstream &out) const {
    out << "(datatype Expr\n";
    out << "  (Const i64)\n";
    out << "  (Var i64)\n";
    out << "  (Neg Expr)\n";
    out << "  (Add Expr Expr)\n";
    out << "  (Mul Expr Expr)\n";
    out << "  (F Expr Expr)\n";
    out << "  (F2 Expr)\n";
    out << "  (F3 Expr Expr Expr)\n";
    out << "  (Test Expr)\n";
    out << ")\n";
    for (auto const &line : lines_) {
      out << line << "\n";
    }
  }

  auto next_name() -> std::string { return "n" + std::to_string(counter_++); }

  std::vector<std::string> lines_;
  std::size_t counter_{0};
};

auto write_temp_program(std::string const &program, std::string_view tag) -> std::optional<std::filesystem::path> {
  auto temp_path = std::filesystem::temp_directory_path() /
                   ("planner_bench_egglog_" + std::string(tag) + "_" + std::to_string(::getpid()) + "_" +
                    std::to_string(std::rand()) + ".egg");
  {
    std::ofstream f(temp_path);
    if (!f) return std::nullopt;
    f << program;
  }
  return temp_path;
}

auto run_egglog_queries_from_file(std::filesystem::path const &program_path,
                                  std::span<std::string const> relation_names) -> EgglogRunResult {
  auto result = EgglogRunResult{};
  auto const cmd = std::string("egglog ") + program_path.string() + " 2>&1";
  auto out = run_cmd_capture(cmd);
  if (!out) return result;

  result.success = true;
  result.output = std::move(*out);
  for (auto const &relation_name : relation_names) {
    result.match_counts[relation_name] = count_relation_rows(result.output, relation_name);
  }
  return result;
}

auto run_egglog_nocapture_from_file(std::filesystem::path const &program_path) -> bool {
  auto const cmd = std::string("egglog ") + program_path.string() + " >/dev/null 2>&1";
  return run_cmd_nocapture(cmd);
}

auto parse_csv_size_t(std::string const &csv) -> std::vector<std::size_t> {
  std::vector<std::size_t> values;
  std::stringstream ss(csv);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (item.empty()) continue;
    values.push_back(static_cast<std::size_t>(std::stoull(item)));
  }
  return values;
}

auto run_rust_egglog_bench(std::string const &bin_path, std::size_t iterations, bool compare_results)
    -> RustEgglogBenchResult {
  auto const cmd =
      "\"" + bin_path + "\" --iterations " + std::to_string(iterations) + (compare_results ? " --compare-results" : "");
  auto out = run_cmd_capture(cmd);
  RustEgglogBenchResult result{};
  if (!out) return result;
  result.success = true;
  result.raw_output = *out;

  std::istringstream iss(*out);
  std::string line;
  while (std::getline(iss, line)) {
    if (line.rfind("RUST_EGGLOG_QUERY_ONLY_MS=", 0) == 0) {
      result.query_only_ms = std::stod(line.substr(std::string("RUST_EGGLOG_QUERY_ONLY_MS=").size()));
    } else if (line.rfind("RUST_EGGLOG_TOTAL_FULL_MS=", 0) == 0) {
      result.end_to_end_ms = std::stod(line.substr(std::string("RUST_EGGLOG_TOTAL_FULL_MS=").size()));
    } else if (line.rfind("RUST_EGGLOG_MATCH_COUNTS=", 0) == 0) {
      result.match_counts = parse_csv_size_t(line.substr(std::string("RUST_EGGLOG_MATCH_COUNTS=").size()));
    }
  }
  return result;
}

}  // namespace

class EgglogMatcherBenchmark : public EGraphTestBase {
 protected:
  using Clock = std::chrono::high_resolution_clock;
  using DurationMs = std::chrono::duration<double, std::milli>;

  void build_complex_graph(bool track_egglog = true) {
    constexpr std::size_t kNumConsts = 180;
    constexpr std::size_t kNumVars = 60;
    constexpr std::size_t kNumLayers = 320;

    consts_.clear();
    vars_.clear();
    if (track_egglog) {
      names_.clear();
      egglog_builder_.clear();
    }
    consts_.reserve(kNumConsts);
    vars_.reserve(kNumVars);

    for (std::size_t i = 0; i < kNumConsts; ++i) {
      auto const id = leaf(Op::Const, static_cast<int>(i));
      consts_.push_back(id);
      if (track_egglog) {
        names_[id.value_of()] = egglog_builder_.add_leaf("Const", i);
      }
    }
    for (std::size_t i = 0; i < kNumVars; ++i) {
      auto const id = leaf(Op::Var, static_cast<int>(i));
      vars_.push_back(id);
      if (track_egglog) {
        names_[id.value_of()] = egglog_builder_.add_leaf("Var", i);
      }
    }

    for (std::size_t i = 0; i < kNumLayers; ++i) {
      auto const c1 = consts_[i % consts_.size()];
      auto const c2 = consts_[(i * 7 + 11) % consts_.size()];
      auto const v1 = vars_[(i * 13 + 3) % vars_.size()];

      auto const add = node(Op::Add, c1, c2);
      if (track_egglog) {
        names_[add.value_of()] = egglog_builder_.add_node("Add", {name_of(c1), name_of(c2)});
      }

      auto const neg = node(Op::Neg, c2);
      if (track_egglog) {
        names_[neg.value_of()] = egglog_builder_.add_node("Neg", {name_of(c2)});
      }

      auto const mul = node(Op::Mul, add, neg);
      if (track_egglog) {
        names_[mul.value_of()] = egglog_builder_.add_node("Mul", {name_of(add), name_of(neg)});
      }

      auto const f = node(Op::F, mul, v1);
      if (track_egglog) {
        names_[f.value_of()] = egglog_builder_.add_node("F", {name_of(mul), name_of(v1)});
      }

      auto const f2 = node(Op::F2, f);
      if (track_egglog) {
        names_[f2.value_of()] = egglog_builder_.add_node("F2", {name_of(f)});
      }

      if ((i % 5) == 0) {
        auto const add_swapped = node(Op::Add, c2, c1);
        if (track_egglog) {
          names_[add_swapped.value_of()] = egglog_builder_.add_node("Add", {name_of(c2), name_of(c1)});
        }
        auto const merged = merge(add, add_swapped);
        (void)merged;
        if (track_egglog) {
          egglog_builder_.add_union(name_of(add), name_of(add_swapped));
        }
      }

      if ((i % 9) == 0) {
        auto const neg2 = node(Op::Neg, c2);
        if (track_egglog) {
          names_[neg2.value_of()] = egglog_builder_.add_node("Neg", {name_of(c2)});
        }
        auto const merged = merge(neg, neg2);
        (void)merged;
        if (track_egglog) {
          egglog_builder_.add_union(name_of(neg), name_of(neg2));
        }
      }
    }

    rebuild_egraph();
  }

  auto make_queries() -> std::vector<QueryDef> {
    std::vector<QueryDef> queries;
    queries.push_back(QueryDef{
        .name = "Neg(?x)",
        .pattern = TestPattern::build(Op::Neg, {Var{kVarX}}, kTestRoot),
        .egglog_pattern = "(Neg ?x)",
        .egglog_vars = {"?x"},
    });
    queries.push_back(QueryDef{
        .name = "Add(?x, ?x)",
        .pattern = TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarX}}, kTestRoot),
        .egglog_pattern = "(Add ?x ?x)",
        .egglog_vars = {"?x"},
    });
    queries.push_back(QueryDef{
        .name = "F(Add(?x, ?y), Neg(?z))",
        .pattern =
            TestPattern::build(Op::F, {Sym(Op::Add, Var{kVarX}, Var{kVarY}), Sym(Op::Neg, Var{kVarZ})}, kTestRoot),
        .egglog_pattern = "(F (Add ?x ?y) (Neg ?z))",
        .egglog_vars = {"?x", "?y", "?z"},
    });
    queries.push_back(QueryDef{
        .name = "F2(F(Mul(?x, ?y), ?z))",
        .pattern =
            TestPattern::build(Op::F2, {Sym(Op::F, Sym(Op::Mul, Var{kVarX}, Var{kVarY}), Var{kVarZ})}, kTestRoot),
        .egglog_pattern = "(F2 (F (Mul ?x ?y) ?z))",
        .egglog_vars = {"?x", "?y", "?z"},
    });
    queries.push_back(QueryDef{
        .name = "Test(?x) [expected no matches]",
        .pattern = TestPattern::build(Op::Test, {Var{kVarX}}, kTestRoot),
        .egglog_pattern = "(Test ?x)",
        .egglog_vars = {"?x"},
    });
    return queries;
  }

  auto name_of(EClassId id) const -> std::string const & {
    auto const it = names_.find(id.value_of());
    if (it == names_.end()) {
      throw std::runtime_error("missing egglog name for eclass");
    }
    return it->second;
  }

  EgglogProgramBuilder egglog_builder_;
  std::vector<EClassId> consts_;
  std::vector<EClassId> vars_;
  std::unordered_map<uint64_t, std::string> names_;
};

TEST_F(EgglogMatcherBenchmark, ComplexGraph_ConsecutivePatterns) {
  if (!egglog_exists()) {
    GTEST_SKIP() << "egglog binary not found in PATH";
  }

  auto const compare_results = is_truthy_env("BENCH_EGGLOG_COMPARE_RESULTS");
  auto const iterations = env_to_size_t("BENCH_EGGLOG_ITERATIONS", 5);

  build_complex_graph();
  auto const queries = make_queries();
  std::vector<std::string> egglog_relation_names;
  egglog_relation_names.reserve(queries.size());
  for (std::size_t i = 0; i < queries.size(); ++i) {
    egglog_relation_names.push_back("MatchResult_" + std::to_string(i));
  }
  auto const full_program = egglog_builder_.build_multi_query_program(queries, egglog_relation_names);
  auto const baseline_program = egglog_builder_.build_baseline_program();
  auto full_program_path = write_temp_program(full_program, "full");
  auto baseline_program_path = write_temp_program(baseline_program, "base");
  ASSERT_TRUE(full_program_path.has_value()) << "Failed to write egglog full program";
  ASSERT_TRUE(baseline_program_path.has_value()) << "Failed to write egglog baseline program";

  EMatcher<Op, NoAnalysis> ematcher(egraph);
  vm::PatternCompiler<Op> compiler;
  vm::VMExecutorVerify<Op, NoAnalysis> vm_executor(egraph);
  EMatchContext ctx;

  std::vector<PatternMatch> matches;
  std::vector<std::size_t> ematcher_counts(queries.size(), 0);
  std::vector<std::size_t> vm_counts(queries.size(), 0);
  std::vector<std::size_t> egglog_counts(queries.size(), 0);

  struct PrecompiledVMQuery {
    vm::CompiledPattern<Op> compiled;
    std::vector<EClassId> candidates;
  };

  std::vector<PrecompiledVMQuery> precompiled_vm_queries;
  precompiled_vm_queries.reserve(queries.size());

  // Warmup all engines once.
  for (std::size_t i = 0; i < queries.size(); ++i) {
    matches.clear();
    ematcher.match_into(queries[i].pattern, ctx, matches);
    ematcher_counts[i] = matches.size();

    auto compiled = compiler.compile(queries[i].pattern);
    ASSERT_TRUE(compiled.has_value()) << "Pattern did not compile: " << queries[i].name;
    std::vector<EClassId> candidates;
    if (auto entry = compiled->entry_symbol(); entry.has_value()) {
      ematcher.candidates_for_symbol(*entry, candidates);
    } else {
      ematcher.all_candidates(candidates);
    }
    matches.clear();
    vm_executor.execute(*compiled, candidates, ctx, matches);
    vm_counts[i] = matches.size();
    precompiled_vm_queries.push_back(
        PrecompiledVMQuery{.compiled = std::move(*compiled), .candidates = std::move(candidates)});
  }
  {
    auto const warmup_ok = run_egglog_nocapture_from_file(*full_program_path);
    ASSERT_TRUE(warmup_ok) << "Egglog warmup execution failed";
    auto egg = run_egglog_queries_from_file(*full_program_path, egglog_relation_names);
    ASSERT_TRUE(egg.success) << "Egglog failed during warmup";
    for (std::size_t i = 0; i < queries.size(); ++i) {
      egglog_counts[i] = egg.match_counts[egglog_relation_names[i]];
    }
  }

  std::size_t ematcher_total_rows = 0;
  std::size_t vm_total_rows = 0;
  std::size_t egglog_total_rows = 0;
  std::size_t ematcher_end_to_end_rows = 0;
  std::size_t vm_end_to_end_rows = 0;

  auto const ematcher_start = Clock::now();
  for (std::size_t it = 0; it < iterations; ++it) {
    for (std::size_t i = 0; i < queries.size(); ++i) {
      matches.clear();
      ematcher.match_into(queries[i].pattern, ctx, matches);
      ematcher_total_rows += matches.size();
      ematcher_counts[i] = matches.size();
    }
  }
  auto const ematcher_time = std::chrono::duration_cast<DurationMs>(Clock::now() - ematcher_start);

  auto const vm_start = Clock::now();
  for (std::size_t it = 0; it < iterations; ++it) {
    for (std::size_t i = 0; i < queries.size(); ++i) {
      auto compiled = compiler.compile(queries[i].pattern);
      ASSERT_TRUE(compiled.has_value()) << "Pattern did not compile: " << queries[i].name;
      std::vector<EClassId> candidates;
      if (auto entry = compiled->entry_symbol(); entry.has_value()) {
        ematcher.candidates_for_symbol(*entry, candidates);
      } else {
        ematcher.all_candidates(candidates);
      }
      matches.clear();
      vm_executor.execute(*compiled, candidates, ctx, matches);
      vm_total_rows += matches.size();
      vm_counts[i] = matches.size();
    }
  }
  auto const vm_time = std::chrono::duration_cast<DurationMs>(Clock::now() - vm_start);

  std::size_t vm_exec_only_rows = 0;
  auto const vm_exec_only_start = Clock::now();
  for (std::size_t it = 0; it < iterations; ++it) {
    for (std::size_t i = 0; i < queries.size(); ++i) {
      matches.clear();
      vm_executor.execute(precompiled_vm_queries[i].compiled, precompiled_vm_queries[i].candidates, ctx, matches);
      vm_exec_only_rows += matches.size();
    }
  }
  auto const vm_exec_only_time = std::chrono::duration_cast<DurationMs>(Clock::now() - vm_exec_only_start);

  DurationMs ematcher_end_to_end_time{0.0};
  DurationMs vm_end_to_end_time{0.0};

  // End-to-end EMatcher: build + query per iteration
  for (std::size_t it = 0; it < iterations; ++it) {
    auto const t0 = Clock::now();
    egraph.clear();
    build_complex_graph(false);
    EMatcher<Op, NoAnalysis> ematcher_e2e(egraph);
    EMatchContext local_ctx;
    std::vector<PatternMatch> local_matches;
    for (std::size_t i = 0; i < queries.size(); ++i) {
      local_matches.clear();
      ematcher_e2e.match_into(queries[i].pattern, local_ctx, local_matches);
      ematcher_end_to_end_rows += local_matches.size();
    }
    ematcher_end_to_end_time += std::chrono::duration_cast<DurationMs>(Clock::now() - t0);
  }

  // End-to-end VM: build + compile + candidate selection + execute per iteration
  for (std::size_t it = 0; it < iterations; ++it) {
    auto const t0 = Clock::now();
    egraph.clear();
    build_complex_graph(false);
    EMatcher<Op, NoAnalysis> ematcher_for_candidates(egraph);
    vm::PatternCompiler<Op> compiler_e2e;
    vm::VMExecutorVerify<Op, NoAnalysis> vm_executor_e2e(egraph);
    EMatchContext local_ctx;
    std::vector<PatternMatch> local_matches;
    for (std::size_t i = 0; i < queries.size(); ++i) {
      auto compiled = compiler_e2e.compile(queries[i].pattern);
      ASSERT_TRUE(compiled.has_value()) << "Pattern did not compile: " << queries[i].name;
      std::vector<EClassId> candidates;
      if (auto entry = compiled->entry_symbol(); entry.has_value()) {
        ematcher_for_candidates.candidates_for_symbol(*entry, candidates);
      } else {
        ematcher_for_candidates.all_candidates(candidates);
      }
      local_matches.clear();
      vm_executor_e2e.execute(*compiled, candidates, local_ctx, local_matches);
      vm_end_to_end_rows += local_matches.size();
    }
    vm_end_to_end_time += std::chrono::duration_cast<DurationMs>(Clock::now() - t0);
  }

  DurationMs egglog_query_only_time{0.0};
  DurationMs egglog_end_to_end_time{0.0};
  for (std::size_t it = 0; it < iterations; ++it) {
    auto const full_start = Clock::now();
    auto const full_ok = run_egglog_nocapture_from_file(*full_program_path);
    auto const full_time = std::chrono::duration_cast<DurationMs>(Clock::now() - full_start);
    ASSERT_TRUE(full_ok) << "Egglog full run failed during benchmark iteration " << it;
    egglog_end_to_end_time += full_time;

    auto const base_start = Clock::now();
    auto const base_ok = run_egglog_nocapture_from_file(*baseline_program_path);
    auto const base_time = std::chrono::duration_cast<DurationMs>(Clock::now() - base_start);
    ASSERT_TRUE(base_ok) << "Egglog baseline run failed during benchmark iteration " << it;

    if (full_time > base_time) {
      egglog_query_only_time += (full_time - base_time);
    }
  }
  auto const rows_per_iteration = [&]() {
    std::size_t total = 0;
    for (auto const c : egglog_counts) total += c;
    return total;
  }();
  egglog_total_rows = rows_per_iteration * iterations;

  std::cout << "\n=== Egglog vs EMatcher vs VM Benchmark ===\n";
  std::cout << "iterations: " << iterations << ", queries per iteration: " << queries.size() << "\n";
  std::cout << "\n-- Query-only mode (prebuilt graph) --\n";
  std::cout << "EMatcher total: " << ematcher_time.count() << " ms, rows=" << ematcher_total_rows << "\n";
  std::cout << "VM total:       " << vm_time.count() << " ms, rows=" << vm_total_rows
            << " (compile+candidates+execute)\n";
  std::cout << "VM exec-only:   " << vm_exec_only_time.count() << " ms, rows=" << vm_exec_only_rows << "\n";
  std::cout << "Egglog total:   " << egglog_query_only_time.count()
            << " ms (baseline-subtracted), rows=" << egglog_total_rows << "\n";
  std::cout << "Speedup (EMatcher/VM): " << (ematcher_time / vm_time) << "x\n";
  std::cout << "Speedup (Egglog/VM):   " << (egglog_query_only_time / vm_time) << "x\n";
  std::cout << "\n-- End-to-end mode (build + query) --\n";
  std::cout << "EMatcher total: " << ematcher_end_to_end_time.count() << " ms, rows=" << ematcher_end_to_end_rows
            << "\n";
  std::cout << "VM total:       " << vm_end_to_end_time.count() << " ms, rows=" << vm_end_to_end_rows << "\n";
  std::cout << "Egglog total:   " << egglog_end_to_end_time.count() << " ms, rows=" << egglog_total_rows << "\n";
  std::cout << "Speedup (EMatcher/VM): " << (ematcher_end_to_end_time / vm_end_to_end_time) << "x\n";
  std::cout << "Speedup (Egglog/VM):   " << (egglog_end_to_end_time / vm_end_to_end_time) << "x\n";

  if (auto const *rust_bin = std::getenv("BENCH_EGGLOG_RUST_BIN");
      rust_bin != nullptr && std::string_view(rust_bin).size() > 0) {
    auto rust_result = run_rust_egglog_bench(rust_bin, iterations, compare_results);
    ASSERT_TRUE(rust_result.success) << "Rust egglog benchmark failed: " << rust_bin;
    std::cout << "Rust egglog total (query-only): " << rust_result.query_only_ms
              << " ms (library, baseline-subtracted)\n";
    std::cout << "Rust egglog total (end-to-end): " << rust_result.end_to_end_ms << " ms\n";
    std::cout << "Speedup (RustEgglog/VM, query-only): " << (rust_result.query_only_ms / vm_time.count()) << "x\n";
    std::cout << "Speedup (RustEgglog/VM, end-to-end): " << (rust_result.end_to_end_ms / vm_end_to_end_time.count())
              << "x\n";
    if (compare_results) {
      ASSERT_EQ(rust_result.match_counts.size(), ematcher_counts.size())
          << "Rust egglog returned different number of query counts.\n"
          << rust_result.raw_output;
      for (std::size_t i = 0; i < ematcher_counts.size(); ++i) {
        EXPECT_EQ(ematcher_counts[i], rust_result.match_counts[i])
            << "Mismatch between EMatcher and Rust egglog for query: " << queries[i].name;
      }
    }
  }

  std::cout << "\nPer-query last-iteration match counts:\n";
  for (std::size_t i = 0; i < queries.size(); ++i) {
    std::cout << "  " << queries[i].name << " => "
              << "EMatcher=" << ematcher_counts[i] << ", VM=" << vm_counts[i] << ", Egglog=" << egglog_counts[i]
              << "\n";
  }

  if (compare_results) {
    for (std::size_t i = 0; i < queries.size(); ++i) {
      EXPECT_EQ(ematcher_counts[i], vm_counts[i]) << "Mismatch between EMatcher and VM for query: " << queries[i].name;
      EXPECT_EQ(ematcher_counts[i], egglog_counts[i])
          << "Mismatch between EMatcher and Egglog for query: " << queries[i].name;
    }
  }

  std::filesystem::remove(*full_program_path);
  std::filesystem::remove(*baseline_program_path);
}

}  // namespace memgraph::planner::core
