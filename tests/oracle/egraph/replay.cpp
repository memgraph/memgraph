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

// Drives the planner's EGraph from commands on stdin, one per line, and
// answers each with one line on stdout, so that a test written in any
// language can check the e-graph without being compiled against it.
//
// Terms are numbered from 0 in the order `add` creates them; `add` and
// `merge` take term numbers, and answers report e-class ids.
//
//   reset                          -> ok
//   add <sym> <dis> <n> <t1>..<tn> -> ok <did_insert> <eclass>
//   merge <ta> <tb>                -> ok <did_merge> <eclass>
//   rebuild                        -> ok
//   clear_touched                  -> ok
//   find <t>                       -> ok <eclass>
//   needs_rebuild                  -> ok <0|1>
//   stats                          -> ok <num_classes> <num_live_nodes>
//   class <eclass>                 -> ok <k> then k e-nodes, each <sym> <dis> <n> <child eclass>..
//   touched                        -> ok <k> <eclass>..
//   refresh <0|1>                  -> ok <updates that widened a class>  (1: everything, 0: what changed)
//   facts <t>                      -> ok <facts of t's class, as a 16-bit set>
//   match <k> <pattern>..          -> ok <raw> <v> <var>.. then raw matches, each v e-classes
//   match_inc <k> <pattern>..      -> as match, over an index kept between calls
//
// `match` compiles k patterns into one matcher, runs it over a freshly built
// symbol index, and reports every match the VM emitted, duplicates included:
// for each, the canonical e-class bound to each variable, variables in
// ascending id order. `match_inc` keeps one index from its first call until
// `reset`, and before each match adds to it the e-classes of every e-node
// `add` inserted since the previous call, as the rewriter does between
// passes. A pattern is written in prefix order:
//
//   v <id>                         a variable
//   w                              a wildcard
//   s <sym> <n> <bind|-1> <kid>..  a symbol with n children, optionally binding a variable
//
// Facts are the analysis the Rust model specifies (egraph_oracle::analysis):
// a set of bits joined by union. Symbol 0 seeds bit <dis> % 16, symbol 2
// derives the union of its children's facts, and other symbols do neither.
//
// Anything malformed answers `error <reason>`.

#include <algorithm>
#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

// The module instantiates boost::container templates whose placement new
// overload must be visible in this translation unit.
#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/pattern/match.hpp"
#include "planner/pattern/match_index.hpp"
#include "planner/pattern/pattern.hpp"
#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "utils/small_vector.hpp"

import memgraph.planner.core.egraph;

namespace {

using memgraph::planner::core::EClassId;
using memgraph::planner::core::EGraph;
using memgraph::planner::core::ProcessingContext;

enum class Sym : uint8_t {};

struct Mask {
  uint16_t bits = 0;

  void merge(Mask const &other) { bits |= other.bits; }

  auto operator==(Mask const &) const -> bool = default;
};

constexpr uint8_t kLeaf = 0;
constexpr uint8_t kPlus = 2;

auto Seed(uint32_t symbol, uint64_t disambiguator) -> Mask {
  return symbol == kLeaf ? Mask{static_cast<uint16_t>(1U << (disambiguator % 16))} : Mask{};
}

using Graph = EGraph<Sym, Mask>;
using Pattern = memgraph::planner::core::pattern::Pattern<Sym>;
using memgraph::planner::core::pattern::PatternNodeId;
using memgraph::planner::core::pattern::PatternVar;
using Index = memgraph::planner::core::pattern::MatcherIndex<Sym, Mask>;

class Replay {
 public:
  auto Handle(std::string const &line) -> std::string {
    std::istringstream in(line);
    std::string command;
    in >> command;
    if (command == "reset") {
      graph_ = Graph{};
      terms_.clear();
      index_.reset();
      inserted_.clear();
      return "ok";
    }
    if (command == "add") return Add(in);
    if (command == "merge") {
      uint32_t a = 0;
      uint32_t b = 0;
      if (!(in >> a >> b) || a >= terms_.size() || b >= terms_.size()) return "error bad merge";
      auto const result = graph_.merge(terms_[a], terms_[b]);
      return "ok " + std::to_string(int{result.did_merge}) + " " + Id(result.eclass_id);
    }
    if (command == "rebuild") {
      graph_.rebuild(ctx_);
      return "ok";
    }
    if (command == "clear_touched") {
      graph_.clear_touched();
      return "ok";
    }
    if (command == "find") {
      uint32_t t = 0;
      if (!(in >> t) || t >= terms_.size()) return "error bad term";
      return "ok " + Id(graph_.find(terms_[t]));
    }
    if (command == "needs_rebuild") return std::string("ok ") + (graph_.needs_rebuild() ? "1" : "0");
    if (command == "stats") {
      return "ok " + std::to_string(graph_.num_classes()) + " " + std::to_string(graph_.num_live_nodes());
    }
    if (command == "class") return Class(in);
    if (command == "refresh") {
      int everything = 0;
      if (!(in >> everything)) return "error bad refresh";
      return "ok " + std::to_string(everything != 0 ? Refresh<Graph::AnalysisScope::Everything>()
                                                    : Refresh<Graph::AnalysisScope::Changed>());
    }
    if (command == "facts") {
      uint32_t t = 0;
      if (!(in >> t) || t >= terms_.size()) return "error bad term";
      return "ok " + std::to_string(graph_.analysis_of(terms_[t]).bits);
    }
    if (command == "match") {
      Index index(graph_);
      return MatchCommand(in, index);
    }
    if (command == "match_inc") {
      if (!index_) {
        index_.emplace(graph_);
      } else {
        index_->rebuild_index(std::span<EClassId const>(inserted_));
      }
      inserted_.clear();
      return MatchCommand(in, *index_);
    }
    if (command == "touched") {
      boost::unordered_flat_set<EClassId> touched;
      graph_.touched_eclasses_into(touched);
      std::string out = "ok " + std::to_string(touched.size());
      for (auto id : touched) out += " " + Id(id);
      return out;
    }
    return "error unknown command";
  }

 private:
  static auto Id(EClassId id) -> std::string { return std::to_string(id.value_of()); }

  auto Add(std::istringstream &in) -> std::string {
    uint32_t symbol = 0;
    uint64_t disambiguator = 0;
    std::size_t arity = 0;
    if (!(in >> symbol >> disambiguator >> arity) || symbol > 255) return "error bad add";
    memgraph::utils::small_vector<EClassId> children;
    for (std::size_t k = 0; k < arity; ++k) {
      uint32_t t = 0;
      if (!(in >> t) || t >= terms_.size()) return "error bad child";
      children.push_back(graph_.find(terms_[t]));
    }
    auto const sym = static_cast<Sym>(symbol);
    auto const seed = Seed(symbol, disambiguator);
    auto const result = arity == 0 ? graph_.emplace(sym, disambiguator, seed)
                                   : graph_.emplace(sym, std::move(children), disambiguator, seed);
    terms_.push_back(result.eclass_id);
    if (result.did_insert) inserted_.push_back(result.eclass_id);
    return "ok " + std::to_string(int{result.did_insert}) + " " + Id(result.eclass_id);
  }

  auto Class(std::istringstream &in) -> std::string {
    uint32_t raw = 0;
    if (!(in >> raw)) return "error bad class";
    auto const id = EClassId{raw};
    if (!graph_.has_class(id)) return "error not a canonical class";
    auto const &eclass = graph_.eclass(id);
    std::string out = "ok " + std::to_string(eclass.size());
    for (auto enode_id : eclass.nodes()) {
      auto const &enode = graph_.get_enode(enode_id);
      out += " " + std::to_string(static_cast<uint32_t>(enode.symbol())) + " " + std::to_string(enode.disambiguator()) +
             " " + std::to_string(enode.arity());
      for (auto child : enode.children()) out += " " + Id(graph_.find(child));
    }
    return out;
  }

  /// Reads one pattern node and its subtree, adding children before their
  /// parent so that the node read first becomes the pattern's root.
  static auto ReadNode(std::istringstream &in, Pattern::Builder &builder) -> std::optional<PatternNodeId> {
    std::string kind;
    if (!(in >> kind)) return std::nullopt;
    if (kind == "w") return builder.wildcard();
    if (kind == "v") {
      uint32_t id = 0;
      if (!(in >> id) || id > 255) return std::nullopt;
      return builder.var(PatternVar{static_cast<uint8_t>(id)});
    }
    if (kind != "s") return std::nullopt;
    uint32_t symbol = 0;
    std::size_t arity = 0;
    int bind = -1;
    if (!(in >> symbol >> arity >> bind) || symbol > 255 || bind > 255) return std::nullopt;
    memgraph::utils::small_vector<PatternNodeId> children;
    for (std::size_t k = 0; k < arity; ++k) {
      auto child = ReadNode(in, builder);
      if (!child) return std::nullopt;
      children.push_back(*child);
    }
    auto const binding = bind < 0 ? std::nullopt : std::optional{PatternVar{static_cast<uint8_t>(bind)}};
    auto const sym = static_cast<Sym>(symbol);
    return arity == 0 ? builder.sym(sym, binding) : builder.sym(sym, std::move(children), binding);
  }

  auto MatchCommand(std::istringstream &in, Index &index) -> std::string {
    std::size_t count = 0;
    if (!(in >> count) || count == 0) return "error bad match";
    std::vector<Pattern> patterns;
    for (std::size_t p = 0; p < count; ++p) {
      Pattern::Builder builder;
      if (!ReadNode(in, builder)) return "error bad pattern";
      patterns.push_back(std::move(builder).build());
    }
    try {
      memgraph::planner::core::pattern::vm::PatternsCompiler<Sym> compiler;
      auto const compiled = compiler.compile(std::span<Pattern const>(patterns));
      memgraph::planner::core::pattern::vm::VMExecutor<Sym, Mask> executor(graph_);
      memgraph::planner::core::pattern::EMatchContext ctx;
      std::vector<memgraph::planner::core::pattern::PatternMatch> matches;
      executor.execute(compiled, index, ctx.arena(), matches);

      std::vector<PatternVar> vars;
      for (auto const &[var, slot] : compiled.var_slots()) vars.push_back(var);
      std::ranges::sort(vars);
      std::string out = "ok " + std::to_string(matches.size()) + " " + std::to_string(vars.size());
      for (auto var : vars) out += " " + std::to_string(var.id);
      memgraph::planner::core::pattern::MatchBindings bindings(compiled.var_slots(), ctx.arena());
      for (auto const &pattern_match : matches) {
        auto const match = bindings.match(pattern_match);
        for (auto var : vars) out += " " + Id(graph_.find(match[var]));
      }
      return out;
    } catch (std::exception const &e) {
      return std::string("error ") + e.what();
    }
  }

  template <Graph::AnalysisScope Scope>
  auto Refresh() -> std::size_t {
    return graph_.template refresh_analysis<Scope>(ctx_, [this](auto const &enode, Mask &facts) {
      if (static_cast<uint8_t>(enode.symbol()) != kPlus) return false;
      uint16_t derived = 0;
      for (auto child : enode.children()) derived |= graph_.analysis_of(child).bits;
      auto const before = facts.bits;
      facts.bits |= derived;
      return facts.bits != before;
    });
  }

  Graph graph_;
  ProcessingContext<Sym> ctx_;
  std::vector<EClassId> terms_;
  /// The index `match_inc` keeps, and the e-classes inserted since it last saw them.
  std::optional<Index> index_;
  std::vector<EClassId> inserted_;
};

}  // namespace

auto main() -> int {
  std::ios::sync_with_stdio(false);
  Replay replay;
  std::string line;
  while (std::getline(std::cin, line)) {
    std::cout << replay.Handle(line) << '\n' << std::flush;
  }
  return 0;
}
