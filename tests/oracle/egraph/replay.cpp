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
//
// Anything malformed answers `error <reason>`.

#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

// The module instantiates boost::container templates whose placement new
// overload must be visible in this translation unit.
#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "utils/small_vector.hpp"

import memgraph.planner.core.egraph;

namespace {

using memgraph::planner::core::EClassId;
using memgraph::planner::core::EGraph;
using memgraph::planner::core::ProcessingContext;

enum class Sym : uint8_t {};

struct NoFacts {
  void merge(NoFacts const & /*other*/) {}

  auto operator==(NoFacts const &) const -> bool = default;
};

class Replay {
 public:
  auto Handle(std::string const &line) -> std::string {
    std::istringstream in(line);
    std::string command;
    in >> command;
    if (command == "reset") {
      graph_ = EGraph<Sym, NoFacts>{};
      terms_.clear();
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
    auto const result =
        arity == 0 ? graph_.emplace(sym, disambiguator) : graph_.emplace(sym, std::move(children), disambiguator);
    terms_.push_back(result.eclass_id);
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

  EGraph<Sym, NoFacts> graph_;
  ProcessingContext<Sym> ctx_;
  std::vector<EClassId> terms_;
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
