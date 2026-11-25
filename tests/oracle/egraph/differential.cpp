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

// The e-graph holds the congruence closure of what it was told, as computed
// by the Rust reference model, whenever it has just been rebuilt:
//
//  - two terms share an e-class exactly when the model puts them in one block;
//  - there is one e-class per block, and its e-nodes are exactly the distinct
//    canonical e-nodes of that block, so every duplicate has been removed;
//  - no rebuild is pending;
//  - the touched set names every e-class inserted, merged or merged by a
//    rebuild's congruence cascade since it was last cleared.
//
// Between rebuilds the e-graph may lag the closure, so only these hold there:
// a merge of two terms the closure keeps apart reports did_merge, a merge on a
// rebuilt graph reports did_merge exactly when the closure keeps them apart,
// and an emplace on a rebuilt graph reports did_insert exactly when no term
// has that canonical form.
//
// Terms are referred to by the order they were added, never by id, so the two
// sides are free to number their e-classes differently.

#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <variant>
#include <vector>

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>
// The module instantiates boost::container templates whose placement new
// overload must be visible in this translation unit.
#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "egraph_oracle.h"
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

using Graph = EGraph<Sym, NoFacts>;

struct OracleDeleter {
  void operator()(egraph_oracle *model) const { egraph_oracle_free(model); }
};

using Oracle = std::unique_ptr<egraph_oracle, OracleDeleter>;

// Indices into the terms added so far are drawn as raw values and taken modulo
// the count when applied, so any sequence is valid and shrinks freely.
struct Add {
  uint8_t symbol;
  uint64_t disambiguator;
  std::vector<uint32_t> children;
};

struct Merge {
  uint32_t a;
  uint32_t b;
};

struct Rebuild {};

struct ClearTouched {};

using Op = std::variant<Add, Merge, Rebuild, ClearTouched>;

std::ostream &operator<<(std::ostream &os, Add const &op) {
  os << "Add(" << int{op.symbol} << "#" << op.disambiguator;
  for (auto c : op.children) os << ", t" << c;
  return os << ")";
}

std::ostream &operator<<(std::ostream &os, Merge const &op) { return os << "Merge(t" << op.a << ", t" << op.b << ")"; }

std::ostream &operator<<(std::ostream &os, Rebuild const &) { return os << "Rebuild"; }

std::ostream &operator<<(std::ostream &os, ClearTouched const &) { return os << "ClearTouched"; }

std::ostream &operator<<(std::ostream &os, Op const &op) {
  return std::visit([&](auto const &o) -> std::ostream & { return os << o; }, op);
}

// Few symbols and disambiguators so that adds collide with existing terms and
// merges set off congruence cascades.
rc::Gen<Op> GenOp() {
  auto children = rc::gen::mapcat(rc::gen::inRange<size_t>(0, 4), [](size_t arity) {
    return rc::gen::container<std::vector<uint32_t>>(arity, rc::gen::arbitrary<uint32_t>());
  });
  auto add = rc::gen::build<Add>(rc::gen::set(&Add::symbol, rc::gen::inRange<uint8_t>(0, 4)),
                                 rc::gen::set(&Add::disambiguator, rc::gen::inRange<uint64_t>(0, 3)),
                                 rc::gen::set(&Add::children, children));
  return rc::gen::weightedOneOf<Op>({
      {6, rc::gen::cast<Op>(add)},
      {3,
       rc::gen::map(rc::gen::pair(rc::gen::arbitrary<uint32_t>(), rc::gen::arbitrary<uint32_t>()),
                    [](auto ab) { return Op{Merge{ab.first, ab.second}}; })},
      {1, rc::gen::just(Op{Rebuild{}})},
      {1, rc::gen::just(Op{ClearTouched{}})},
  });
}

class Harness {
 public:
  void Apply(Op const &op) {
    std::visit([&](auto const &o) { Do(o); }, op);
  }

  // A final rebuild, so every sequence ends with the full comparison.
  void Finish() { Do(Rebuild{}); }

 private:
  auto Term(uint32_t raw) const -> uint32_t { return raw % static_cast<uint32_t>(cpp_ids_.size()); }

  auto Find(uint32_t term) const -> EClassId { return graph_.find(cpp_ids_[term]); }

  void Do(Add const &op) {
    if (cpp_ids_.empty() && !op.children.empty()) return;
    std::vector<uint32_t> children;
    for (auto c : op.children) children.push_back(Term(c));

    memgraph::utils::small_vector<EClassId> cpp_children;
    for (auto c : children) cpp_children.push_back(Find(c));

    bool const clean = !graph_.needs_rebuild();
    std::vector<uint32_t> child_blocks;
    uint32_t existing = EGRAPH_ORACLE_NONE;
    if (clean) {
      egraph_oracle_close(model_.get());
      for (auto c : children) child_blocks.push_back(egraph_oracle_block_of(model_.get(), c));
      existing =
          egraph_oracle_find_enode(model_.get(), op.symbol, op.disambiguator, child_blocks.data(), child_blocks.size());
    }

    auto const sym = static_cast<Sym>(op.symbol);
    auto const result = op.children.empty() ? graph_.emplace(sym, op.disambiguator)
                                            : graph_.emplace(sym, std::move(cpp_children), op.disambiguator);
    if (clean) {
      RC_ASSERT(result.did_insert == (existing == EGRAPH_ORACLE_NONE));
    }
    cpp_ids_.push_back(result.eclass_id);
    auto const term = egraph_oracle_add(model_.get(), op.symbol, op.disambiguator, children.data(), children.size());
    RC_ASSERT(term == cpp_ids_.size() - 1);
    if (result.did_insert) changed_since_clear_.push_back(term);
  }

  void Do(Merge const &op) {
    if (cpp_ids_.empty()) return;
    auto const a = Term(op.a);
    auto const b = Term(op.b);
    bool const clean = !graph_.needs_rebuild();
    egraph_oracle_close(model_.get());
    bool const apart = !egraph_oracle_equivalent(model_.get(), a, b);

    auto const result = graph_.merge(cpp_ids_[a], cpp_ids_[b]);
    if (apart) RC_ASSERT(result.did_merge);
    if (clean) RC_ASSERT(result.did_merge == apart);
    if (result.did_merge) {
      RC_ASSERT(graph_.needs_rebuild());
      changed_since_clear_.push_back(a);
    }
    egraph_oracle_merge(model_.get(), a, b);
  }

  void Do(ClearTouched const &) {
    graph_.clear_touched();
    changed_since_clear_.clear();
  }

  void Do(Rebuild const &) {
    std::vector<EClassId> before;
    for (uint32_t t = 0; t < cpp_ids_.size(); ++t) before.push_back(Find(t));

    graph_.rebuild(ctx_);
    egraph_oracle_close(model_.get());
    RC_ASSERT(!graph_.needs_rebuild());

    // Terms that were in different e-classes before this rebuild and share
    // one now were merged by its congruence cascade.
    std::map<EClassId, EClassId> first_before_of_class;
    for (uint32_t t = 0; t < cpp_ids_.size(); ++t) {
      auto [it, fresh] = first_before_of_class.try_emplace(Find(t), before[t]);
      if (!fresh && it->second != before[t]) changed_since_clear_.push_back(t);
    }

    CheckPartition();
    CheckENodes();
    CheckTouched();
  }

  void CheckPartition() {
    std::map<EClassId, uint32_t> block_of_class;
    std::map<uint32_t, EClassId> class_of_block;
    for (uint32_t t = 0; t < cpp_ids_.size(); ++t) {
      auto const cls = Find(t);
      auto const block = egraph_oracle_block_of(model_.get(), t);
      RC_ASSERT(graph_.has_class(cls));
      auto [b, b_fresh] = block_of_class.try_emplace(cls, block);
      auto [c, c_fresh] = class_of_block.try_emplace(block, cls);
      RC_ASSERT(b->second == block);
      RC_ASSERT(c->second == cls);
    }
    RC_ASSERT(graph_.num_classes() == egraph_oracle_num_blocks(model_.get()));
    block_of_class_ = std::move(block_of_class);
  }

  void CheckENodes() {
    RC_ASSERT(graph_.num_live_nodes() == egraph_oracle_num_enodes(model_.get()));
    for (auto const &[cls, block] : block_of_class_) {
      auto const &eclass = graph_.eclass(cls);
      RC_ASSERT(eclass.size() == egraph_oracle_enodes_in_block(model_.get(), block));
      for (auto enode_id : eclass.nodes()) {
        auto const &enode = graph_.get_enode(enode_id);
        std::vector<uint32_t> child_blocks;
        for (auto child : enode.children()) {
          auto it = block_of_class_.find(graph_.find(child));
          RC_ASSERT(it != block_of_class_.end());
          child_blocks.push_back(it->second);
        }
        auto const found = egraph_oracle_find_enode(model_.get(),
                                                    static_cast<uint8_t>(enode.symbol()),
                                                    enode.disambiguator(),
                                                    child_blocks.data(),
                                                    child_blocks.size());
        RC_ASSERT(found == block);
      }
    }
  }

  void CheckTouched() {
    boost::unordered_flat_set<EClassId> touched;
    graph_.touched_eclasses_into(touched);
    for (auto t : changed_since_clear_) {
      RC_ASSERT(touched.contains(Find(t)));
    }
    for (auto cls : touched) {
      RC_ASSERT(block_of_class_.contains(cls));
    }
  }

  Graph graph_;
  ProcessingContext<Sym> ctx_;
  Oracle model_{egraph_oracle_new()};
  std::vector<EClassId> cpp_ids_;
  std::vector<uint32_t> changed_since_clear_;
  std::map<EClassId, uint32_t> block_of_class_;
};

}  // namespace

RC_GTEST_PROP(egraph_differential, HoldsTheCongruenceClosureAfterEveryRebuild, ()) {
  auto const ops = *rc::gen::container<std::vector<Op>>(GenOp());
  Harness harness;
  for (auto const &op : ops) harness.Apply(op);
  harness.Finish();
}
