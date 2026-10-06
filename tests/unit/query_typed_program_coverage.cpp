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

// Counts how many of the filters in a body of real queries a typed program can
// take, and names what the rest hold. The queries are stripped and parsed the
// way a cached query is, so a literal arrives as a parameter, and then planned,
// because the filter an operator runs per row is not the WHERE clause that was
// written: the planner folds the label a pattern names into it, and splits and
// reorders the rest.
//
// The corpus is a file of one query per line, named by MG_QUERY_CORPUS. With
// none given there is nothing to measure and the test says so rather than
// reporting a number it did not take.

#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "query_plan_checker.hpp"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/stripped.hpp"
#include "query/interpret/typed_program.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/planner.hpp"
#include "utils/typeinfo.hpp"

namespace {

using memgraph::query::AstStorage;
using memgraph::query::Expression;
using memgraph::query::TypedProgram;

// Collects the expressions that decide whether a row survives, which are the
// ones that run once per row and so are the ones worth compiling.
class FilterCollector : public memgraph::query::plan::HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool PreVisit(memgraph::query::plan::Filter &filter) override {
    if (filter.expression_ != nullptr) filters.push_back(filter.expression_);
    return true;
  }

  bool Visit(memgraph::query::plan::Once & /*unused*/) override { return true; }

  std::vector<Expression *> filters;
};

// The planner turns a conjunction into one Filter per conjunct where it can,
// but not always, so a filter holding one expression this cannot take would
// otherwise charge every conjunct in it for the worst one.
void SplitConjuncts(Expression *expression, std::vector<Expression *> &out) {
  if (expression->GetTypeInfo().id == memgraph::utils::TypeId::AST_AND_OPERATOR) {
    auto *conjunction = static_cast<memgraph::query::AndOperator *>(expression);
    SplitConjuncts(conjunction->expression1_, out);
    SplitConjuncts(conjunction->expression2_, out);
    return;
  }
  out.push_back(expression);
}

std::vector<std::string> ReadCorpus(char const *path) {
  std::vector<std::string> queries;
  std::ifstream input{path};
  std::string line;
  while (std::getline(input, line)) {
    if (!line.empty()) queries.push_back(line);
  }
  return queries;
}

}  // namespace

TEST(TypedProgramCoverage, ReportsWhatShareOfRealFiltersCompile) {
  char const *corpus_path = std::getenv("MG_QUERY_CORPUS");
  if (corpus_path == nullptr) {
    GTEST_SKIP() << "set MG_QUERY_CORPUS to a file of one query per line";
  }

  auto const queries = ReadCorpus(corpus_path);
  ASSERT_FALSE(queries.empty()) << "no queries read from " << corpus_path;

  size_t parsed = 0;
  size_t unparsed = 0;
  size_t other = 0;
  size_t filters = 0;
  size_t compiled = 0;
  std::map<std::string, size_t> refused_holding;

  for (auto const &query : queries) {
    AstStorage storage;
    FilterCollector collector;
    try {
      memgraph::query::frontend::StrippedQuery const stripped{query};
      memgraph::query::frontend::opencypher::Parser parser{stripped.stripped_query().str()};
      memgraph::query::Parameters parameters;
      memgraph::query::frontend::ParsingContext context;
      context.is_query_cached = true;
      memgraph::query::frontend::CypherMainVisitor visitor{context, &storage, &parameters};
      visitor.visit(parser.tree());
      auto *cypher = dynamic_cast<memgraph::query::CypherQuery *>(visitor.query());
      if (cypher == nullptr) {
        ++other;
        continue;
      }
      auto symbols = memgraph::query::MakeSymbolTable(cypher);
      memgraph::query::plan::FakeDbAccessor dba;
      auto planning_context = memgraph::query::plan::MakePlanningContext(&storage, &symbols, cypher, &dba);
      auto query_parts = memgraph::query::plan::CollectQueryParts(symbols, storage, cypher, false);
      auto plan = memgraph::query::plan::MakeLogicalPlanForSingleQuery<memgraph::query::plan::RuleBasedPlanner>(
          query_parts, &planning_context);
      memgraph::query::plan::PostProcessor post_processor{parameters, {}, planning_context.db};
      plan = post_processor.Rewrite(std::move(plan), &planning_context);
      plan->Accept(collector);
      ++parsed;
    } catch (std::exception const &) {
      ++unparsed;
      continue;
    }

    std::vector<Expression *> conjuncts;
    for (auto *filter : collector.filters) SplitConjuncts(filter, conjuncts);

    for (auto *filter : conjuncts) {
      ++filters;
      Expression *refused_on = nullptr;
      if (TypedProgram::Compile(filter, &refused_on).has_value()) {
        ++compiled;
        continue;
      }
      if (refused_on == nullptr) {
        ++refused_holding["unknown"];
        continue;
      }
      std::string reason = refused_on->GetTypeInfo().name;
      // A function is only a reason once it is named: which ones come up
      // decides whether any are worth taking.
      if (refused_on->GetTypeInfo().id == memgraph::utils::TypeId::AST_FUNCTION) {
        reason += " " + static_cast<memgraph::query::Function *>(refused_on)->function_name_;
      }
      ++refused_holding[reason];
    }
  }

  std::cerr << "queries " << parsed << " parsed, " << unparsed << " rejected, " << other << " not a read\n"
            << "filters " << filters << ", compiled " << compiled << "\n";
  if (filters != 0) {
    std::cerr << "share " << (100.0 * static_cast<double>(compiled) / static_cast<double>(filters)) << "%\n";
  }
  std::cerr << "what stopped the refused filters:\n";
  std::vector<std::pair<size_t, std::string>> ranked;
  ranked.reserve(refused_holding.size());
  for (auto const &[name, count] : refused_holding) ranked.emplace_back(count, name);
  std::sort(ranked.rbegin(), ranked.rend());
  for (auto const &[count, name] : ranked) std::cerr << "  " << count << "  " << name << "\n";

  SUCCEED();
}
