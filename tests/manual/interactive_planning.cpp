// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "interactive_planning.hpp"

#include <chrono>
#include <cstdlib>
#include <optional>
#include <string>

#include <gflags/gflags.h>

#include "interactive/db_accessor.hpp"
#include "interactive/plan.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/pretty_print.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/string.hpp"

DEFINE_string(save_mock_db_file, "", "File where the mock database should be saved (on exit)");
DEFINE_string(load_mock_db_file, "", "File from which the mock database should be loaded");

DEFCOMMAND(Top) {
  int64_t n_plans = 0;
  std::stringstream ss(args[0]);
  ss >> n_plans;
  if (ss.fail() || !ss.eof()) return;
  n_plans = std::min(static_cast<int64_t>(plans.size()), n_plans);
  for (int64_t i = 0; i < n_plans; ++i) {
    std::cout << "---- Plan #" << i << " ---- " << std::endl;
    std::cout << "cost: " << plans[i].cost << std::endl;
    query::plan::PrettyPrint(dba, plans[i].final_plan.get());
    std::cout << std::endl;
  }
}

DEFCOMMAND(Show) {
  int64_t plan_ix = 0;
  std::stringstream ss(args[0]);
  ss >> plan_ix;
  if (ss.fail() || !ss.eof() || plan_ix >= plans.size()) return;
  const auto &plan = plans[plan_ix].final_plan;
  auto cost = plans[plan_ix].cost;
  std::cout << "Plan cost: " << cost << std::endl;
  query::plan::PrettyPrint(dba, plan.get());
}

DEFCOMMAND(ShowUnoptimized) {
  int64_t plan_ix = 0;
  std::stringstream ss(args[0]);
  ss >> plan_ix;
  if (ss.fail() || !ss.eof() || plan_ix >= plans.size()) return;
  const auto &plan = plans[plan_ix].unoptimized_plan;
  query::plan::PrettyPrint(dba, plan.get());
}

DEFCOMMAND(Help);

std::map<std::string, Command> commands = {
    {"top", {TopCommand, 1, "Show top N plans"}},
    {"show", {ShowCommand, 1, "Show the Nth plan"}},
    {"show-unoptimized", {ShowUnoptimizedCommand, 1, "Show the Nth plan in its original, unoptimized form"}},
    {"help", {HelpCommand, 0, "Show available commands"}},
};

void AddCommand(const std::string &name, const Command &command) { commands[name] = command; }

DEFCOMMAND(Help) {
  std::cout << "Available commands:" << std::endl;
  for (const auto &command : commands) {
    std::cout << command.first;
    for (int i = 1; i <= command.second.arg_count; ++i) {
      std::cout << " arg" << i;
    }
    std::cout << "  --  " << command.second.documentation << std::endl;
  }
}

void ExaminePlans(query::DbAccessor *dba, const query::SymbolTable &symbol_table, std::vector<InteractivePlan> &plans,
                  const query::AstStorage &ast) {
  while (true) {
    auto line = ReadLine("plan? ");
    if (!line || *line == "quit") break;
    auto words = utils::Split(utils::ToLowerCase(*line));
    if (words.empty()) continue;
    auto command_name = words[0];
    std::vector<std::string> args(words.begin() + 1, words.end());
    auto command_it = commands.find(command_name);
    if (command_it == commands.end()) {
      std::cout << "Undefined command: '" << command_name << "'. Try 'help'." << std::endl;
      continue;
    }
    const auto &command = command_it->second;
    if (args.size() < command.arg_count) {
      std::cout << command_name << " expects " << command.arg_count << " arguments" << std::endl;
      continue;
    }
    command.function(*dba, symbol_table, plans, args, ast);
  }
}

void RunInteractivePlanning(query::DbAccessor *dba) {
  std::string in_db_filename(utils::Trim(FLAGS_load_mock_db_file));
  if (!in_db_filename.empty() && !std::filesystem::exists(in_db_filename)) {
    std::cerr << "File '" << in_db_filename << "' does not exist!" << std::endl;
    std::exit(EXIT_FAILURE);
  }
  Timer planning_timer;
  InteractiveDbAccessor interactive_db(dba, in_db_filename.empty() ? ReadInt("Vertices in DB: ") : 0, planning_timer);
  if (!in_db_filename.empty()) {
    std::ifstream db_file(in_db_filename);
    interactive_db.Load(db_file);
  }
  while (true) {
    auto line = ReadLine("query? ");
    if (!line || *line == "quit") break;
    if (line->empty()) continue;
    try {
      query::AstStorage ast;
      auto *query = dynamic_cast<query::CypherQuery *>(MakeAst(*line, &ast));
      if (!query) {
        throw utils::BasicException(
            "Interactive planning is only avaialable for regular openCypher "
            "queries.");
      }
      auto symbol_table = query::MakeSymbolTable(query);
      planning_timer.Start();
      auto plans = MakeLogicalPlans(query, ast, symbol_table, &interactive_db);
      auto planning_time = planning_timer.Elapsed();
      std::cout << "Planning took " << std::chrono::duration<double, std::milli>(planning_time).count() << "ms"
                << std::endl;
      std::cout << "Generated " << plans.size() << " plans" << std::endl;
      ExaminePlans(dba, symbol_table, plans, ast);
    } catch (const utils::BasicException &e) {
      std::cout << "Error: " << e.what() << std::endl;
    }
  }
  std::string db_filename(utils::Trim(FLAGS_save_mock_db_file));
  if (!db_filename.empty()) {
    std::ofstream db_file(db_filename);
    interactive_db.Save(db_file);
  }
}
