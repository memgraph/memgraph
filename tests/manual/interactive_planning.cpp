// Copyright 2023 Memgraph Ltd.
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

#ifdef HAS_READLINE
// TODO: This should probably be moved to some utils file.

#include "readline/history.h"
#include "readline/readline.h"

/**
 * Helper function that reads a line from the
 * standard input using the 'readline' lib.
 * Adds support for history and reverse-search.
 *
 * @param prompt The prompt to display.
 * @return  A single command the user entered, or nullopt on EOF.
 */
std::optional<std::string> ReadLine(const std::string &prompt) {
  char *line = readline(prompt.c_str());
  if (!line) return std::nullopt;

  if (*line) add_history(line);
  std::string r_val(line);
  free(line);
  return r_val;
}

#else

std::optional<std::string> ReadLine(const std::string &prompt) {
  std::cout << prompt;
  std::string line;
  std::getline(std::cin, line);
  if (std::cin.eof()) return std::nullopt;
  return line;
}

#endif  // HAS_READLINE

// Repeats the prompt untile the user inputs an integer.
int64_t ReadInt(const std::string &prompt) {
  int64_t val = 0;
  std::stringstream ss;
  do {
    auto line = ReadLine(prompt);
    if (!line) continue;
    ss.str(*line);
    ss.clear();
    ss >> val;
  } while (ss.fail() || !ss.eof());
  return val;
}

bool AskYesNo(const std::string &prompt) {
  while (auto line = ReadLine(prompt + " (y/n) ")) {
    if (*line == "y" || *line == "Y") return true;
    if (*line == "n" || *line == "N") return false;
  }
  return false;
}

class Timer {
 public:
  void Start() {
    duration_ = duration_.zero();
    start_time_ = std::chrono::steady_clock::now();
  }

  void Pause() {
    if (pause_ == 0) {
      duration_ += std::chrono::steady_clock::now() - start_time_;
    }
    ++pause_;
  }

  void Resume() {
    if (pause_ == 1) {
      start_time_ = std::chrono::steady_clock::now();
    }
    pause_ = std::max(0, pause_ - 1);
  }

  template <class TFun>
  auto WithPause(const TFun &fun) {
    Pause();
    auto ret = fun();
    Resume();
    return std::move(ret);
  }

  std::chrono::duration<double> Elapsed() {
    if (pause_ == 0) {
      return duration_ + (std::chrono::steady_clock::now() - start_time_);
    }
    return duration_;
  }

 private:
  std::chrono::duration<double> duration_;
  std::chrono::time_point<std::chrono::steady_clock> start_time_;
  int pause_ = 0;
};

// Dummy DbAccessor which forwards user input for various vertex counts.
class InteractiveDbAccessor {
 public:
  InteractiveDbAccessor(memgraph::query::DbAccessor *dba, int64_t vertices_count, Timer &timer)
      : dba_(dba), vertices_count_(vertices_count), timer_(timer) {}

  auto NameToLabel(const std::string &name) { return dba_->NameToLabel(name); }
  auto NameToProperty(const std::string &name) { return dba_->NameToProperty(name); }
  auto NameToEdgeType(const std::string &name) { return dba_->NameToEdgeType(name); }

  int64_t VerticesCount() { return vertices_count_; }

  int64_t VerticesCount(memgraph::storage::LabelId label_id) {
    auto label = dba_->LabelToName(label_id);
    if (label_vertex_count_.find(label) == label_vertex_count_.end()) {
      label_vertex_count_[label] = ReadVertexCount("label '" + label + "'");
    }
    return label_vertex_count_.at(label);
  }

  int64_t VerticesCount(memgraph::storage::LabelId label_id, memgraph::storage::PropertyId property_id) {
    auto label = dba_->LabelToName(label_id);
    auto property = dba_->PropertyToName(property_id);
    auto key = std::make_pair(label, property);
    if (label_property_vertex_count_.find(key) == label_property_vertex_count_.end()) {
      label_property_vertex_count_[key] = ReadVertexCount("label '" + label + "' and property '" + property + "'");
    }
    return label_property_vertex_count_.at(key);
  }

  int64_t VerticesCount(memgraph::storage::LabelId label_id, memgraph::storage::PropertyId property_id,
                        const memgraph::storage::PropertyValue &value) {
    auto label = dba_->LabelToName(label_id);
    auto property = dba_->PropertyToName(property_id);
    auto label_prop = std::make_pair(label, property);
    if (label_property_index_.find(label_prop) == label_property_index_.end()) {
      return 0;
    }
    auto &value_vertex_count = property_value_vertex_count_[label_prop];
    if (value_vertex_count.find(value) == value_vertex_count.end()) {
      std::stringstream ss;
      ss << value;
      int64_t count = ReadVertexCount("label '" + label + "' and property '" + property + "' value '" + ss.str() + "'");
      value_vertex_count[value] = count;
    }
    return value_vertex_count.at(value);
  }

  int64_t VerticesCount(memgraph::storage::LabelId label_id, memgraph::storage::PropertyId property_id,
                        const std::optional<memgraph::utils::Bound<memgraph::storage::PropertyValue>> lower,
                        const std::optional<memgraph::utils::Bound<memgraph::storage::PropertyValue>> upper) {
    auto label = dba_->LabelToName(label_id);
    auto property = dba_->PropertyToName(property_id);
    std::stringstream range_string;
    if (lower) {
      range_string << (lower->IsInclusive() ? "[" : "(") << lower->value() << (upper ? "," : ", inf)");
    } else {
      range_string << "(-inf, ";
    }
    if (upper) {
      range_string << upper->value() << (upper->IsInclusive() ? "]" : ")");
    }
    return ReadVertexCount("label '" + label + "' and property '" + property + "' in range " + range_string.str());
  }

  bool LabelIndexExists(memgraph::storage::LabelId label) { return true; }

  bool LabelPropertyIndexExists(memgraph::storage::LabelId label_id, memgraph::storage::PropertyId property_id) {
    auto label = dba_->LabelToName(label_id);
    auto property = dba_->PropertyToName(property_id);
    auto key = std::make_pair(label, property);
    if (label_property_index_.find(key) == label_property_index_.end()) {
      bool resp = timer_.WithPause(
          [&label, &property]() { return AskYesNo("Index for ':" + label + "(" + property + ")' exists:"); });
      label_property_index_[key] = resp;
    }
    return label_property_index_.at(key);
  }

  std::optional<memgraph::storage::IndexStats> GetIndexStats(memgraph::storage::LabelId label,
                                                             memgraph::storage::PropertyId property) const {
    return dba_->GetIndexStats(label, property);
  }

  // Save the cached vertex counts to a stream.
  void Save(std::ostream &out) {
    out << "vertex-count " << vertices_count_ << std::endl;
    out << "label-index-count " << label_vertex_count_.size() << std::endl;
    for (const auto &label_count : label_vertex_count_) {
      out << "  " << label_count.first << " " << label_count.second << std::endl;
    }
    auto save_label_prop_map = [&](const auto &name, const auto &label_prop_map) {
      out << name << " " << label_prop_map.size() << std::endl;
      for (const auto &label_prop : label_prop_map) {
        out << "  " << label_prop.first.first << " " << label_prop.first.second << " " << label_prop.second
            << std::endl;
      }
    };
    save_label_prop_map("label-property-index-exists", label_property_index_);
    save_label_prop_map("label-property-index-count", label_property_vertex_count_);
    out << "label-property-value-index-count " << property_value_vertex_count_.size() << std::endl;
    for (const auto &prop_value_count : property_value_vertex_count_) {
      out << "  " << prop_value_count.first.first << " " << prop_value_count.first.second << " "
          << prop_value_count.second.size() << std::endl;
      for (const auto &value_count : prop_value_count.second) {
        const auto &value = value_count.first;
        out << "    " << value.type() << " " << value << " " << value_count.second << std::endl;
      }
    }
  }

  // Load the cached vertex counts from a stream.
  // If loading fails, raises memgraph::utils::BasicException.
  void Load(std::istream &in) {
    auto load_named_size = [&](const auto &name) {
      int size;
      in.ignore(std::numeric_limits<std::streamsize>::max(), ' ') >> size;
      if (in.fail()) {
        throw memgraph::utils::BasicException("Unable to load {}", name);
      }
      SPDLOG_INFO("Load {} {}", name, size);
      return size;
    };
    vertices_count_ = load_named_size("vertex-count");
    int label_vertex_size = load_named_size("label-index-count");
    for (int i = 0; i < label_vertex_size; ++i) {
      std::string label;
      int64_t count;
      in >> label >> count;
      if (in.fail()) {
        throw memgraph::utils::BasicException("Unable to load label count");
      }
      label_vertex_count_[label] = count;
      SPDLOG_INFO("Load {} {}", label, count);
    }
    auto load_label_prop_map = [&](const auto &name, auto &label_prop_map) {
      int size = load_named_size(name);
      for (int i = 0; i < size; ++i) {
        std::string label;
        std::string property;
        in >> label >> property;
        auto &mapped = label_prop_map[std::make_pair(label, property)];
        in >> mapped;
        if (in.fail()) {
          throw memgraph::utils::BasicException("Unable to load label property");
        }
        SPDLOG_INFO("Load {} {} {}", label, property, mapped);
      }
    };
    load_label_prop_map("label-property-index-exists", label_property_index_);
    load_label_prop_map("label-property-index-count", label_property_vertex_count_);
    int label_property_value_index_size = load_named_size("label-property-value-index-count");
    for (int i = 0; i < label_property_value_index_size; ++i) {
      std::string label;
      std::string property;
      int64_t value_count;
      in >> label >> property >> value_count;
      if (in.fail()) {
        throw memgraph::utils::BasicException("Unable to load label property value");
      }
      SPDLOG_INFO("Load {} {} {}", label, property, value_count);
      for (int v = 0; v < value_count; ++v) {
        auto value = LoadPropertyValue(in);
        int64_t count;
        in >> count;
        if (in.fail()) {
          throw memgraph::utils::BasicException("Unable to load label property value");
        }
        SPDLOG_INFO("Load {} {} {}", value.type(), value, count);
        property_value_vertex_count_[std::make_pair(label, property)][value] = count;
      }
    }
  }

 private:
  typedef std::pair<std::string, std::string> LabelPropertyKey;

  memgraph::query::DbAccessor *dba_;
  int64_t vertices_count_;
  Timer &timer_;
  std::map<std::string, int64_t> label_vertex_count_;
  std::map<std::pair<std::string, std::string>, int64_t> label_property_vertex_count_;
  std::map<std::pair<std::string, std::string>, bool> label_property_index_;
  std::map<std::pair<std::string, std::string>, std::map<memgraph::storage::PropertyValue, int64_t>>
      property_value_vertex_count_;
  // TODO: Cache faked index counts by range.

  int64_t ReadVertexCount(const std::string &message) const {
    return timer_.WithPause([&message]() { return ReadInt("Vertices with " + message + ": "); });
  }

  memgraph::storage::PropertyValue LoadPropertyValue(std::istream &in) {
    std::string type;
    in >> type;
    if (type == "bool") {
      return LoadPropertyValue<bool>(in);
    } else if (type == "int") {
      return LoadPropertyValue<int64_t>(in);
    } else if (type == "double") {
      return LoadPropertyValue<double>(in);
    } else if (type == "string") {
      return LoadPropertyValue<std::string>(in);
    } else {
      throw memgraph::utils::BasicException("Unable to read type '{}'", type);
    }
  }

  template <typename T>
  memgraph::storage::PropertyValue LoadPropertyValue(std::istream &in) {
    T val;
    in >> val;
    return memgraph::storage::PropertyValue(val);
  }
};

DEFCOMMAND(Top) {
  int64_t n_plans = 0;
  std::stringstream ss(args[0]);
  ss >> n_plans;
  if (ss.fail() || !ss.eof()) return;
  n_plans = std::min(static_cast<int64_t>(plans.size()), n_plans);
  for (int64_t i = 0; i < n_plans; ++i) {
    std::cout << "---- Plan #" << i << " ---- " << std::endl;
    std::cout << "cost: " << plans[i].cost << std::endl;
    memgraph::query::plan::PrettyPrint(dba, plans[i].final_plan.get());
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
  memgraph::query::plan::PrettyPrint(dba, plan.get());
}

DEFCOMMAND(ShowUnoptimized) {
  int64_t plan_ix = 0;
  std::stringstream ss(args[0]);
  ss >> plan_ix;
  if (ss.fail() || !ss.eof() || plan_ix >= plans.size()) return;
  const auto &plan = plans[plan_ix].unoptimized_plan;
  memgraph::query::plan::PrettyPrint(dba, plan.get());
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

void ExaminePlans(memgraph::query::DbAccessor *dba, const memgraph::query::SymbolTable &symbol_table,
                  std::vector<InteractivePlan> &plans, const memgraph::query::AstStorage &ast) {
  while (true) {
    auto line = ReadLine("plan? ");
    if (!line || *line == "quit") break;
    auto words = memgraph::utils::Split(memgraph::utils::ToLowerCase(*line));
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

memgraph::query::Query *MakeAst(const std::string &query, memgraph::query::AstStorage *storage) {
  memgraph::query::frontend::ParsingContext parsing_context;
  parsing_context.is_query_cached = false;
  // query -> AST
  auto parser = std::make_unique<memgraph::query::frontend::opencypher::Parser>(query);
  // AST -> high level tree
  memgraph::query::frontend::CypherMainVisitor visitor(parsing_context, storage);
  visitor.visit(parser->tree());
  return visitor.query();
}

// Returns a list of InteractivePlan instances, sorted in the ascending order by
// cost.
auto MakeLogicalPlans(memgraph::query::CypherQuery *query, memgraph::query::AstStorage &ast,
                      memgraph::query::SymbolTable &symbol_table, InteractiveDbAccessor *dba) {
  auto query_parts = memgraph::query::plan::CollectQueryParts(symbol_table, ast, query);
  std::vector<InteractivePlan> interactive_plans;
  auto ctx = memgraph::query::plan::MakePlanningContext(&ast, &symbol_table, query, dba);
  if (query_parts.query_parts.size() <= 0) {
    std::cerr << "Failed to extract query parts" << std::endl;
    std::exit(EXIT_FAILURE);
  }
  memgraph::query::Parameters parameters;
  memgraph::query::plan::PostProcessor post_process(parameters);
  auto plans = memgraph::query::plan::MakeLogicalPlanForSingleQuery<memgraph::query::plan::VariableStartPlanner>(
      query_parts, &ctx);
  for (auto plan : plans) {
    memgraph::query::AstStorage ast_copy;
    auto unoptimized_plan = plan->Clone(&ast_copy);
    auto rewritten_plan = post_process.Rewrite(std::move(plan), &ctx);
    double cost = post_process.EstimatePlanCost(rewritten_plan, dba);
    interactive_plans.push_back(
        InteractivePlan{std::move(unoptimized_plan), std::move(ast_copy), std::move(rewritten_plan), cost});
  }
  std::stable_sort(interactive_plans.begin(), interactive_plans.end(),
                   [](const auto &a, const auto &b) { return a.cost < b.cost; });
  return interactive_plans;
}

void RunInteractivePlanning(memgraph::query::DbAccessor *dba) {
  std::string in_db_filename(memgraph::utils::Trim(FLAGS_load_mock_db_file));
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
      memgraph::query::AstStorage ast;
      auto *query = dynamic_cast<memgraph::query::CypherQuery *>(MakeAst(*line, &ast));
      if (!query) {
        throw memgraph::utils::BasicException(
            "Interactive planning is only available for regular openCypher "
            "queries.");
      }
      auto symbol_table = memgraph::query::MakeSymbolTable(query);
      planning_timer.Start();
      auto plans = MakeLogicalPlans(query, ast, symbol_table, &interactive_db);
      auto planning_time = planning_timer.Elapsed();
      std::cout << "Planning took " << std::chrono::duration<double, std::milli>(planning_time).count() << "ms"
                << std::endl;
      std::cout << "Generated " << plans.size() << " plans" << std::endl;
      ExaminePlans(dba, symbol_table, plans, ast);
    } catch (const memgraph::utils::BasicException &e) {
      std::cout << "Error: " << e.what() << std::endl;
    }
  }
  std::string db_filename(memgraph::utils::Trim(FLAGS_save_mock_db_file));
  if (!db_filename.empty()) {
    std::ofstream db_file(db_filename);
    interactive_db.Save(db_file);
  }
}
