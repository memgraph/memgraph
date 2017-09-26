#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <experimental/filesystem>
#include <experimental/optional>
#include <fstream>
#include <iostream>
#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "database/dbms.hpp"
#include "query/context.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/stripped.hpp"
#include "query/plan/cost_estimator.hpp"
#include "query/plan/planner.hpp"
#include "query/typed_value.hpp"
#include "utils/hashing/fnv.hpp"
#include "utils/string.hpp"

DEFINE_string(save_mock_db_file, "",
              "File where the mock database should be saved (on exit)");

DEFINE_string(load_mock_db_file, "",
              "File from which the mock database should be loaded");

#ifdef HAS_READLINE
// TODO: This is copied from src/query/console.cpp
// It should probably be moved to some utils file.

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
std::experimental::optional<std::string> ReadLine(const std::string &prompt) {
  char *line = readline(prompt.c_str());
  if (!line) return std::experimental::nullopt;

  if (*line) add_history(line);
  std::string r_val(line);
  free(line);
  return r_val;
}

#else

std::experimental::optional<std::string> ReadLine(const std::string &prompt) {
  std::cout << prompt;
  std::string line;
  std::getline(std::cin, line);
  if (std::cin.eof()) return std::experimental::nullopt;
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
  InteractiveDbAccessor(int64_t vertices_count, Timer &timer)
      : vertices_count_(vertices_count), timer_(timer) {}

  int64_t VerticesCount() const { return vertices_count_; }

  int64_t VerticesCount(const GraphDbTypes::Label &label) const {
    if (label_vertex_count_.find(*label) == label_vertex_count_.end()) {
      label_vertex_count_[*label] = ReadVertexCount("label '" + *label + "'");
    }
    return label_vertex_count_.at(*label);
  }

  int64_t VerticesCount(const GraphDbTypes::Label &label,
                        const GraphDbTypes::Property &property) const {
    auto key = std::make_pair(*label, *property);
    if (label_property_vertex_count_.find(key) ==
        label_property_vertex_count_.end()) {
      label_property_vertex_count_[key] = ReadVertexCount(
          "label '" + *label + "' and property '" + *property + "'");
    }
    return label_property_vertex_count_.at(key);
  }

  int64_t VerticesCount(const GraphDbTypes::Label &label,
                        const GraphDbTypes::Property &property,
                        const PropertyValue &value) const {
    auto label_prop = std::make_pair(*label, *property);
    if (label_property_index_.find(label_prop) == label_property_index_.end()) {
      return 0;
    }
    auto &value_vertex_count = property_value_vertex_count_[label_prop];
    if (value_vertex_count.find(value) == value_vertex_count.end()) {
      std::stringstream ss;
      ss << value;
      int64_t count = ReadVertexCount("label '" + *label + "' and property '" +
                                      *property + "' value '" + ss.str() + "'");
      value_vertex_count[value] = count;
    }
    return value_vertex_count.at(value);
  }

  int64_t VerticesCount(
      const GraphDbTypes::Label &label, const GraphDbTypes::Property &property,
      const std::experimental::optional<utils::Bound<PropertyValue>> lower,
      const std::experimental::optional<utils::Bound<PropertyValue>> upper)
      const {
    std::stringstream range_string;
    if (lower) {
      range_string << (lower->IsInclusive() ? "[" : "(") << lower->value()
                   << (upper ? "," : ", inf)");
    } else {
      range_string << "(-inf, ";
    }
    if (upper) {
      range_string << upper->value() << (upper->IsInclusive() ? "]" : ")");
    }
    return ReadVertexCount("label '" + *label + "' and property '" + *property +
                           "' in range " + range_string.str());
  }

  bool LabelPropertyIndexExists(const GraphDbTypes::Label &label,
                                const GraphDbTypes::Property &property) const {
    auto key = std::make_pair(*label, *property);
    if (label_property_index_.find(key) == label_property_index_.end()) {
      bool resp = timer_.WithPause([&label, &property]() {
        return AskYesNo("Index for ':" + *label + "(" + *property +
                        ")' exists:");
      });
      label_property_index_[key] = resp;
    }
    return label_property_index_.at(key);
  }

  // Save the cached vertex counts to a stream.
  void Save(std::ostream &out) {
    out << "vertex-count " << vertices_count_ << std::endl;
    out << "label-index-count " << label_vertex_count_.size() << std::endl;
    for (const auto &label_count : label_vertex_count_) {
      out << "  " << label_count.first << " " << label_count.second
          << std::endl;
    }
    auto save_label_prop_map = [&](const auto &name,
                                   const auto &label_prop_map) {
      out << name << " " << label_prop_map.size() << std::endl;
      for (const auto &label_prop : label_prop_map) {
        out << "  " << label_prop.first.first << " " << label_prop.first.second
            << " " << label_prop.second << std::endl;
      }
    };
    save_label_prop_map("label-property-index-exists", label_property_index_);
    save_label_prop_map("label-property-index-count",
                        label_property_vertex_count_);
    out << "label-property-value-index-count "
        << property_value_vertex_count_.size() << std::endl;
    for (const auto &prop_value_count : property_value_vertex_count_) {
      out << "  " << prop_value_count.first.first << " "
          << prop_value_count.first.second << " "
          << prop_value_count.second.size() << std::endl;
      for (const auto &value_count : prop_value_count.second) {
        const auto &value = value_count.first;
        out << "    " << value.type() << " " << value << " "
            << value_count.second << std::endl;
      }
    }
  }

  // Load the cached vertex counts from a stream.
  // If loading fails, raises utils::BasicException.
  void Load(std::istream &in) {
    auto load_named_size = [&](const auto &name) {
      int size;
      in.ignore(std::numeric_limits<std::streamsize>::max(), ' ') >> size;
      if (in.fail()) {
        throw utils::BasicException("Unable to load {}", name);
      }
      DLOG(INFO) << "Load " << name << " " << size;
      return size;
    };
    vertices_count_ = load_named_size("vertex-count");
    int label_vertex_size = load_named_size("label-index-count");
    for (int i = 0; i < label_vertex_size; ++i) {
      std::string label;
      int64_t count;
      in >> label >> count;
      if (in.fail()) {
        throw utils::BasicException("Unable to load label count");
      }
      label_vertex_count_[label] = count;
      DLOG(INFO) << "Load " << label << " " << count;
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
          throw utils::BasicException("Unable to load label property");
        }
        DLOG(INFO) << "Load " << label << " " << property << " " << mapped;
      }
    };
    load_label_prop_map("label-property-index-exists", label_property_index_);
    load_label_prop_map("label-property-index-count",
                        label_property_vertex_count_);
    int label_property_value_index_size =
        load_named_size("label-property-value-index-count");
    for (int i = 0; i < label_property_value_index_size; ++i) {
      std::string label;
      std::string property;
      int64_t value_count;
      in >> label >> property >> value_count;
      if (in.fail()) {
        throw utils::BasicException("Unable to load label property value");
      }
      DLOG(INFO) << "Load " << label << " " << property << " " << value_count;
      for (int v = 0; v < value_count; ++v) {
        auto value = LoadTypedValue(in);
        int64_t count;
        in >> count;
        if (in.fail()) {
          throw utils::BasicException("Unable to load label property value");
        }
        DLOG(INFO) << "Load " << value.type() << " " << value << " " << count;
        property_value_vertex_count_[std::make_pair(label, property)][value] =
            count;
      }
    }
  }

 private:
  typedef std::pair<std::string, std::string> LabelPropertyKey;

  int64_t vertices_count_;
  Timer &timer_;
  mutable std::map<std::string, int64_t> label_vertex_count_;
  mutable std::map<std::pair<std::string, std::string>, int64_t>
      label_property_vertex_count_;
  mutable std::map<std::pair<std::string, std::string>, bool>
      label_property_index_;
  mutable std::map<
      std::pair<std::string, std::string>,
      std::unordered_map<query::TypedValue, int64_t, query::TypedValue::Hash,
                         query::TypedValue::BoolEqual>>
      property_value_vertex_count_;
  // TODO: Cache faked index counts by range.

  int64_t ReadVertexCount(const std::string &message) const {
    return timer_.WithPause(
        [&message]() { return ReadInt("Vertices with " + message + ": "); });
  }

  query::TypedValue LoadTypedValue(std::istream &in) {
    std::string type;
    in >> type;
    if (type == "bool") {
      return LoadTypedValue<bool>(in);
    } else if (type == "int") {
      return LoadTypedValue<int64_t>(in);
    } else if (type == "double") {
      return LoadTypedValue<double>(in);
    } else if (type == "string") {
      return LoadTypedValue<std::string>(in);
    } else {
      throw utils::BasicException("Unable to read type '{}'", type);
    }
  }

  template <typename T>
  query::TypedValue LoadTypedValue(std::istream &in) {
    T val;
    in >> val;
    return query::TypedValue(val);
  }
};

class PlanPrinter : public query::plan::HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;
  using HierarchicalLogicalOperatorVisitor::PostVisit;

#define PRE_VISIT(TOp)                                   \
  bool PreVisit(query::plan::TOp &) override {           \
    WithPrintLn([](auto &out) { out << "* " << #TOp; }); \
    return true;                                         \
  }

  PRE_VISIT(CreateNode);
  PRE_VISIT(CreateExpand);
  PRE_VISIT(Delete);

  bool PreVisit(query::plan::ScanAll &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAll"
          << " (" << op.output_symbol().name() << ")";
    });
    return true;
  }

  bool PreVisit(query::plan::ScanAllByLabel &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAllByLabel"
          << " (" << op.output_symbol().name() << " :" << *op.label() << ")";
    });
    return true;
  }

  bool PreVisit(query::plan::ScanAllByLabelPropertyValue &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAllByLabelPropertyValue"
          << " (" << op.output_symbol().name() << " :" << *op.label() << " {"
          << *op.property() << "})";
    });
    return true;
  }

  bool PreVisit(query::plan::ScanAllByLabelPropertyRange &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ScanAllByLabelPropertyRange"
          << " (" << op.output_symbol().name() << " :" << *op.label() << " {"
          << *op.property() << "})";
    });
    return true;
  }

  bool PreVisit(query::plan::Expand &op) override {
    WithPrintLn([&](auto &out) {
      out << "* Expand";
      PrintExpand(out, op);
    });
    return true;
  }

  bool PreVisit(query::plan::ExpandVariable &op) override {
    WithPrintLn([&](auto &out) {
      out << "* ExpandVariable";
      PrintExpand(out, op);
    });
    return true;
  }

  bool PreVisit(query::plan::Produce &op) override {
    WithPrintLn([&](auto &out) {
      out << "* Produce {";
      utils::PrintIterable(
          out, op.named_expressions(), ", ",
          [](auto &out, const auto &nexpr) { out << nexpr->name_; });
      out << "}";
    });
    return true;
  }

  PRE_VISIT(ExpandBreadthFirst);
  PRE_VISIT(ConstructNamedPath);
  PRE_VISIT(Filter);
  PRE_VISIT(SetProperty);
  PRE_VISIT(SetProperties);
  PRE_VISIT(SetLabels);
  PRE_VISIT(RemoveProperty);
  PRE_VISIT(RemoveLabels);
  PRE_VISIT(ExpandUniquenessFilter<VertexAccessor>);
  PRE_VISIT(ExpandUniquenessFilter<EdgeAccessor>);
  PRE_VISIT(Accumulate);
  PRE_VISIT(Aggregate);
  PRE_VISIT(Skip);
  PRE_VISIT(Limit);
  PRE_VISIT(OrderBy);

  bool PreVisit(query::plan::Merge &op) override {
    WithPrintLn([](auto &out) { out << "* Merge"; });
    Branch(*op.merge_match(), "On Match");
    Branch(*op.merge_create(), "On Create");
    op.input()->Accept(*this);
    return false;
  }

  bool PreVisit(query::plan::Optional &op) override {
    WithPrintLn([](auto &out) { out << "* Optional"; });
    Branch(*op.optional());
    op.input()->Accept(*this);
    return false;
  }

  PRE_VISIT(Unwind);
  PRE_VISIT(Distinct);

  bool Visit(query::plan::Once &op) override {
    // Ignore checking Once, it is implicitly at the end.
    return true;
  }

  bool Visit(query::plan::CreateIndex &op) override {
    WithPrintLn([](auto &out) { out << "* CreateIndex"; });
    return true;
  }
#undef PRE_VISIT

 private:
  // Call fun with output stream. The stream is prefixed with amount of spaces
  // corresponding to the current depth_.
  template <class TFun>
  void WithPrintLn(TFun fun) {
    std::cout << std::string(depth_ * 2, ' ');
    fun(std::cout);
    std::cout << std::endl;
  }

  // Forward this printer to another operator branch by incrementing the depth
  // and printing the branch name.
  void Branch(query::plan::LogicalOperator &op,
              const std::string &branch_name = "") {
    WithPrintLn([&](auto &out) { out << " \\ " << branch_name; });
    ++depth_;
    op.Accept(*this);
    --depth_;
  }

  void PrintExpand(std::ostream &out, const query::plan::ExpandCommon &op) {
    out << " (" << op.input_symbol().name() << ")"
        << (op.direction() == query::EdgeAtom::Direction::IN ? "<-" : "-")
        << "[" << op.edge_symbol().name() << "]"
        << (op.direction() == query::EdgeAtom::Direction::OUT ? "->" : "-")
        << "(" << op.node_symbol().name() << ")";
  }

  int depth_ = 0;
};

// Shorthand for a vector of pairs (logical_plan, cost).
typedef std::vector<
    std::pair<std::unique_ptr<query::plan::LogicalOperator>, double>>
    PlansWithCost;

// Encapsulates a consoles command function.
struct Command {
  typedef std::vector<std::string> Args;
  // Function of this command
  std::function<void(PlansWithCost &, const Args &)> function;
  // Number of arguments the function works with.
  int arg_count;
  // Explanation of the command.
  std::string documentation;
};

#define DEFCOMMAND(Name) \
  void Name##Command(PlansWithCost &plans, const Command::Args &args)

DEFCOMMAND(Top) {
  int64_t n_plans = 0;
  std::stringstream ss(args[0]);
  ss >> n_plans;
  if (ss.fail() || !ss.eof()) return;
  PlanPrinter printer;
  n_plans = std::min(static_cast<int64_t>(plans.size()), n_plans);
  for (int64_t i = 0; i < n_plans; ++i) {
    auto &plan_pair = plans[i];
    std::cout << "---- Plan #" << i << " ---- " << std::endl;
    std::cout << "cost: " << plan_pair.second << std::endl;
    plan_pair.first->Accept(printer);
    std::cout << std::endl;
  }
}

DEFCOMMAND(Show) {
  int64_t plan_ix = 0;
  std::stringstream ss(args[0]);
  ss >> plan_ix;
  if (ss.fail() || !ss.eof() || plan_ix >= plans.size()) return;
  const auto &plan = plans[plan_ix].first;
  auto cost = plans[plan_ix].second;
  std::cout << "Plan cost: " << cost << std::endl;
  PlanPrinter printer;
  plan->Accept(printer);
}

DEFCOMMAND(Help);

std::map<std::string, Command> commands = {
    {"top", {TopCommand, 1, "Show top N plans"}},
    {"show", {ShowCommand, 1, "Show the Nth plan"}},
    {"help", {HelpCommand, 0, "Show available commands"}},
};

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

#undef DEFCOMMAND

void ExaminePlans(
    std::vector<std::pair<std::unique_ptr<query::plan::LogicalOperator>,
                          double>> &plans) {
  while (true) {
    auto line = ReadLine("plan? ");
    if (!line || *line == "quit") break;
    auto words = utils::Split(utils::ToLowerCase(*line));
    if (words.empty()) continue;
    auto command_name = words[0];
    std::vector<std::string> args(words.begin() + 1, words.end());
    auto command_it = commands.find(command_name);
    if (command_it == commands.end()) {
      std::cout << "Undefined command: '" << command_name << "'. Try 'help'."
                << std::endl;
      continue;
    }
    const auto &command = command_it->second;
    if (args.size() < command.arg_count) {
      std::cout << command_name << " expects " << command.arg_count
                << " arguments" << std::endl;
      continue;
    }
    command.function(plans, args);
  }
}

query::AstTreeStorage MakeAst(const std::string &query, GraphDbAccessor &dba) {
  query::Context ctx(dba);
  // query -> AST
  auto parser = std::make_unique<query::frontend::opencypher::Parser>(query);
  // AST -> high level tree
  query::frontend::CypherMainVisitor visitor(ctx);
  visitor.visit(parser->tree());
  return std::move(visitor.storage());
}

query::SymbolTable MakeSymbolTable(const query::AstTreeStorage &ast) {
  query::SymbolTable symbol_table;
  query::SymbolGenerator symbol_generator(symbol_table);
  ast.query()->Accept(symbol_generator);
  return symbol_table;
}

// Returns a list of pairs (plan, estimated cost), sorted in the ascending
// order by cost.
auto MakeLogicalPlans(query::AstTreeStorage &ast,
                      query::SymbolTable &symbol_table,
                      InteractiveDbAccessor &dba) {
  std::vector<std::pair<std::unique_ptr<query::plan::LogicalOperator>, double>>
      plans_with_cost;
  auto plans = query::plan::MakeLogicalPlan<query::plan::VariableStartPlanner>(
      ast, symbol_table, dba);
  Parameters parameters;
  for (auto &plan : plans) {
    query::plan::CostEstimator<InteractiveDbAccessor> estimator(dba,
                                                                parameters);
    plan->Accept(estimator);
    plans_with_cost.emplace_back(std::move(plan), estimator.cost());
  }
  std::stable_sort(
      plans_with_cost.begin(), plans_with_cost.end(),
      [](const auto &a, const auto &b) { return a.second < b.second; });
  return plans_with_cost;
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  auto in_db_filename = utils::Trim(FLAGS_load_mock_db_file);
  if (!in_db_filename.empty() &&
      !std::experimental::filesystem::exists(in_db_filename)) {
    std::cerr << "File '" << in_db_filename << "' does not exist!" << std::endl;
    std::exit(EXIT_FAILURE);
  }
  Dbms dbms;
  Timer planning_timer;
  InteractiveDbAccessor interactive_db(
      in_db_filename.empty() ? ReadInt("Vertices in DB: ") : 0, planning_timer);
  if (!in_db_filename.empty()) {
    std::ifstream db_file(in_db_filename);
    interactive_db.Load(db_file);
  }
  while (true) {
    auto line = ReadLine("query? ");
    if (!line || *line == "quit") break;
    if (line->empty()) continue;
    try {
      auto dba = dbms.active();
      auto ast = MakeAst(*line, *dba);
      auto symbol_table = MakeSymbolTable(ast);
      planning_timer.Start();
      auto plans = MakeLogicalPlans(ast, symbol_table, interactive_db);
      auto planning_time = planning_timer.Elapsed();
      std::cout
          << "Planning took "
          << std::chrono::duration<double, std::milli>(planning_time).count()
          << "ms" << std::endl;
      std::cout << "Generated " << plans.size() << " plans" << std::endl;
      ExaminePlans(plans);
    } catch (const utils::BasicException &e) {
      std::cout << "Error: " << e.what() << std::endl;
    }
  }
  auto db_filename = utils::Trim(FLAGS_save_mock_db_file);
  if (!db_filename.empty()) {
    std::ofstream db_file(db_filename);
    interactive_db.Save(db_file);
  }
  return 0;
}
