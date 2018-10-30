#include "query/interpreter.hpp"

#include <glog/logging.h>
#include <limits>

#include "glue/communication.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/eval.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/vertex_count_cache.hpp"
#include "utils/flag_validation.hpp"
#include "utils/string.hpp"

DEFINE_HIDDEN_bool(query_cost_planner, true,
                   "Use the cost-estimating query planner.");
DEFINE_VALIDATED_int32(query_plan_cache_ttl, 60,
                       "Time to live for cached query plans, in seconds.",
                       FLAG_IN_RANGE(0, std::numeric_limits<int32_t>::max()));

namespace query {

class SingleNodeLogicalPlan final : public LogicalPlan {
 public:
  SingleNodeLogicalPlan(std::unique_ptr<plan::LogicalOperator> root,
                        double cost, AstStorage storage,
                        const SymbolTable &symbol_table)
      : root_(std::move(root)),
        cost_(cost),
        storage_(std::move(storage)),
        symbol_table_(symbol_table) {}

  const plan::LogicalOperator &GetRoot() const override { return *root_; }
  double GetCost() const override { return cost_; }
  const SymbolTable &GetSymbolTable() const override { return symbol_table_; }

 private:
  std::unique_ptr<plan::LogicalOperator> root_;
  double cost_;
  AstStorage storage_;
  SymbolTable symbol_table_;
};

Interpreter::CachedPlan::CachedPlan(std::unique_ptr<LogicalPlan> plan)
    : plan_(std::move(plan)) {}

void Interpreter::PrettyPrintPlan(const database::GraphDbAccessor &dba,
                                  const plan::LogicalOperator *plan_root,
                                  std::ostream *out) {
  plan::PrettyPrint(dba, plan_root, out);
}

struct Callback {
  std::vector<std::string> header;
  std::function<std::vector<std::vector<TypedValue>>()> fn;
};

TypedValue EvaluateOptionalExpression(Expression *expression,
                                      ExpressionEvaluator *eval) {
  return expression ? expression->Accept(*eval) : TypedValue::Null;
}

Callback HandleIndexQuery(IndexQuery *index_query,
                          std::function<void()> invalidate_plan_cache,
                          database::GraphDbAccessor *db_accessor) {
  auto action = index_query->action_;
  auto label = index_query->label_;
  auto properties = index_query->properties_;

  if (properties.size() > 1) {
    throw utils::NotYetImplemented("index on multiple properties");
  }

  Callback callback;
  switch (index_query->action_) {
    case IndexQuery::Action::CREATE:
    case IndexQuery::Action::CREATE_UNIQUE:
      callback.fn = [action, label, properties, db_accessor,
                     invalidate_plan_cache] {
        try {
          CHECK(properties.size() == 1);
          db_accessor->BuildIndex(label, properties[0],
                                  action == IndexQuery::Action::CREATE_UNIQUE);
          invalidate_plan_cache();
        } catch (const database::IndexConstraintViolationException &e) {
          throw QueryRuntimeException(e.what());
        } catch (const database::IndexExistsException &e) {
          if (action == IndexQuery::Action::CREATE_UNIQUE) {
            throw QueryRuntimeException(e.what());
          }
          // Otherwise ignore creating an existing index.
        } catch (const database::IndexTransactionException &e) {
          throw QueryRuntimeException(e.what());
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case IndexQuery::Action::DROP:
      callback.fn = [label, properties, db_accessor, invalidate_plan_cache] {
        try {
          CHECK(properties.size() == 1);
          db_accessor->DeleteIndex(label, properties[0]);
          invalidate_plan_cache();
        } catch (const database::IndexTransactionException &e) {
          throw QueryRuntimeException(e.what());
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
  }
}

Interpreter::Results Interpreter::operator()(
    const std::string &query_string, database::GraphDbAccessor &db_accessor,
    const std::map<std::string, PropertyValue> &params,
    bool in_explicit_transaction) {
  utils::Timer frontend_timer;

  // Strip the input query.
  StrippedQuery stripped_query(query_string);

  Context execution_context(db_accessor);

  auto &evaluation_context = execution_context.evaluation_context_;
  evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();

  evaluation_context.parameters = stripped_query.literals();
  for (const auto &param_pair : stripped_query.parameters()) {
    auto param_it = params.find(param_pair.second);
    if (param_it == params.end()) {
      throw query::UnprovidedParameterError("Parameter ${} not provided.",
                                            param_pair.second);
    }
    evaluation_context.parameters.Add(param_pair.first, param_it->second);
  }

  ParsingContext parsing_context;
  parsing_context.is_query_cached = true;
  AstStorage ast_storage;

  auto parsed_query = ParseQuery(stripped_query.query(), query_string,
                                 parsing_context, &ast_storage, &db_accessor);
  auto frontend_time = frontend_timer.Elapsed();

  // Build summary.
  std::map<std::string, TypedValue> summary;
  summary["parsing_time"] = frontend_time.count();
  // TODO: set summary['type'] based on transaction metadata
  // the type can't be determined based only on top level LogicalOp
  // (for example MATCH DELETE RETURN will have Produce as it's top).
  // For now always use "rw" because something must be set, but it doesn't
  // have to be correct (for Bolt clients).
  summary["type"] = "rw";

  utils::Timer planning_timer;

  // This local shared_ptr might be the only owner of the CachedPlan, so
  // we must ensure it lives during the whole interpretation.
  std::shared_ptr<CachedPlan> plan{nullptr};

  if (auto *cypher_query = dynamic_cast<CypherQuery *>(parsed_query.query)) {
    plan = CypherQueryToPlan(stripped_query.hash(), cypher_query,
                             std::move(ast_storage),
                             evaluation_context.parameters, &db_accessor);
    auto planning_time = planning_timer.Elapsed();
    summary["planning_time"] = planning_time.count();
    summary["cost_estimate"] = plan->cost();

    execution_context.symbol_table_ = plan->symbol_table();
    auto output_symbols =
        plan->plan().OutputSymbols(execution_context.symbol_table_);

    std::vector<std::string> header;
    for (const auto &symbol : output_symbols) {
      // When the symbol is aliased or expanded from '*' (inside RETURN or
      // WITH), then there is no token position, so use symbol name.
      // Otherwise, find the name from stripped query.
      header.push_back(utils::FindOr(stripped_query.named_expressions(),
                                     symbol.token_position(), symbol.name())
                           .first);
    }

    auto cursor = plan->plan().MakeCursor(db_accessor);

    return Results(std::move(execution_context), plan, std::move(cursor),
                   output_symbols, header, summary);
  }

  if (auto *explain_query = dynamic_cast<ExplainQuery *>(parsed_query.query)) {
    const std::string kExplainQueryStart = "explain ";
    CHECK(utils::StartsWith(utils::ToLowerCase(stripped_query.query()),
                            kExplainQueryStart))
        << "Expected stripped query to start with '" << kExplainQueryStart
        << "'";

    auto cypher_query_hash =
        fnv(stripped_query.query().substr(kExplainQueryStart.size()));
    std::shared_ptr<CachedPlan> cypher_query_plan = CypherQueryToPlan(
        cypher_query_hash, explain_query->cypher_query_, std::move(ast_storage),
        evaluation_context.parameters, &db_accessor);

    std::stringstream printed_plan;
    PrettyPrintPlan(db_accessor, &cypher_query_plan->plan(), &printed_plan);

    std::vector<std::vector<TypedValue>> printed_plan_rows;
    for (const auto &row :
         utils::Split(utils::RTrim(printed_plan.str()), "\n")) {
      printed_plan_rows.push_back(std::vector<TypedValue>{row});
    }

    auto query_plan_symbol =
        execution_context.symbol_table_.CreateSymbol("QUERY PLAN", false);
    std::unique_ptr<plan::OutputTable> output_plan =
        std::make_unique<plan::OutputTable>(
            std::vector<Symbol>{query_plan_symbol}, printed_plan_rows);

    plan = std::make_shared<CachedPlan>(std::make_unique<SingleNodeLogicalPlan>(
        std::move(output_plan), 0.0, AstStorage{},
        execution_context.symbol_table_));

    auto planning_time = planning_timer.Elapsed();
    summary["planning_time"] = planning_time.count();

    execution_context.symbol_table_ = plan->symbol_table();
    std::vector<Symbol> output_symbols{query_plan_symbol};
    std::vector<std::string> header{query_plan_symbol.name()};

    auto cursor = plan->plan().MakeCursor(db_accessor);

    return Results(std::move(execution_context), plan, std::move(cursor),
                   output_symbols, header, summary);
  }

  Callback callback;
  if (auto *index_query = dynamic_cast<IndexQuery *>(parsed_query.query)) {
    if (in_explicit_transaction) {
      throw IndexInMulticommandTxException();
    }
    // Creating an index influences computed plan costs.
    auto invalidate_plan_cache = [plan_cache = &this->plan_cache_] {
      auto access = plan_cache->access();
      for (auto &kv : access) {
        access.remove(kv.first);
      }
    };
    callback =
        HandleIndexQuery(index_query, invalidate_plan_cache, &db_accessor);
  } else {
    LOG(FATAL) << "Should not get here -- unknown query type!";
  }

  std::vector<Symbol> output_symbols;
  for (const auto &column : callback.header) {
    output_symbols.emplace_back(
        execution_context.symbol_table_.CreateSymbol(column, "false"));
  }

  plan = std::make_shared<CachedPlan>(std::make_unique<SingleNodeLogicalPlan>(
      std::make_unique<plan::OutputTable>(output_symbols, callback.fn), 0.0,
      AstStorage{}, execution_context.symbol_table_));

  auto planning_time = planning_timer.Elapsed();
  summary["planning_time"] = planning_time.count();
  summary["cost_estimate"] = 0.0;

  auto cursor = plan->plan().MakeCursor(db_accessor);

  return Results(std::move(execution_context), plan, std::move(cursor),
                 output_symbols, callback.header, summary);
}

std::shared_ptr<Interpreter::CachedPlan> Interpreter::CypherQueryToPlan(
    HashType query_hash, CypherQuery *query, AstStorage ast_storage,
    const Parameters &parameters, database::GraphDbAccessor *db_accessor) {
  auto plan_cache_access = plan_cache_.access();
  auto it = plan_cache_access.find(query_hash);
  if (it != plan_cache_access.end()) {
    if (it->second->IsExpired()) {
      plan_cache_access.remove(query_hash);
    } else {
      return it->second;
    }
  }
  return plan_cache_access
      .insert(query_hash,
              std::make_shared<CachedPlan>(MakeLogicalPlan(
                  query, std::move(ast_storage), parameters, db_accessor)))
      .first->second;
}

Interpreter::ParsedQuery Interpreter::ParseQuery(
    const std::string &stripped_query, const std::string &original_query,
    const ParsingContext &context, AstStorage *ast_storage,
    database::GraphDbAccessor *db_accessor) {
  if (!context.is_query_cached) {
    // Parse original query into antlr4 AST.
    auto parser = [&] {
      // Be careful about unlocking since parser can throw.
      std::unique_lock<utils::SpinLock> guard(antlr_lock_);
      return std::make_unique<frontend::opencypher::Parser>(original_query);
    }();
    // Convert antlr4 AST into Memgraph AST.
    frontend::CypherMainVisitor visitor(context, ast_storage, db_accessor);
    visitor.visit(parser->tree());
    return ParsedQuery{visitor.query()};
  }

  auto stripped_query_hash = fnv(stripped_query);

  auto ast_cache_accessor = ast_cache_.access();
  auto ast_it = ast_cache_accessor.find(stripped_query_hash);
  if (ast_it == ast_cache_accessor.end()) {
    // Parse stripped query into antlr4 AST.
    auto parser = [&] {
      // Be careful about unlocking since parser can throw.
      std::unique_lock<utils::SpinLock> guard(antlr_lock_);
      try {
        return std::make_unique<frontend::opencypher::Parser>(stripped_query);
      } catch (const SyntaxException &e) {
        // There is syntax exception in stripped query. Rerun parser on
        // the original query to get appropriate error messsage.
        auto parser =
            std::make_unique<frontend::opencypher::Parser>(original_query);
        // If exception was not thrown here, StrippedQuery messed
        // something up.
        LOG(FATAL) << "Stripped query can't be parsed, but the original can.";
        return parser;
      }
    }();
    // Convert antlr4 AST into Memgraph AST.
    AstStorage cached_ast_storage;
    frontend::CypherMainVisitor visitor(context, &cached_ast_storage,
                                        db_accessor);
    visitor.visit(parser->tree());
    CachedQuery cached_query{std::move(cached_ast_storage), visitor.query()};
    // Cache it.
    ast_it =
        ast_cache_accessor.insert(stripped_query_hash, std::move(cached_query))
            .first;
  }
  return ParsedQuery{ast_it->second.query->Clone(*ast_storage)};
}

std::unique_ptr<LogicalPlan> Interpreter::MakeLogicalPlan(
    CypherQuery *query, AstStorage ast_storage, const Parameters &parameters,
    database::GraphDbAccessor *db_accessor) {
  auto vertex_counts = plan::MakeVertexCountCache(*db_accessor);

  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);

  auto planning_context = plan::MakePlanningContext(ast_storage, symbol_table,
                                                    query, vertex_counts);
  std::unique_ptr<plan::LogicalOperator> root;
  double cost;
  std::tie(root, cost) = plan::MakeLogicalPlan(planning_context, parameters,
                                               FLAGS_query_cost_planner);
  return std::make_unique<SingleNodeLogicalPlan>(
      std::move(root), cost, std::move(ast_storage), std::move(symbol_table));
}

}  // namespace query
