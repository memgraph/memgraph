#include "query/interpreter.hpp"

#include <limits>

#include <glog/logging.h>

#include "glue/communication.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/eval.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/profile.hpp"
#include "query/plan/vertex_count_cache.hpp"
#include "utils/exceptions.hpp"
#include "utils/flag_validation.hpp"
#include "utils/string.hpp"
#include "utils/tsc.hpp"

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
  const AstStorage &GetAstStorage() const override { return storage_; }

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

std::string Interpreter::PlanToJson(const database::GraphDbAccessor &dba,
                                    const plan::LogicalOperator *plan_root) {
  return plan::PlanToJson(dba, plan_root).dump();
}

struct Callback {
  std::vector<std::string> header;
  std::function<std::vector<std::vector<TypedValue>>()> fn;
  bool should_abort_query{false};
};

TypedValue EvaluateOptionalExpression(Expression *expression,
                                      ExpressionEvaluator *eval) {
  return expression ? expression->Accept(*eval) : TypedValue();
}

Callback HandleIndexQuery(IndexQuery *index_query,
                          std::function<void()> invalidate_plan_cache,
                          database::GraphDbAccessor *db_accessor) {
  auto label = db_accessor->Label(index_query->label_.name);
  std::vector<storage::Property> properties;
  properties.reserve(index_query->properties_.size());
  for (const auto &prop : index_query->properties_) {
    properties.push_back(db_accessor->Property(prop.name));
  }

  if (properties.size() > 1) {
    throw utils::NotYetImplemented("index on multiple properties");
  }

  Callback callback;
  switch (index_query->action_) {
    case IndexQuery::Action::CREATE:
      callback.fn = [label, properties, db_accessor,
                     invalidate_plan_cache] {
        try {
          CHECK(properties.size() == 1);
          db_accessor->BuildIndex(label, properties[0]);
          invalidate_plan_cache();
        } catch (const database::ConstraintViolationException &e) {
          throw QueryRuntimeException(e.what());
        } catch (const database::IndexExistsException &e) {
          // Ignore creating an existing index.
        } catch (const database::TransactionException &e) {
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
        } catch (const database::TransactionException &e) {
          throw QueryRuntimeException(e.what());
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
  }
}

Callback HandleInfoQuery(InfoQuery *info_query,
                         database::GraphDbAccessor *db_accessor) {
  Callback callback;
  switch (info_query->info_type_) {
    case InfoQuery::InfoType::STORAGE:
      callback.header = {"storage info", "value"};
      callback.fn = [db_accessor] {
        auto info = db_accessor->StorageInfo();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.size());
        for (const auto &pair : info) {
          results.push_back({TypedValue(pair.first), TypedValue(pair.second)});
        }
        return results;
      };
      break;
    case InfoQuery::InfoType::INDEX:
      callback.header = {"created index"};
      callback.fn = [db_accessor] {
        auto info = db_accessor->IndexInfo();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.size());
        for (const auto &index : info) {
          results.push_back({TypedValue(index)});
        }
        return results;
      };
      break;
    case InfoQuery::InfoType::CONSTRAINT:
      callback.header = {"constraint type", "label", "properties"};
      callback.fn = [db_accessor] {
        std::vector<std::vector<TypedValue>> results;
        for (auto &e : db_accessor->ListUniqueConstraints()) {
          std::vector<std::string> property_names(e.properties.size());
          std::transform(e.properties.begin(), e.properties.end(),
                         property_names.begin(), [&db_accessor](const auto &p) {
                           return db_accessor->PropertyName(p);
                         });

          std::vector<TypedValue> constraint{
              TypedValue("unique"), TypedValue(db_accessor->LabelName(e.label)),
              TypedValue(utils::Join(property_names, ","))};

          results.emplace_back(constraint);
        }
        return results;
      };
      break;
  }
  return callback;
}

Callback HandleConstraintQuery(ConstraintQuery *constraint_query,
                               database::GraphDbAccessor *db_accessor) {
  std::vector<storage::Property> properties;
  auto label = db_accessor->Label(constraint_query->constraint_.label.name);
  properties.reserve(constraint_query->constraint_.properties.size());
  for (const auto &prop : constraint_query->constraint_.properties) {
    properties.push_back(db_accessor->Property(prop.name));
  }

  Callback callback;
  switch (constraint_query->action_type_) {
    case ConstraintQuery::ActionType::CREATE: {
      switch (constraint_query->constraint_.type) {
        case Constraint::Type::NODE_KEY:
          throw utils::NotYetImplemented("Node key constraints");
        case Constraint::Type::EXISTS:
          throw utils::NotYetImplemented("Existence constraints");
        case Constraint::Type::UNIQUE:
          callback.fn = [label, properties, db_accessor] {
            try {
              db_accessor->BuildUniqueConstraint(label, properties);
              return std::vector<std::vector<TypedValue>>();
            } catch (const database::ConstraintViolationException &e) {
              throw QueryRuntimeException(e.what());
            } catch (const database::TransactionException &e) {
              throw QueryRuntimeException(e.what());
            } catch (const mvcc::SerializationError &e) {
              throw QueryRuntimeException(e.what());
            }
          };
          break;
      }
    } break;
    case ConstraintQuery::ActionType::DROP: {
      switch (constraint_query->constraint_.type) {
        case Constraint::Type::NODE_KEY:
          throw utils::NotYetImplemented("Node key constraints");
        case Constraint::Type::EXISTS:
          throw utils::NotYetImplemented("Existence constraints");
        case Constraint::Type::UNIQUE:
          callback.fn = [label, properties, db_accessor] {
            try {
              db_accessor->DeleteUniqueConstraint(label, properties);
              return std::vector<std::vector<TypedValue>>();
            } catch (const database::TransactionException &e) {
              throw QueryRuntimeException(e.what());
            }
          };
          break;
      }
    } break;
  }
  return callback;
}

Interpreter::Interpreter() : is_tsc_available_(utils::CheckAvailableTSC()) {}

Interpreter::Results Interpreter::operator()(
    const std::string &query_string, database::GraphDbAccessor &db_accessor,
    const std::map<std::string, PropertyValue> &params,
    bool in_explicit_transaction, utils::MemoryResource *execution_memory) {
  AstStorage ast_storage;
  Parameters parameters;
  std::map<std::string, TypedValue> summary;

  utils::Timer parsing_timer;
  auto queries = StripAndParseQuery(query_string, &parameters, &ast_storage,
                                    &db_accessor, params);
  frontend::StrippedQuery &stripped_query = queries.first;
  ParsedQuery &parsed_query = queries.second;
  auto parsing_time = parsing_timer.Elapsed();

  summary["parsing_time"] = parsing_time.count();
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

  if (auto *cypher_query = utils::Downcast<CypherQuery>(parsed_query.query)) {
    plan = CypherQueryToPlan(stripped_query.hash(), cypher_query,
                             std::move(ast_storage), parameters, &db_accessor);
    auto planning_time = planning_timer.Elapsed();
    summary["planning_time"] = planning_time.count();
    summary["cost_estimate"] = plan->cost();

    auto output_symbols = plan->plan().OutputSymbols(plan->symbol_table());

    std::vector<std::string> header;
    for (const auto &symbol : output_symbols) {
      // When the symbol is aliased or expanded from '*' (inside RETURN or
      // WITH), then there is no token position, so use symbol name.
      // Otherwise, find the name from stripped query.
      header.push_back(utils::FindOr(stripped_query.named_expressions(),
                                     symbol.token_position(), symbol.name())
                           .first);
    }

    return Results(&db_accessor, parameters, plan, std::move(output_symbols),
                   std::move(header), std::move(summary),
                   execution_memory);
  }

  if (utils::IsSubtype(*parsed_query.query, ExplainQuery::kType)) {
    const std::string kExplainQueryStart = "explain ";
    CHECK(utils::StartsWith(utils::ToLowerCase(stripped_query.query()),
                            kExplainQueryStart))
        << "Expected stripped query to start with '" << kExplainQueryStart
        << "'";

    // We want to cache the Cypher query that appears within this "metaquery".
    // However, we can't just use the hash of that Cypher query string (as the
    // cache key) but then continue to use the AST that was constructed with the
    // full string. The parameters within the AST are looked up using their
    // token positions, which depend on the query string as they're computed at
    // the time the query string is parsed. So, for example, if one first runs
    // EXPLAIN (or PROFILE) on a Cypher query and *then* runs the same Cypher
    // query standalone, the second execution will crash because the cached AST
    // (constructed using the first query string but cached using the substring
    // (equivalent to the second query string)) will use the old token
    // positions. For that reason, we fully strip and parse the substring as
    // well.
    //
    // Note that the stripped subquery string's hash will be equivalent to the
    // hash of the stripped query as if it was run standalone. This guarantees
    // that we will reuse any cached plans from before, rather than create a new
    // one every time. This is important because the planner takes the values of
    // the query parameters into account when planning and might produce a
    // totally different plan if we were to create a new one right now. Doing so
    // would result in discrepancies between the explained (or profiled) plan
    // and the one that's executed when the query is ran standalone.
    auto queries =
        StripAndParseQuery(query_string.substr(kExplainQueryStart.size()),
                           &parameters, &ast_storage, &db_accessor, params);
    frontend::StrippedQuery &stripped_query = queries.first;
    ParsedQuery &parsed_query = queries.second;
    auto *cypher_query = utils::Downcast<CypherQuery>(parsed_query.query);
    CHECK(cypher_query)
        << "Cypher grammar should not allow other queries in EXPLAIN";
    std::shared_ptr<CachedPlan> cypher_query_plan =
        CypherQueryToPlan(stripped_query.hash(), cypher_query,
                          std::move(ast_storage), parameters, &db_accessor);

    std::stringstream printed_plan;
    PrettyPrintPlan(db_accessor, &cypher_query_plan->plan(), &printed_plan);

    std::vector<std::vector<TypedValue>> printed_plan_rows;
    for (const auto &row :
         utils::Split(utils::RTrim(printed_plan.str()), "\n")) {
      printed_plan_rows.push_back(std::vector<TypedValue>{TypedValue(row)});
    }

    summary["explain"] = PlanToJson(db_accessor, &cypher_query_plan->plan());

    SymbolTable symbol_table;
    auto query_plan_symbol = symbol_table.CreateSymbol("QUERY PLAN", false);
    std::vector<Symbol> output_symbols{query_plan_symbol};

    auto output_plan =
        std::make_unique<plan::OutputTable>(output_symbols, printed_plan_rows);

    plan = std::make_shared<CachedPlan>(std::make_unique<SingleNodeLogicalPlan>(
        std::move(output_plan), 0.0, AstStorage{}, symbol_table));

    auto planning_time = planning_timer.Elapsed();
    summary["planning_time"] = planning_time.count();

    std::vector<std::string> header{query_plan_symbol.name()};

    return Results(&db_accessor, parameters, plan, std::move(output_symbols),
                   std::move(header), std::move(summary),
                   execution_memory);
  }

  if (utils::IsSubtype(*parsed_query.query, ProfileQuery::kType)) {
    const std::string kProfileQueryStart = "profile ";
    CHECK(utils::StartsWith(utils::ToLowerCase(stripped_query.query()),
                            kProfileQueryStart))
        << "Expected stripped query to start with '" << kProfileQueryStart
        << "'";

    if (in_explicit_transaction) {
      throw ProfileInMulticommandTxException();
    }

    if (!is_tsc_available_) {
      throw QueryException("TSC support is missing for PROFILE");
    }

    // See the comment regarding the caching of Cypher queries within
    // "metaqueries" for explain queries
    auto queries =
        StripAndParseQuery(query_string.substr(kProfileQueryStart.size()),
                           &parameters, &ast_storage, &db_accessor, params);
    frontend::StrippedQuery &stripped_query = queries.first;
    ParsedQuery &parsed_query = queries.second;
    auto *cypher_query = utils::Downcast<CypherQuery>(parsed_query.query);
    CHECK(cypher_query)
        << "Cypher grammar should not allow other queries in PROFILE";
    auto cypher_query_plan =
        CypherQueryToPlan(stripped_query.hash(), cypher_query,
                          std::move(ast_storage), parameters, &db_accessor);

    // Copy the symbol table and add our own symbols (used by the `OutputTable`
    // operator below)
    SymbolTable symbol_table(cypher_query_plan->symbol_table());

    auto operator_symbol = symbol_table.CreateSymbol("OPERATOR", false);
    auto actual_hits_symbol = symbol_table.CreateSymbol("ACTUAL HITS", false);
    auto relative_time_symbol =
        symbol_table.CreateSymbol("RELATIVE TIME", false);
    auto absolute_time_symbol =
        symbol_table.CreateSymbol("ABSOLUTE TIME", false);

    std::vector<Symbol> output_symbols = {operator_symbol, actual_hits_symbol,
                                          relative_time_symbol,
                                          absolute_time_symbol};
    std::vector<std::string> header{
        operator_symbol.name(), actual_hits_symbol.name(),
        relative_time_symbol.name(), absolute_time_symbol.name()};

    auto output_plan = std::make_unique<plan::OutputTable>(
        output_symbols,
        [cypher_query_plan](Frame *frame, ExecutionContext *context) {
          utils::MonotonicBufferResource execution_memory(1 * 1024 * 1024);
          auto cursor = cypher_query_plan->plan().MakeCursor(&execution_memory);

          // We are pulling from another plan, so set up the EvaluationContext
          // correctly. The rest of the context should be good for sharing.
          context->evaluation_context.properties =
              NamesToProperties(cypher_query_plan->ast_storage().properties_,
                                context->db_accessor);
          context->evaluation_context.labels = NamesToLabels(
              cypher_query_plan->ast_storage().labels_, context->db_accessor);

          // Pull everything to profile the execution
          utils::Timer timer;
          while (cursor->Pull(*frame, *context)) continue;
          auto execution_time = timer.Elapsed();

          context->profile_execution_time = execution_time;

          return ProfilingStatsToTable(context->stats, execution_time);
        });

    plan = std::make_shared<CachedPlan>(std::make_unique<SingleNodeLogicalPlan>(
        std::move(output_plan), 0.0, AstStorage{}, symbol_table));

    auto planning_time = planning_timer.Elapsed();
    summary["planning_time"] = planning_time.count();

    return Results(&db_accessor, parameters, plan, std::move(output_symbols),
                   std::move(header), std::move(summary),
                   execution_memory,
                   /* is_profile_query */ true, /* should_abort_query */ true);
  }

  Callback callback;
  if (auto *index_query = utils::Downcast<IndexQuery>(parsed_query.query)) {
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
  } else if (auto *info_query =
                 utils::Downcast<InfoQuery>(parsed_query.query)) {
    callback = HandleInfoQuery(info_query, &db_accessor);
  } else if (auto *constraint_query =
                 utils::Downcast<ConstraintQuery>(parsed_query.query)) {
    callback = HandleConstraintQuery(constraint_query, &db_accessor);
  } else {
    LOG(FATAL) << "Should not get here -- unknown query type!";
  }

  SymbolTable symbol_table;
  std::vector<Symbol> output_symbols;
  for (const auto &column : callback.header) {
    output_symbols.emplace_back(symbol_table.CreateSymbol(column, "false"));
  }

  plan = std::make_shared<CachedPlan>(std::make_unique<SingleNodeLogicalPlan>(
      std::make_unique<plan::OutputTable>(
          output_symbols,
          [fn = callback.fn](Frame *, ExecutionContext *) { return fn(); }),
      0.0, AstStorage{}, symbol_table));

  auto planning_time = planning_timer.Elapsed();
  summary["planning_time"] = planning_time.count();
  summary["cost_estimate"] = 0.0;

  return Results(&db_accessor, parameters, plan, std::move(output_symbols),
                 callback.header, std::move(summary),
                 execution_memory,
                 /* is_profile_query */ false, callback.should_abort_query);
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
    const frontend::ParsingContext &context, AstStorage *ast_storage,
    database::GraphDbAccessor *db_accessor) {
  if (!context.is_query_cached) {
    // Parse original query into antlr4 AST.
    auto parser = [&] {
      // Be careful about unlocking since parser can throw.
      std::unique_lock<utils::SpinLock> guard(antlr_lock_);
      return std::make_unique<frontend::opencypher::Parser>(original_query);
    }();
    // Convert antlr4 AST into Memgraph AST.
    frontend::CypherMainVisitor visitor(context, ast_storage);
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
    frontend::CypherMainVisitor visitor(context, &cached_ast_storage);
    visitor.visit(parser->tree());
    CachedQuery cached_query{std::move(cached_ast_storage), visitor.query()};
    // Cache it.
    ast_it =
        ast_cache_accessor.insert(stripped_query_hash, std::move(cached_query))
            .first;
  }
  ast_storage->properties_ = ast_it->second.ast_storage.properties_;
  ast_storage->labels_ = ast_it->second.ast_storage.labels_;
  ast_storage->edge_types_ = ast_it->second.ast_storage.edge_types_;
  return ParsedQuery{ast_it->second.query->Clone(ast_storage)};
}

std::pair<frontend::StrippedQuery, Interpreter::ParsedQuery>
Interpreter::StripAndParseQuery(
    const std::string &query_string, Parameters *parameters,
    AstStorage *ast_storage, database::GraphDbAccessor *db_accessor,
    const std::map<std::string, PropertyValue> &params) {
  frontend::StrippedQuery stripped_query(query_string);

  *parameters = stripped_query.literals();
  for (const auto &param_pair : stripped_query.parameters()) {
    auto param_it = params.find(param_pair.second);
    if (param_it == params.end()) {
      throw query::UnprovidedParameterError("Parameter ${} not provided.",
                                            param_pair.second);
    }
    parameters->Add(param_pair.first, param_it->second);
  }

  frontend::ParsingContext parsing_context;
  parsing_context.is_query_cached = true;

  auto parsed_query = ParseQuery(stripped_query.query(), query_string,
                                 parsing_context, ast_storage, db_accessor);

  return {std::move(stripped_query), std::move(parsed_query)};
}

std::unique_ptr<LogicalPlan> Interpreter::MakeLogicalPlan(
    CypherQuery *query, AstStorage ast_storage, const Parameters &parameters,
    database::GraphDbAccessor *db_accessor) {
  auto vertex_counts = plan::MakeVertexCountCache(db_accessor);

  auto symbol_table = MakeSymbolTable(query);

  auto planning_context = plan::MakePlanningContext(&ast_storage, &symbol_table,
                                                    query, &vertex_counts);
  std::unique_ptr<plan::LogicalOperator> root;
  double cost;
  std::tie(root, cost) = plan::MakeLogicalPlan(&planning_context, parameters,
                                               FLAGS_query_cost_planner);
  return std::make_unique<SingleNodeLogicalPlan>(
      std::move(root), cost, std::move(ast_storage), std::move(symbol_table));
}

}  // namespace query
