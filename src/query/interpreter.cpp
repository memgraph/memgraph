#include "query/interpreter.hpp"

#include <limits>

#include <glog/logging.h>

#include "glue/communication.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/dump.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/required_privileges.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/eval.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/profile.hpp"
#include "query/plan/vertex_count_cache.hpp"
#include "query/typed_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"
#include "utils/flag_validation.hpp"
#include "utils/memory.hpp"
#include "utils/string.hpp"
#include "utils/tsc.hpp"

DEFINE_HIDDEN_bool(query_cost_planner, true,
                   "Use the cost-estimating query planner.");
DEFINE_VALIDATED_int32(query_plan_cache_ttl, 60,
                       "Time to live for cached query plans, in seconds.",
                       FLAG_IN_RANGE(0, std::numeric_limits<int32_t>::max()));

namespace query {

/**
 * A container for data related to the parsing of a query.
 */
struct ParsedQuery {
  std::string query_string;
  std::map<std::string, storage::PropertyValue> user_parameters;
  Parameters parameters;
  frontend::StrippedQuery stripped_query;
  AstStorage ast_storage;
  Query *query;
  std::vector<AuthQuery::Privilege> required_privileges;
};

ParsedQuery ParseQuery(
    const std::string &query_string,
    const std::map<std::string, storage::PropertyValue> &params,
    utils::SkipList<QueryCacheEntry> *cache, utils::SpinLock *antlr_lock) {
  // Strip the query for caching purposes. The process of stripping a query
  // "normalizes" it by replacing any literals with new parameters . This
  // results in just the *structure* of the query being taken into account for
  // caching.
  frontend::StrippedQuery stripped_query{query_string};

  // Copy over the parameters that were introduced during stripping.
  Parameters parameters{stripped_query.literals()};

  // Check that all user-specified parameters are provided.
  for (const auto &param_pair : stripped_query.parameters()) {
    auto it = params.find(param_pair.second);

    if (it == params.end()) {
      throw query::UnprovidedParameterError("Parameter ${} not provided.",
                                            param_pair.second);
    }

    parameters.Add(param_pair.first, it->second);
  }

  // Cache the query's AST if it isn't already.
  auto hash = stripped_query.hash();
  auto accessor = cache->access();
  auto it = accessor.find(hash);
  std::unique_ptr<frontend::opencypher::Parser> parser;

  if (it == accessor.end()) {
    {
      std::unique_lock<utils::SpinLock> guard(*antlr_lock);

      try {
        parser = std::make_unique<frontend::opencypher::Parser>(
            stripped_query.query());
      } catch (const SyntaxException &e) {
        // There is a syntax exception in the stripped query. Re-run the parser
        // on the original query to get an appropriate error messsage.
        parser = std::make_unique<frontend::opencypher::Parser>(query_string);

        // If an exception was not thrown here, the stripper messed something
        // up.
        LOG(FATAL)
            << "The stripped query can't be parsed, but the original can.";
      }
    }

    // Convert the ANTLR4 parse tree into an AST.
    AstStorage ast_storage;
    frontend::ParsingContext context{true};
    frontend::CypherMainVisitor visitor(context, &ast_storage);

    visitor.visit(parser->tree());

    CachedQuery cached_query{std::move(ast_storage), visitor.query(),
                             query::GetRequiredPrivileges(visitor.query())};

    it = accessor.insert({hash, std::move(cached_query)}).first;
  }

  // Return a copy of both the AST storage and the query.
  AstStorage ast_storage;
  ast_storage.properties_ = it->second.ast_storage.properties_;
  ast_storage.labels_ = it->second.ast_storage.labels_;
  ast_storage.edge_types_ = it->second.ast_storage.edge_types_;

  Query *query = it->second.query->Clone(&ast_storage);

  return ParsedQuery{query_string,
                     params,
                     std::move(parameters),
                     std::move(stripped_query),
                     std::move(ast_storage),
                     query,
                     it->second.required_privileges};
}

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

CachedPlan::CachedPlan(std::unique_ptr<LogicalPlan> plan)
    : plan_(std::move(plan)) {}

struct Callback {
  std::vector<std::string> header;
  std::function<std::vector<std::vector<TypedValue>>()> fn;
  bool should_abort_query{false};
};

TypedValue EvaluateOptionalExpression(Expression *expression,
                                      ExpressionEvaluator *eval) {
  return expression ? expression->Accept(*eval) : TypedValue();
}

Callback HandleAuthQuery(AuthQuery *auth_query, AuthQueryHandler *auth,
                         const Parameters &parameters,
                         DbAccessor *db_accessor) {
  // Empty frame for evaluation of password expression. This is OK since
  // password should be either null or string literal and it's evaluation
  // should not depend on frame.
  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  // TODO: MemoryResource for EvaluationContext, it should probably be passed as
  // the argument to Callback.
  evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  evaluation_context.parameters = parameters;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context,
                                db_accessor, storage::View::OLD);

  std::string username = auth_query->user_;
  std::string rolename = auth_query->role_;
  std::string user_or_role = auth_query->user_or_role_;
  std::vector<AuthQuery::Privilege> privileges = auth_query->privileges_;
  auto password = EvaluateOptionalExpression(auth_query->password_, &evaluator);

  Callback callback;

  switch (auth_query->action_) {
    case AuthQuery::Action::CREATE_USER:
      callback.fn = [auth, username, password] {
        CHECK(password.IsString() || password.IsNull());
        if (!auth->CreateUser(username, password.IsString()
                                            ? std::make_optional(std::string(
                                                  password.ValueString()))
                                            : std::nullopt)) {
          throw QueryRuntimeException("User '{}' already exists.", username);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DROP_USER:
      callback.fn = [auth, username] {
        if (!auth->DropUser(username)) {
          throw QueryRuntimeException("User '{}' doesn't exist.", username);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::SET_PASSWORD:
      callback.fn = [auth, username, password] {
        CHECK(password.IsString() || password.IsNull());
        auth->SetPassword(
            username,
            password.IsString()
                ? std::make_optional(std::string(password.ValueString()))
                : std::nullopt);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::CREATE_ROLE:
      callback.fn = [auth, rolename] {
        if (!auth->CreateRole(rolename)) {
          throw QueryRuntimeException("Role '{}' already exists.", rolename);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DROP_ROLE:
      callback.fn = [auth, rolename] {
        if (!auth->DropRole(rolename)) {
          throw QueryRuntimeException("Role '{}' doesn't exist.", rolename);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::SHOW_USERS:
      callback.header = {"user"};
      callback.fn = [auth] {
        std::vector<std::vector<TypedValue>> rows;
        auto usernames = auth->GetUsernames();
        rows.reserve(usernames.size());
        for (auto &&username : usernames) {
          rows.emplace_back(std::vector<TypedValue>{username});
        }
        return rows;
      };
      return callback;
    case AuthQuery::Action::SHOW_ROLES:
      callback.header = {"role"};
      callback.fn = [auth] {
        std::vector<std::vector<TypedValue>> rows;
        auto rolenames = auth->GetRolenames();
        rows.reserve(rolenames.size());
        for (auto &&rolename : rolenames) {
          rows.emplace_back(std::vector<TypedValue>{rolename});
        }
        return rows;
      };
      return callback;
    case AuthQuery::Action::SET_ROLE:
      callback.fn = [auth, username, rolename] {
        auth->SetRole(username, rolename);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::CLEAR_ROLE:
      callback.fn = [auth, username] {
        auth->ClearRole(username);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::GRANT_PRIVILEGE:
      callback.fn = [auth, user_or_role, privileges] {
        auth->GrantPrivilege(user_or_role, privileges);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DENY_PRIVILEGE:
      callback.fn = [auth, user_or_role, privileges] {
        auth->DenyPrivilege(user_or_role, privileges);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::REVOKE_PRIVILEGE: {
      callback.fn = [auth, user_or_role, privileges] {
        auth->RevokePrivilege(user_or_role, privileges);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    }
    case AuthQuery::Action::SHOW_PRIVILEGES:
      callback.header = {"privilege", "effective", "description"};
      callback.fn = [auth, user_or_role] {
        return auth->GetPrivileges(user_or_role);
      };
      return callback;
    case AuthQuery::Action::SHOW_ROLE_FOR_USER:
      callback.header = {"role"};
      callback.fn = [auth, username] {
        auto maybe_rolename = auth->GetRolenameForUser(username);
        return std::vector<std::vector<TypedValue>>{std::vector<TypedValue>{
            TypedValue(maybe_rolename ? *maybe_rolename : "null")}};
      };
      return callback;
    case AuthQuery::Action::SHOW_USERS_FOR_ROLE:
      callback.header = {"users"};
      callback.fn = [auth, rolename] {
        std::vector<std::vector<TypedValue>> rows;
        auto usernames = auth->GetUsernamesForRole(rolename);
        rows.reserve(usernames.size());
        for (auto &&username : usernames) {
          rows.emplace_back(std::vector<TypedValue>{username});
        }
        return rows;
      };
      return callback;
    default:
      break;
  }
}

Callback HandleReplicationQuery(ReplicationQuery *repl_query,
                                ReplicationQueryHandler *handler,
                                const Parameters &parameters,
                                DbAccessor *db_accessor) {
  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  evaluation_context.parameters = parameters;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context,
                                db_accessor, storage::View::OLD);

  Callback callback;
  switch (repl_query->action_) {
    case ReplicationQuery::Action::SET_REPLICATION_MODE: {
      callback.fn = [handler, mode = repl_query->mode_] {
        if (!handler->SetReplicationMode(mode)) {
          throw QueryRuntimeException(
              "Couldn't set the desired replication mode.");
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    }
    case ReplicationQuery::Action::SHOW_REPLICATION_MODE: {
      callback.header = {"replication mode"};
      callback.fn = [handler] {
        auto mode = handler->ShowReplicationMode();
        switch (mode) {
          case ReplicationQuery::ReplicationMode::MAIN: {
            return std::vector<std::vector<TypedValue>>{{TypedValue("main")}};
          }
          case ReplicationQuery::ReplicationMode::REPLICA: {
            return std::vector<std::vector<TypedValue>>{
                {TypedValue("replica")}};
          }
        }
      };
      return callback;
    }
    case ReplicationQuery::Action::CREATE_REPLICA: {
      const auto &name = repl_query->replica_name_;
      const auto &sync_mode = repl_query->sync_mode_;
      auto hostname =
          EvaluateOptionalExpression(repl_query->hostname_, &evaluator);
      auto timeout =
          EvaluateOptionalExpression(repl_query->timeout_, &evaluator);
      std::optional<double> opt_timeout;
      if (timeout.IsDouble()) {
        opt_timeout = timeout.ValueDouble();
      } else if (timeout.IsInt()) {
        opt_timeout = static_cast<double>(timeout.ValueInt());
      }
      callback.fn = [handler, name, hostname, sync_mode, opt_timeout] {
        CHECK(hostname.IsString());
        if (!handler->CreateReplica(name, std::string(hostname.ValueString()),
                                    sync_mode, opt_timeout)) {
          throw QueryRuntimeException(
              "Couldn't create the desired replica '{}'.", name);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    }
    case ReplicationQuery::Action::DROP_REPLICA: {
      const auto &name = repl_query->replica_name_;
      callback.fn = [handler, name] {
        if (!handler->DropReplica(name)) {
          throw QueryRuntimeException("Couldn't drop the replica '{}'.", name);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    }
    case ReplicationQuery::Action::SHOW_REPLICAS: {
      callback.header = {"name", "hostname", "sync_mode", "timeout"};
      callback.fn = [handler, replica_nfields = callback.header.size()] {
        const auto &replicas = handler->ShowReplicas();
        auto typed_replicas = std::vector<std::vector<TypedValue>>{};
        typed_replicas.reserve(replicas.size());
        for (auto &replica : replicas) {
          std::vector<TypedValue> typed_replica;
          typed_replica.reserve(replica_nfields);

          typed_replica.emplace_back(TypedValue(replica.name));
          typed_replica.emplace_back(TypedValue(replica.hostname));
          switch (replica.sync_mode) {
            case ReplicationQuery::SyncMode::SYNC:
              typed_replica.emplace_back(TypedValue("sync"));
              break;
            case ReplicationQuery::SyncMode::ASYNC:
              typed_replica.emplace_back(TypedValue("async"));
              break;
          }
          typed_replica.emplace_back(
              TypedValue(static_cast<int64_t>(replica.sync_mode)));
          if (replica.timeout) {
            typed_replica.emplace_back(TypedValue(*replica.timeout));
          } else {
            typed_replica.emplace_back(TypedValue());
          }

          typed_replicas.emplace_back(std::move(typed_replica));
        }
        return typed_replicas;
      };
      return callback;
    }
      return callback;
  }
}

Interpreter::Interpreter(InterpreterContext *interpreter_context)
    : interpreter_context_(interpreter_context) {
  CHECK(interpreter_context_) << "Interpreter context must not be NULL";
}

namespace {
// Struct for lazy pulling from a vector
struct PullPlanVector {
  explicit PullPlanVector(std::vector<std::vector<TypedValue>> values)
      : values_(std::move(values)) {}

  // @return true if there are more unstreamed elements in vector,
  // false otherwise.
  bool Pull(AnyStream *stream, std::optional<int> n) {
    int local_counter{0};
    while (global_counter < values_.size() && (!n || local_counter < n)) {
      stream->Result(values_[global_counter]);
      ++global_counter;
      ++local_counter;
    }

    return global_counter == values_.size();
  }

 private:
  int global_counter{0};
  std::vector<std::vector<TypedValue>> values_;
};

struct PullPlan {
  explicit PullPlan(std::shared_ptr<CachedPlan> plan,
                    const Parameters &parameters, bool is_profile_query,
                    DbAccessor *dba, InterpreterContext *interpreter_context,
                    utils::MonotonicBufferResource *execution_memory);
  std::optional<ExecutionContext> Pull(
      AnyStream *stream, std::optional<int> n,
      const std::vector<Symbol> &output_symbols,
      std::map<std::string, TypedValue> *summary);

 private:
  std::shared_ptr<CachedPlan> plan_ = nullptr;
  plan::UniqueCursorPtr cursor_ = nullptr;
  Frame frame_;
  ExecutionContext ctx_;

  // As it's possible to query execution using multiple pulls
  // we need the keep track of the total execution time across
  // those pulls by accumulating the execution time.
  std::chrono::duration<double> execution_time_{0};

  // To pull the results from a query we call the `Pull` method on
  // the cursor which saves the results in a Frame.
  // Becuase we can't find out if there are some saved results in a frame,
  // and the cursor cannot deduce if the next pull will have a result,
  // we have to keep track of any unsent results from previous `PullPlan::Pull`
  // manually by using this flag.
  bool has_unsent_results_ = false;
};

PullPlan::PullPlan(const std::shared_ptr<CachedPlan> plan,
                   const Parameters &parameters, const bool is_profile_query,
                   DbAccessor *dba, InterpreterContext *interpreter_context,
                   utils::MonotonicBufferResource *execution_memory)
    : plan_(plan),
      cursor_(plan->plan().MakeCursor(execution_memory)),
      frame_(plan->symbol_table().max_position(), execution_memory) {
  ctx_.db_accessor = dba;
  ctx_.symbol_table = plan->symbol_table();
  ctx_.evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  ctx_.evaluation_context.parameters = parameters;
  ctx_.evaluation_context.properties =
      NamesToProperties(plan->ast_storage().properties_, dba);
  ctx_.evaluation_context.labels =
      NamesToLabels(plan->ast_storage().labels_, dba);
  ctx_.execution_tsc_timer =
      utils::TSCTimer(interpreter_context->tsc_frequency);
  ctx_.max_execution_time_sec = interpreter_context->execution_timeout_sec;
  ctx_.is_shutting_down = &interpreter_context->is_shutting_down;
  ctx_.is_profile_query = is_profile_query;
}

std::optional<ExecutionContext> PullPlan::Pull(
    AnyStream *stream, std::optional<int> n,
    const std::vector<Symbol> &output_symbols,
    std::map<std::string, TypedValue> *summary) {
  // Set up temporary memory for a single Pull. Initial memory comes from the
  // stack. 256 KiB should fit on the stack and should be more than enough for a
  // single `Pull`.
  constexpr size_t stack_size = 256 * 1024;
  char stack_data[stack_size];

  // Returns true if a result was pulled.
  const auto pull_result = [&]() -> bool {
    utils::MonotonicBufferResource monotonic_memory(&stack_data[0], stack_size);
    // TODO (mferencevic): Tune the parameters accordingly.
    utils::PoolResource pool_memory(128, 1024, &monotonic_memory);
    ctx_.evaluation_context.memory = &pool_memory;

    return cursor_->Pull(frame_, ctx_);
  };

  const auto stream_values = [&]() {
    // TODO: The streamed values should also probably use the above memory.
    std::vector<TypedValue> values;
    values.reserve(output_symbols.size());

    for (const auto &symbol : output_symbols) {
      values.emplace_back(frame_[symbol]);
    }

    stream->Result(values);
  };

  // Get the execution time of all possible result pulls and streams.
  utils::Timer timer;

  int i = 0;
  if (has_unsent_results_ && !output_symbols.empty()) {
    // stream unsent results from previous pull
    stream_values();
    ++i;
  }

  for (; !n || i < n; ++i) {
    if (!pull_result()) {
      break;
    }

    if (!output_symbols.empty()) {
      stream_values();
    }
  }

  // If we finished because we streamed the requested n results,
  // we try to pull the next result to see if there is more.
  // If there is additional result, we leave the pulled result in the frame
  // and set the flag to true.
  has_unsent_results_ = i == n && pull_result();

  execution_time_ += timer.Elapsed();

  if (has_unsent_results_) {
    return std::nullopt;
  }

  summary->insert_or_assign("plan_execution_time", execution_time_.count());
  cursor_->Shutdown();
  ctx_.profile_execution_time = execution_time_;
  return ctx_;
}
}  // namespace

/**
 * Convert a parsed *Cypher* query's AST into a logical plan.
 *
 * The created logical plan will take ownership of the `AstStorage` within
 * `ParsedQuery` and might modify it during planning.
 */
std::unique_ptr<LogicalPlan> MakeLogicalPlan(AstStorage ast_storage,
                                             CypherQuery *query,
                                             const Parameters &parameters,
                                             DbAccessor *db_accessor) {
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

/**
 * Return the parsed *Cypher* query's AST cached logical plan, or create and
 * cache a fresh one if it doesn't yet exist.
 */
std::shared_ptr<CachedPlan> CypherQueryToPlan(
    uint64_t hash, AstStorage ast_storage, CypherQuery *query,
    const Parameters &parameters, utils::SkipList<PlanCacheEntry> *plan_cache,
    DbAccessor *db_accessor) {
  auto plan_cache_access = plan_cache->access();
  auto it = plan_cache_access.find(hash);
  if (it != plan_cache_access.end()) {
    if (it->second->IsExpired()) {
      plan_cache_access.remove(hash);
    } else {
      return it->second;
    }
  }
  return plan_cache_access
      .insert({hash,
               std::make_shared<CachedPlan>(MakeLogicalPlan(
                   std::move(ast_storage), (query), parameters, db_accessor))})
      .first->second;
}

PreparedQuery Interpreter::PrepareTransactionQuery(
    std::string_view query_upper) {
  std::function<void()> handler;

  if (query_upper == "BEGIN") {
    handler = [this] {
      if (in_explicit_transaction_) {
        throw ExplicitTransactionUsageException(
            "Nested transactions are not supported.");
      }
      in_explicit_transaction_ = true;
      expect_rollback_ = false;

      db_accessor_.emplace(interpreter_context_->db->Access());
      execution_db_accessor_.emplace(&*db_accessor_);
    };
  } else if (query_upper == "COMMIT") {
    handler = [this] {
      if (!in_explicit_transaction_) {
        throw ExplicitTransactionUsageException(
            "No current transaction to commit.");
      }
      if (expect_rollback_) {
        throw ExplicitTransactionUsageException(
            "Transaction can't be committed because there was a previous "
            "error. Please invoke a rollback instead.");
      }

      try {
        Commit();
      } catch (const utils::BasicException &) {
        AbortCommand(nullptr);
        throw;
      }

      expect_rollback_ = false;
      in_explicit_transaction_ = false;
    };
  } else if (query_upper == "ROLLBACK") {
    handler = [this] {
      if (!in_explicit_transaction_) {
        throw ExplicitTransactionUsageException(
            "No current transaction to rollback.");
      }
      Abort();
      expect_rollback_ = false;
      in_explicit_transaction_ = false;
    };
  } else {
    LOG(FATAL) << "Should not get here -- unknown transaction query!";
  }

  return {
      {}, {}, [handler = std::move(handler)](AnyStream *, std::optional<int>) {
        handler();
        return QueryHandlerResult::NOTHING;
      }};
}

PreparedQuery PrepareCypherQuery(
    ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context, DbAccessor *dba,
    utils::MonotonicBufferResource *execution_memory) {
  auto plan = CypherQueryToPlan(
      parsed_query.stripped_query.hash(), std::move(parsed_query.ast_storage),
      utils::Downcast<CypherQuery>(parsed_query.query), parsed_query.parameters,
      &interpreter_context->plan_cache, dba);

  summary->insert_or_assign("cost_estimate", plan->cost());

  auto output_symbols = plan->plan().OutputSymbols(plan->symbol_table());

  std::vector<std::string> header;
  header.reserve(output_symbols.size());

  for (const auto &symbol : output_symbols) {
    // When the symbol is aliased or expanded from '*' (inside RETURN or
    // WITH), then there is no token position, so use symbol name.
    // Otherwise, find the name from stripped query.
    header.push_back(
        utils::FindOr(parsed_query.stripped_query.named_expressions(),
                      symbol.token_position(), symbol.name())
            .first);
  }

  auto pull_plan =
      std::make_shared<PullPlan>(plan, parsed_query.parameters, false, dba,
                                 interpreter_context, execution_memory);
  return PreparedQuery{
      std::move(header), std::move(parsed_query.required_privileges),
      [pull_plan = std::move(pull_plan),
       output_symbols = std::move(output_symbols),
       summary](AnyStream *stream,
                std::optional<int> n) -> std::optional<QueryHandlerResult> {
        if (pull_plan->Pull(stream, n, output_symbols, summary)) {
          return QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      }};
}

PreparedQuery PrepareExplainQuery(
    ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context, DbAccessor *dba,
    utils::MonotonicBufferResource *execution_memory) {
  const std::string kExplainQueryStart = "explain ";
  CHECK(
      utils::StartsWith(utils::ToLowerCase(parsed_query.stripped_query.query()),
                        kExplainQueryStart))
      << "Expected stripped query to start with '" << kExplainQueryStart << "'";

  // Parse and cache the inner query separately (as if it was a standalone
  // query), producing a fresh AST. Note that currently we cannot just reuse
  // part of the already produced AST because the parameters within ASTs are
  // looked up using their positions within the string that was parsed. These
  // wouldn't match up if if we were to reuse the AST (produced by parsing the
  // full query string) when given just the inner query to execute.
  ParsedQuery parsed_inner_query =
      ParseQuery(parsed_query.query_string.substr(kExplainQueryStart.size()),
                 parsed_query.user_parameters, &interpreter_context->ast_cache,
                 &interpreter_context->antlr_lock);

  auto *cypher_query = utils::Downcast<CypherQuery>(parsed_inner_query.query);
  CHECK(cypher_query)
      << "Cypher grammar should not allow other queries in EXPLAIN";

  auto cypher_query_plan = CypherQueryToPlan(
      parsed_inner_query.stripped_query.hash(),
      std::move(parsed_inner_query.ast_storage), cypher_query,
      parsed_inner_query.parameters, &interpreter_context->plan_cache, dba);

  std::stringstream printed_plan;
  plan::PrettyPrint(*dba, &cypher_query_plan->plan(), &printed_plan);

  std::vector<std::vector<TypedValue>> printed_plan_rows;
  for (const auto &row : utils::Split(utils::RTrim(printed_plan.str()), "\n")) {
    printed_plan_rows.push_back(std::vector<TypedValue>{TypedValue(row)});
  }

  summary->insert_or_assign(
      "explain", plan::PlanToJson(*dba, &cypher_query_plan->plan()).dump());

  return PreparedQuery{
      {"QUERY PLAN"},
      std::move(parsed_query.required_privileges),
      [pull_plan =
           std::make_shared<PullPlanVector>(std::move(printed_plan_rows))](
          AnyStream *stream,
          std::optional<int> n) -> std::optional<QueryHandlerResult> {
        if (pull_plan->Pull(stream, n)) {
          return QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      }};
}

PreparedQuery PrepareProfileQuery(
    ParsedQuery parsed_query, bool in_explicit_transaction,
    std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context, DbAccessor *dba,
    utils::MonotonicBufferResource *execution_memory) {
  const std::string kProfileQueryStart = "profile ";

  CHECK(
      utils::StartsWith(utils::ToLowerCase(parsed_query.stripped_query.query()),
                        kProfileQueryStart))
      << "Expected stripped query to start with '" << kProfileQueryStart << "'";

  // PROFILE isn't allowed inside multi-command (explicit) transactions. This is
  // because PROFILE executes each PROFILE'd query and collects additional
  // perfomance metadata that it displays to the user instead of the results
  // yielded by the query. Because PROFILE has side-effects, each transaction
  // that is used to execute a PROFILE query *MUST* be aborted. That isn't
  // possible when using multicommand (explicit) transactions (because the user
  // controls the lifetime of the transaction) and that is why PROFILE is
  // explicitly disabled here in multicommand (explicit) transactions.
  // NOTE: Unlike PROFILE, EXPLAIN doesn't have any unwanted side-effects (in
  // transaction terms) because it doesn't execute the query, it just prints its
  // query plan. That is why EXPLAIN can be used in multicommand (explicit)
  // transactions.
  if (in_explicit_transaction) {
    throw ProfileInMulticommandTxException();
  }

  if (!interpreter_context->tsc_frequency) {
    throw QueryException("TSC support is missing for PROFILE");
  }

  // Parse and cache the inner query separately (as if it was a standalone
  // query), producing a fresh AST. Note that currently we cannot just reuse
  // part of the already produced AST because the parameters within ASTs are
  // looked up using their positions within the string that was parsed. These
  // wouldn't match up if if we were to reuse the AST (produced by parsing the
  // full query string) when given just the inner query to execute.
  ParsedQuery parsed_inner_query =
      ParseQuery(parsed_query.query_string.substr(kProfileQueryStart.size()),
                 parsed_query.user_parameters, &interpreter_context->ast_cache,
                 &interpreter_context->antlr_lock);

  auto *cypher_query = utils::Downcast<CypherQuery>(parsed_inner_query.query);
  CHECK(cypher_query)
      << "Cypher grammar should not allow other queries in PROFILE";

  auto cypher_query_plan = CypherQueryToPlan(
      parsed_inner_query.stripped_query.hash(),
      std::move(parsed_inner_query.ast_storage), cypher_query,
      parsed_inner_query.parameters, &interpreter_context->plan_cache, dba);

  return PreparedQuery{
      {"OPERATOR", "ACTUAL HITS", "RELATIVE TIME", "ABSOLUTE TIME"},
      std::move(parsed_query.required_privileges),
      [plan = std::move(cypher_query_plan),
       parameters = std::move(parsed_inner_query.parameters), summary, dba,
       interpreter_context, execution_memory,
       // We want to execute the query we are profiling lazily, so we delay
       // the construction of the corresponding context.
       ctx = std::optional<ExecutionContext>{},
       pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
          AnyStream *stream,
          std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
        // No output symbols are given so that nothing is streamed.
        if (!ctx) {
          ctx = PullPlan(plan, parameters, true, dba, interpreter_context,
                         execution_memory)
                    .Pull(stream, {}, {}, summary);
          pull_plan = std::make_shared<PullPlanVector>(
              ProfilingStatsToTable(ctx->stats, ctx->profile_execution_time));
        }

        CHECK(ctx) << "Failed to execute the query!";

        if (pull_plan->Pull(stream, n)) {
          summary->insert_or_assign(
              "profile",
              ProfilingStatsToJson(ctx->stats, ctx->profile_execution_time)
                  .dump());
          return QueryHandlerResult::ABORT;
        }

        return std::nullopt;
      }};
}

PreparedQuery PrepareDumpQuery(
    ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
    DbAccessor *dba, utils::MonotonicBufferResource *execution_memory) {
  return PreparedQuery{
      {"QUERY"},
      std::move(parsed_query.required_privileges),
      [pull_plan = std::make_shared<PullPlanDump>(dba)](
          AnyStream *stream,
          std::optional<int> n) -> std::optional<QueryHandlerResult> {
        if (pull_plan->Pull(stream, n)) {
          return QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      }};
}

PreparedQuery PrepareIndexQuery(
    ParsedQuery parsed_query, bool in_explicit_transaction,
    std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context,
    utils::MonotonicBufferResource *execution_memory) {
  if (in_explicit_transaction) {
    throw IndexInMulticommandTxException();
  }

  auto *index_query = utils::Downcast<IndexQuery>(parsed_query.query);
  std::function<void()> handler;

  // Creating an index influences computed plan costs.
  auto invalidate_plan_cache = [plan_cache = &interpreter_context->plan_cache] {
    auto access = plan_cache->access();
    for (auto &kv : access) {
      access.remove(kv.first);
    }
  };

  auto label = interpreter_context->db->NameToLabel(index_query->label_.name);
  std::vector<storage::PropertyId> properties;
  properties.reserve(index_query->properties_.size());
  for (const auto &prop : index_query->properties_) {
    properties.push_back(interpreter_context->db->NameToProperty(prop.name));
  }

  if (properties.size() > 1) {
    throw utils::NotYetImplemented("index on multiple properties");
  }

  switch (index_query->action_) {
    case IndexQuery::Action::CREATE: {
      handler = [interpreter_context, label, properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)] {
        if (properties.empty()) {
          interpreter_context->db->CreateIndex(label);
        } else {
          CHECK(properties.size() == 1U);
          interpreter_context->db->CreateIndex(label, properties[0]);
        }
        invalidate_plan_cache();
      };
      break;
    }
    case IndexQuery::Action::DROP: {
      handler = [interpreter_context, label, properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)] {
        if (properties.empty()) {
          interpreter_context->db->DropIndex(label);
        } else {
          CHECK(properties.size() == 1U);
          interpreter_context->db->DropIndex(label, properties[0]);
        }
        invalidate_plan_cache();
      };
      break;
    }
  }

  return PreparedQuery{
      {},
      std::move(parsed_query.required_privileges),
      [handler = std::move(handler)](AnyStream *stream, std::optional<int>) {
        handler();
        return QueryHandlerResult::NOTHING;
      }};
}

PreparedQuery PrepareAuthQuery(
    ParsedQuery parsed_query, bool in_explicit_transaction,
    std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context, DbAccessor *dba,
    utils::MonotonicBufferResource *execution_memory) {
  if (in_explicit_transaction) {
    throw UserModificationInMulticommandTxException();
  }

  auto *auth_query = utils::Downcast<AuthQuery>(parsed_query.query);

  auto callback = HandleAuthQuery(auth_query, interpreter_context->auth,
                                  parsed_query.parameters, dba);

  SymbolTable symbol_table;
  std::vector<Symbol> output_symbols;
  for (const auto &column : callback.header) {
    output_symbols.emplace_back(symbol_table.CreateSymbol(column, "false"));
  }

  auto plan =
      std::make_shared<CachedPlan>(std::make_unique<SingleNodeLogicalPlan>(
          std::make_unique<plan::OutputTable>(
              output_symbols,
              [fn = callback.fn](Frame *, ExecutionContext *) { return fn(); }),
          0.0, AstStorage{}, symbol_table));

  auto pull_plan =
      std::make_shared<PullPlan>(plan, parsed_query.parameters, false, dba,
                                 interpreter_context, execution_memory);
  return PreparedQuery{
      callback.header, std::move(parsed_query.required_privileges),
      [pull_plan = std::move(pull_plan), callback = std::move(callback),
       output_symbols = std::move(output_symbols),
       summary](AnyStream *stream,
                std::optional<int> n) -> std::optional<QueryHandlerResult> {
        if (pull_plan->Pull(stream, n, output_symbols, summary)) {
          return callback.should_abort_query ? QueryHandlerResult::ABORT
                                             : QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      }};
}

PreparedQuery PrepareReplicationQuery(
    ParsedQuery parsed_query, bool in_explicit_transaction,
    std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context, DbAccessor *dba,
    utils::MonotonicBufferResource *execution_memory) {
  if (in_explicit_transaction) {
    throw ReplicationModificationInMulticommandTxException();
  }

  auto *replication_query =
      utils::Downcast<ReplicationQuery>(parsed_query.query);
  auto callback =
      HandleReplicationQuery(replication_query, interpreter_context->repl,
                             parsed_query.parameters, dba);

  SymbolTable symbol_table;
  std::vector<Symbol> output_symbols;
  for (const auto &column : callback.header) {
    output_symbols.emplace_back(symbol_table.CreateSymbol(column, "false"));
  }

  auto plan =
      std::make_shared<CachedPlan>(std::make_unique<SingleNodeLogicalPlan>(
          std::make_unique<plan::OutputTable>(
              output_symbols,
              [fn = callback.fn](Frame *, ExecutionContext *) { return fn(); }),
          0.0, AstStorage{}, symbol_table));
  auto pull_plan =
      std::make_shared<PullPlan>(plan, parsed_query.parameters, false, dba,
                                 interpreter_context, execution_memory);
  return PreparedQuery{
      callback.header, std::move(parsed_query.required_privileges),
      [pull_plan = std::move(pull_plan), callback = std::move(callback),
       output_symbols = std::move(output_symbols),
       summary](AnyStream *stream,
                std::optional<int> n) -> std::optional<QueryHandlerResult> {
        if (pull_plan->Pull(stream, n, output_symbols, summary)) {
          return callback.should_abort_query ? QueryHandlerResult::ABORT
                                             : QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      }};
}
 
PreparedQuery PrepareInfoQuery(
    ParsedQuery parsed_query, bool in_explicit_transaction,
    std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context, storage::Storage *db,
    utils::MonotonicBufferResource *execution_memory) {
  if (in_explicit_transaction) {
    throw InfoInMulticommandTxException();
  }

  auto *info_query = utils::Downcast<InfoQuery>(parsed_query.query);
  std::vector<std::string> header;
  std::function<
      std::pair<std::vector<std::vector<TypedValue>>, QueryHandlerResult>()>
      handler;

  switch (info_query->info_type_) {
    case InfoQuery::InfoType::STORAGE:
      header = {"storage info", "value"};
      handler = [db] {
        auto info = db->GetInfo();
        std::vector<std::vector<TypedValue>> results{
            {TypedValue("vertex_count"),
             TypedValue(static_cast<int64_t>(info.vertex_count))},
            {TypedValue("edge_count"),
             TypedValue(static_cast<int64_t>(info.edge_count))},
            {TypedValue("average_degree"), TypedValue(info.average_degree)},
            {TypedValue("memory_usage"),
             TypedValue(static_cast<int64_t>(info.memory_usage))},
            {TypedValue("disk_usage"),
             TypedValue(static_cast<int64_t>(info.disk_usage))}};
        return std::pair{results, QueryHandlerResult::COMMIT};
      };
      break;
    case InfoQuery::InfoType::INDEX:
      header = {"index type", "label", "property"};
      handler = [interpreter_context] {
        auto *db = interpreter_context->db;
        auto info = db->ListAllIndices();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.label.size() + info.label_property.size());
        for (const auto &item : info.label) {
          results.push_back({TypedValue("label"),
                             TypedValue(db->LabelToName(item)), TypedValue()});
        }
        for (const auto &item : info.label_property) {
          results.push_back({TypedValue("label+property"),
                             TypedValue(db->LabelToName(item.first)),
                             TypedValue(db->PropertyToName(item.second))});
        }
        return std::pair{results, QueryHandlerResult::NOTHING};
      };
      break;
    case InfoQuery::InfoType::CONSTRAINT:
      header = {"constraint type", "label", "properties"};
      handler = [interpreter_context] {
        auto *db = interpreter_context->db;
        auto info = db->ListAllConstraints();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.existence.size() + info.unique.size());
        for (const auto &item : info.existence) {
          results.push_back({TypedValue("exists"),
                             TypedValue(db->LabelToName(item.first)),
                             TypedValue(db->PropertyToName(item.second))});
        }
        for (const auto &item : info.unique) {
          std::vector<TypedValue> properties;
          properties.reserve(item.second.size());
          for (const auto &property : item.second) {
            properties.emplace_back(db->PropertyToName(property));
          }
          results.push_back({TypedValue("unique"),
                             TypedValue(db->LabelToName(item.first)),
                             TypedValue(std::move(properties))});
        }
        return std::pair{results, QueryHandlerResult::NOTHING};
      };
      break;
  }

  return PreparedQuery{
      std::move(header), std::move(parsed_query.required_privileges),
      [handler = std::move(handler), action = QueryHandlerResult::NOTHING,
       pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
          AnyStream *stream,
          std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
        if (!pull_plan) {
          auto [results, action_on_complete] = handler();
          action = action_on_complete;
          pull_plan = std::make_shared<PullPlanVector>(std::move(results));
        }

        if (pull_plan->Pull(stream, n)) {
          return action;
        }
        return std::nullopt;
      }};
}

PreparedQuery PrepareConstraintQuery(
    ParsedQuery parsed_query, bool in_explicit_transaction,
    std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context,
    utils::MonotonicBufferResource *execution_memory) {
  if (in_explicit_transaction) {
    throw ConstraintInMulticommandTxException();
  }

  auto *constraint_query = utils::Downcast<ConstraintQuery>(parsed_query.query);
  std::function<void()> handler;

  auto label = interpreter_context->db->NameToLabel(
      constraint_query->constraint_.label.name);
  std::vector<storage::PropertyId> properties;
  properties.reserve(constraint_query->constraint_.properties.size());
  for (const auto &prop : constraint_query->constraint_.properties) {
    properties.push_back(interpreter_context->db->NameToProperty(prop.name));
  }

  switch (constraint_query->action_type_) {
    case ConstraintQuery::ActionType::CREATE: {
      switch (constraint_query->constraint_.type) {
        case Constraint::Type::NODE_KEY:
          throw utils::NotYetImplemented("Node key constraints");
        case Constraint::Type::EXISTS:
          if (properties.empty() || properties.size() > 1) {
            throw SyntaxException(
                "Exactly one property must be used for existence constraints.");
          }
          handler = [interpreter_context, label,
                     properties = std::move(properties)] {
            auto res = interpreter_context->db->CreateExistenceConstraint(
                label, properties[0]);
            if (res.HasError()) {
              auto violation = res.GetError();
              auto label_name =
                  interpreter_context->db->LabelToName(violation.label);
              CHECK(violation.properties.size() == 1U);
              auto property_name = interpreter_context->db->PropertyToName(
                  *violation.properties.begin());
              throw QueryRuntimeException(
                  "Unable to create existence constraint :{}({}), because an "
                  "existing node violates it.",
                  label_name, property_name);
            }
          };
          break;
        case Constraint::Type::UNIQUE:
          std::set<storage::PropertyId> property_set;
          for (const auto &property : properties) {
            property_set.insert(property);
          }
          if (property_set.size() != properties.size()) {
            throw SyntaxException(
                "The given set of properties contains duplicates.");
          }
          handler = [interpreter_context, label,
                     property_set = std::move(property_set)] {
            auto res = interpreter_context->db->CreateUniqueConstraint(
                label, property_set);
            if (res.HasError()) {
              auto violation = res.GetError();
              auto label_name =
                  interpreter_context->db->LabelToName(violation.label);
              std::stringstream property_names_stream;
              utils::PrintIterable(
                  property_names_stream, violation.properties, ", ",
                  [&interpreter_context](auto &stream, const auto &prop) {
                    stream << interpreter_context->db->PropertyToName(prop);
                  });
              throw QueryRuntimeException(
                  "Unable to create unique constraint :{}({}), because an "
                  "existing node violates it.",
                  label_name, property_names_stream.str());
            } else {
              switch (res.GetValue()) {
                case storage::UniqueConstraints::CreationStatus::
                    EMPTY_PROPERTIES:
                  throw SyntaxException(
                      "At least one property must be used for unique "
                      "constraints.");
                  break;
                case storage::UniqueConstraints::CreationStatus::
                    PROPERTIES_SIZE_LIMIT_EXCEEDED:
                  throw SyntaxException(
                      "Too many properties specified. Limit of {} properties "
                      "for unique constraints is exceeded.",
                      storage::kUniqueConstraintsMaxProperties);
                  break;
                case storage::UniqueConstraints::CreationStatus::ALREADY_EXISTS:
                case storage::UniqueConstraints::CreationStatus::SUCCESS:
                  break;
              }
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
          if (properties.empty() || properties.size() > 1) {
            throw SyntaxException(
                "Exactly one property must be used for existence constraints.");
          }
          handler = [interpreter_context, label,
                     properties = std::move(properties)] {
            interpreter_context->db->DropExistenceConstraint(label,
                                                             properties[0]);
            return std::vector<std::vector<TypedValue>>();
          };
          break;
        case Constraint::Type::UNIQUE:
          std::set<storage::PropertyId> property_set;
          for (const auto &property : properties) {
            property_set.insert(property);
          }
          if (property_set.size() != properties.size()) {
            throw SyntaxException(
                "The given set of properties contains duplicates.");
          }
          handler = [interpreter_context, label,
                     property_set = std::move(property_set)] {
            auto res = interpreter_context->db->DropUniqueConstraint(
                label, property_set);
            switch (res) {
              case storage::UniqueConstraints::DeletionStatus::EMPTY_PROPERTIES:
                throw SyntaxException(
                    "At least one property must be used for unique "
                    "constraints.");
                break;
              case storage::UniqueConstraints::DeletionStatus::
                  PROPERTIES_SIZE_LIMIT_EXCEEDED:
                throw SyntaxException(
                    "Too many properties specified. Limit of {} properties for "
                    "unique constraints is exceeded.",
                    storage::kUniqueConstraintsMaxProperties);
                break;
              case storage::UniqueConstraints::DeletionStatus::NOT_FOUND:
              case storage::UniqueConstraints::DeletionStatus::SUCCESS:
                break;
            }
            return std::vector<std::vector<TypedValue>>();
          };
      }
    } break;
  }

  return PreparedQuery{
      {},
      std::move(parsed_query.required_privileges),
      [handler = std::move(handler)](AnyStream *stream, std::optional<int> n) {
        handler();
        return QueryHandlerResult::COMMIT;
      }};
}

void Interpreter::BeginTransaction() {
  const auto prepared_query = PrepareTransactionQuery("BEGIN");
  prepared_query.query_handler(nullptr, {});
}

void Interpreter::CommitTransaction() {
  const auto prepared_query = PrepareTransactionQuery("COMMIT");
  prepared_query.query_handler(nullptr, {});
  query_executions_.clear();
}

void Interpreter::RollbackTransaction() {
  const auto prepared_query = PrepareTransactionQuery("ROLLBACK");
  prepared_query.query_handler(nullptr, {});
  query_executions_.clear();
}

Interpreter::PrepareResult Interpreter::Prepare(
    const std::string &query_string,
    const std::map<std::string, storage::PropertyValue> &params) {
  if (!in_explicit_transaction_) {
    query_executions_.clear();
  }

  query_executions_.emplace_back(std::make_unique<QueryExecution>());
  auto &query_execution = query_executions_.back();
  std::optional<int> qid = in_explicit_transaction_
                               ? static_cast<int>(query_executions_.size() - 1)
                               : std::optional<int>{};

  // Handle transaction control queries.
  auto query_upper = utils::Trim(utils::ToUpperCase(query_string));

  if (query_upper == "BEGIN" || query_upper == "COMMIT" ||
      query_upper == "ROLLBACK") {
    query_execution->prepared_query.emplace(
        PrepareTransactionQuery(query_upper));
    return {query_execution->prepared_query->header,
            query_execution->prepared_query->privileges, qid};
  }

  // All queries other than transaction control queries advance the command in
  // an explicit transaction block.
  if (in_explicit_transaction_) {
    AdvanceCommand();
  }
  // If we're not in an explicit transaction block and we have an open
  // transaction, abort it since we're about to prepare a new query.
  else if (db_accessor_) {
    AbortCommand(&query_execution);
  }

  try {
    // TODO: Set summary['type'] based on transaction metadata. The type can't
    // be determined based only on the toplevel logical operator -- for example
    // `MATCH DELETE RETURN`, which is a write query, will have `Produce` as its
    // toplevel operator). For now we always set "rw" because something must be
    // set, but it doesn't have to be correct (for Bolt clients).
    query_execution->summary["type"] = "rw";

    // Set a default cost estimate of 0. Individual queries can overwrite this
    // field with an improved estimate.
    query_execution->summary["cost_estimate"] = 0.0;

    utils::Timer parsing_timer;
    ParsedQuery parsed_query =
        ParseQuery(query_string, params, &interpreter_context_->ast_cache,
                   &interpreter_context_->antlr_lock);
    query_execution->summary["parsing_time"] = parsing_timer.Elapsed().count();

    // Some queries require an active transaction in order to be prepared.
    if (!in_explicit_transaction_ &&
        (utils::Downcast<CypherQuery>(parsed_query.query) ||
         utils::Downcast<ExplainQuery>(parsed_query.query) ||
         utils::Downcast<ProfileQuery>(parsed_query.query) ||
         utils::Downcast<DumpQuery>(parsed_query.query))) {
      db_accessor_.emplace(interpreter_context_->db->Access());
      execution_db_accessor_.emplace(&*db_accessor_);
    }

    utils::Timer planning_timer;
    PreparedQuery prepared_query;

    if (utils::Downcast<CypherQuery>(parsed_query.query)) {
      prepared_query =
          PrepareCypherQuery(std::move(parsed_query), &query_execution->summary,
                             interpreter_context_, &*execution_db_accessor_,
                             &query_execution->execution_memory);
    } else if (utils::Downcast<ExplainQuery>(parsed_query.query)) {
      prepared_query = PrepareExplainQuery(
          std::move(parsed_query), &query_execution->summary,
          interpreter_context_, &*execution_db_accessor_,
          &query_execution->execution_memory);
    } else if (utils::Downcast<ProfileQuery>(parsed_query.query)) {
      prepared_query = PrepareProfileQuery(
          std::move(parsed_query), in_explicit_transaction_,
          &query_execution->summary, interpreter_context_,
          &*execution_db_accessor_, &query_execution->execution_memory);
    } else if (utils::Downcast<DumpQuery>(parsed_query.query)) {
      prepared_query = PrepareDumpQuery(
          std::move(parsed_query), &query_execution->summary,
          &*execution_db_accessor_, &query_execution->execution_memory);
    } else if (utils::Downcast<IndexQuery>(parsed_query.query)) {
      prepared_query =
          PrepareIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                            &query_execution->summary, interpreter_context_,
                            &query_execution->execution_memory);
    } else if (utils::Downcast<AuthQuery>(parsed_query.query)) {
      prepared_query = PrepareAuthQuery(
          std::move(parsed_query), in_explicit_transaction_,
          &query_execution->summary, interpreter_context_,
          &*execution_db_accessor_, &query_execution->execution_memory);
    } else if (utils::Downcast<InfoQuery>(parsed_query.query)) {
      prepared_query = PrepareInfoQuery(
          std::move(parsed_query), in_explicit_transaction_,
          &query_execution->summary, interpreter_context_,
          interpreter_context_->db, &query_execution->execution_memory);
    } else if (utils::Downcast<ConstraintQuery>(parsed_query.query)) {
      prepared_query = PrepareConstraintQuery(
          std::move(parsed_query), in_explicit_transaction_,
          &query_execution->summary, interpreter_context_,
          &query_execution->execution_memory);
    } else if (utils::Downcast<ReplicationQuery>(parsed_query.query)) {
      prepared_query = PrepareReplicationQuery(
          std::move(parsed_query), in_explicit_transaction_,
          &query_execution->summary, interpreter_context_,
          &*execution_db_accessor_, &query_execution->execution_memory);
    } else {
      LOG(FATAL) << "Should not get here -- unknown query type!";
    }

    query_execution->summary["planning_time"] =
        planning_timer.Elapsed().count();
    query_execution->prepared_query.emplace(std::move(prepared_query));

    return {query_execution->prepared_query->header,
            query_execution->prepared_query->privileges, qid};
  } catch (const utils::BasicException &) {
    AbortCommand(&query_execution);
    throw;
  }
}

void Interpreter::Abort() {
  expect_rollback_ = false;
  in_explicit_transaction_ = false;
  if (!db_accessor_) return;
  db_accessor_->Abort();
  execution_db_accessor_ = std::nullopt;
  db_accessor_ = std::nullopt;
}

void Interpreter::Commit() {
  // It's possible that some queries did not finish because the user did
  // not pull all of the results from the query.
  // For now, we will not check if there are some unfinished queries.
  // We should document clearly that all results should be pulled to complete
  // a query.
  if (!db_accessor_) return;
  auto maybe_constraint_violation = db_accessor_->Commit();
  if (maybe_constraint_violation.HasError()) {
    const auto &constraint_violation = maybe_constraint_violation.GetError();
    switch (constraint_violation.type) {
      case storage::ConstraintViolation::Type::EXISTENCE: {
        auto label_name =
            execution_db_accessor_->LabelToName(constraint_violation.label);
        CHECK(constraint_violation.properties.size() == 1U);
        auto property_name = execution_db_accessor_->PropertyToName(
            *constraint_violation.properties.begin());
        execution_db_accessor_ = std::nullopt;
        db_accessor_ = std::nullopt;
        throw QueryException(
            "Unable to commit due to existence constraint violation on :{}({})",
            label_name, property_name);
        break;
      }
      case storage::ConstraintViolation::Type::UNIQUE: {
        auto label_name =
            execution_db_accessor_->LabelToName(constraint_violation.label);
        std::stringstream property_names_stream;
        utils::PrintIterable(
            property_names_stream, constraint_violation.properties, ", ",
            [this](auto &stream, const auto &prop) {
              stream << execution_db_accessor_->PropertyToName(prop);
            });
        execution_db_accessor_ = std::nullopt;
        db_accessor_ = std::nullopt;
        throw QueryException(
            "Unable to commit due to unique constraint violation on :{}({})",
            label_name, property_names_stream.str());
        break;
      }
    }
  }
  execution_db_accessor_ = std::nullopt;
  db_accessor_ = std::nullopt;
}

void Interpreter::AdvanceCommand() {
  if (!db_accessor_) return;
  db_accessor_->AdvanceCommand();
}

void Interpreter::AbortCommand(
    std::unique_ptr<QueryExecution> *query_execution) {
  if (query_execution) {
    query_execution->reset(nullptr);
  }
  if (in_explicit_transaction_) {
    expect_rollback_ = true;
  } else {
    Abort();
  }
}

}  // namespace query
