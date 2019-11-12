#include "query/interpreter.hpp"

#include <limits>

#include <glog/logging.h>

#include "auth/auth.hpp"
#include "glue/auth.hpp"
#include "glue/communication.hpp"
#ifndef MG_SINGLE_NODE_HA
#include "query/dump.hpp"
#endif
#include "query/exceptions.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/required_privileges.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/eval.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/profile.hpp"
#include "query/plan/vertex_count_cache.hpp"
#ifdef MG_SINGLE_NODE_HA
#include "raft/exceptions.hpp"
#endif
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

/**
 * A container for data related to the parsing of a query.
 */
struct ParsedQuery {
  std::string query_string;
  std::map<std::string, PropertyValue> user_parameters;
  Parameters parameters;
  frontend::StrippedQuery stripped_query;
  AstStorage ast_storage;
  Query *query;
  std::vector<AuthQuery::Privilege> required_privileges;
};

ParsedQuery ParseQuery(const std::string &query_string,
                       const std::map<std::string, PropertyValue> &params,
                       utils::SkipList<QueryCacheEntry> *cache,
                       utils::SpinLock *antlr_lock) {
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

Callback HandleAuthQuery(AuthQuery *auth_query, auth::Auth *auth,
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

  AuthQuery::Action action = auth_query->action_;
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

        std::lock_guard<std::mutex> lock(auth->WithLock());
        auto user = auth->AddUser(
            username,
            password.IsString()
                ? std::make_optional(std::string(password.ValueString()))
                : std::nullopt);
        if (!user) {
          throw QueryRuntimeException("User or role '{}' already exists.",
                                      username);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DROP_USER:
      callback.fn = [auth, username] {
        std::lock_guard<std::mutex> lock(auth->WithLock());
        auto user = auth->GetUser(username);
        if (!user) {
          throw QueryRuntimeException("User '{}' doesn't exist.", username);
        }
        if (!auth->RemoveUser(username)) {
          throw QueryRuntimeException("Couldn't remove user '{}'.", username);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::SET_PASSWORD:
      callback.fn = [auth, username, password] {
        CHECK(password.IsString() || password.IsNull());

        std::lock_guard<std::mutex> lock(auth->WithLock());
        auto user = auth->GetUser(username);
        if (!user) {
          throw QueryRuntimeException("User '{}' doesn't exist.", username);
        }
        user->UpdatePassword(
            password.IsString()
                ? std::make_optional(std::string(password.ValueString()))
                : std::nullopt);
        auth->SaveUser(*user);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::CREATE_ROLE:
      callback.fn = [auth, rolename] {
        std::lock_guard<std::mutex> lock(auth->WithLock());
        auto role = auth->AddRole(rolename);
        if (!role) {
          throw QueryRuntimeException("User or role '{}' already exists.",
                                      rolename);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DROP_ROLE:
      callback.fn = [auth, rolename] {
        std::lock_guard<std::mutex> lock(auth->WithLock());
        auto role = auth->GetRole(rolename);
        if (!role) {
          throw QueryRuntimeException("Role '{}' doesn't exist.", rolename);
        }
        if (!auth->RemoveRole(rolename)) {
          throw QueryRuntimeException("Couldn't remove role '{}'.", rolename);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::SHOW_USERS:
      callback.header = {"user"};
      callback.fn = [auth] {
        std::lock_guard<std::mutex> lock(auth->WithLock());
        std::vector<std::vector<TypedValue>> users;
        for (const auto &user : auth->AllUsers()) {
          users.push_back({TypedValue(user.username())});
        }
        return users;
      };
      return callback;
    case AuthQuery::Action::SHOW_ROLES:
      callback.header = {"role"};
      callback.fn = [auth] {
        std::lock_guard<std::mutex> lock(auth->WithLock());
        std::vector<std::vector<TypedValue>> roles;
        for (const auto &role : auth->AllRoles()) {
          roles.push_back({TypedValue(role.rolename())});
        }
        return roles;
      };
      return callback;
    case AuthQuery::Action::SET_ROLE:
      callback.fn = [auth, username, rolename] {
        std::lock_guard<std::mutex> lock(auth->WithLock());
        auto user = auth->GetUser(username);
        if (!user) {
          throw QueryRuntimeException("User '{}' doesn't exist .", username);
        }
        auto role = auth->GetRole(rolename);
        if (!role) {
          throw QueryRuntimeException("Role '{}' doesn't exist .", rolename);
        }
        if (user->role()) {
          throw QueryRuntimeException(
              "User '{}' is already a member of role '{}'.", username,
              user->role()->rolename());
        }
        user->SetRole(*role);
        auth->SaveUser(*user);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::CLEAR_ROLE:
      callback.fn = [auth, username] {
        std::lock_guard<std::mutex> lock(auth->WithLock());
        auto user = auth->GetUser(username);
        if (!user) {
          throw QueryRuntimeException("User '{}' doesn't exist .", username);
        }
        user->ClearRole();
        auth->SaveUser(*user);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::GRANT_PRIVILEGE:
    case AuthQuery::Action::DENY_PRIVILEGE:
    case AuthQuery::Action::REVOKE_PRIVILEGE: {
      callback.fn = [auth, user_or_role, action, privileges] {
        std::lock_guard<std::mutex> lock(auth->WithLock());
        std::vector<auth::Permission> permissions;
        for (const auto &privilege : privileges) {
          permissions.push_back(glue::PrivilegeToPermission(privilege));
        }
        auto user = auth->GetUser(user_or_role);
        auto role = auth->GetRole(user_or_role);
        if (!user && !role) {
          throw QueryRuntimeException("User or role '{}' doesn't exist.",
                                      user_or_role);
        }
        if (user) {
          for (const auto &permission : permissions) {
            // TODO (mferencevic): should we first check that the privilege
            // is granted/denied/revoked before unconditionally
            // granting/denying/revoking it?
            if (action == AuthQuery::Action::GRANT_PRIVILEGE) {
              user->permissions().Grant(permission);
            } else if (action == AuthQuery::Action::DENY_PRIVILEGE) {
              user->permissions().Deny(permission);
            } else {
              user->permissions().Revoke(permission);
            }
          }
          auth->SaveUser(*user);
        } else {
          for (const auto &permission : permissions) {
            // TODO (mferencevic): should we first check that the privilege
            // is granted/denied/revoked before unconditionally
            // granting/denying/revoking it?
            if (action == AuthQuery::Action::GRANT_PRIVILEGE) {
              role->permissions().Grant(permission);
            } else if (action == AuthQuery::Action::DENY_PRIVILEGE) {
              role->permissions().Deny(permission);
            } else {
              role->permissions().Revoke(permission);
            }
          }
          auth->SaveRole(*role);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    }
    case AuthQuery::Action::SHOW_PRIVILEGES:
      callback.header = {"privilege", "effective", "description"};
      callback.fn = [auth, user_or_role] {
        std::lock_guard<std::mutex> lock(auth->WithLock());
        std::vector<std::vector<TypedValue>> grants;
        auto user = auth->GetUser(user_or_role);
        auto role = auth->GetRole(user_or_role);
        if (!user && !role) {
          throw QueryRuntimeException("User or role '{}' doesn't exist.",
                                      user_or_role);
        }
        if (user) {
          const auto &permissions = user->GetPermissions();
          for (const auto &privilege : kPrivilegesAll) {
            auto permission = glue::PrivilegeToPermission(privilege);
            auto effective = permissions.Has(permission);
            if (permissions.Has(permission) != auth::PermissionLevel::NEUTRAL) {
              std::vector<std::string> description;
              auto user_level = user->permissions().Has(permission);
              if (user_level == auth::PermissionLevel::GRANT) {
                description.push_back("GRANTED TO USER");
              } else if (user_level == auth::PermissionLevel::DENY) {
                description.push_back("DENIED TO USER");
              }
              if (user->role()) {
                auto role_level = user->role()->permissions().Has(permission);
                if (role_level == auth::PermissionLevel::GRANT) {
                  description.push_back("GRANTED TO ROLE");
                } else if (role_level == auth::PermissionLevel::DENY) {
                  description.push_back("DENIED TO ROLE");
                }
              }
              grants.push_back(
                  {TypedValue(auth::PermissionToString(permission)),
                   TypedValue(auth::PermissionLevelToString(effective)),
                   TypedValue(utils::Join(description, ", "))});
            }
          }
        } else {
          const auto &permissions = role->permissions();
          for (const auto &privilege : kPrivilegesAll) {
            auto permission = glue::PrivilegeToPermission(privilege);
            auto effective = permissions.Has(permission);
            if (effective != auth::PermissionLevel::NEUTRAL) {
              std::string description;
              if (effective == auth::PermissionLevel::GRANT) {
                description = "GRANTED TO ROLE";
              } else if (effective == auth::PermissionLevel::DENY) {
                description = "DENIED TO ROLE";
              }
              grants.push_back(
                  {TypedValue(auth::PermissionToString(permission)),
                   TypedValue(auth::PermissionLevelToString(effective)),
                   TypedValue(description)});
            }
          }
        }
        return grants;
      };
      return callback;
    case AuthQuery::Action::SHOW_ROLE_FOR_USER:
      callback.header = {"role"};
      callback.fn = [auth, username] {
        std::lock_guard<std::mutex> lock(auth->WithLock());
        auto user = auth->GetUser(username);
        if (!user) {
          throw QueryRuntimeException("User '{}' doesn't exist .", username);
        }
        return std::vector<std::vector<TypedValue>>{std::vector<TypedValue>{
            TypedValue(user->role() ? user->role()->rolename() : "null")}};
      };
      return callback;
    case AuthQuery::Action::SHOW_USERS_FOR_ROLE:
      callback.header = {"users"};
      callback.fn = [auth, rolename] {
        std::lock_guard<std::mutex> lock(auth->WithLock());
        auto role = auth->GetRole(rolename);
        if (!role) {
          throw QueryRuntimeException("Role '{}' doesn't exist.", rolename);
        }
        std::vector<std::vector<TypedValue>> users;
        for (const auto &user : auth->AllUsersForRole(rolename)) {
          users.emplace_back(
              std::vector<TypedValue>{TypedValue(user.username())});
        }
        return users;
      };
      return callback;
    default:
      break;
  }
}

Interpreter::Interpreter(InterpreterContext *interpreter_context)
    : interpreter_context_(interpreter_context) {
  CHECK(interpreter_context_) << "Interpreter context must not be NULL";
}

ExecutionContext PullAllPlan(AnyStream *stream, const CachedPlan &plan,
                             const Parameters &parameters,
                             const std::vector<Symbol> &output_symbols,
                             bool is_profile_query,
                             std::map<std::string, TypedValue> *summary,
                             DbAccessor *dba,
                             utils::MonotonicBufferResource *execution_memory) {
  auto cursor = plan.plan().MakeCursor(execution_memory);
  Frame frame(plan.symbol_table().max_position(), execution_memory);

  // Set up temporary memory for a single Pull. Initial memory comes from the
  // stack. 256 KiB should fit on the stack and should be more than enough for a
  // single `Pull`.
  constexpr size_t stack_size = 256 * 1024;
  char stack_data[stack_size];

  ExecutionContext ctx;
  ctx.db_accessor = dba;
  ctx.symbol_table = plan.symbol_table();
  ctx.evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  ctx.evaluation_context.parameters = parameters;
  ctx.evaluation_context.properties =
      NamesToProperties(plan.ast_storage().properties_, dba);
  ctx.evaluation_context.labels =
      NamesToLabels(plan.ast_storage().labels_, dba);
  ctx.is_profile_query = is_profile_query;

  utils::Timer timer;

  while (true) {
    utils::MonotonicBufferResource monotonic_memory(&stack_data[0], stack_size);
    // TODO (mferencevic): Tune the parameters accordingly.
    utils::PoolResource pool_memory(128, 1024, &monotonic_memory);
    ctx.evaluation_context.memory = &pool_memory;

    if (!cursor->Pull(frame, ctx)) {
      break;
    }

    if (!output_symbols.empty()) {
      // TODO: The streamed values should also probably use the above memory.
      std::vector<TypedValue> values;
      values.reserve(output_symbols.size());

      for (const auto &symbol : output_symbols) {
        values.emplace_back(frame[symbol]);
      }

      stream->Result(values);
    }
  }

  auto execution_time = timer.Elapsed();
  ctx.profile_execution_time = execution_time;
  summary->insert_or_assign("plan_execution_time", execution_time.count());
  cursor->Shutdown();

  return ctx;
}

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
    HashType hash, AstStorage ast_storage, CypherQuery *query,
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
        AbortCommand();
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

  return {{}, {}, [handler = std::move(handler)](AnyStream *) {
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

  return PreparedQuery{
      std::move(header), std::move(parsed_query.required_privileges),
      [plan = std::move(plan), parameters = std::move(parsed_query.parameters),
       output_symbols = std::move(output_symbols), summary, dba,
       execution_memory](AnyStream *stream) {
        PullAllPlan(stream, *plan, parameters, output_symbols, false, summary,
                    dba, execution_memory);
        return QueryHandlerResult::COMMIT;
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
      parsed_query.stripped_query.hash(), std::move(parsed_query.ast_storage),
      utils::Downcast<CypherQuery>(parsed_query.query), parsed_query.parameters,
      &interpreter_context->plan_cache, dba);

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
      [rows = std::move(printed_plan_rows)](AnyStream *stream) {
        for (const auto &row : rows) {
          stream->Result(row);
        }

        return QueryHandlerResult::ABORT;
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

  if (in_explicit_transaction) {
    throw ProfileInMulticommandTxException();
  }

  if (interpreter_context->is_tsc_available) {
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
      parsed_query.stripped_query.hash(), std::move(parsed_query.ast_storage),
      utils::Downcast<CypherQuery>(parsed_query.query), parsed_query.parameters,
      &interpreter_context->plan_cache, dba);

  return PreparedQuery{
      {"OPERATOR", "ACTUAL HITS", "RELATIVE TIME", "ABSOLUTE TIME"},
      std::move(parsed_query.required_privileges),
      [plan = std::move(cypher_query_plan),
       parameters = std::move(parsed_query.parameters), summary, dba,
       execution_memory](AnyStream *stream) {
        // No output symbols are given so that nothing is streamed.
        auto ctx = PullAllPlan(stream, *plan, parameters, {}, true, summary,
                               dba, execution_memory);

        for (const auto &row :
             ProfilingStatsToTable(ctx.stats, ctx.profile_execution_time)) {
          stream->Result(row);
        }

        summary->insert_or_assign(
            "profile",
            ProfilingStatsToJson(ctx.stats, ctx.profile_execution_time).dump());

        return QueryHandlerResult::ABORT;
      }};
}

PreparedQuery PrepareDumpQuery(
    ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context,
    utils::MonotonicBufferResource *execution_memory) {
#ifndef MG_SINGLE_NODE_HA
  return PreparedQuery{
      {"QUERY"},
      std::move(parsed_query.required_privileges),
      [interpreter_context](AnyStream *stream) {
        auto dba = interpreter_context->db->Access();
        query::DbAccessor query_dba{&dba};
        DumpDatabaseToCypherQueries(&query_dba, stream);
        return QueryHandlerResult::NOTHING;
      }};
#else
  throw utils::NotYetImplemented("Dump database");
#endif
}

PreparedQuery PrepareIndexQuery(
    ParsedQuery parsed_query, bool in_explicit_transaction,
    std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context, DbAccessor *dba,
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

#ifdef MG_SINGLE_NODE_V2
  auto label = interpreter_context->db->NameToLabel(index_query->label_.name);
  std::vector<storage::PropertyId> properties;
  properties.reserve(index_query->properties_.size());
  for (const auto &prop : index_query->properties_) {
    properties.push_back(interpreter_context->db->NameToProperty(prop.name));
  }
#else
  auto label = dba->NameToLabel(index_query->label_.name);
  std::vector<storage::Property> properties;
  properties.reserve(index_query->properties_.size());
  for (const auto &prop : index_query->properties_) {
    properties.push_back(dba->NameToProperty(prop.name));
  }
#endif

  if (properties.size() > 1) {
    throw utils::NotYetImplemented("index on multiple properties");
  }

  switch (index_query->action_) {
    case IndexQuery::Action::CREATE: {
#ifdef MG_SINGLE_NODE_V2
      handler = [interpreter_context, label, properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)] {
        CHECK(properties.size() == 1);
        interpreter_context->db->CreateIndex(label, properties[0]);
        invalidate_plan_cache();
      };
#else
      handler = [dba, label, properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)] {
        try {
          CHECK(properties.size() == 1);
          dba->CreateIndex(label, properties[0]);
          invalidate_plan_cache();
        } catch (const database::ConstraintViolationException &e) {
          throw QueryRuntimeException(e.what());
        } catch (const database::IndexExistsException &e) {
          // Ignore creating an existing index.
        } catch (const database::TransactionException &e) {
          throw QueryRuntimeException(e.what());
        }
      };
#endif
      break;
    }
    case IndexQuery::Action::DROP: {
#ifdef MG_SINGLE_NODE_V2
      handler = [interpreter_context, label, properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)] {
        CHECK(properties.size() == 1);
        interpreter_context->db->DropIndex(label, properties[0]);
        invalidate_plan_cache();
      };
#else
      handler = [dba, label, properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)] {
        try {
          CHECK(properties.size() == 1);
          dba->DropIndex(label, properties[0]);
          invalidate_plan_cache();
        } catch (const database::TransactionException &e) {
          throw QueryRuntimeException(e.what());
        }
      };
#endif
      break;
    }
  }

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [handler = std::move(handler)](AnyStream *stream) {
                         handler();
#ifdef MG_SINGLE_NODE_V2
                         return QueryHandlerResult::NOTHING;
#else
                         return QueryHandlerResult::COMMIT;
#endif
                       }};
}

PreparedQuery PrepareAuthQuery(
    ParsedQuery parsed_query, bool in_explicit_transaction,
    std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context, DbAccessor *dba,
    utils::MonotonicBufferResource *execution_memory) {
#ifdef MG_SINGLE_NODE_HA
  throw utils::NotYetImplemented(
      "Managing user privileges is not yet supported in Memgraph HA "
      "instance.");
#else
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

  return PreparedQuery{
      callback.header, std::move(parsed_query.required_privileges),
      [callback = std::move(callback), plan = std::move(plan),
       parameters = std::move(parsed_query.parameters),
       output_symbols = std::move(output_symbols), summary, dba,
       execution_memory](AnyStream *stream) {
        PullAllPlan(stream, *plan, parameters, output_symbols, false, summary,
                    dba, execution_memory);
        return callback.should_abort_query ? QueryHandlerResult::ABORT
                                           : QueryHandlerResult::COMMIT;
      }};
#endif
}

PreparedQuery PrepareInfoQuery(
    ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context, DbAccessor *dba,
    utils::MonotonicBufferResource *execution_memory) {
  auto *info_query = utils::Downcast<InfoQuery>(parsed_query.query);
  std::vector<std::string> header;
  std::function<
      std::pair<std::vector<std::vector<TypedValue>>, QueryHandlerResult>()>
      handler;

  switch (info_query->info_type_) {
    case InfoQuery::InfoType::STORAGE:
#if defined(MG_SINGLE_NODE)
      header = {"storage info", "value"};
      handler = [dba] {
        auto info = dba->StorageInfo();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.size());
        for (const auto &pair : info) {
          results.push_back({TypedValue(pair.first), TypedValue(pair.second)});
        }
        return std::pair{results, QueryHandlerResult::COMMIT};
      };
#elif defined(MG_SINGLE_NODE_HA)
      header = {"server id", "storage info", "value"};
      handler = [dba] {
        auto info = dba->StorageInfo();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.size());
        for (const auto &peer_info : info) {
          for (const auto &pair : peer_info.second) {
            results.push_back({TypedValue(peer_info.first),
                               TypedValue(pair.first),
                               TypedValue(pair.second)});
          }
        }
        return std::pair{results, QueryHandlerResult::COMMIT};
      };
#else
      throw utils::NotYetImplemented("storage info");
#endif
      break;
    case InfoQuery::InfoType::INDEX:
#ifdef MG_SINGLE_NODE_V2
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
#else
      header = {"created index"};
      handler = [dba] {
        auto info = dba->IndexInfo();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.size());
        for (const auto &index : info) {
          results.push_back({TypedValue(index)});
        }
        return std::pair{results, QueryHandlerResult::COMMIT};
      };
      break;
#endif
    case InfoQuery::InfoType::CONSTRAINT:
#ifdef MG_SINGLE_NODE_V2
      header = {"constraint type", "label", "properties"};
      handler = [interpreter_context] {
        auto *db = interpreter_context->db;
        auto info = db->ListAllConstraints();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.existence.size());
        for (const auto &item : info.existence) {
          results.push_back({TypedValue("exists"),
                             TypedValue(db->LabelToName(item.first)),
                             TypedValue(db->PropertyToName(item.second))});
        }
        return std::pair{results, QueryHandlerResult::NOTHING};
      };
      break;
#else
      header = {"constraint type", "label", "properties"};
      handler = [dba] {
        std::vector<std::vector<TypedValue>> results;
        for (auto &e : dba->ListUniqueConstraints()) {
          std::vector<std::string> property_names(e.properties.size());
          std::transform(
              e.properties.begin(), e.properties.end(), property_names.begin(),
              [dba](const auto &p) { return dba->PropertyToName(p); });

          std::vector<TypedValue> constraint{
              TypedValue("unique"), TypedValue(dba->LabelToName(e.label)),
              TypedValue(utils::Join(property_names, ","))};

          results.emplace_back(constraint);
        }
        return std::pair{results, QueryHandlerResult::COMMIT};
      };
      break;
#endif
    case InfoQuery::InfoType::RAFT:
#if defined(MG_SINGLE_NODE_HA)
      header = {"info", "value"};
      handler = [dba] {
        std::vector<std::vector<TypedValue>> results(
            {{TypedValue("is_leader"), TypedValue(dba->raft()->IsLeader())},
             {TypedValue("term_id"),
              TypedValue(static_cast<int64_t>(dba->raft()->TermId()))}});
        // It is critical to abort this query because it can be executed on
        // machines that aren't the leader.
        return std::pair{results, QueryHandlerResult::ABORT};
      };
#else
      throw utils::NotYetImplemented("raft info");
#endif
      break;
  }

  return PreparedQuery{std::move(header),
                       std::move(parsed_query.required_privileges),
                       [handler = std::move(handler)](AnyStream *stream) {
                         auto [results, action] = handler();

                         for (const auto &result : results) {
                           stream->Result(result);
                         }

                         return action;
                       }};
}

PreparedQuery PrepareConstraintQuery(
    ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
    InterpreterContext *interpreter_context, DbAccessor *dba,
    utils::MonotonicBufferResource *execution_memory) {
  auto *constraint_query = utils::Downcast<ConstraintQuery>(parsed_query.query);
  std::function<void()> handler;

#ifdef MG_SINGLE_NODE_V2
  auto label = interpreter_context->db->NameToLabel(
      constraint_query->constraint_.label.name);
  std::vector<storage::PropertyId> properties;
  properties.reserve(constraint_query->constraint_.properties.size());
  for (const auto &prop : constraint_query->constraint_.properties) {
    properties.push_back(interpreter_context->db->NameToProperty(prop.name));
  }
#else
  auto label = dba->NameToLabel(constraint_query->constraint_.label.name);
  std::vector<storage::Property> properties;
  properties.reserve(constraint_query->constraint_.properties.size());
  for (const auto &prop : constraint_query->constraint_.properties) {
    properties.push_back(dba->NameToProperty(prop.name));
  }
#endif

  switch (constraint_query->action_type_) {
    case ConstraintQuery::ActionType::CREATE: {
      switch (constraint_query->constraint_.type) {
        case Constraint::Type::NODE_KEY:
          throw utils::NotYetImplemented("Node key constraints");
        case Constraint::Type::EXISTS:
#ifdef MG_SINGLE_NODE_V2
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
              auto property_name =
                  interpreter_context->db->PropertyToName(violation.property);
              throw QueryRuntimeException(
                  "Unable to create a constraint :{}({}), because an existing "
                  "node violates it.",
                  label_name, property_name);
            }
          };
          break;
#else
          throw utils::NotYetImplemented("Existence constraints");
#endif
        case Constraint::Type::UNIQUE:
#ifdef MG_SINGLE_NODE_V2
          throw utils::NotYetImplemented("Unique constraints");
#else
          handler = [dba, label, properties = std::move(properties)] {
            try {
              dba->BuildUniqueConstraint(label, properties);
            } catch (const database::ConstraintViolationException &e) {
              throw QueryRuntimeException(e.what());
            } catch (const database::TransactionException &e) {
              throw QueryRuntimeException(e.what());
            } catch (const mvcc::SerializationError &e) {
              throw QueryRuntimeException(e.what());
            }
          };
          break;
#endif
      }
    } break;
    case ConstraintQuery::ActionType::DROP: {
      switch (constraint_query->constraint_.type) {
        case Constraint::Type::NODE_KEY:
          throw utils::NotYetImplemented("Node key constraints");
        case Constraint::Type::EXISTS:
#ifdef MG_SINGLE_NODE_V2
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
#else
          throw utils::NotYetImplemented("Existence constraints");
#endif
        case Constraint::Type::UNIQUE:
#ifdef MG_SINGLE_NODE_V2
          throw utils::NotYetImplemented("Unique constraints");
#else
          handler = [dba, label, properties = std::move(properties)] {
            try {
              dba->DeleteUniqueConstraint(label, properties);
              return std::vector<std::vector<TypedValue>>();
            } catch (const database::TransactionException &e) {
              throw QueryRuntimeException(e.what());
            }
          };
          break;
#endif
      }
    } break;
  }

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [handler = std::move(handler)](AnyStream *stream) {
                         handler();
                         return QueryHandlerResult::COMMIT;
                       }};
}

std::pair<std::vector<std::string>, std::vector<query::AuthQuery::Privilege>>
Interpreter::Prepare(const std::string &query_string,
                     const std::map<std::string, PropertyValue> &params) {
  // Clear the last prepared query.
  prepared_query_ = std::nullopt;
  execution_memory_.Release();

  // Handle transaction control queries.
  auto query_upper = utils::Trim(utils::ToUpperCase(query_string));

  if (query_upper == "BEGIN" || query_upper == "COMMIT" ||
      query_upper == "ROLLBACK") {
    prepared_query_ = PrepareTransactionQuery(query_upper);
    return {prepared_query_->header, prepared_query_->privileges};
  }

  // All queries other than transaction control queries advance the command in
  // an explicit transaction block.
  if (in_explicit_transaction_) {
    AdvanceCommand();
  }
  // If we're not in an explicit transaction block and we have an open
  // transaction, abort it since we're about to prepare a new query.
  else if (db_accessor_) {
    AbortCommand();
  }

  try {
    summary_ = {};

    // TODO: Set summary['type'] based on transaction metadata. The type can't
    // be determined based only on the toplevel logical operator -- for example
    // `MATCH DELETE RETURN`, which is a write query, will have `Produce` as its
    // toplevel operator). For now we always set "rw" because something must be
    // set, but it doesn't have to be correct (for Bolt clients).
    summary_["type"] = "rw";

    // Set a default cost estimate of 0. Individual queries can overwrite this
    // field with an improved estimate.
    summary_["cost_estimate"] = 0.0;

    utils::Timer parsing_timer;
    ParsedQuery parsed_query =
        ParseQuery(query_string, params, &interpreter_context_->ast_cache,
                   &interpreter_context_->antlr_lock);
    summary_["parsing_time"] = parsing_timer.Elapsed().count();

    // Some queries require an active transaction in order to be prepared.
#ifdef MG_SINGLE_NODE_V2
    if (!in_explicit_transaction_ &&
        !utils::Downcast<IndexQuery>(parsed_query.query) &&
        !utils::Downcast<DumpQuery>(parsed_query.query) &&
        !utils::Downcast<ConstraintQuery>(parsed_query.query) &&
        !utils::Downcast<InfoQuery>(parsed_query.query)) {
      db_accessor_.emplace(interpreter_context_->db->Access());
      execution_db_accessor_.emplace(&*db_accessor_);
    }
#else
    if (!in_explicit_transaction_ &&
        !utils::Downcast<DumpQuery>(parsed_query.query)) {
      db_accessor_.emplace(interpreter_context_->db->Access());
      execution_db_accessor_.emplace(&*db_accessor_);
    }
#endif

#ifdef MG_SINGLE_NODE_HA
    {
      InfoQuery *info_query = nullptr;
      if (!execution_db_accessor_->raft()->IsLeader() &&
          (!(info_query = utils::Downcast<InfoQuery>(parsed_query.query)) ||
           info_query->info_type_ != InfoQuery::InfoType::RAFT)) {
        throw raft::CantExecuteQueries();
      }
    }
#endif

    utils::Timer planning_timer;
    PreparedQuery prepared_query;

    if (utils::Downcast<CypherQuery>(parsed_query.query)) {
      prepared_query = PrepareCypherQuery(
          std::move(parsed_query), &summary_, interpreter_context_,
          &*execution_db_accessor_, &execution_memory_);
    } else if (utils::Downcast<ExplainQuery>(parsed_query.query)) {
      prepared_query = PrepareExplainQuery(
          std::move(parsed_query), &summary_, interpreter_context_,
          &*execution_db_accessor_, &execution_memory_);
    } else if (utils::Downcast<ProfileQuery>(parsed_query.query)) {
      prepared_query = PrepareProfileQuery(
          std::move(parsed_query), in_explicit_transaction_, &summary_,
          interpreter_context_, &*execution_db_accessor_, &execution_memory_);
    } else if (utils::Downcast<DumpQuery>(parsed_query.query)) {
      prepared_query =
          PrepareDumpQuery(std::move(parsed_query), &summary_,
                           interpreter_context_, &execution_memory_);
    } else if (utils::Downcast<IndexQuery>(parsed_query.query)) {
#ifdef MG_SINGLE_NODE_V2
      DbAccessor *dba = nullptr;
#else
      auto dba = &*execution_db_accessor_;
#endif
      prepared_query = PrepareIndexQuery(
          std::move(parsed_query), in_explicit_transaction_, &summary_,
          interpreter_context_, dba, &execution_memory_);
    } else if (utils::Downcast<AuthQuery>(parsed_query.query)) {
      prepared_query = PrepareAuthQuery(
          std::move(parsed_query), in_explicit_transaction_, &summary_,
          interpreter_context_, &*execution_db_accessor_, &execution_memory_);
    } else if (utils::Downcast<InfoQuery>(parsed_query.query)) {
#ifdef MG_SINGLE_NODE_V2
      DbAccessor *dba = nullptr;
#else
      auto dba = &*execution_db_accessor_;
#endif
      prepared_query =
          PrepareInfoQuery(std::move(parsed_query), &summary_,
                           interpreter_context_, dba, &execution_memory_);
    } else if (utils::Downcast<ConstraintQuery>(parsed_query.query)) {
#ifdef MG_SINGLE_NODE_V2
      DbAccessor *dba = nullptr;
#else
      auto dba = &*execution_db_accessor_;
#endif
      prepared_query =
          PrepareConstraintQuery(std::move(parsed_query), &summary_,
                                 interpreter_context_, dba, &execution_memory_);
    } else {
      LOG(FATAL) << "Should not get here -- unknown query type!";
    }

    summary_["planning_time"] = planning_timer.Elapsed().count();
    prepared_query_ = std::move(prepared_query);

    return {prepared_query_->header, prepared_query_->privileges};
  } catch (const utils::BasicException &) {
    AbortCommand();
    throw;
  }
}

void Interpreter::Abort() {
  prepared_query_ = std::nullopt;
  execution_memory_.Release();
  expect_rollback_ = false;
  in_explicit_transaction_ = false;
  if (!db_accessor_) return;
  db_accessor_->Abort();
  execution_db_accessor_ = std::nullopt;
  db_accessor_ = std::nullopt;
}

void Interpreter::Commit() {
  prepared_query_ = std::nullopt;
  execution_memory_.Release();
  if (!db_accessor_) return;
#ifdef MG_SINGLE_NODE_V2
  auto maybe_constraint_violation = db_accessor_->Commit();
  if (maybe_constraint_violation.HasError()) {
    const auto &constraint_violation = maybe_constraint_violation.GetError();
    auto label_name =
        execution_db_accessor_->LabelToName(constraint_violation.label);
    auto property_name =
        execution_db_accessor_->PropertyToName(constraint_violation.property);
    execution_db_accessor_ = std::nullopt;
    db_accessor_ = std::nullopt;
    throw QueryException(
        "Unable to commit due to existence constraint violation on :{}({}).",
        label_name, property_name);
  }
#else
  db_accessor_->Commit();
#endif
  execution_db_accessor_ = std::nullopt;
  db_accessor_ = std::nullopt;
}

void Interpreter::AdvanceCommand() {
  prepared_query_ = std::nullopt;
  execution_memory_.Release();
  if (!db_accessor_) return;
  db_accessor_->AdvanceCommand();
}

void Interpreter::AbortCommand() {
  prepared_query_ = std::nullopt;
  execution_memory_.Release();
  if (in_explicit_transaction_) {
    expect_rollback_ = true;
  } else {
    Abort();
  }
}

}  // namespace query
