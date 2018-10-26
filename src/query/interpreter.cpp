#include "query/interpreter.hpp"

#include <glog/logging.h>
#include <limits>

#include "auth/auth.hpp"
#include "glue/auth.hpp"
#include "glue/communication.hpp"
#include "integrations/kafka/exceptions.hpp"
#include "integrations/kafka/streams.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/required_privileges.hpp"
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

Callback HandleAuthQuery(AuthQuery *auth_query, auth::Auth *auth,
                         const EvaluationContext &evaluation_context,
                         database::GraphDbAccessor *db_accessor) {
  // Empty frame for evaluation of password expression. This is OK since
  // password should be either null or string literal and it's evaluation
  // should not depend on frame.
  Frame frame(0);
  SymbolTable symbol_table;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context,
                                db_accessor, GraphView::OLD);

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
            username, password.IsString() ? std::experimental::make_optional(
                                                password.ValueString())
                                          : std::experimental::nullopt);
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
                ? std::experimental::make_optional(password.ValueString())
                : std::experimental::nullopt);
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
          users.push_back({user.username()});
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
          roles.push_back({role.rolename()});
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
              grants.push_back({auth::PermissionToString(permission),
                                auth::PermissionLevelToString(effective),
                                utils::Join(description, ", ")});
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
              grants.push_back({auth::PermissionToString(permission),
                                auth::PermissionLevelToString(effective),
                                description});
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
            user->role() ? user->role()->rolename() : "null"}};
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
          users.emplace_back(std::vector<TypedValue>{user.username()});
        }
        return users;
      };
      return callback;
    default:
      break;
  }
}

Callback HandleStreamQuery(StreamQuery *stream_query,
                           integrations::kafka::Streams *streams,
                           const EvaluationContext &evaluation_context,
                           database::GraphDbAccessor *db_accessor) {
  // Empty frame and symbol table for evaluation of expressions. This is OK
  // since all expressions should be literals or parameter lookups.
  Frame frame(0);
  SymbolTable symbol_table;
  ExpressionEvaluator eval(&frame, symbol_table, evaluation_context,
                           db_accessor, GraphView::OLD);

  std::string stream_name = stream_query->stream_name_;
  auto stream_uri =
      EvaluateOptionalExpression(stream_query->stream_uri_, &eval);
  auto stream_topic =
      EvaluateOptionalExpression(stream_query->stream_topic_, &eval);
  auto transform_uri =
      EvaluateOptionalExpression(stream_query->transform_uri_, &eval);
  auto batch_interval_in_ms =
      EvaluateOptionalExpression(stream_query->batch_interval_in_ms_, &eval);
  auto batch_size =
      EvaluateOptionalExpression(stream_query->batch_size_, &eval);
  auto limit_batches =
      EvaluateOptionalExpression(stream_query->limit_batches_, &eval);

  Callback callback;

  switch (stream_query->action_) {
    case StreamQuery::Action::CREATE_STREAM:
      callback.fn = [streams, stream_name, stream_uri, stream_topic,
                     transform_uri, batch_interval_in_ms, batch_size] {
        CHECK(stream_uri.IsString());
        CHECK(stream_topic.IsString());
        CHECK(transform_uri.IsString());
        CHECK(batch_interval_in_ms.IsInt() || batch_interval_in_ms.IsNull());
        CHECK(batch_size.IsInt() || batch_size.IsNull());

        integrations::kafka::StreamInfo info;
        info.stream_name = stream_name;

        info.stream_uri = stream_uri.ValueString();
        info.stream_topic = stream_topic.ValueString();
        info.transform_uri = transform_uri.ValueString();
        info.batch_interval_in_ms = batch_interval_in_ms.IsInt()
                                        ? std::experimental::make_optional(
                                              batch_interval_in_ms.ValueInt())
                                        : std::experimental::nullopt;
        info.batch_size =
            batch_size.IsInt()
                ? std::experimental::make_optional(batch_size.ValueInt())
                : std::experimental::nullopt;

        try {
          streams->Create(info);
        } catch (const integrations::kafka::KafkaStreamException &e) {
          throw QueryRuntimeException(e.what());
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case StreamQuery::Action::DROP_STREAM:
      callback.fn = [streams, stream_name] {
        try {
          streams->Drop(stream_name);
        } catch (const integrations::kafka::KafkaStreamException &e) {
          throw QueryRuntimeException(e.what());
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case StreamQuery::Action::SHOW_STREAMS:
      callback.header = {"name", "uri", "topic", "transform", "status"};
      callback.fn = [streams] {
        std::vector<std::vector<TypedValue>> status;
        for (const auto &stream : streams->Show()) {
          status.push_back(std::vector<TypedValue>{
              stream.stream_name, stream.stream_uri, stream.stream_topic,
              stream.transform_uri, stream.stream_status});
        }
        return status;
      };
      return callback;
    case StreamQuery::Action::START_STREAM:
      callback.fn = [streams, stream_name, limit_batches] {
        CHECK(limit_batches.IsInt() || limit_batches.IsNull());

        try {
          streams->Start(stream_name, limit_batches.IsInt()
                                          ? std::experimental::make_optional(
                                                limit_batches.ValueInt())
                                          : std::experimental::nullopt);
        } catch (integrations::kafka::KafkaStreamException &e) {
          throw QueryRuntimeException(e.what());
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case StreamQuery::Action::STOP_STREAM:
      callback.fn = [streams, stream_name] {
        try {
          streams->Stop(stream_name);
        } catch (integrations::kafka::KafkaStreamException &e) {
          throw QueryRuntimeException(e.what());
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case StreamQuery::Action::START_ALL_STREAMS:
      callback.fn = [streams] {
        try {
          streams->StartAll();
        } catch (integrations::kafka::KafkaStreamException &e) {
          throw QueryRuntimeException(e.what());
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case StreamQuery::Action::STOP_ALL_STREAMS:
      callback.fn = [streams] {
        try {
          streams->StopAll();
        } catch (integrations::kafka::KafkaStreamException &e) {
          throw QueryRuntimeException(e.what());
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case StreamQuery::Action::TEST_STREAM:
      callback.header = {"query", "params"};
      callback.fn = [streams, stream_name, limit_batches] {
        CHECK(limit_batches.IsInt() || limit_batches.IsNull());

        std::vector<std::vector<TypedValue>> rows;
        try {
          auto results = streams->Test(
              stream_name,
              limit_batches.IsInt()
                  ? std::experimental::make_optional(limit_batches.ValueInt())
                  : std::experimental::nullopt);
          for (const auto &result : results) {
            std::map<std::string, TypedValue> params;
            for (const auto &param : result.second) {
              params.emplace(param.first, glue::ToTypedValue(param.second));
            }

            rows.emplace_back(std::vector<TypedValue>{result.first, params});
          }
        } catch (integrations::kafka::KafkaStreamException &e) {
          throw QueryRuntimeException(e.what());
        }
        return rows;
      };
      return callback;
  }
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
                   output_symbols, header, summary,
                   parsed_query.required_privileges);
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
                   output_symbols, header, summary,
                   parsed_query.required_privileges);
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
  } else if (auto *auth_query = dynamic_cast<AuthQuery *>(parsed_query.query)) {
    if (in_explicit_transaction) {
      throw UserModificationInMulticommandTxException();
    }
    callback =
        HandleAuthQuery(auth_query, auth_, evaluation_context, &db_accessor);
  } else if (auto *stream_query =
                 dynamic_cast<StreamQuery *>(parsed_query.query)) {
    if (in_explicit_transaction) {
      throw StreamClauseInMulticommandTxException();
    }
    callback = HandleStreamQuery(stream_query, kafka_streams_,
                                 evaluation_context, &db_accessor);
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
                 output_symbols, callback.header, summary,
                 parsed_query.required_privileges);
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
    return ParsedQuery{visitor.query(),
                       query::GetRequiredPrivileges(visitor.query())};
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
    CachedQuery cached_query{std::move(cached_ast_storage), visitor.query(),
                             query::GetRequiredPrivileges(visitor.query())};
    // Cache it.
    ast_it =
        ast_cache_accessor.insert(stripped_query_hash, std::move(cached_query))
            .first;
  }
  return ParsedQuery{ast_it->second.query->Clone(*ast_storage),
                     ast_it->second.required_privileges};
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
