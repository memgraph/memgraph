#include "query/interpreter.hpp"

#include <limits>

#include <glog/logging.h>

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
};

TypedValue EvaluateOptionalExpression(Expression *expression,
                                      ExpressionEvaluator *eval) {
  return expression ? expression->Accept(*eval) : TypedValue::Null;
}

Callback HandleAuthQuery(AuthQuery *auth_query, auth::Auth *auth,
                         const Parameters &parameters,
                         database::GraphDbAccessor *db_accessor) {
  // Empty frame for evaluation of password expression. This is OK since
  // password should be either null or string literal and it's evaluation
  // should not depend on frame.
  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  evaluation_context.parameters = parameters;
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
                           const Parameters &parameters,
                           database::GraphDbAccessor *db_accessor) {
  // Empty frame and symbol table for evaluation of expressions. This is OK
  // since all expressions should be literals or parameter lookups.
  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  evaluation_context.parameters = parameters;
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

Callback HandleInfoQuery(InfoQuery *info_query, database::GraphDbAccessor *db_accessor) {
  Callback callback;
  switch (info_query->info_type_) {
    case InfoQuery::InfoType::STORAGE:
#if defined(MG_SINGLE_NODE)
      callback.header = {"storage info", "value"};
      callback.fn = [db_accessor] {
        auto info = db_accessor->StorageInfo();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.size());
        for (const auto &pair : info) {
          results.push_back({pair.first, pair.second});
        }
        return results;
      };
#elif defined(MG_SINGLE_NODE_HA)
      callback.header = {"server id", "storage info", "value"};
      callback.fn = [db_accessor] {
        auto info = db_accessor->StorageInfo();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.size());
        for (const auto &peer_info : info) {
          for (const auto &pair : peer_info.second) {
            results.push_back({peer_info.first, pair.first, pair.second});
          }
        }
        return results;
      };
#else
      throw utils::NotYetImplemented("storage info");
#endif
      break;
    case InfoQuery::InfoType::INDEX:
      callback.header = {"created index"};
      callback.fn = [db_accessor] {
        auto info = db_accessor->IndexInfo();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.size());
        for (const auto &index : info) {
          results.push_back({index});
        }
        return results;
      };
      break;
    case InfoQuery::InfoType::CONSTRAINT:
      throw utils::NotYetImplemented("constraint info");
      break;
  }
  return callback;
}

Callback HandleConstraintQuery(ConstraintQuery *constraint_query,
                               database::GraphDbAccessor *db_accessor) {
  Callback callback;
  std::vector<std::string> property_names;
  property_names.reserve(constraint_query->properties_.size());
  for (const auto &prop_ix : constraint_query->properties_) {
    property_names.push_back(prop_ix.name);
  }
  std::string label_name = constraint_query->label_.name;
  switch (constraint_query->action_type_) {
    case ConstraintQuery::ActionType::CREATE:
      throw utils::NotYetImplemented("create constraint :{}({}) exists",
                                     label_name,
                                     utils::Join(property_names, ", "));
      break;
    case ConstraintQuery::ActionType::DROP:
      throw utils::NotYetImplemented("drop constraint :{}({}) exists",
                                     label_name,
                                     utils::Join(property_names, ", "));
      break;
  }
  return callback;
}

Interpreter::Interpreter() : is_tsc_available_(utils::CheckAvailableTSC()) {}

Interpreter::Results Interpreter::operator()(
    const std::string &query_string, database::GraphDbAccessor &db_accessor,
    const std::map<std::string, PropertyValue> &params,
    bool in_explicit_transaction) {
#ifdef MG_SINGLE_NODE_HA
  if (!db_accessor.raft()->IsLeader()) {
    throw QueryException(
        "Memgraph High Availability: Can't execute queries if not leader.");
  }
#endif

  AstStorage ast_storage;
  Parameters parameters;
  std::map<std::string, TypedValue> summary;

  utils::Timer parsing_timer;
  auto queries = StripAndParseQuery(query_string, &parameters, &ast_storage,
                                    &db_accessor, params);
  StrippedQuery &stripped_query = queries.first;
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

    return Results(&db_accessor, parameters, plan, output_symbols, header,
                   summary, parsed_query.required_privileges);
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
    StrippedQuery &stripped_query = queries.first;
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
      printed_plan_rows.push_back(std::vector<TypedValue>{row});
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

    return Results(&db_accessor, parameters, plan, output_symbols, header,
                   summary, parsed_query.required_privileges);
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
    StrippedQuery &stripped_query = queries.first;
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
          auto cursor =
              cypher_query_plan->plan().MakeCursor(*context->db_accessor);

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

    return Results(&db_accessor, parameters, plan, output_symbols, header,
                   summary, parsed_query.required_privileges,
                   /* is_profile_query */ true);
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
  } else if (auto *auth_query =
                 utils::Downcast<AuthQuery>(parsed_query.query)) {
#ifdef MG_SINGLE_NODE_HA
    throw utils::NotYetImplemented(
        "Managing user privileges is not yet supported in Memgraph HA "
        "instance.");
#else
    if (in_explicit_transaction) {
      throw UserModificationInMulticommandTxException();
    }
    callback = HandleAuthQuery(auth_query, auth_, parameters, &db_accessor);
#endif
  } else if (auto *stream_query =
                 utils::Downcast<StreamQuery>(parsed_query.query)) {
#ifdef MG_SINGLE_NODE_HA
    throw utils::NotYetImplemented(
        "Graph streams are not yet supported in Memgraph HA instance.");
#else
    if (in_explicit_transaction) {
      throw StreamClauseInMulticommandTxException();
    }
    callback = HandleStreamQuery(stream_query, kafka_streams_, parameters,
                                 &db_accessor);
#endif
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

  return Results(&db_accessor, parameters, plan, output_symbols,
                 callback.header, summary, parsed_query.required_privileges);
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
    frontend::CypherMainVisitor visitor(context, &cached_ast_storage);
    visitor.visit(parser->tree());
    CachedQuery cached_query{std::move(cached_ast_storage), visitor.query(),
                             query::GetRequiredPrivileges(visitor.query())};
    // Cache it.
    ast_it =
        ast_cache_accessor.insert(stripped_query_hash, std::move(cached_query))
            .first;
  }
  ast_storage->properties_ = ast_it->second.ast_storage.properties_;
  ast_storage->labels_ = ast_it->second.ast_storage.labels_;
  ast_storage->edge_types_ = ast_it->second.ast_storage.edge_types_;
  return ParsedQuery{ast_it->second.query->Clone(ast_storage),
                     ast_it->second.required_privileges};
}

std::pair<StrippedQuery, Interpreter::ParsedQuery>
Interpreter::StripAndParseQuery(
    const std::string &query_string, Parameters *parameters,
    AstStorage *ast_storage, database::GraphDbAccessor *db_accessor,
    const std::map<std::string, PropertyValue> &params) {
  StrippedQuery stripped_query(query_string);

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
