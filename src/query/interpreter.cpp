#include "query/interpreter.hpp"

#include <atomic>
#include <limits>

#include "glue/communication.hpp"
#include "query/constants.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/dump.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/required_privileges.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/eval.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/profile.hpp"
#include "query/plan/vertex_count_cache.hpp"
#include "query/trigger.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/csv_parsing.hpp"
#include "utils/event_counter.hpp"
#include "utils/exceptions.hpp"
#include "utils/flag_validation.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/readable_size.hpp"
#include "utils/string.hpp"
#include "utils/tsc.hpp"

namespace EventCounter {
extern Event ReadQuery;
extern Event WriteQuery;
extern Event ReadWriteQuery;

extern const Event LabelIndexCreated;
extern const Event LabelPropertyIndexCreated;
}  // namespace EventCounter

namespace query {

namespace {
void UpdateTypeCount(const plan::ReadWriteTypeChecker::RWType type) {
  switch (type) {
    case plan::ReadWriteTypeChecker::RWType::R:
      EventCounter::IncrementCounter(EventCounter::ReadQuery);
      break;
    case plan::ReadWriteTypeChecker::RWType::W:
      EventCounter::IncrementCounter(EventCounter::WriteQuery);
      break;
    case plan::ReadWriteTypeChecker::RWType::RW:
      EventCounter::IncrementCounter(EventCounter::ReadWriteQuery);
      break;
    default:
      break;
  }
}

struct Callback {
  std::vector<std::string> header;
  std::function<std::vector<std::vector<TypedValue>>()> fn;
  bool should_abort_query{false};
};

TypedValue EvaluateOptionalExpression(Expression *expression, ExpressionEvaluator *eval) {
  return expression ? expression->Accept(*eval) : TypedValue();
}

class ReplQueryHandler final : public query::ReplicationQueryHandler {
 public:
  explicit ReplQueryHandler(storage::Storage *db) : db_(db) {}

  /// @throw QueryRuntimeException if an error ocurred.
  void SetReplicationRole(ReplicationQuery::ReplicationRole replication_role, std::optional<int64_t> port) override {
    if (replication_role == ReplicationQuery::ReplicationRole::MAIN) {
      if (!db_->SetMainReplicationRole()) {
        throw QueryRuntimeException("Couldn't set role to main!");
      }
    }
    if (replication_role == ReplicationQuery::ReplicationRole::REPLICA) {
      if (!port || *port < 0 || *port > std::numeric_limits<uint16_t>::max()) {
        throw QueryRuntimeException("Port number invalid!");
      }
      if (!db_->SetReplicaRole(
              io::network::Endpoint(query::kDefaultReplicationServerIp, static_cast<uint16_t>(*port)))) {
        throw QueryRuntimeException("Couldn't set role to replica!");
      }
    }
  }

  /// @throw QueryRuntimeException if an error ocurred.
  ReplicationQuery::ReplicationRole ShowReplicationRole() const override {
    switch (db_->GetReplicationRole()) {
      case storage::ReplicationRole::MAIN:
        return ReplicationQuery::ReplicationRole::MAIN;
      case storage::ReplicationRole::REPLICA:
        return ReplicationQuery::ReplicationRole::REPLICA;
    }
    throw QueryRuntimeException("Couldn't show replication role - invalid role set!");
  }

  /// @throw QueryRuntimeException if an error ocurred.
  void RegisterReplica(const std::string &name, const std::string &socket_address,
                       const ReplicationQuery::SyncMode sync_mode, const std::optional<double> timeout) override {
    if (db_->GetReplicationRole() == storage::ReplicationRole::REPLICA) {
      // replica can't register another replica
      throw QueryRuntimeException("Replica can't register another replica!");
    }

    storage::replication::ReplicationMode repl_mode;
    switch (sync_mode) {
      case ReplicationQuery::SyncMode::ASYNC: {
        repl_mode = storage::replication::ReplicationMode::ASYNC;
        break;
      }
      case ReplicationQuery::SyncMode::SYNC: {
        repl_mode = storage::replication::ReplicationMode::SYNC;
        break;
      }
    }

    auto maybe_ip_and_port =
        io::network::Endpoint::ParseSocketOrIpAddress(socket_address, query::kDefaultReplicationPort);
    if (maybe_ip_and_port) {
      auto [ip, port] = *maybe_ip_and_port;
      auto ret =
          db_->RegisterReplica(name, {std::move(ip), port}, repl_mode, {.timeout = timeout, .ssl = std::nullopt});
      if (ret.HasError()) {
        throw QueryRuntimeException(fmt::format("Couldn't register replica '{}'!", name));
      }
    } else {
      throw QueryRuntimeException("Invalid socket address!");
    }
  }

  /// @throw QueryRuntimeException if an error ocurred.
  void DropReplica(const std::string &replica_name) override {
    if (db_->GetReplicationRole() == storage::ReplicationRole::REPLICA) {
      // replica can't unregister a replica
      throw QueryRuntimeException("Replica can't unregister a replica!");
    }
    if (!db_->UnregisterReplica(replica_name)) {
      throw QueryRuntimeException(fmt::format("Couldn't unregister the replica '{}'", replica_name));
    }
  }

  using Replica = ReplicationQueryHandler::Replica;
  std::vector<Replica> ShowReplicas() const override {
    if (db_->GetReplicationRole() == storage::ReplicationRole::REPLICA) {
      // replica can't show registered replicas (it shouldn't have any)
      throw QueryRuntimeException("Replica can't show registered replicas (it shouldn't have any)!");
    }

    auto repl_infos = db_->ReplicasInfo();
    std::vector<Replica> replicas;
    replicas.reserve(repl_infos.size());

    const auto from_info = [](const auto &repl_info) -> Replica {
      Replica replica;
      replica.name = repl_info.name;
      replica.socket_address = repl_info.endpoint.SocketAddress();
      switch (repl_info.mode) {
        case storage::replication::ReplicationMode::SYNC:
          replica.sync_mode = ReplicationQuery::SyncMode::SYNC;
          break;
        case storage::replication::ReplicationMode::ASYNC:
          replica.sync_mode = ReplicationQuery::SyncMode::ASYNC;
          break;
      }
      if (repl_info.timeout) {
        replica.timeout = *repl_info.timeout;
      }

      return replica;
    };

    std::transform(repl_infos.begin(), repl_infos.end(), std::back_inserter(replicas), from_info);
    return replicas;
  }

 private:
  storage::Storage *db_;
};
/// returns false if the replication role can't be set
/// @throw QueryRuntimeException if an error ocurred.

Callback HandleAuthQuery(AuthQuery *auth_query, AuthQueryHandler *auth, const Parameters &parameters,
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
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  evaluation_context.parameters = parameters;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, db_accessor, storage::View::OLD);

  std::string username = auth_query->user_;
  std::string rolename = auth_query->role_;
  std::string user_or_role = auth_query->user_or_role_;
  std::vector<AuthQuery::Privilege> privileges = auth_query->privileges_;
  auto password = EvaluateOptionalExpression(auth_query->password_, &evaluator);

  Callback callback;

  switch (auth_query->action_) {
    case AuthQuery::Action::CREATE_USER:
      callback.fn = [auth, username, password] {
        MG_ASSERT(password.IsString() || password.IsNull());
        if (!auth->CreateUser(username, password.IsString() ? std::make_optional(std::string(password.ValueString()))
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
        MG_ASSERT(password.IsString() || password.IsNull());
        auth->SetPassword(username,
                          password.IsString() ? std::make_optional(std::string(password.ValueString())) : std::nullopt);
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
      callback.fn = [auth, user_or_role] { return auth->GetPrivileges(user_or_role); };
      return callback;
    case AuthQuery::Action::SHOW_ROLE_FOR_USER:
      callback.header = {"role"};
      callback.fn = [auth, username] {
        auto maybe_rolename = auth->GetRolenameForUser(username);
        return std::vector<std::vector<TypedValue>>{
            std::vector<TypedValue>{TypedValue(maybe_rolename ? *maybe_rolename : "null")}};
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

Callback HandleReplicationQuery(ReplicationQuery *repl_query, const Parameters &parameters,
                                InterpreterContext *interpreter_context, DbAccessor *db_accessor) {
  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  // TODO: MemoryResource for EvaluationContext, it should probably be passed as
  // the argument to Callback.
  evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  evaluation_context.parameters = parameters;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, db_accessor, storage::View::OLD);

  Callback callback;
  switch (repl_query->action_) {
    case ReplicationQuery::Action::SET_REPLICATION_ROLE: {
      auto port = EvaluateOptionalExpression(repl_query->port_, &evaluator);
      std::optional<int64_t> maybe_port;
      if (port.IsInt()) {
        maybe_port = port.ValueInt();
      }
      callback.fn = [handler = ReplQueryHandler{interpreter_context->db}, role = repl_query->role_,
                     maybe_port]() mutable {
        handler.SetReplicationRole(role, maybe_port);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    }
    case ReplicationQuery::Action::SHOW_REPLICATION_ROLE: {
      callback.header = {"replication mode"};
      callback.fn = [handler = ReplQueryHandler{interpreter_context->db}] {
        auto mode = handler.ShowReplicationRole();
        switch (mode) {
          case ReplicationQuery::ReplicationRole::MAIN: {
            return std::vector<std::vector<TypedValue>>{{TypedValue("main")}};
          }
          case ReplicationQuery::ReplicationRole::REPLICA: {
            return std::vector<std::vector<TypedValue>>{{TypedValue("replica")}};
          }
        }
      };
      return callback;
    }
    case ReplicationQuery::Action::REGISTER_REPLICA: {
      const auto &name = repl_query->replica_name_;
      const auto &sync_mode = repl_query->sync_mode_;
      auto socket_address = repl_query->socket_address_->Accept(evaluator);
      auto timeout = EvaluateOptionalExpression(repl_query->timeout_, &evaluator);
      std::optional<double> maybe_timeout;
      if (timeout.IsDouble()) {
        maybe_timeout = timeout.ValueDouble();
      } else if (timeout.IsInt()) {
        maybe_timeout = static_cast<double>(timeout.ValueInt());
      }
      callback.fn = [handler = ReplQueryHandler{interpreter_context->db}, name, socket_address, sync_mode,
                     maybe_timeout]() mutable {
        handler.RegisterReplica(name, std::string(socket_address.ValueString()), sync_mode, maybe_timeout);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    }
    case ReplicationQuery::Action::DROP_REPLICA: {
      const auto &name = repl_query->replica_name_;
      callback.fn = [handler = ReplQueryHandler{interpreter_context->db}, name]() mutable {
        handler.DropReplica(name);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    }
    case ReplicationQuery::Action::SHOW_REPLICAS: {
      callback.header = {"name", "socket_address", "sync_mode", "timeout"};
      callback.fn = [handler = ReplQueryHandler{interpreter_context->db}, replica_nfields = callback.header.size()] {
        const auto &replicas = handler.ShowReplicas();
        auto typed_replicas = std::vector<std::vector<TypedValue>>{};
        typed_replicas.reserve(replicas.size());
        for (const auto &replica : replicas) {
          std::vector<TypedValue> typed_replica;
          typed_replica.reserve(replica_nfields);

          typed_replica.emplace_back(TypedValue(replica.name));
          typed_replica.emplace_back(TypedValue(replica.socket_address));
          switch (replica.sync_mode) {
            case ReplicationQuery::SyncMode::SYNC:
              typed_replica.emplace_back(TypedValue("sync"));
              break;
            case ReplicationQuery::SyncMode::ASYNC:
              typed_replica.emplace_back(TypedValue("async"));
              break;
          }
          typed_replica.emplace_back(TypedValue(static_cast<int64_t>(replica.sync_mode)));
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
  }
}

// Struct for lazy pulling from a vector
struct PullPlanVector {
  explicit PullPlanVector(std::vector<std::vector<TypedValue>> values) : values_(std::move(values)) {}

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
  explicit PullPlan(std::shared_ptr<CachedPlan> plan, const Parameters &parameters, bool is_profile_query,
                    DbAccessor *dba, InterpreterContext *interpreter_context, utils::MemoryResource *execution_memory,
                    TriggerContextCollector *trigger_context_collector = nullptr,
                    std::optional<size_t> memory_limit = {});
  std::optional<plan::ProfilingStatsWithTotalTime> Pull(AnyStream *stream, std::optional<int> n,
                                                        const std::vector<Symbol> &output_symbols,
                                                        std::map<std::string, TypedValue> *summary);

 private:
  std::shared_ptr<CachedPlan> plan_ = nullptr;
  plan::UniqueCursorPtr cursor_ = nullptr;
  Frame frame_;
  ExecutionContext ctx_;
  std::optional<size_t> memory_limit_;

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

PullPlan::PullPlan(const std::shared_ptr<CachedPlan> plan, const Parameters &parameters, const bool is_profile_query,
                   DbAccessor *dba, InterpreterContext *interpreter_context, utils::MemoryResource *execution_memory,
                   TriggerContextCollector *trigger_context_collector, const std::optional<size_t> memory_limit)
    : plan_(plan),
      cursor_(plan->plan().MakeCursor(execution_memory)),
      frame_(plan->symbol_table().max_position(), execution_memory),
      memory_limit_(memory_limit) {
  ctx_.db_accessor = dba;
  ctx_.symbol_table = plan->symbol_table();
  ctx_.evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  ctx_.evaluation_context.parameters = parameters;
  ctx_.evaluation_context.properties = NamesToProperties(plan->ast_storage().properties_, dba);
  ctx_.evaluation_context.labels = NamesToLabels(plan->ast_storage().labels_, dba);
  if (interpreter_context->execution_timeout_sec > 0) {
    ctx_.timer = utils::AsyncTimer{interpreter_context->execution_timeout_sec};
  }
  ctx_.is_shutting_down = &interpreter_context->is_shutting_down;
  ctx_.is_profile_query = is_profile_query;
  ctx_.trigger_context_collector = trigger_context_collector;
}

std::optional<plan::ProfilingStatsWithTotalTime> PullPlan::Pull(AnyStream *stream, std::optional<int> n,
                                                                const std::vector<Symbol> &output_symbols,
                                                                std::map<std::string, TypedValue> *summary) {
  // Set up temporary memory for a single Pull. Initial memory comes from the
  // stack. 256 KiB should fit on the stack and should be more than enough for a
  // single `Pull`.
  constexpr size_t stack_size = 256 * 1024;
  char stack_data[stack_size];
  utils::ResourceWithOutOfMemoryException resource_with_exception;
  utils::MonotonicBufferResource monotonic_memory(&stack_data[0], stack_size, &resource_with_exception);
  // We can throw on every query because a simple queries for deleting will use only
  // the stack allocated buffer.
  // Also, we want to throw only when the query engine requests more memory and not the storage
  // so we add the exception to the allocator.
  // TODO (mferencevic): Tune the parameters accordingly.
  utils::PoolResource pool_memory(128, 1024, &monotonic_memory);
  std::optional<utils::LimitedMemoryResource> maybe_limited_resource;

  if (memory_limit_) {
    maybe_limited_resource.emplace(&pool_memory, *memory_limit_);
    ctx_.evaluation_context.memory = &*maybe_limited_resource;
  } else {
    ctx_.evaluation_context.memory = &pool_memory;
  }

  // Returns true if a result was pulled.
  const auto pull_result = [&]() -> bool { return cursor_->Pull(frame_, ctx_); };

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
  return GetStatsWithTotalTime(ctx_);
}

using RWType = plan::ReadWriteTypeChecker::RWType;
}  // namespace

InterpreterContext::InterpreterContext(storage::Storage *db, const std::filesystem::path &data_directory) : db(db) {
  auto storage_accessor = db->Access();
  DbAccessor dba{&storage_accessor};
  trigger_store.emplace(data_directory / "triggers", &ast_cache, &dba, &antlr_lock);
}

Interpreter::Interpreter(InterpreterContext *interpreter_context) : interpreter_context_(interpreter_context) {
  MG_ASSERT(interpreter_context_, "Interpreter context must not be NULL");
}

PreparedQuery Interpreter::PrepareTransactionQuery(std::string_view query_upper) {
  std::function<void()> handler;

  if (query_upper == "BEGIN") {
    handler = [this] {
      if (in_explicit_transaction_) {
        throw ExplicitTransactionUsageException("Nested transactions are not supported.");
      }
      in_explicit_transaction_ = true;
      expect_rollback_ = false;

      db_accessor_ =
          std::make_unique<storage::Storage::Accessor>(interpreter_context_->db->Access(GetIsolationLevelOverride()));
      execution_db_accessor_.emplace(db_accessor_.get());

      if (interpreter_context_->trigger_store->HasTriggers()) {
        trigger_context_collector_.emplace(interpreter_context_->trigger_store->GetEventTypes());
      }
    };
  } else if (query_upper == "COMMIT") {
    handler = [this] {
      if (!in_explicit_transaction_) {
        throw ExplicitTransactionUsageException("No current transaction to commit.");
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
        throw ExplicitTransactionUsageException("No current transaction to rollback.");
      }
      Abort();
      expect_rollback_ = false;
      in_explicit_transaction_ = false;
    };
  } else {
    LOG_FATAL("Should not get here -- unknown transaction query!");
  }

  return {{},
          {},
          [handler = std::move(handler)](AnyStream *, std::optional<int>) {
            handler();
            return QueryHandlerResult::NOTHING;
          },
          RWType::NONE};
}

PreparedQuery PrepareCypherQuery(ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
                                 InterpreterContext *interpreter_context, DbAccessor *dba,
                                 utils::MemoryResource *execution_memory,
                                 TriggerContextCollector *trigger_context_collector = nullptr) {
  auto *cypher_query = utils::Downcast<CypherQuery>(parsed_query.query);

  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  evaluation_context.parameters = parsed_query.parameters;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, dba, storage::View::OLD);
  const auto memory_limit = EvaluateMemoryLimit(&evaluator, cypher_query->memory_limit_, cypher_query->memory_scale_);
  if (memory_limit) {
    spdlog::info("Running query with memory limit of {}", utils::GetReadableSize(*memory_limit));
  }

  auto plan = CypherQueryToPlan(parsed_query.stripped_query.hash(), std::move(parsed_query.ast_storage), cypher_query,
                                parsed_query.parameters,
                                parsed_query.is_cacheable ? &interpreter_context->plan_cache : nullptr, dba);

  summary->insert_or_assign("cost_estimate", plan->cost());
  auto rw_type_checker = plan::ReadWriteTypeChecker();
  rw_type_checker.InferRWType(const_cast<plan::LogicalOperator &>(plan->plan()));

  auto output_symbols = plan->plan().OutputSymbols(plan->symbol_table());

  std::vector<std::string> header;
  header.reserve(output_symbols.size());

  for (const auto &symbol : output_symbols) {
    // When the symbol is aliased or expanded from '*' (inside RETURN or
    // WITH), then there is no token position, so use symbol name.
    // Otherwise, find the name from stripped query.
    header.push_back(
        utils::FindOr(parsed_query.stripped_query.named_expressions(), symbol.token_position(), symbol.name()).first);
  }

  auto pull_plan = std::make_shared<PullPlan>(plan, parsed_query.parameters, false, dba, interpreter_context,
                                              execution_memory, trigger_context_collector, memory_limit);
  return PreparedQuery{std::move(header), std::move(parsed_query.required_privileges),
                       [pull_plan = std::move(pull_plan), output_symbols = std::move(output_symbols), summary](
                           AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
                         if (pull_plan->Pull(stream, n, output_symbols, summary)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       rw_type_checker.type};
}

PreparedQuery PrepareExplainQuery(ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
                                  InterpreterContext *interpreter_context, DbAccessor *dba,
                                  utils::MemoryResource *execution_memory) {
  const std::string kExplainQueryStart = "explain ";
  MG_ASSERT(utils::StartsWith(utils::ToLowerCase(parsed_query.stripped_query.query()), kExplainQueryStart),
            "Expected stripped query to start with '{}'", kExplainQueryStart);

  // Parse and cache the inner query separately (as if it was a standalone
  // query), producing a fresh AST. Note that currently we cannot just reuse
  // part of the already produced AST because the parameters within ASTs are
  // looked up using their positions within the string that was parsed. These
  // wouldn't match up if if we were to reuse the AST (produced by parsing the
  // full query string) when given just the inner query to execute.
  ParsedQuery parsed_inner_query =
      ParseQuery(parsed_query.query_string.substr(kExplainQueryStart.size()), parsed_query.user_parameters,
                 &interpreter_context->ast_cache, &interpreter_context->antlr_lock);

  auto *cypher_query = utils::Downcast<CypherQuery>(parsed_inner_query.query);
  MG_ASSERT(cypher_query, "Cypher grammar should not allow other queries in EXPLAIN");

  auto cypher_query_plan = CypherQueryToPlan(
      parsed_inner_query.stripped_query.hash(), std::move(parsed_inner_query.ast_storage), cypher_query,
      parsed_inner_query.parameters, parsed_inner_query.is_cacheable ? &interpreter_context->plan_cache : nullptr, dba);

  std::stringstream printed_plan;
  plan::PrettyPrint(*dba, &cypher_query_plan->plan(), &printed_plan);

  std::vector<std::vector<TypedValue>> printed_plan_rows;
  for (const auto &row : utils::Split(utils::RTrim(printed_plan.str()), "\n")) {
    printed_plan_rows.push_back(std::vector<TypedValue>{TypedValue(row)});
  }

  summary->insert_or_assign("explain", plan::PlanToJson(*dba, &cypher_query_plan->plan()).dump());

  return PreparedQuery{{"QUERY PLAN"},
                       std::move(parsed_query.required_privileges),
                       [pull_plan = std::make_shared<PullPlanVector>(std::move(printed_plan_rows))](
                           AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareProfileQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                  std::map<std::string, TypedValue> *summary, InterpreterContext *interpreter_context,
                                  DbAccessor *dba, utils::MemoryResource *execution_memory) {
  const std::string kProfileQueryStart = "profile ";

  MG_ASSERT(utils::StartsWith(utils::ToLowerCase(parsed_query.stripped_query.query()), kProfileQueryStart),
            "Expected stripped query to start with '{}'", kProfileQueryStart);

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
      ParseQuery(parsed_query.query_string.substr(kProfileQueryStart.size()), parsed_query.user_parameters,
                 &interpreter_context->ast_cache, &interpreter_context->antlr_lock);

  auto *cypher_query = utils::Downcast<CypherQuery>(parsed_inner_query.query);
  MG_ASSERT(cypher_query, "Cypher grammar should not allow other queries in PROFILE");
  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  evaluation_context.parameters = parsed_inner_query.parameters;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, dba, storage::View::OLD);
  const auto memory_limit = EvaluateMemoryLimit(&evaluator, cypher_query->memory_limit_, cypher_query->memory_scale_);

  auto cypher_query_plan = CypherQueryToPlan(
      parsed_inner_query.stripped_query.hash(), std::move(parsed_inner_query.ast_storage), cypher_query,
      parsed_inner_query.parameters, parsed_inner_query.is_cacheable ? &interpreter_context->plan_cache : nullptr, dba);
  auto rw_type_checker = plan::ReadWriteTypeChecker();
  rw_type_checker.InferRWType(const_cast<plan::LogicalOperator &>(cypher_query_plan->plan()));

  return PreparedQuery{{"OPERATOR", "ACTUAL HITS", "RELATIVE TIME", "ABSOLUTE TIME"},
                       std::move(parsed_query.required_privileges),
                       [plan = std::move(cypher_query_plan), parameters = std::move(parsed_inner_query.parameters),
                        summary, dba, interpreter_context, execution_memory, memory_limit,
                        // We want to execute the query we are profiling lazily, so we delay
                        // the construction of the corresponding context.
                        stats_and_total_time = std::optional<plan::ProfilingStatsWithTotalTime>{},
                        pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         // No output symbols are given so that nothing is streamed.
                         if (!stats_and_total_time) {
                           stats_and_total_time = PullPlan(plan, parameters, true, dba, interpreter_context,
                                                           execution_memory, nullptr, memory_limit)
                                                      .Pull(stream, {}, {}, summary);
                           pull_plan = std::make_shared<PullPlanVector>(ProfilingStatsToTable(*stats_and_total_time));
                         }

                         MG_ASSERT(stats_and_total_time, "Failed to execute the query!");

                         if (pull_plan->Pull(stream, n)) {
                           summary->insert_or_assign("profile", ProfilingStatsToJson(*stats_and_total_time).dump());
                           return QueryHandlerResult::ABORT;
                         }

                         return std::nullopt;
                       },
                       rw_type_checker.type};
}

PreparedQuery PrepareDumpQuery(ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary, DbAccessor *dba,
                               utils::MemoryResource *execution_memory) {
  return PreparedQuery{{"QUERY"},
                       std::move(parsed_query.required_privileges),
                       [pull_plan = std::make_shared<PullPlanDump>(dba)](
                           AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::R};
}

PreparedQuery PrepareIndexQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                std::map<std::string, TypedValue> *summary, InterpreterContext *interpreter_context,
                                utils::MemoryResource *execution_memory) {
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
          EventCounter::IncrementCounter(EventCounter::LabelIndexCreated);
        } else {
          MG_ASSERT(properties.size() == 1U);
          interpreter_context->db->CreateIndex(label, properties[0]);
          EventCounter::IncrementCounter(EventCounter::LabelPropertyIndexCreated);
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
          MG_ASSERT(properties.size() == 1U);
          interpreter_context->db->DropIndex(label, properties[0]);
        }
        invalidate_plan_cache();
      };
      break;
    }
  }

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [handler = std::move(handler)](AnyStream *stream, std::optional<int>) {
                         handler();
                         return QueryHandlerResult::NOTHING;
                       },
                       RWType::W};
}

PreparedQuery PrepareAuthQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                               std::map<std::string, TypedValue> *summary, InterpreterContext *interpreter_context,
                               DbAccessor *dba, utils::MemoryResource *execution_memory) {
  if (in_explicit_transaction) {
    throw UserModificationInMulticommandTxException();
  }

  auto *auth_query = utils::Downcast<AuthQuery>(parsed_query.query);

  auto callback = HandleAuthQuery(auth_query, interpreter_context->auth, parsed_query.parameters, dba);

  SymbolTable symbol_table;
  std::vector<Symbol> output_symbols;
  for (const auto &column : callback.header) {
    output_symbols.emplace_back(symbol_table.CreateSymbol(column, "false"));
  }

  auto plan = std::make_shared<CachedPlan>(std::make_unique<SingleNodeLogicalPlan>(
      std::make_unique<plan::OutputTable>(output_symbols,
                                          [fn = callback.fn](Frame *, ExecutionContext *) { return fn(); }),
      0.0, AstStorage{}, symbol_table));

  auto pull_plan =
      std::make_shared<PullPlan>(plan, parsed_query.parameters, false, dba, interpreter_context, execution_memory);
  return PreparedQuery{
      callback.header, std::move(parsed_query.required_privileges),
      [pull_plan = std::move(pull_plan), callback = std::move(callback), output_symbols = std::move(output_symbols),
       summary](AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
        if (pull_plan->Pull(stream, n, output_symbols, summary)) {
          return callback.should_abort_query ? QueryHandlerResult::ABORT : QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      },
      RWType::NONE};
}

PreparedQuery PrepareReplicationQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                      InterpreterContext *interpreter_context, DbAccessor *dba) {
  if (in_explicit_transaction) {
    throw ReplicationModificationInMulticommandTxException();
  }

  auto *replication_query = utils::Downcast<ReplicationQuery>(parsed_query.query);
  auto callback = HandleReplicationQuery(replication_query, parsed_query.parameters, interpreter_context, dba);

  return PreparedQuery{callback.header, std::move(parsed_query.required_privileges),
                       [callback_fn = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>{nullptr}](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (UNLIKELY(!pull_plan)) {
                           pull_plan = std::make_shared<PullPlanVector>(callback_fn());
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
  // False positive report for the std::make_shared above
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
}

PreparedQuery PrepareLockPathQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                   InterpreterContext *interpreter_context, DbAccessor *dba) {
  if (in_explicit_transaction) {
    throw LockPathModificationInMulticommandTxException();
  }

  auto *lock_path_query = utils::Downcast<LockPathQuery>(parsed_query.query);

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [interpreter_context, action = lock_path_query->action_](
                           AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
                         switch (action) {
                           case LockPathQuery::Action::LOCK_PATH:
                             if (!interpreter_context->db->LockPath()) {
                               throw QueryRuntimeException("Failed to lock the data directory");
                             }
                             break;
                           case LockPathQuery::Action::UNLOCK_PATH:
                             if (!interpreter_context->db->UnlockPath()) {
                               throw QueryRuntimeException("Failed to unlock the data directory");
                             }
                             break;
                         }
                         return QueryHandlerResult::COMMIT;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareFreeMemoryQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                     InterpreterContext *interpreter_context) {
  if (in_explicit_transaction) {
    throw FreeMemoryModificationInMulticommandTxException();
  }

  return PreparedQuery{
      {},
      std::move(parsed_query.required_privileges),
      [interpreter_context](AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
        interpreter_context->db->FreeMemory();
        return QueryHandlerResult::COMMIT;
      },
      RWType::NONE};
}

TriggerEventType ToTriggerEventType(const TriggerQuery::EventType event_type) {
  switch (event_type) {
    case TriggerQuery::EventType::ANY:
      return TriggerEventType::ANY;

    case TriggerQuery::EventType::CREATE:
      return TriggerEventType::CREATE;

    case TriggerQuery::EventType::VERTEX_CREATE:
      return TriggerEventType::VERTEX_CREATE;

    case TriggerQuery::EventType::EDGE_CREATE:
      return TriggerEventType::EDGE_CREATE;

    case TriggerQuery::EventType::DELETE:
      return TriggerEventType::DELETE;

    case TriggerQuery::EventType::VERTEX_DELETE:
      return TriggerEventType::VERTEX_DELETE;

    case TriggerQuery::EventType::EDGE_DELETE:
      return TriggerEventType::EDGE_DELETE;

    case TriggerQuery::EventType::UPDATE:
      return TriggerEventType::UPDATE;

    case TriggerQuery::EventType::VERTEX_UPDATE:
      return TriggerEventType::VERTEX_UPDATE;

    case TriggerQuery::EventType::EDGE_UPDATE:
      return TriggerEventType::EDGE_UPDATE;
  }
}

Callback CreateTrigger(TriggerQuery *trigger_query,
                       const std::map<std::string, storage::PropertyValue> &user_parameters,
                       InterpreterContext *interpreter_context, DbAccessor *dba) {
  return {
      {},
      [trigger_name = std::move(trigger_query->trigger_name_), trigger_statement = std::move(trigger_query->statement_),
       event_type = trigger_query->event_type_, before_commit = trigger_query->before_commit_, interpreter_context, dba,
       user_parameters]() -> std::vector<std::vector<TypedValue>> {
        interpreter_context->trigger_store->AddTrigger(
            trigger_name, trigger_statement, user_parameters, ToTriggerEventType(event_type),
            before_commit ? TriggerPhase::BEFORE_COMMIT : TriggerPhase::AFTER_COMMIT, &interpreter_context->ast_cache,
            dba, &interpreter_context->antlr_lock);
        return {};
      }};
}

Callback DropTrigger(TriggerQuery *trigger_query, InterpreterContext *interpreter_context) {
  return {{},
          [trigger_name = std::move(trigger_query->trigger_name_),
           interpreter_context]() -> std::vector<std::vector<TypedValue>> {
            interpreter_context->trigger_store->DropTrigger(trigger_name);
            return {};
          }};
}

Callback ShowTriggers(InterpreterContext *interpreter_context) {
  return {{"trigger name", "statement", "event type", "phase"}, [interpreter_context] {
            std::vector<std::vector<TypedValue>> results;
            auto trigger_infos = interpreter_context->trigger_store->GetTriggerInfo();
            results.reserve(trigger_infos.size());
            for (auto &trigger_info : trigger_infos) {
              std::vector<TypedValue> typed_trigger_info;
              typed_trigger_info.reserve(4);
              typed_trigger_info.emplace_back(std::move(trigger_info.name));
              typed_trigger_info.emplace_back(std::move(trigger_info.statement));
              typed_trigger_info.emplace_back(TriggerEventTypeToString(trigger_info.event_type));
              typed_trigger_info.emplace_back(trigger_info.phase == TriggerPhase::BEFORE_COMMIT ? "BEFORE COMMIT"
                                                                                                : "AFTER COMMIT");
              results.push_back(std::move(typed_trigger_info));
            }

            return results;
          }};
}

PreparedQuery PrepareTriggerQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                  InterpreterContext *interpreter_context, DbAccessor *dba,
                                  const std::map<std::string, storage::PropertyValue> &user_parameters) {
  if (in_explicit_transaction) {
    throw TriggerModificationInMulticommandTxException();
  }

  auto *trigger_query = utils::Downcast<TriggerQuery>(parsed_query.query);
  MG_ASSERT(trigger_query);

  auto callback = [trigger_query, interpreter_context, dba, &user_parameters] {
    switch (trigger_query->action_) {
      case TriggerQuery::Action::CREATE_TRIGGER:
        return CreateTrigger(trigger_query, user_parameters, interpreter_context, dba);
      case TriggerQuery::Action::DROP_TRIGGER:
        return DropTrigger(trigger_query, interpreter_context);
      case TriggerQuery::Action::SHOW_TRIGGERS:
        return ShowTriggers(interpreter_context);
    }
  }();

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
                       [callback_fn = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>{nullptr}](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (UNLIKELY(!pull_plan)) {
                           pull_plan = std::make_shared<PullPlanVector>(callback_fn());
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
  // False positive report for the std::make_shared above
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
}

constexpr auto ToStorageIsolationLevel(const IsolationLevelQuery::IsolationLevel isolation_level) noexcept {
  switch (isolation_level) {
    case IsolationLevelQuery::IsolationLevel::SNAPSHOT_ISOLATION:
      return storage::IsolationLevel::SNAPSHOT_ISOLATION;
    case IsolationLevelQuery::IsolationLevel::READ_COMMITTED:
      return storage::IsolationLevel::READ_COMMITTED;
    case IsolationLevelQuery::IsolationLevel::READ_UNCOMMITTED:
      return storage::IsolationLevel::READ_UNCOMMITTED;
  }
}

PreparedQuery PrepareIsolationLevelQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                         InterpreterContext *interpreter_context, Interpreter *interpreter) {
  if (in_explicit_transaction) {
    throw IsolationLevelModificationInMulticommandTxException();
  }

  auto *isolation_level_query = utils::Downcast<IsolationLevelQuery>(parsed_query.query);
  MG_ASSERT(isolation_level_query);

  const auto isolation_level = ToStorageIsolationLevel(isolation_level_query->isolation_level_);

  auto callback = [isolation_level_query, isolation_level, interpreter_context,
                   interpreter]() -> std::function<void()> {
    switch (isolation_level_query->isolation_level_scope_) {
      case IsolationLevelQuery::IsolationLevelScope::GLOBAL:
        return [interpreter_context, isolation_level] { interpreter_context->db->SetIsolationLevel(isolation_level); };
      case IsolationLevelQuery::IsolationLevelScope::SESSION:
        return [interpreter, isolation_level] { interpreter->SetSessionIsolationLevel(isolation_level); };
      case IsolationLevelQuery::IsolationLevelScope::NEXT:
        return [interpreter, isolation_level] { interpreter->SetNextTransactionIsolationLevel(isolation_level); };
    }
  }();

  return PreparedQuery{
      {},
      std::move(parsed_query.required_privileges),
      [callback = std::move(callback)](AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
        callback();
        return QueryHandlerResult::COMMIT;
      },
      RWType::NONE};
}

PreparedQuery PrepareInfoQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                               std::map<std::string, TypedValue> *summary, InterpreterContext *interpreter_context,
                               storage::Storage *db, utils::MemoryResource *execution_memory) {
  if (in_explicit_transaction) {
    throw InfoInMulticommandTxException();
  }

  auto *info_query = utils::Downcast<InfoQuery>(parsed_query.query);
  std::vector<std::string> header;
  std::function<std::pair<std::vector<std::vector<TypedValue>>, QueryHandlerResult>()> handler;

  switch (info_query->info_type_) {
    case InfoQuery::InfoType::STORAGE:
      header = {"storage info", "value"};
      handler = [db] {
        auto info = db->GetInfo();
        std::vector<std::vector<TypedValue>> results{
            {TypedValue("vertex_count"), TypedValue(static_cast<int64_t>(info.vertex_count))},
            {TypedValue("edge_count"), TypedValue(static_cast<int64_t>(info.edge_count))},
            {TypedValue("average_degree"), TypedValue(info.average_degree)},
            {TypedValue("memory_usage"), TypedValue(static_cast<int64_t>(info.memory_usage))},
            {TypedValue("disk_usage"), TypedValue(static_cast<int64_t>(info.disk_usage))},
            {TypedValue("memory_allocated"), TypedValue(static_cast<int64_t>(utils::total_memory_tracker.Amount()))},
            {TypedValue("allocation_limit"),
             TypedValue(static_cast<int64_t>(utils::total_memory_tracker.HardLimit()))}};
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
          results.push_back({TypedValue("label"), TypedValue(db->LabelToName(item)), TypedValue()});
        }
        for (const auto &item : info.label_property) {
          results.push_back({TypedValue("label+property"), TypedValue(db->LabelToName(item.first)),
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
          results.push_back({TypedValue("exists"), TypedValue(db->LabelToName(item.first)),
                             TypedValue(db->PropertyToName(item.second))});
        }
        for (const auto &item : info.unique) {
          std::vector<TypedValue> properties;
          properties.reserve(item.second.size());
          for (const auto &property : item.second) {
            properties.emplace_back(db->PropertyToName(property));
          }
          results.push_back(
              {TypedValue("unique"), TypedValue(db->LabelToName(item.first)), TypedValue(std::move(properties))});
        }
        return std::pair{results, QueryHandlerResult::NOTHING};
      };
      break;
  }

  return PreparedQuery{std::move(header), std::move(parsed_query.required_privileges),
                       [handler = std::move(handler), action = QueryHandlerResult::NOTHING,
                        pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (!pull_plan) {
                           auto [results, action_on_complete] = handler();
                           action = action_on_complete;
                           pull_plan = std::make_shared<PullPlanVector>(std::move(results));
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return action;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareConstraintQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                     std::map<std::string, TypedValue> *summary,
                                     InterpreterContext *interpreter_context, utils::MemoryResource *execution_memory) {
  if (in_explicit_transaction) {
    throw ConstraintInMulticommandTxException();
  }

  auto *constraint_query = utils::Downcast<ConstraintQuery>(parsed_query.query);
  std::function<void()> handler;

  auto label = interpreter_context->db->NameToLabel(constraint_query->constraint_.label.name);
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
            throw SyntaxException("Exactly one property must be used for existence constraints.");
          }
          handler = [interpreter_context, label, properties = std::move(properties)] {
            auto res = interpreter_context->db->CreateExistenceConstraint(label, properties[0]);
            if (res.HasError()) {
              auto violation = res.GetError();
              auto label_name = interpreter_context->db->LabelToName(violation.label);
              MG_ASSERT(violation.properties.size() == 1U);
              auto property_name = interpreter_context->db->PropertyToName(*violation.properties.begin());
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
            throw SyntaxException("The given set of properties contains duplicates.");
          }
          handler = [interpreter_context, label, property_set = std::move(property_set)] {
            auto res = interpreter_context->db->CreateUniqueConstraint(label, property_set);
            if (res.HasError()) {
              auto violation = res.GetError();
              auto label_name = interpreter_context->db->LabelToName(violation.label);
              std::stringstream property_names_stream;
              utils::PrintIterable(property_names_stream, violation.properties, ", ",
                                   [&interpreter_context](auto &stream, const auto &prop) {
                                     stream << interpreter_context->db->PropertyToName(prop);
                                   });
              throw QueryRuntimeException(
                  "Unable to create unique constraint :{}({}), because an "
                  "existing node violates it.",
                  label_name, property_names_stream.str());
            } else {
              switch (res.GetValue()) {
                case storage::UniqueConstraints::CreationStatus::EMPTY_PROPERTIES:
                  throw SyntaxException(
                      "At least one property must be used for unique "
                      "constraints.");
                  break;
                case storage::UniqueConstraints::CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED:
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
            throw SyntaxException("Exactly one property must be used for existence constraints.");
          }
          handler = [interpreter_context, label, properties = std::move(properties)] {
            interpreter_context->db->DropExistenceConstraint(label, properties[0]);
            return std::vector<std::vector<TypedValue>>();
          };
          break;
        case Constraint::Type::UNIQUE:
          std::set<storage::PropertyId> property_set;
          for (const auto &property : properties) {
            property_set.insert(property);
          }
          if (property_set.size() != properties.size()) {
            throw SyntaxException("The given set of properties contains duplicates.");
          }
          handler = [interpreter_context, label, property_set = std::move(property_set)] {
            auto res = interpreter_context->db->DropUniqueConstraint(label, property_set);
            switch (res) {
              case storage::UniqueConstraints::DeletionStatus::EMPTY_PROPERTIES:
                throw SyntaxException(
                    "At least one property must be used for unique "
                    "constraints.");
                break;
              case storage::UniqueConstraints::DeletionStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED:
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

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [handler = std::move(handler)](AnyStream *stream, std::optional<int> n) {
                         handler();
                         return QueryHandlerResult::COMMIT;
                       },
                       RWType::NONE};
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

Interpreter::PrepareResult Interpreter::Prepare(const std::string &query_string,
                                                const std::map<std::string, storage::PropertyValue> &params) {
  if (!in_explicit_transaction_) {
    query_executions_.clear();
  }

  query_executions_.emplace_back(std::make_unique<QueryExecution>());
  auto &query_execution = query_executions_.back();
  std::optional<int> qid =
      in_explicit_transaction_ ? static_cast<int>(query_executions_.size() - 1) : std::optional<int>{};

  // Handle transaction control queries.

  const auto upper_case_query = utils::ToUpperCase(query_string);
  const auto trimmed_query = utils::Trim(upper_case_query);

  if (trimmed_query == "BEGIN" || trimmed_query == "COMMIT" || trimmed_query == "ROLLBACK") {
    query_execution->prepared_query.emplace(PrepareTransactionQuery(trimmed_query));
    return {query_execution->prepared_query->header, query_execution->prepared_query->privileges, qid};
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
    // Set a default cost estimate of 0. Individual queries can overwrite this
    // field with an improved estimate.
    query_execution->summary["cost_estimate"] = 0.0;

    utils::Timer parsing_timer;
    ParsedQuery parsed_query =
        ParseQuery(query_string, params, &interpreter_context_->ast_cache, &interpreter_context_->antlr_lock);
    query_execution->summary["parsing_time"] = parsing_timer.Elapsed().count();

    // Some queries require an active transaction in order to be prepared.
    if (!in_explicit_transaction_ &&
        (utils::Downcast<CypherQuery>(parsed_query.query) || utils::Downcast<ExplainQuery>(parsed_query.query) ||
         utils::Downcast<ProfileQuery>(parsed_query.query) || utils::Downcast<DumpQuery>(parsed_query.query) ||
         utils::Downcast<TriggerQuery>(parsed_query.query))) {
      db_accessor_ =
          std::make_unique<storage::Storage::Accessor>(interpreter_context_->db->Access(GetIsolationLevelOverride()));
      execution_db_accessor_.emplace(db_accessor_.get());

      if (utils::Downcast<CypherQuery>(parsed_query.query) && interpreter_context_->trigger_store->HasTriggers()) {
        trigger_context_collector_.emplace(interpreter_context_->trigger_store->GetEventTypes());
      }
    }

    utils::Timer planning_timer;
    PreparedQuery prepared_query;

    if (utils::Downcast<CypherQuery>(parsed_query.query)) {
      prepared_query = PrepareCypherQuery(std::move(parsed_query), &query_execution->summary, interpreter_context_,
                                          &*execution_db_accessor_, &query_execution->execution_memory,
                                          trigger_context_collector_ ? &*trigger_context_collector_ : nullptr);
    } else if (utils::Downcast<ExplainQuery>(parsed_query.query)) {
      prepared_query = PrepareExplainQuery(std::move(parsed_query), &query_execution->summary, interpreter_context_,
                                           &*execution_db_accessor_, &query_execution->execution_memory);
    } else if (utils::Downcast<ProfileQuery>(parsed_query.query)) {
      prepared_query =
          PrepareProfileQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->summary,
                              interpreter_context_, &*execution_db_accessor_, &query_execution->execution_memory);
    } else if (utils::Downcast<DumpQuery>(parsed_query.query)) {
      prepared_query = PrepareDumpQuery(std::move(parsed_query), &query_execution->summary, &*execution_db_accessor_,
                                        &query_execution->execution_memory);
    } else if (utils::Downcast<IndexQuery>(parsed_query.query)) {
      prepared_query = PrepareIndexQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->summary,
                                         interpreter_context_, &query_execution->execution_memory);
    } else if (utils::Downcast<AuthQuery>(parsed_query.query)) {
      prepared_query =
          PrepareAuthQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->summary,
                           interpreter_context_, &*execution_db_accessor_, &query_execution->execution_memory);
    } else if (utils::Downcast<InfoQuery>(parsed_query.query)) {
      prepared_query =
          PrepareInfoQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->summary,
                           interpreter_context_, interpreter_context_->db, &query_execution->execution_memory);
    } else if (utils::Downcast<ConstraintQuery>(parsed_query.query)) {
      prepared_query =
          PrepareConstraintQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->summary,
                                 interpreter_context_, &query_execution->execution_memory);
    } else if (utils::Downcast<ReplicationQuery>(parsed_query.query)) {
      prepared_query = PrepareReplicationQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_,
                                               &*execution_db_accessor_);
    } else if (utils::Downcast<LockPathQuery>(parsed_query.query)) {
      prepared_query = PrepareLockPathQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_,
                                            &*execution_db_accessor_);
    } else if (utils::Downcast<FreeMemoryQuery>(parsed_query.query)) {
      prepared_query = PrepareFreeMemoryQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_);
    } else if (utils::Downcast<TriggerQuery>(parsed_query.query)) {
      prepared_query = PrepareTriggerQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_,
                                           &*execution_db_accessor_, params);
    } else if (utils::Downcast<IsolationLevelQuery>(parsed_query.query)) {
      prepared_query =
          PrepareIsolationLevelQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_, this);
    } else {
      LOG_FATAL("Should not get here -- unknown query type!");
    }

    query_execution->summary["planning_time"] = planning_timer.Elapsed().count();
    query_execution->prepared_query.emplace(std::move(prepared_query));

    const auto rw_type = query_execution->prepared_query->rw_type;
    query_execution->summary["type"] = plan::ReadWriteTypeChecker::TypeToString(rw_type);

    UpdateTypeCount(rw_type);

    if (const auto query_type = query_execution->prepared_query->rw_type;
        interpreter_context_->db->GetReplicationRole() == storage::ReplicationRole::REPLICA &&
        (query_type == RWType::W || query_type == RWType::RW)) {
      query_execution = nullptr;
      throw QueryException("Write query forbidden on the replica!");
    }

    return {query_execution->prepared_query->header, query_execution->prepared_query->privileges, qid};
  } catch (const utils::BasicException &) {
    EventCounter::IncrementCounter(EventCounter::FailedQuery);
    AbortCommand(&query_execution);
    throw;
  }
}

void Interpreter::Abort() {
  expect_rollback_ = false;
  in_explicit_transaction_ = false;
  if (!db_accessor_) return;
  db_accessor_->Abort();
  execution_db_accessor_.reset();
  db_accessor_.reset();
  trigger_context_collector_.reset();
}

namespace {
void RunTriggersIndividually(const utils::SkipList<Trigger> &triggers, InterpreterContext *interpreter_context,
                             TriggerContext trigger_context) {
  // Run the triggers
  for (const auto &trigger : triggers.access()) {
    utils::MonotonicBufferResource execution_memory{kExecutionMemoryBlockSize};

    // create a new transaction for each trigger
    auto storage_acc = interpreter_context->db->Access();
    DbAccessor db_accessor{&storage_acc};

    trigger_context.AdaptForAccessor(&db_accessor);
    try {
      trigger.Execute(&db_accessor, &execution_memory, interpreter_context->execution_timeout_sec,
                      &interpreter_context->is_shutting_down, trigger_context);
    } catch (const utils::BasicException &exception) {
      spdlog::warn("Trigger '{}' failed with exception:\n{}", trigger.Name(), exception.what());
      db_accessor.Abort();
      continue;
    }

    auto maybe_constraint_violation = db_accessor.Commit();
    if (maybe_constraint_violation.HasError()) {
      const auto &constraint_violation = maybe_constraint_violation.GetError();
      switch (constraint_violation.type) {
        case storage::ConstraintViolation::Type::EXISTENCE: {
          const auto &label_name = db_accessor.LabelToName(constraint_violation.label);
          MG_ASSERT(constraint_violation.properties.size() == 1U);
          const auto &property_name = db_accessor.PropertyToName(*constraint_violation.properties.begin());
          spdlog::warn("Trigger '{}' failed to commit due to existence constraint violation on :{}({})", trigger.Name(),
                       label_name, property_name);
          break;
        }
        case storage::ConstraintViolation::Type::UNIQUE: {
          const auto &label_name = db_accessor.LabelToName(constraint_violation.label);
          std::stringstream property_names_stream;
          utils::PrintIterable(property_names_stream, constraint_violation.properties, ", ",
                               [&](auto &stream, const auto &prop) { stream << db_accessor.PropertyToName(prop); });
          spdlog::warn("Trigger '{}' failed to commit due to unique constraint violation on :{}({})", trigger.Name(),
                       label_name, property_names_stream.str());
          break;
        }
      }
    }
  }
}
}  // namespace

void Interpreter::Commit() {
  // It's possible that some queries did not finish because the user did
  // not pull all of the results from the query.
  // For now, we will not check if there are some unfinished queries.
  // We should document clearly that all results should be pulled to complete
  // a query.
  if (!db_accessor_) return;

  std::optional<TriggerContext> trigger_context = std::nullopt;
  if (trigger_context_collector_) {
    trigger_context.emplace(std::move(*trigger_context_collector_).TransformToTriggerContext());
    trigger_context_collector_.reset();
  }

  if (trigger_context) {
    // Run the triggers
    for (const auto &trigger : interpreter_context_->trigger_store->BeforeCommitTriggers().access()) {
      utils::MonotonicBufferResource execution_memory{kExecutionMemoryBlockSize};
      AdvanceCommand();
      try {
        trigger.Execute(&*execution_db_accessor_, &execution_memory, interpreter_context_->execution_timeout_sec,
                        &interpreter_context_->is_shutting_down, *trigger_context);
      } catch (const utils::BasicException &e) {
        throw utils::BasicException(
            fmt::format("Trigger '{}' caused the transaction to fail.\nException: {}", trigger.Name(), e.what()));
      }
    }
    SPDLOG_DEBUG("Finished executing before commit triggers");
  }

  const auto reset_necessary_members = [this]() {
    execution_db_accessor_.reset();
    db_accessor_.reset();
    trigger_context_collector_.reset();
  };

  auto maybe_constraint_violation = db_accessor_->Commit();
  if (maybe_constraint_violation.HasError()) {
    const auto &constraint_violation = maybe_constraint_violation.GetError();
    switch (constraint_violation.type) {
      case storage::ConstraintViolation::Type::EXISTENCE: {
        auto label_name = execution_db_accessor_->LabelToName(constraint_violation.label);
        MG_ASSERT(constraint_violation.properties.size() == 1U);
        auto property_name = execution_db_accessor_->PropertyToName(*constraint_violation.properties.begin());
        reset_necessary_members();
        throw QueryException("Unable to commit due to existence constraint violation on :{}({})", label_name,
                             property_name);
        break;
      }
      case storage::ConstraintViolation::Type::UNIQUE: {
        auto label_name = execution_db_accessor_->LabelToName(constraint_violation.label);
        std::stringstream property_names_stream;
        utils::PrintIterable(
            property_names_stream, constraint_violation.properties, ", ",
            [this](auto &stream, const auto &prop) { stream << execution_db_accessor_->PropertyToName(prop); });
        reset_necessary_members();
        throw QueryException("Unable to commit due to unique constraint violation on :{}({})", label_name,
                             property_names_stream.str());
        break;
      }
    }
  }

  // The ordered execution of after commit triggers is heavily depending on the exclusiveness of db_accessor_->Commit():
  // only one of the transactions can be commiting at the same time, so when the commit is finished, that transaction
  // probably will schedule its after commit triggers, because the other transactions that want to commit are still
  // waiting for commiting or one of them just started commiting its changes.
  // This means the ordered execution of after commit triggers are not guaranteed.
  if (trigger_context && interpreter_context_->trigger_store->AfterCommitTriggers().size() > 0) {
    interpreter_context_->after_commit_trigger_pool.AddTask(
        [trigger_context = std::move(*trigger_context), interpreter_context = this->interpreter_context_,
         user_transaction = std::shared_ptr(std::move(db_accessor_))]() mutable {
          RunTriggersIndividually(interpreter_context->trigger_store->AfterCommitTriggers(), interpreter_context,
                                  std::move(trigger_context));
          user_transaction->FinalizeTransaction();
          SPDLOG_DEBUG("Finished executing after commit triggers");  // NOLINT(bugprone-lambda-function-name)
        });
  }

  reset_necessary_members();

  SPDLOG_DEBUG("Finished comitting the transaction");
}

void Interpreter::AdvanceCommand() {
  if (!db_accessor_) return;
  db_accessor_->AdvanceCommand();
}

void Interpreter::AbortCommand(std::unique_ptr<QueryExecution> *query_execution) {
  if (query_execution) {
    query_execution->reset(nullptr);
  }
  if (in_explicit_transaction_) {
    expect_rollback_ = true;
  } else {
    Abort();
  }
}

std::optional<storage::IsolationLevel> Interpreter::GetIsolationLevelOverride() {
  if (next_transaction_isolation_level) {
    const auto isolation_level = *next_transaction_isolation_level;
    next_transaction_isolation_level.reset();
    return isolation_level;
  }

  return interpreter_isolation_level;
}

void Interpreter::SetNextTransactionIsolationLevel(const storage::IsolationLevel isolation_level) {
  next_transaction_isolation_level.emplace(isolation_level);
}

void Interpreter::SetSessionIsolationLevel(const storage::IsolationLevel isolation_level) {
  interpreter_isolation_level.emplace(isolation_level);
}

}  // namespace query
