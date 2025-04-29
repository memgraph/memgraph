// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <optional>
#include <utility>

#include "audit/log.hpp"
#include "auth/auth.hpp"
#include "auth/exceptions.hpp"
#include "dbms/constants.hpp"
#include "flags/run_time_configurable.hpp"
#include "frontend/ast/ast.hpp"
#include "glue/SessionHL.hpp"
#include "glue/auth_checker.hpp"
#include "glue/communication.hpp"
#include "glue/run_id.hpp"
#include "license/license.hpp"
#include "query/discard_value_stream.hpp"
#include "query/interpreter_context.hpp"
#include "query/query_user.hpp"
#include "utils/event_map.hpp"
#include "utils/priorities.hpp"
#include "utils/typeinfo.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::metrics {
extern const Event ActiveBoltSessions;
}  // namespace memgraph::metrics

namespace {

auto ToQueryExtras(const memgraph::glue::bolt_value_t &extra) -> memgraph::query::QueryExtras {
  auto metadata_pv = memgraph::storage::PropertyValue::map_t{};
  auto const &as_map = extra.ValueMap();
  // user-defined metadata
  if (auto const it = as_map.find("tx_metadata"); it != as_map.cend() && it->second.IsMap()) {
    for (const auto &[key, bolt_md] : it->second.ValueMap()) {
      metadata_pv.emplace(key, memgraph::glue::ToPropertyValue(bolt_md, nullptr));
    }
  }
  // timeout
  auto tx_timeout = std::optional<int64_t>{};
  if (auto const it = as_map.find("tx_timeout"); it != as_map.cend() && it->second.IsInt()) {
    tx_timeout = it->second.ValueInt();
  }
  // rw type
  bool is_read = false;
  if (auto const it = as_map.find("mode"); it != as_map.cend() && it->second.IsString()) {
    is_read = it->second.ValueString() == "r";
  }
  return memgraph::query::QueryExtras{std::move(metadata_pv), tx_timeout, is_read};
}

/// Wrapper around TEncoder which converts TypedValue to Value
/// before forwarding the calls to original TEncoder.
template <typename TEncoder>
class TypedValueResultStream {
 public:
  TypedValueResultStream(TEncoder *encoder, memgraph::storage::Storage *storage)
      : storage_{storage}, encoder_(encoder) {}

  void Result(const std::vector<memgraph::query::TypedValue> &values) {
    // Splitting the MessageRecord allows us to skip vector insertion and just directly encode the value
    encoder_->MessageRecordHeader(values.size());
    for (const auto &v : values) {
      auto maybe_value = memgraph::glue::ToBoltValue(v, storage_, memgraph::storage::View::NEW);
      if (maybe_value.HasError()) {
        switch (maybe_value.GetError()) {
          case memgraph::storage::Error::DELETED_OBJECT:
            throw memgraph::communication::bolt::ClientError("Returning a deleted object as a result.");
          case memgraph::storage::Error::NONEXISTENT_OBJECT:
            throw memgraph::communication::bolt::ClientError("Returning a nonexistent object as a result.");
          case memgraph::storage::Error::VERTEX_HAS_EDGES:
          case memgraph::storage::Error::SERIALIZATION_ERROR:
          case memgraph::storage::Error::PROPERTIES_DISABLED:
            throw memgraph::communication::bolt::ClientError("Unexpected storage error when streaming results.");
        }
      }
      encoder_->MessageRecordAppendValue(maybe_value.GetValue());
    }
    if (!encoder_->MessageRecordFinalize()) {
      throw memgraph::communication::bolt::ClientError("Failed to send result to client!");
    }
  }

 private:
  // NOTE: Needed only for ToBoltValue conversions
  memgraph::storage::Storage *storage_;
  TEncoder *encoder_;
};

#ifdef MG_ENTERPRISE
void MultiDatabaseAuth(memgraph::query::QueryUserOrRole *user, std::string_view db) {
  if (user && !user->IsAuthorized({}, db, &memgraph::query::session_long_policy)) {
    throw memgraph::communication::bolt::ClientError(
        "You are not authorized on the database \"{}\"! Please contact your database administrator.", db);
  }
}

void ImpersonateUserAuth(memgraph::query::QueryUserOrRole *user_or_role, const std::string &impersonated_user) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    throw memgraph::communication::bolt::ClientError(memgraph::license::LicenseCheckErrorToString(
        memgraph::license::LicenseCheckError::NOT_ENTERPRISE_LICENSE, "impersonate user"));
  }
  if (!user_or_role) {
    throw memgraph::communication::bolt::ClientError(
        "No session userYou must be logged-in in order to use the impersonate-user feature.");
  }
  if (!user_or_role->CanImpersonate(impersonated_user, &memgraph::query::session_long_policy)) {
    throw memgraph::communication::bolt::ClientError(
        "Failed to impersonate user '{}'. Make sure you have the right privileges and that the user exists.",
        impersonated_user);
  }
}
#endif
}  // namespace

namespace memgraph::glue {

#ifdef MG_ENTERPRISE
std::optional<std::string> SessionHL::GetDefaultDB() const {
  if (user_or_role_) {
    try {
      return user_or_role_->GetDefaultDB();
    } catch (auth::AuthException &) {
      // Support non-db connection
      return {};
    }
  }
  return std::string{memgraph::dbms::kDefaultDB};
}

std::string SessionHL::GetCurrentUser() const {
  if (interpreter_.user_or_role_) {
    if (const auto &name = interpreter_.user_or_role_->username()) return *name;
    if (const auto &name = interpreter_.user_or_role_->rolename()) return *name;
  }
  return "";
}
#endif

std::string SessionHL::GetCurrentDB() const {
  if (!interpreter_.current_db_.db_acc_) return "";
  const auto *db = interpreter_.current_db_.db_acc_->get();
  return db->name();
}

std::optional<std::string> SessionHL::GetServerNameForInit() {
  const auto &name = flags::run_time::GetServerName();
  return name.empty() ? std::nullopt : std::make_optional(name);
}

utils::Priority SessionHL::ApproximateQueryPriority() const {
  // Query has been parsed and a priority can be determined
  if (parsed_res_ && state_ == memgraph::communication::bolt::State::Parsed) {
    return std::visit(utils::Overloaded{
                          [](const query::Interpreter::TransactionQuery &) {
                            // BEGIN; COMMIT; ROLLBACK
                            return utils::Priority::LOW;
                          },
                          [](const query::Interpreter::ParseInfo &parse_info) {
                            // Many variants of queries
                            // Cypher -> low
                            // all others -> high
                            const auto &query = parse_info.parsed_query.query;
                            // Most often query type
                            if (utils::Downcast<query::CypherQuery>(query)) [[likely]]
                              return utils::Priority::LOW;
                            // For now return HIGH only for hand-picked queries (non-system and non-db queries)
                            auto high_priority = utils::Downcast<query::ShowConfigQuery>(query) ||
                                                 utils::Downcast<query::SettingQuery>(query) ||
                                                 utils::Downcast<query::VersionQuery>(query) ||
                                                 utils::Downcast<query::TransactionQueueQuery>(query) ||
                                                 utils::Downcast<query::UseDatabaseQuery>(query) ||
                                                 utils::Downcast<query::ShowDatabaseQuery>(query) ||
                                                 utils::Downcast<query::ShowDatabasesQuery>(query) ||
                                                 utils::Downcast<query::ReplicationInfoQuery>(query);
                            return high_priority ? utils::Priority::HIGH : utils::Priority::LOW;
                          },
                          [](const auto &) { MG_ASSERT(false, "Unexpected ParseRes variant!"); },
                      },
                      parsed_res_->parsed_query);
  }

  // Result means query has been prepared and we are pulling
  return state_ == memgraph::communication::bolt::State::Result ? interpreter_.ApproximateNextQueryPriority()
                                                                : utils::Priority::HIGH;
}

void SessionHL::TryDefaultDB() {
#ifdef MG_ENTERPRISE
  const auto default_db = GetDefaultDB();
  if (default_db) {
    // Start off with the default database
    interpreter_.SetCurrentDB(*default_db, false);
  } else {
    // Failed to get default db, connect without db
    interpreter_.ResetDB();
  }
  auto db = GetCurrentDB();
  if (db.empty())
    runtime_db_.implicit_config_.reset();
  else
    runtime_db_.implicit_config_.emplace(std::move(db));
#else
  // Community has to connect to the default database
  interpreter_.SetCurrentDB();
#endif
}

// This is called on connection establishment
bool SessionHL::Authenticate(const std::string &username, const std::string &password) {
  bool res = true;
  interpreter_.ResetUser();
  {
    auto locked_auth = auth_->Lock();
    if (locked_auth->AccessControlled()) {
      const auto user_or_role = locked_auth->Authenticate(username, password);
      if (user_or_role.has_value()) {
        user_or_role_ = AuthChecker::GenQueryUser(auth_, *user_or_role);
        interpreter_.SetUser(user_or_role_);
        interpreter_.SetSessionInfo(
            UUID(),
            interpreter_.user_or_role_->username().has_value() ? interpreter_.user_or_role_->username().value() : "",
            GetLoginTimestamp());
      } else {
        res = false;
      }
    } else {
      // No access control -> give empty user
      user_or_role_ = AuthChecker::GenQueryUser(auth_, std::nullopt);
      interpreter_.SetUser(user_or_role_);
      interpreter_.SetSessionInfo(UUID(), "", GetLoginTimestamp());
    }
  }

  TryDefaultDB();
  return res;
}

bool SessionHL::SSOAuthenticate(const std::string &scheme, const std::string &identity_provider_response) {
  interpreter_.ResetUser();

  auto locked_auth = auth_->Lock();

  const auto user_or_role = locked_auth->SSOAuthenticate(scheme, identity_provider_response);
  if (!user_or_role.has_value()) {
    return false;
  }

  user_or_role_ = AuthChecker::GenQueryUser(auth_, *user_or_role);
  interpreter_.SetUser(user_or_role_);

  TryDefaultDB();
  return true;
}

void SessionHL::Abort() { interpreter_.Abort(); }

bolt_map_t SessionHL::Discard(std::optional<int> n, std::optional<int> qid) {
  try {
    memgraph::query::DiscardValueResultStream stream;
    return DecodeSummary(interpreter_.Pull(&stream, n, qid));
  } catch (const memgraph::query::QueryException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw memgraph::communication::bolt::ClientError(e.what());
  }
}

bolt_map_t SessionHL::Pull(std::optional<int> n, std::optional<int> qid) {
  try {
    using TEncoder =
        communication::bolt::Encoder<communication::bolt::ChunkedEncoderBuffer<communication::v2::OutputStream>>;
    auto &db = interpreter_.current_db_.db_acc_;
    auto *storage = db ? db->get()->storage() : nullptr;
    TypedValueResultStream<TEncoder> stream(&encoder_, storage);
    return DecodeSummary(interpreter_.Pull(&stream, n, qid));
  } catch (const memgraph::query::QueryException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw memgraph::communication::bolt::ClientError(e.what());
  } catch (const utils::BasicException &) {
    // Exceptions inheriting from BasicException will result in a TransientError
    // i. e. client will be encouraged to retry execution because it
    // could succeed if executed again.
    throw;
  }
}

void SessionHL::InterpretParse(const std::string &query, bolt_map_t params, const bolt_map_t &extra) {
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    auto &db = interpreter_.current_db_.db_acc_;
    const auto username = user_or_role_ ? (user_or_role_->username() ? *user_or_role_->username() : "") : "";
    audit_log_->Record(fmt::format("{}:{}", endpoint_.address().to_string(), std::to_string(endpoint_.port())),
                       username, query, params, db ? db->get()->name() : "");
  }
#endif

  auto get_params_pv =
      [params = std::move(params)](storage::Storage const *storage) -> memgraph::storage::PropertyValue::map_t {
    auto params_pv = memgraph::storage::PropertyValue::map_t{};
    params_pv.reserve(params.size());
    for (const auto &[key, bolt_param] : params) {
      params_pv.try_emplace(key, ToPropertyValue(bolt_param, storage));
    }
    return params_pv;
  };

  try {
    auto query_extras = ToQueryExtras(extra);
    auto parsed_query = interpreter_.Parse(query, get_params_pv, query_extras);
    parsed_res_.emplace(std::move(parsed_query), std::move(get_params_pv), std::move(query_extras));
  } catch (const memgraph::query::QueryException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw memgraph::communication::bolt::ClientError(e.what());
  } catch (const memgraph::query::ReplicationException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    throw memgraph::communication::bolt::ClientError(e.what());
  }
}

std::pair<std::vector<std::string>, std::optional<int>> SessionHL::InterpretPrepare() {
  if (!parsed_res_) {
    throw memgraph::communication::bolt::ClientError("Trying to prepare a query that was not parsed.");
  }

  try {
    auto parsed_res = *std::move(parsed_res_);
    parsed_res_.reset();
    auto result =
        interpreter_.Prepare(std::move(parsed_res.parsed_query), std::move(parsed_res.get_params_pv), parsed_res.extra);
    const std::string db_name = result.db ? *result.db : "";
    if (user_or_role_ && !user_or_role_->IsAuthorized(result.privileges, db_name, &query::session_long_policy)) {
      interpreter_.Abort();
      if (db_name.empty()) {
        throw memgraph::communication::bolt::ClientError(
            "You are not authorized to execute this query! Please contact your database administrator.");
      }
      throw memgraph::communication::bolt::ClientError(
          "You are not authorized to execute this query on database \"{}\"! Please contact your database "
          "administrator.",
          db_name);
    }
    return {std::move(result.headers), result.qid};

  } catch (const memgraph::query::QueryException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw memgraph::communication::bolt::ClientError(e.what());
  } catch (const memgraph::query::ReplicationException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    throw memgraph::communication::bolt::ClientError(e.what());
  }
}

#ifdef MG_ENTERPRISE
auto SessionHL::Route(bolt_map_t const &routing, std::vector<bolt_value_t> const & /*bookmarks*/, bolt_map_t const &
                      /*extra*/) -> bolt_map_t {
  auto routing_map = ranges::views::transform(
                         routing, [](auto const &pair) { return std::pair(pair.first, pair.second.ValueString()); }) |
                     ranges::to<std::map<std::string, std::string>>();

  auto routing_table_res = interpreter_.Route(routing_map);

  auto create_server = [](auto const &server_info) -> bolt_value_t {
    auto const &[addresses, role] = server_info;
    bolt_map_t server_map;
    auto bolt_addresses = ranges::views::transform(addresses, [](auto const &addr) { return bolt_value_t{addr}; }) |
                          ranges::to<std::vector<bolt_value_t>>();

    server_map["addresses"] = std::move(bolt_addresses);
    server_map["role"] = bolt_value_t{role};
    return bolt_value_t{std::move(server_map)};
  };

  bolt_map_t communication_res;
  communication_res["ttl"] = bolt_value_t{routing_table_res.ttl};
  communication_res["db"] = bolt_value_t{};

  auto servers =
      ranges::views::transform(routing_table_res.servers, create_server) | ranges::to<std::vector<bolt_value_t>>();
  communication_res["servers"] = bolt_value_t{std::move(servers)};

  return {{"rt", bolt_value_t{std::move(communication_res)}}};
}
#endif

void SessionHL::RollbackTransaction() {
  try {
    interpreter_.RollbackTransaction();
  } catch (const memgraph::query::QueryException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw memgraph::communication::bolt::ClientError(e.what());
  } catch (const memgraph::query::ReplicationException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    throw memgraph::communication::bolt::ClientError(e.what());
  }
}

void SessionHL::CommitTransaction() {
  try {
    interpreter_.CommitTransaction();
  } catch (const memgraph::query::QueryException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw memgraph::communication::bolt::ClientError(e.what());
  } catch (const memgraph::query::ReplicationException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    throw memgraph::communication::bolt::ClientError(e.what());
  }
}

void SessionHL::BeginTransaction(const bolt_map_t &extra) {
  try {
    interpreter_.BeginTransaction(ToQueryExtras(extra));
  } catch (const memgraph::query::QueryException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw memgraph::communication::bolt::ClientError(e.what());
  } catch (const memgraph::query::ReplicationException &e) {
    // Count the number of specific exceptions thrown
    metrics::IncrementCounter(GetExceptionName(e));
    throw memgraph::communication::bolt::ClientError(e.what());
  }
}

void SessionHL::Configure(const bolt_map_t &run_time_info) {
#ifdef MG_ENTERPRISE
  runtime_user_.Configure(run_time_info, interpreter_.in_explicit_transaction_);
  runtime_db_.Configure(run_time_info, interpreter_.in_explicit_transaction_);
#else
  (void)run_time_info;
#endif
}
SessionHL::SessionHL(Context context, memgraph::communication::v2::InputStream *input_stream,
                     memgraph::communication::v2::OutputStream *output_stream)
    : Session<memgraph::communication::v2::InputStream, memgraph::communication::v2::OutputStream>(input_stream,
                                                                                                   output_stream),
      interpreter_context_(context.ic),
      interpreter_(interpreter_context_),
#ifdef MG_ENTERPRISE
      audit_log_(context.audit_log),
      runtime_db_{"db", [this]() { return GetCurrentDB(); }, [this]() { return GetDefaultDB(); },
                  [this](std::optional<std::string> defined_db, bool user_defined) {
                    if (defined_db) {  // Db connection
                      MultiDatabaseAuth(user_or_role_.get(), *defined_db);
                      interpreter_.SetCurrentDB(*defined_db, user_defined);
                    } else {  // Non-db connection
                      interpreter_.ResetDB();
                    }
                  }},
      runtime_user_{"imp_user", [this]() { return GetCurrentUser(); },
                    []() {
                      // Only one possible default
                      return std::nullopt;
                    },
                    [this](std::optional<std::string> defined_user, bool impersonate_user) {
                      if (impersonate_user) {
                        ImpersonateUserAuth(user_or_role_.get(), *defined_user);
                        const auto &imp_usr = auth_->ReadLock()->GetUser(*defined_user);
                        if (!imp_usr) throw auth::AuthException("Trying to impersonate a user that doesn't exist.");
                        interpreter_.SetUser(AuthChecker::GenQueryUser(auth_, imp_usr));
                      } else {
                        // Set our default user/role
                        interpreter_.SetUser(user_or_role_);
                      }
                    }},
#endif
      auth_(context.auth),
      endpoint_(std::move(context.endpoint)) {
  // Metrics update
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveBoltSessions);
#ifdef MG_ENTERPRISE
  interpreter_.OnChangeCB([&](std::string_view db_name) { MultiDatabaseAuth(user_or_role_.get(), db_name); });
#endif
  interpreter_context_->interpreters.WithLock([this](auto &interpreters) { interpreters.insert(&interpreter_); });
}

SessionHL::~SessionHL() {
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveBoltSessions);
  interpreter_context_->interpreters.WithLock([this](auto &interpreters) { interpreters.erase(&interpreter_); });
}

bolt_map_t SessionHL::DecodeSummary(const std::map<std::string, memgraph::query::TypedValue> &summary) {
  auto &db_acc = interpreter_.current_db_.db_acc_;
  auto *storage = db_acc ? db_acc->get()->storage() : nullptr;
  bolt_map_t decoded_summary;
  for (const auto &kv : summary) {
    auto maybe_value = ToBoltValue(kv.second, storage, memgraph::storage::View::NEW);
    if (maybe_value.HasError()) {
      switch (maybe_value.GetError()) {
        case memgraph::storage::Error::DELETED_OBJECT:
        case memgraph::storage::Error::SERIALIZATION_ERROR:
        case memgraph::storage::Error::VERTEX_HAS_EDGES:
        case memgraph::storage::Error::PROPERTIES_DISABLED:
        case memgraph::storage::Error::NONEXISTENT_OBJECT:
          throw memgraph::communication::bolt::ClientError("Unexpected storage error when streaming summary.");
      }
    }
    decoded_summary.emplace(kv.first, std::move(*maybe_value));
  }
  // Add this memgraph instance run_id, received from telemetry
  // This is sent with every query, instead of only on bolt init inside
  // communication/bolt/v1/states/init.hpp because neo4jdriver does not
  // read the init message.
  decoded_summary.emplace("run_id", memgraph::glue::run_id_);

  return decoded_summary;
}

void RunTimeConfig::Configure(auto run_time_info, bool in_explicit_tx) {
  std::optional<std::string> defined_config{};
  bool update = false;

  // Check if user explicitly defined this config
  if (run_time_info.contains(key_)) {
    const auto &info = run_time_info.at(key_);
    if (!info.IsString()) {
      throw memgraph::communication::bolt::ClientError("Malformed config input.");
    }
    defined_config = info.ValueString();
    const auto &current_config = get_current_();
    update = defined_config != current_config;
    if (!explicit_) {
      implicit_config_.emplace(current_config);  // Still not in an explicit database, save for recovery
      update = true;
    }
    explicit_ = true;
    // NOTE: Once in a transaction, the drivers stop explicitly sending the config and count on using it until commit
  } else if (explicit_ && !in_explicit_tx) {  // Just on a switch
    if (implicit_config_) {
      defined_config = *implicit_config_;
    } else {
      defined_config = get_default_();
    }
    update = true;  // We have to update in order to update the explicit flag
    explicit_ = false;
  }

  // Check if the underlying config needs updating
  if (update) {
    update_(defined_config, explicit_);
  }
}
}  // namespace memgraph::glue
