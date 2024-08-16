// Copyright 2024 Memgraph Ltd.
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
#include "auth/auth.hpp"
#include "auth/exceptions.hpp"
#include "gflags/gflags.h"

#include "audit/log.hpp"
#include "dbms/constants.hpp"
#include "flags/run_time_configurable.hpp"
#include "glue/SessionHL.hpp"
#include "glue/auth_checker.hpp"
#include "glue/communication.hpp"
#include "glue/query_user.hpp"
#include "glue/run_id.hpp"
#include "license/license.hpp"
#include "query/auth_checker.hpp"
#include "query/discard_value_stream.hpp"
#include "query/interpreter_context.hpp"
#include "query/query_user.hpp"
#include "utils/event_map.hpp"
#include "utils/spin_lock.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::metrics {
extern const Event ActiveBoltSessions;
}  // namespace memgraph::metrics

namespace {
auto ToQueryExtras(const memgraph::glue::bolt_value_t &extra) -> memgraph::query::QueryExtras {
  auto const &as_map = extra.ValueMap();

  auto metadata_pv = memgraph::storage::PropertyValue::map_t{};

  if (auto const it = as_map.find("tx_metadata"); it != as_map.cend() && it->second.IsMap()) {
    for (const auto &[key, bolt_md] : it->second.ValueMap()) {
      metadata_pv.emplace(key, memgraph::glue::ToPropertyValue(bolt_md, nullptr));
    }
  }

  auto tx_timeout = std::optional<int64_t>{};
  if (auto const it = as_map.find("tx_timeout"); it != as_map.cend() && it->second.IsInt()) {
    tx_timeout = it->second.ValueInt();
  }

  return memgraph::query::QueryExtras{std::move(metadata_pv), tx_timeout};
}

class TypedValueResultStreamBase {
 public:
  explicit TypedValueResultStreamBase(memgraph::storage::Storage *storage);

  void DecodeValues(const std::vector<memgraph::query::TypedValue> &values);

  auto AccessValues() const -> std::vector<memgraph::glue::bolt_value_t> const & { return decoded_values_; }

 protected:
  // NOTE: Needed only for ToBoltValue conversions
  memgraph::storage::Storage *storage_;
  std::vector<memgraph::glue::bolt_value_t> decoded_values_;
};

/// Wrapper around TEncoder which converts TypedValue to Value
/// before forwarding the calls to original TEncoder.
template <typename TEncoder>
class TypedValueResultStream : public TypedValueResultStreamBase {
 public:
  TypedValueResultStream(TEncoder *encoder, memgraph::storage::Storage *storage)
      : TypedValueResultStreamBase{storage}, encoder_(encoder) {}

  void Result(const std::vector<memgraph::query::TypedValue> &values) {
    DecodeValues(values);
    encoder_->MessageRecord(AccessValues());
  }

 private:
  TEncoder *encoder_;
};

void TypedValueResultStreamBase::DecodeValues(const std::vector<memgraph::query::TypedValue> &values) {
  decoded_values_.reserve(values.size());
  decoded_values_.clear();
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
    decoded_values_.emplace_back(std::move(*maybe_value));
  }
}

TypedValueResultStreamBase::TypedValueResultStreamBase(memgraph::storage::Storage *storage) : storage_(storage) {}

#ifdef MG_ENTERPRISE
void MultiDatabaseAuth(memgraph::query::QueryUserOrRole *user, std::string_view db) {
  if (user && !user->IsAuthorized({}, db, &memgraph::query::session_long_policy)) {
    throw memgraph::communication::bolt::ClientError(
        "You are not authorized on the database \"{}\"! Please contact your database administrator.", db);
  }
}
#endif
}  // namespace
namespace memgraph::glue {

#ifdef MG_ENTERPRISE
std::optional<std::string> SessionHL::GetDefaultDB() {
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
    implicit_db_.reset();
  else
    implicit_db_.emplace(std::move(db));
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
        interpreter_.SetUser(AuthChecker::GenQueryUser(auth_, *user_or_role));
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
      interpreter_.SetUser(AuthChecker::GenQueryUser(auth_, std::nullopt));
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
  interpreter_.SetUser(AuthChecker::GenQueryUser(auth_, *user_or_role));

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
bolt_map_t SessionHL::Pull(SessionHL::TEncoder *encoder, std::optional<int> n, std::optional<int> qid) {
  try {
    auto &db = interpreter_.current_db_.db_acc_;
    auto *storage = db ? db->get()->storage() : nullptr;
    TypedValueResultStream<TEncoder> stream(encoder, storage);
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

std::pair<std::vector<std::string>, std::optional<int>> SessionHL::Interpret(const std::string &query,
                                                                             const bolt_map_t &params,
                                                                             const bolt_map_t &extra) {
  auto get_params_pv = [params](storage::Storage const *storage) -> memgraph::storage::PropertyValue::map_t {
    auto params_pv = memgraph::storage::PropertyValue::map_t{};
    params_pv.reserve(params.size());
    for (const auto &[key, bolt_param] : params) {
      params_pv.try_emplace(key, ToPropertyValue(bolt_param, storage));
    }
    return params_pv;
  };

#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    auto &db = interpreter_.current_db_.db_acc_;
    const auto username = user_or_role_ ? (user_or_role_->username() ? *user_or_role_->username() : "") : "";
    audit_log_->Record(fmt::format("{}:{}", endpoint_.address().to_string(), std::to_string(endpoint_.port())),
                       username, query, params, db ? db->get()->name() : "");
  }
#endif
  try {
    auto result = interpreter_.Prepare(query, get_params_pv, ToQueryExtras(extra));
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
  std::optional<std::string> db{};
  bool update = false;
  // Check if user explicitly defined the database to use
  if (run_time_info.contains("db")) {
    const auto &db_info = run_time_info.at("db");
    if (!db_info.IsString()) {
      throw memgraph::communication::bolt::ClientError("Malformed database name.");
    }
    db = db_info.ValueString();
    const auto &current = GetCurrentDB();
    update = db != current;
    if (!in_explicit_db_) {
      implicit_db_.emplace(current);  // Still not in an explicit database, save for recovery
      update = true;
    }
    in_explicit_db_ = true;
    // NOTE: Once in a transaction, the drivers stop explicitly sending the db and count on using it until commit
  } else if (in_explicit_db_ && !interpreter_.in_explicit_transaction_) {  // Just on a switch
    if (implicit_db_) {
      db = *implicit_db_;
    } else {
      db = GetDefaultDB();
    }
    update = true;  // We have to update in order to update the explicit flag
    in_explicit_db_ = false;
  }

  // Check if the underlying database needs to be updated
  if (update) {
    if (db) {  // Db connection
      MultiDatabaseAuth(user_or_role_.get(), *db);
      interpreter_.SetCurrentDB(*db, in_explicit_db_);
    } else {  // Non-db connection
      interpreter_.ResetDB();
    }
  }
#endif
}
SessionHL::SessionHL(memgraph::query::InterpreterContext *interpreter_context,
                     memgraph::communication::v2::ServerEndpoint endpoint,
                     memgraph::communication::v2::InputStream *input_stream,
                     memgraph::communication::v2::OutputStream *output_stream, memgraph::auth::SynchedAuth *auth
#ifdef MG_ENTERPRISE
                     ,
                     memgraph::audit::Log *audit_log
#endif
                     )
    : Session<memgraph::communication::v2::InputStream, memgraph::communication::v2::OutputStream>(input_stream,
                                                                                                   output_stream),
      interpreter_context_(interpreter_context),
      interpreter_(interpreter_context_),
#ifdef MG_ENTERPRISE
      audit_log_(audit_log),
#endif
      auth_(auth),
      endpoint_(std::move(endpoint)),
      implicit_db_(std::nullopt) {
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
}  // namespace memgraph::glue
