// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "glue/SessionHL.hpp"

#include "audit/log.hpp"
#include "dbms/constants.hpp"
#include "glue/auth_checker.hpp"
#include "glue/communication.hpp"
#include "license/license.hpp"
#include "query/discard_value_stream.hpp"

#include "gflags/gflags.h"
#include "query/interpreter.hpp"

namespace memgraph::metrics {
extern const Event ActiveBoltSessions;
}  // namespace memgraph::metrics

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(bolt_server_name_for_init, "",
              "Server name which the database should send to the client in the "
              "Bolt INIT message.");

auto ToQueryExtras(const memgraph::communication::bolt::Value &extra) -> memgraph::query::QueryExtras {
  auto const &as_map = extra.ValueMap();

  auto metadata_pv = std::map<std::string, memgraph::storage::PropertyValue>{};

  if (auto const it = as_map.find("tx_metadata"); it != as_map.cend() && it->second.IsMap()) {
    for (const auto &[key, bolt_md] : it->second.ValueMap()) {
      metadata_pv.emplace(key, memgraph::glue::ToPropertyValue(bolt_md));
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

  std::vector<memgraph::communication::bolt::Value> DecodeValues(
      const std::vector<memgraph::query::TypedValue> &values) const;

 protected:
  // NOTE: Needed only for ToBoltValue conversions
  memgraph::storage::Storage *storage_;
};

/// Wrapper around TEncoder which converts TypedValue to Value
/// before forwarding the calls to original TEncoder.
template <typename TEncoder>
class TypedValueResultStream : public TypedValueResultStreamBase {
 public:
  TypedValueResultStream(TEncoder *encoder, memgraph::storage::Storage *storage)
      : TypedValueResultStreamBase{storage}, encoder_(encoder) {}

  void Result(const std::vector<memgraph::query::TypedValue> &values) { encoder_->MessageRecord(DecodeValues(values)); }

 private:
  TEncoder *encoder_;
};

std::vector<memgraph::communication::bolt::Value> TypedValueResultStreamBase::DecodeValues(
    const std::vector<memgraph::query::TypedValue> &values) const {
  std::vector<memgraph::communication::bolt::Value> decoded_values;
  decoded_values.reserve(values.size());
  for (const auto &v : values) {
    auto maybe_value = memgraph::glue::ToBoltValue(v, *storage_, memgraph::storage::View::NEW);
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
    decoded_values.emplace_back(std::move(*maybe_value));
  }
  return decoded_values;
}
TypedValueResultStreamBase::TypedValueResultStreamBase(memgraph::storage::Storage *storage) : storage_(storage) {}

namespace memgraph::glue {

inline static void MultiDatabaseAuth(const std::optional<auth::User> &user, std::string_view db) {
  if (user && !AuthChecker::IsUserAuthorized(*user, {}, db.data())) {
    throw memgraph::communication::bolt::ClientError(
        "You are not authorized on the database \"{}\"! Please contact your database administrator.", db);
  }
}
std::string SessionHL::GetDefaultDB() {
#ifdef MG_ENTERPRISE
  if (user_.has_value()) {
    return user_->db_access().GetDefault();
  }
#endif
  return memgraph::dbms::kDefaultDB;
}

std::string SessionHL::GetDatabaseName() const { return interpreter_.db_->id(); }

std::optional<std::string> SessionHL::GetServerNameForInit() {
  if (FLAGS_bolt_server_name_for_init.empty()) return std::nullopt;
  return FLAGS_bolt_server_name_for_init;
}

bool SessionHL::Authenticate(const std::string &username, const std::string &password) {
  bool res = true;
  {
    auto locked_auth = auth_->Lock();
    if (locked_auth->HasUsers()) {
      user_ = locked_auth->Authenticate(username, password);
      res = user_.has_value();
    }
  }
  // Start off with the default database
  interpreter_.SetCurrentDB(GetDefaultDB());
  implicit_db_.emplace(GetDatabaseName());
  return res;
}

void SessionHL::Abort() { interpreter_.Abort(); }

std::map<std::string, memgraph::communication::bolt::Value> SessionHL::Discard(std::optional<int> n,
                                                                               std::optional<int> qid) {
  try {
    memgraph::query::DiscardValueResultStream stream;
    return DecodeSummary(interpreter_.Pull(&stream, n, qid));
  } catch (const memgraph::query::QueryException &e) {
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw memgraph::communication::bolt::ClientError(e.what());
  }
}
std::map<std::string, memgraph::communication::bolt::Value> SessionHL::Pull(SessionHL::TEncoder *encoder,
                                                                            std::optional<int> n,
                                                                            std::optional<int> qid) {
  try {
    TypedValueResultStream<TEncoder> stream(encoder, interpreter_.db_->storage());
    return DecodeSummary(interpreter_.Pull(&stream, n, qid));
  } catch (const memgraph::query::QueryException &e) {
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw memgraph::communication::bolt::ClientError(e.what());
  }
}
std::pair<std::vector<std::string>, std::optional<int>> SessionHL::Interpret(
    const std::string &query, const std::map<std::string, memgraph::communication::bolt::Value> &params,
    const std::map<std::string, memgraph::communication::bolt::Value> &extra) {
  std::map<std::string, memgraph::storage::PropertyValue> params_pv;
  for (const auto &[key, bolt_param] : params) {
    params_pv.emplace(key, ToPropertyValue(bolt_param));
  }
  const std::string *username{nullptr};
  if (user_) {
    username = &user_->username();
  }

#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    audit_log_->Record(endpoint_.address().to_string(), user_ ? *username : "", query,
                       memgraph::storage::PropertyValue(params_pv), interpreter_.db_->id());
  }
#endif
  try {
    auto result = interpreter_.Prepare(query, params_pv, username, ToQueryExtras(extra), UUID());
    const std::string db_name = result.db ? *result.db : "";
    if (user_ && !AuthChecker::IsUserAuthorized(*user_, result.privileges, db_name)) {
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
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw memgraph::communication::bolt::ClientError(e.what());
  } catch (const memgraph::query::ReplicationException &e) {
    throw memgraph::communication::bolt::ClientError(e.what());
  }
}
void SessionHL::RollbackTransaction() { interpreter_.RollbackTransaction(); }
void SessionHL::CommitTransaction() { interpreter_.CommitTransaction(); }
void SessionHL::BeginTransaction(const std::map<std::string, memgraph::communication::bolt::Value> &extra) {
  interpreter_.BeginTransaction(ToQueryExtras(extra));
}
void SessionHL::Configure(const std::map<std::string, memgraph::communication::bolt::Value> &run_time_info) {
#ifdef MG_ENTERPRISE
  std::string db;
  bool update = false;
  // Check if user explicitly defined the database to use
  if (run_time_info.contains("db")) {
    const auto &db_info = run_time_info.at("db");
    if (!db_info.IsString()) {
      throw memgraph::communication::bolt::ClientError("Malformed database name.");
    }
    db = db_info.ValueString();
    const auto &current = GetDatabaseName();
    update = db != current;
    if (!in_explicit_db_) implicit_db_.emplace(current);  // Still not in an explicit database, save for recovery
    in_explicit_db_ = true;
    // NOTE: Once in a transaction, the drivers stop explicitly sending the db and count on using it until commit
  } else if (in_explicit_db_ && !interpreter_.in_explicit_transaction_) {  // Just on a switch
    if (implicit_db_) {
      db = *implicit_db_;
    } else {
      db = GetDefaultDB();
    }
    update = db != GetDatabaseName();
    in_explicit_db_ = false;
  }

  // Check if the underlying database needs to be updated
  if (update) {
    MultiDatabaseAuth(user_, db);
    interpreter_.SetCurrentDB(db);
  }
#endif
}
SessionHL::SessionHL(memgraph::query::InterpreterContext *interpreter_context,
                     const memgraph::communication::v2::ServerEndpoint &endpoint,
                     memgraph::communication::v2::InputStream *input_stream,
                     memgraph::communication::v2::OutputStream *output_stream,
                     memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
                     memgraph::audit::Log *audit_log)
    : Session<memgraph::communication::v2::InputStream, memgraph::communication::v2::OutputStream>(input_stream,
                                                                                                   output_stream),
      interpreter_context_(interpreter_context),
      interpreter_(std::make_unique<query::Interpreter>(interpreter_context_)),
      auth_(auth),
      audit_log_(audit_log),
      endpoint_(endpoint),
      implicit_db_(dbms::kDefaultDB) {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveBoltSessions);
  interpreter_.SetCurrentDB(dbms::kDefaultDB);  // Connect to the default db NOTE: User can never access this, since it
                                                // gets updated on Authentication
  interpreter_.OnChangeCB([&](std::string_view db_name) { MultiDatabaseAuth(user_, db_name); });
  interpreter_context_->interpreters.WithLock([this](auto &interpreters) { interpreters.insert(interpreter_.get()); });
}

SessionHL::~SessionHL() {
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveBoltSessions);
  interpreter_context_->interpreters.WithLock([this](auto &interpreters) { interpreters.erase(interpreter_.get()); });
}

std::map<std::string, memgraph::communication::bolt::Value> SessionHL::DecodeSummary(
    const std::map<std::string, memgraph::query::TypedValue> &summary) {
  std::map<std::string, memgraph::communication::bolt::Value> decoded_summary;
  for (const auto &kv : summary) {
    auto maybe_value = ToBoltValue(kv.second, *interpreter_.db_->storage(), memgraph::storage::View::NEW);
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
  if (auto run_id = run_id_; run_id) {
    decoded_summary.emplace("run_id", *run_id);
  }

  return decoded_summary;
}
}  // namespace memgraph::glue
