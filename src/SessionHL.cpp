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

#include "SessionHL.hpp"

#include "glue/auth_checker.hpp"
#include "glue/communication.hpp"
#include "license/license.hpp"
#include "query/discard_value_stream.hpp"

#include "gflags/gflags.h"

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
  explicit TypedValueResultStreamBase(memgraph::query::InterpreterContext *interpreterContext);

  std::vector<memgraph::communication::bolt::Value> DecodeValues(
      const std::vector<memgraph::query::TypedValue> &values) const;

 private:
  // NOTE: Needed only for ToBoltValue conversions
  memgraph::query::InterpreterContext *interpreter_context_;
};

/// Wrapper around TEncoder which converts TypedValue to Value
/// before forwarding the calls to original TEncoder.
template <typename TEncoder>
class TypedValueResultStream : public TypedValueResultStreamBase {
 public:
  TypedValueResultStream(TEncoder *encoder, memgraph::query::InterpreterContext *ic)
      : TypedValueResultStreamBase{ic}, encoder_(encoder) {}

  void Result(const std::vector<memgraph::query::TypedValue> &values) { encoder_->MessageRecord(DecodeValues(values)); }

 private:
  TEncoder *encoder_;
};

#ifdef MG_ENTERPRISE

void SessionHL::UpdateAndDefunct(const std::string &db_name) {
  UpdateAndDefunct(ContextWrapper(sc_handler_.Get(db_name)));
}
void SessionHL::UpdateAndDefunct(ContextWrapper &&cntxt) {
  defunct_.emplace(std::move(current_));
  Update(std::forward<ContextWrapper>(cntxt));
  defunct_->Defunct();
}
void SessionHL::Update(const std::string &db_name) {
  ContextWrapper tmp(sc_handler_.Get(db_name));
  Update(std::move(tmp));
}
void SessionHL::Update(ContextWrapper &&cntxt) {
  current_ = std::move(cntxt);
  interpreter_ = current_.interp();
  interpreter_->in_explicit_db_ = in_explicit_db_;
  interpreter_context_ = current_.interpreter_context();
}
void SessionHL::MultiDatabaseAuth(const std::string &db) {
  if (user_ && !memgraph::glue::AuthChecker::IsUserAuthorized(*user_, {}, db)) {
    throw memgraph::communication::bolt::ClientError(
        "You are not authorized on the database \"{}\"! Please contact your database administrator.", db);
  }
}
std::string SessionHL::GetDefaultDB() {
  if (user_.has_value()) {
    return user_->db_access().GetDefault();
  }
  return memgraph::dbms::kDefaultDB;
}

bool SessionHL::OnDelete(const std::string &db_name) {
  MG_ASSERT(current_.interpreter_context()->db->id() != db_name && (!defunct_ || defunct_->defunct()),
            "Trying to delete a database while still in use.");
  return true;
}
memgraph::dbms::SetForResult SessionHL::OnChange(const std::string &db_name) {
  MultiDatabaseAuth(db_name);
  if (db_name != current_.interpreter_context()->db->id()) {
    UpdateAndDefunct(db_name);  // Done during Pull, so we cannot just replace the current db
    return memgraph::dbms::SetForResult::SUCCESS;
  }
  return memgraph::dbms::SetForResult::ALREADY_SET;
}

#endif
std::string SessionHL::GetDatabaseName() const { return interpreter_context_->db->id(); }

std::optional<std::string> SessionHL::GetServerNameForInit() {
  if (FLAGS_bolt_server_name_for_init.empty()) return std::nullopt;
  return FLAGS_bolt_server_name_for_init;
}
bool SessionHL::Authenticate(const std::string &username, const std::string &password) {
  auto locked_auth = auth_->Lock();
  if (!locked_auth->HasUsers()) {
    return true;
  }
  user_ = locked_auth->Authenticate(username, password);
#ifdef MG_ENTERPRISE
  if (user_.has_value()) {
    const auto &db = user_->db_access().GetDefault();
    // Check if the underlying database needs to be updated
    if (db != current_.interpreter_context()->db->id()) {
      const auto &res = sc_handler_.SetFor(UUID(), db);
      return res == memgraph::dbms::SetForResult::SUCCESS || res == memgraph::dbms::SetForResult::ALREADY_SET;
    }
  }
#endif
  return user_.has_value();
}
void SessionHL::Abort() { interpreter_->Abort(); }

std::map<std::string, memgraph::communication::bolt::Value> SessionHL::Discard(std::optional<int> n,
                                                                               std::optional<int> qid) {
  try {
    memgraph::query::DiscardValueResultStream stream;
    return DecodeSummary(interpreter_->Pull(&stream, n, qid));
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
    TypedValueResultStream<TEncoder> stream(encoder, interpreter_context_);
    return DecodeSummary(interpreter_->Pull(&stream, n, qid));
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
    params_pv.emplace(key, memgraph::glue::ToPropertyValue(bolt_param));
  }
  const std::string *username{nullptr};
  if (user_) {
    username = &user_->username();
  }

#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    audit_log_->Record(endpoint_.address().to_string(), user_ ? *username : "", query,
                       memgraph::storage::PropertyValue(params_pv), interpreter_context_->db->id());
  }
#endif
  try {
    auto result = interpreter_->Prepare(query, params_pv, username, ToQueryExtras(extra), UUID());
    const std::string db_name = result.db ? *result.db : "";
    if (user_ && !memgraph::glue::AuthChecker::IsUserAuthorized(*user_, result.privileges, db_name)) {
      interpreter_->Abort();
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
void SessionHL::RollbackTransaction() { interpreter_->RollbackTransaction(); }
void SessionHL::CommitTransaction() { interpreter_->CommitTransaction(); }
void SessionHL::BeginTransaction(const std::map<std::string, memgraph::communication::bolt::Value> &extra) {
  interpreter_->BeginTransaction(ToQueryExtras(extra));
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
    update = db != current_.interpreter_context()->db->id();
    in_explicit_db_ = true;
    // NOTE: Once in a transaction, the drivers stop explicitly sending the db and count on using it until commit
  } else if (in_explicit_db_ && !interpreter_->in_explicit_transaction_) {  // Just on a switch
    db = GetDefaultDB();
    update = db != current_.interpreter_context()->db->id();
    in_explicit_db_ = false;
  }

  // Check if the underlying database needs to be updated
  if (update) {
    sc_handler_.SetInPlace(db, [this](auto new_sc) mutable {
      const auto &db_name = new_sc.interpreter_context->db->id();
      MultiDatabaseAuth(db_name);
      try {
        Update(ContextWrapper(new_sc));
        return memgraph::dbms::SetForResult::SUCCESS;
      } catch (memgraph::dbms::UnknownDatabaseException &e) {
        throw memgraph::communication::bolt::ClientError("No database named \"{}\" found!", db_name);
      }
    });
  }
#endif
}
SessionHL::~SessionHL() { memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveBoltSessions); }
SessionHL::SessionHL(memgraph::dbms::SessionContextHandler &sc_handler,
                     const memgraph::communication::v2::ServerEndpoint &endpoint,
                     memgraph::communication::v2::InputStream *input_stream,
                     memgraph::communication::v2::OutputStream *output_stream, const std::string &default_db)  // NOLINT
    : Session<memgraph::communication::v2::InputStream, memgraph::communication::v2::OutputStream>(input_stream,
                                                                                                   output_stream),
#ifdef MG_ENTERPRISE
      sc_handler_(sc_handler),
      current_(sc_handler_.Get(default_db)),
#else
      current_(sc),
#endif
      interpreter_context_(current_.interpreter_context()),
      interpreter_(current_.interp()),
      auth_(current_.auth()),
#ifdef MG_ENTERPRISE
      audit_log_(current_.audit_log()),
#endif
      endpoint_(endpoint),
      run_id_(current_.run_id()) {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveBoltSessions);
}

/// ContextWrapper
ContextWrapper::ContextWrapper(memgraph::dbms::SessionContext sc)
    : session_context(sc),
      interpreter(std::make_unique<memgraph::query::Interpreter>(session_context.interpreter_context.get())),
      defunct_(false) {
  session_context.interpreter_context->interpreters.WithLock(
      [this](auto &interpreters) { interpreters.insert(interpreter.get()); });
}
ContextWrapper::~ContextWrapper() { Defunct(); }
void ContextWrapper::Defunct() {
  if (!defunct_) {
    session_context.interpreter_context->interpreters.WithLock(
        [this](auto &interpreters) { interpreters.erase(interpreter.get()); });
    defunct_ = true;
  }
}
ContextWrapper::ContextWrapper(ContextWrapper &&in) noexcept
    : session_context(std::move(in.session_context)), interpreter(std::move(in.interpreter)), defunct_(in.defunct_) {
  in.defunct_ = true;
}
ContextWrapper &ContextWrapper::operator=(ContextWrapper &&in) noexcept {
  if (this != &in) {
    Defunct();
    session_context = std::move(in.session_context);
    interpreter = std::move(in.interpreter);
    defunct_ = in.defunct_;
    in.defunct_ = true;
  }
  return *this;
}
memgraph::query::InterpreterContext *ContextWrapper::interpreter_context() {
  return session_context.interpreter_context.get();
}
memgraph::query::Interpreter *ContextWrapper::interp() { return interpreter.get(); }
memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *ContextWrapper::auth()
    const {
  return session_context.auth;
}
std::string ContextWrapper::run_id() const { return session_context.run_id; }
bool ContextWrapper::defunct() const { return defunct_; }
#ifdef MG_ENTERPRISE
memgraph::audit::Log *ContextWrapper::audit_log() const { return session_context.audit_log; }
#endif
std::vector<memgraph::communication::bolt::Value> TypedValueResultStreamBase::DecodeValues(
    const std::vector<memgraph::query::TypedValue> &values) const {
  std::vector<memgraph::communication::bolt::Value> decoded_values;
  decoded_values.reserve(values.size());
  for (const auto &v : values) {
    auto maybe_value = memgraph::glue::ToBoltValue(v, *interpreter_context_->db, memgraph::storage::View::NEW);
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
TypedValueResultStreamBase::TypedValueResultStreamBase(memgraph::query::InterpreterContext *interpreterContext)
    : interpreter_context_(interpreterContext) {}
std::map<std::string, memgraph::communication::bolt::Value> SessionHL::DecodeSummary(
    const std::map<std::string, memgraph::query::TypedValue> &summary) {
  std::map<std::string, memgraph::communication::bolt::Value> decoded_summary;
  for (const auto &kv : summary) {
    auto maybe_value = memgraph::glue::ToBoltValue(kv.second, *interpreter_context_->db, memgraph::storage::View::NEW);
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

  // Clean up previous session (session gets defunct when switching between databases)
  if (defunct_) {
    defunct_.reset();
  }

  return decoded_summary;
}
