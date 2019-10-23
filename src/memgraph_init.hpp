/// @file
#pragma once

#include <csignal>
#include <filesystem>
#include <map>
#include <optional>
#include <string>

#include <gflags/gflags.h>

#include "audit/log.hpp"
#include "auth/auth.hpp"
#include "communication/bolt/v1/exceptions.hpp"
#include "communication/bolt/v1/session.hpp"
#include "communication/init.hpp"
#include "communication/session.hpp"
#include "query/interpreter.hpp"
#include "query/transaction_engine.hpp"

#ifdef MG_SINGLE_NODE_V2
namespace database {
using GraphDb = storage::Storage;
}
#endif

DECLARE_string(durability_directory);

/// Encapsulates Dbms and Interpreter that are passed through the network server
/// and worker to the session.
struct SessionData {
  // Explicit constructor here to ensure that pointers to all objects are
  // supplied.
  SessionData(database::GraphDb *_db,
              query::InterpreterContext *_interpreter_context,
              auth::Auth *_auth, audit::Log *_audit_log)
      : db(_db),
        interpreter_context(_interpreter_context),
        auth(_auth),
        audit_log(_audit_log) {}
  database::GraphDb *db;
  query::InterpreterContext *interpreter_context;
  auth::Auth *auth;
  audit::Log *audit_log;
};

class BoltSession final
    : public communication::bolt::Session<communication::InputStream,
                                          communication::OutputStream> {
 public:
  BoltSession(SessionData *data, const io::network::Endpoint &endpoint,
              communication::InputStream *input_stream,
              communication::OutputStream *output_stream);

  using communication::bolt::Session<communication::InputStream,
                                     communication::OutputStream>::TEncoder;

  std::vector<std::string> Interpret(
      const std::string &query,
      const std::map<std::string, communication::bolt::Value> &params) override;

  std::map<std::string, communication::bolt::Value> PullAll(
      TEncoder *encoder) override;

  void Abort() override;

  bool Authenticate(const std::string &username,
                    const std::string &password) override;

 private:
  /// Wrapper around TEncoder which converts TypedValue to Value
  /// before forwarding the calls to original TEncoder.
  class TypedValueResultStream {
   public:
#ifdef MG_SINGLE_NODE_V2
    TypedValueResultStream(TEncoder *encoder, const storage::Storage *db);
#else
    TypedValueResultStream(TEncoder *encoder);
#endif

    void Result(const std::vector<query::TypedValue> &values);

   private:
    TEncoder *encoder_;
#ifdef MG_SINGLE_NODE_V2
    // NOTE: Needed only for ToBoltValue conversions
    const storage::Storage *db_;
#endif
  };

#ifdef MG_SINGLE_NODE_V2
  // NOTE: Needed only for ToBoltValue conversions
  const storage::Storage *db_;
#endif
  query::Interpreter interpreter_;
  query::TransactionEngine transaction_engine_;
#ifndef MG_SINGLE_NODE_HA
  auth::Auth *auth_;
  std::optional<auth::User> user_;
  audit::Log *audit_log_;
#endif
  io::network::Endpoint endpoint_;
};

/// Class that implements ResultStream API for Kafka.
///
/// Kafka doesn't need to stream the import results back to the client so we
/// don't need any functionality here.
class KafkaResultStream {
 public:
  void Result(const std::vector<query::TypedValue> &) {}
};

/// Writes data streamed from kafka to memgraph.
void KafkaStreamWriter(
    SessionData &session_data, const std::string &query,
    const std::map<std::string, communication::bolt::Value> &params);

/// Set up signal handlers and register `shutdown` on SIGTERM and SIGINT.
/// In most cases you don't have to call this. If you are using a custom server
/// startup function for `WithInit`, then you probably need to use this to
/// shutdown your server.
void InitSignalHandlers(const std::function<void()> &shutdown_fun);

/// Run the Memgraph server.
///
/// Sets up all the required state before running `memgraph_main` and does any
/// required cleanup afterwards.
///
/// Command line arguments and configuration files are read before calling any
/// of the supplied functions. Therefore, you should use flags only from those
/// functions, and *not before* invoking `WithInit`.
///
/// This should be the first and last thing a OS specific main function does.
///
/// A common example of usage is:
///
/// @code
/// int main(int argc, char *argv[]) {
///   return WithInit(argc, argv, SingleNodeMain);
/// }
/// @endcode
///
/// If you wish to start Memgraph server in another way, you can pass a
/// `memgraph_main` functions which does that. You should take care to call
/// `InitSignalHandlers` with appropriate function to shutdown the server you
/// started.
int WithInit(int argc, char **argv, const std::function<void()> &memgraph_main);
