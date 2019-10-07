#include "memgraph_init.hpp"

#include <glog/logging.h>

#include "config.hpp"
#include "glue/auth.hpp"
#include "glue/communication.hpp"
#include "query/exceptions.hpp"
#include "requests/requests.hpp"
#include "storage/v2/view.hpp"
#include "utils/signals.hpp"
#include "utils/sysinfo/memory.hpp"
#include "utils/terminate_handler.hpp"
#include "version.hpp"

DEFINE_string(log_file, "", "Path to where the log should be stored.");
DEFINE_HIDDEN_string(
    log_link_basename, "",
    "Basename used for symlink creation to the last log file.");
DEFINE_uint64(memory_warning_threshold, 1024,
              "Memory warning threshold, in MB. If Memgraph detects there is "
              "less available RAM it will log a warning. Set to 0 to "
              "disable.");

BoltSession::BoltSession(SessionData *data,
                         const io::network::Endpoint &endpoint,
                         communication::InputStream *input_stream,
                         communication::OutputStream *output_stream)
    : communication::bolt::Session<communication::InputStream,
                                   communication::OutputStream>(input_stream,
                                                                output_stream),
#ifdef MG_SINGLE_NODE_V2
      db_(data->db),
#endif
      interpreter_(data->interpreter_context),
      transaction_engine_(data->db, &interpreter_),
#ifndef MG_SINGLE_NODE_HA
      auth_(data->auth),
      audit_log_(data->audit_log),
#endif
      endpoint_(endpoint) {
}

using TEncoder =
    communication::bolt::Session<communication::InputStream,
                                 communication::OutputStream>::TEncoder;

std::vector<std::string> BoltSession::Interpret(
    const std::string &query,
    const std::map<std::string, communication::bolt::Value> &params) {
  std::map<std::string, PropertyValue> params_pv;
  for (const auto &kv : params)
    params_pv.emplace(kv.first, glue::ToPropertyValue(kv.second));
#ifndef MG_SINGLE_NODE_HA
  audit_log_->Record(endpoint_.address(), user_ ? user_->username() : "", query,
                     PropertyValue(params_pv));
#endif
  try {
    auto result = transaction_engine_.Interpret(query, params_pv);
#ifndef MG_SINGLE_NODE_HA
    if (user_) {
      const auto &permissions = user_->GetPermissions();
      for (const auto &privilege : result.second) {
        if (permissions.Has(glue::PrivilegeToPermission(privilege)) !=
            auth::PermissionLevel::GRANT) {
          transaction_engine_.Abort();
          throw communication::bolt::ClientError(
              "You are not authorized to execute this query! Please contact "
              "your database administrator.");
        }
      }
    }
#endif
    return result.first;

  } catch (const query::QueryException &e) {
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw communication::bolt::ClientError(e.what());
  }
}

std::map<std::string, communication::bolt::Value> BoltSession::PullAll(
    TEncoder *encoder) {
  try {
#ifdef MG_SINGLE_NODE_V2
    TypedValueResultStream stream(encoder, db_);
#else
    TypedValueResultStream stream(encoder);
#endif
    const auto &summary = transaction_engine_.PullAll(&stream);
    std::map<std::string, communication::bolt::Value> decoded_summary;
    for (const auto &kv : summary) {
#ifdef MG_SINGLE_NODE_V2
      auto maybe_value = glue::ToBoltValue(kv.second, *db_, storage::View::NEW);
      if (maybe_value.HasError()) {
        switch (maybe_value.GetError()) {
          case storage::Error::DELETED_OBJECT:
          case storage::Error::SERIALIZATION_ERROR:
          case storage::Error::VERTEX_HAS_EDGES:
          case storage::Error::PROPERTIES_DISABLED:
            throw communication::bolt::ClientError(
                "Unexpected storage error when streaming summary.");
        }
      }
      decoded_summary.emplace(kv.first, std::move(*maybe_value));
#else
      decoded_summary.emplace(kv.first,
                              glue::ToBoltValue(kv.second, storage::View::NEW));
#endif
    }
    return decoded_summary;
  } catch (const query::QueryException &e) {
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw communication::bolt::ClientError(e.what());
  }
}

void BoltSession::Abort() { transaction_engine_.Abort(); }

bool BoltSession::Authenticate(const std::string &username,
                               const std::string &password) {
#ifdef MG_SINGLE_NODE_HA
  return true;
#else
  if (!auth_->HasUsers()) return true;
  user_ = auth_->Authenticate(username, password);
  return !!user_;
#endif
}

#ifdef MG_SINGLE_NODE_V2
BoltSession::TypedValueResultStream::TypedValueResultStream(
    TEncoder *encoder, const storage::Storage *db)
    : encoder_(encoder), db_(db) {}
#else
BoltSession::TypedValueResultStream::TypedValueResultStream(TEncoder *encoder)
    : encoder_(encoder) {}
#endif

void BoltSession::TypedValueResultStream::Result(
    const std::vector<query::TypedValue> &values) {
  std::vector<communication::bolt::Value> decoded_values;
  decoded_values.reserve(values.size());
  for (const auto &v : values) {
#ifdef MG_SINGLE_NODE_V2
    auto maybe_value = glue::ToBoltValue(v, *db_, storage::View::NEW);
    if (maybe_value.HasError()) {
      switch (maybe_value.GetError()) {
        case storage::Error::DELETED_OBJECT:
          throw communication::bolt::ClientError(
              "Returning a deleted object as a result.");
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::SERIALIZATION_ERROR:
        case storage::Error::PROPERTIES_DISABLED:
          throw communication::bolt::ClientError(
              "Unexpected storage error when streaming results.");
      }
    }
    decoded_values.emplace_back(std::move(*maybe_value));
#else
    decoded_values.push_back(glue::ToBoltValue(v, storage::View::NEW));
#endif
  }
  encoder_->MessageRecord(decoded_values);
}

void KafkaStreamWriter(
    SessionData &session_data, const std::string &query,
    const std::map<std::string, communication::bolt::Value> &params) {
  auto dba = session_data.db->Access();
  query::DbAccessor execution_dba(&dba);
  KafkaResultStream stream;
  std::map<std::string, PropertyValue> params_pv;
  for (const auto &kv : params)
    params_pv.emplace(kv.first, glue::ToPropertyValue(kv.second));
  try {
    query::Interpreter interpreter{session_data.interpreter_context};
    interpreter(query, &execution_dba, params_pv, false,
                utils::NewDeleteResource())
        .PullAll(stream);
#ifdef MG_SINGLE_NODE_V2
    auto maybe_constraint_violation = dba.Commit();
    if (maybe_constraint_violation.HasError()) {
      const auto &constraint_violation = maybe_constraint_violation.GetError();
      auto label_name = dba.LabelToName(constraint_violation.label);
      auto property_name = dba.PropertyToName(constraint_violation.property);
      LOG(WARNING) << fmt::format(
          "[Kafka] query execution failed with an exception: "
          "Unable to commit due to constraint violation on :{}({}).",
          label_name, property_name);
    }
#else
    dba.Commit();
#endif
  } catch (const utils::BasicException &e) {
    LOG(WARNING) << "[Kafka] query execution failed with an exception: "
                 << e.what();
    dba.Abort();
  }
};

// Needed to correctly handle memgraph destruction from a signal handler.
// Without having some sort of a flag, it is possible that a signal is handled
// when we are exiting main, inside destructors of database::GraphDb and
// similar. The signal handler may then initiate another shutdown on memgraph
// which is in half destructed state, causing invalid memory access and crash.
volatile sig_atomic_t is_shutting_down = 0;

void InitSignalHandlers(const std::function<void()> &shutdown_fun) {
  // Prevent handling shutdown inside a shutdown. For example, SIGINT handler
  // being interrupted by SIGTERM before is_shutting_down is set, thus causing
  // double shutdown.
  sigset_t block_shutdown_signals;
  sigemptyset(&block_shutdown_signals);
  sigaddset(&block_shutdown_signals, SIGTERM);
  sigaddset(&block_shutdown_signals, SIGINT);

  // Wrap the shutdown function in a safe way to prevent recursive shutdown.
  auto shutdown = [shutdown_fun]() {
    if (is_shutting_down) return;
    is_shutting_down = 1;
    shutdown_fun();
  };

  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::Terminate,
                                              shutdown, block_shutdown_signals))
      << "Unable to register SIGTERM handler!";
  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::Interupt, shutdown,
                                              block_shutdown_signals))
      << "Unable to register SIGINT handler!";

  // Setup SIGUSR1 to be used for reopening log files, when e.g. logrotate
  // rotates our logs.
  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::User1, []() {
    google::CloseLogDestination(google::INFO);
  })) << "Unable to register SIGUSR1 handler!";
}

int WithInit(int argc, char **argv,
             const std::function<void()> &memgraph_main) {
  gflags::SetVersionString(version_string);

  // Load config before parsing arguments, so that flags from the command line
  // overwrite the config.
  LoadConfig();
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  google::InitGoogleLogging(argv[0]);
  google::SetLogDestination(google::INFO, FLAGS_log_file.c_str());
  google::SetLogSymlink(google::INFO, FLAGS_log_link_basename.c_str());

  // Unhandled exception handler init.
  std::set_terminate(&utils::TerminateHandler);

  // Initialize the communication library.
  communication::Init();

  // Start memory warning logger.
  utils::Scheduler mem_log_scheduler;
  if (FLAGS_memory_warning_threshold > 0) {
    auto free_ram = utils::sysinfo::AvailableMemoryKilobytes();
    if (free_ram) {
      mem_log_scheduler.Run("Memory warning", std::chrono::seconds(3), [] {
        auto free_ram = utils::sysinfo::AvailableMemoryKilobytes();
        if (free_ram && *free_ram / 1024 < FLAGS_memory_warning_threshold)
          LOG(WARNING) << "Running out of available RAM, only "
                       << *free_ram / 1024 << " MB left.";
      });
    } else {
      // Kernel version for the `MemAvailable` value is from: man procfs
      LOG(WARNING) << "You have an older kernel version (<3.14) or the /proc "
                      "filesystem isn't available so remaining memory warnings "
                      "won't be available.";
    }
  }
  requests::Init();

  memgraph_main();
  return 0;
}
