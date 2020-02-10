#include "memgraph_init.hpp"

#include <glog/logging.h>

#include "config.hpp"
#include "glue/communication.hpp"
#include "py/py.hpp"
#include "query/exceptions.hpp"
#include "requests/requests.hpp"
#include "storage/v2/view.hpp"
#include "utils/signals.hpp"
#include "utils/sysinfo/memory.hpp"
#include "utils/terminate_handler.hpp"
#include "version.hpp"

#ifdef MG_ENTERPRISE
#include "glue/auth.hpp"
#endif

DEFINE_string(log_file, "", "Path to where the log should be stored.");
DEFINE_HIDDEN_string(
    log_link_basename, "",
    "Basename used for symlink creation to the last log file.");
DEFINE_uint64(memory_warning_threshold, 1024,
              "Memory warning threshold, in MB. If Memgraph detects there is "
              "less available RAM it will log a warning. Set to 0 to "
              "disable.");
DEFINE_string(bolt_server_name_for_init, "",
              "Server name which the database should send to the client in the "
              "Bolt INIT message.");

BoltSession::BoltSession(SessionData *data,
                         const io::network::Endpoint &endpoint,
                         communication::InputStream *input_stream,
                         communication::OutputStream *output_stream)
    : communication::bolt::Session<communication::InputStream,
                                   communication::OutputStream>(input_stream,
                                                                output_stream),
      db_(data->db),
      interpreter_(data->interpreter_context),
#ifdef MG_ENTERPRISE
#ifndef MG_SINGLE_NODE_HA
      auth_(data->auth),
      audit_log_(data->audit_log),
#endif
#endif
      endpoint_(endpoint) {
}

using TEncoder =
    communication::bolt::Session<communication::InputStream,
                                 communication::OutputStream>::TEncoder;

std::vector<std::string> BoltSession::Interpret(
    const std::string &query,
    const std::map<std::string, communication::bolt::Value> &params) {
  std::map<std::string, storage::PropertyValue> params_pv;
  for (const auto &kv : params)
    params_pv.emplace(kv.first, glue::ToPropertyValue(kv.second));
#ifdef MG_ENTERPRISE
#ifndef MG_SINGLE_NODE_HA
  audit_log_->Record(endpoint_.address(), user_ ? user_->username() : "", query,
                     storage::PropertyValue(params_pv));
#endif
#endif
  try {
    auto result = interpreter_.Prepare(query, params_pv);
#ifdef MG_ENTERPRISE
#ifndef MG_SINGLE_NODE_HA
    if (user_) {
      const auto &permissions = user_->GetPermissions();
      for (const auto &privilege : result.second) {
        if (permissions.Has(glue::PrivilegeToPermission(privilege)) !=
            auth::PermissionLevel::GRANT) {
          interpreter_.Abort();
          throw communication::bolt::ClientError(
              "You are not authorized to execute this query! Please contact "
              "your database administrator.");
        }
      }
    }
#endif
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
    TypedValueResultStream stream(encoder, db_);
    const auto &summary = interpreter_.PullAll(&stream);
    std::map<std::string, communication::bolt::Value> decoded_summary;
    for (const auto &kv : summary) {
      auto maybe_value = glue::ToBoltValue(kv.second, *db_, storage::View::NEW);
      if (maybe_value.HasError()) {
        switch (maybe_value.GetError()) {
          case storage::Error::DELETED_OBJECT:
          case storage::Error::SERIALIZATION_ERROR:
          case storage::Error::VERTEX_HAS_EDGES:
          case storage::Error::PROPERTIES_DISABLED:
          case storage::Error::NONEXISTENT_OBJECT:
            throw communication::bolt::ClientError(
                "Unexpected storage error when streaming summary.");
        }
      }
      decoded_summary.emplace(kv.first, std::move(*maybe_value));
    }
    return decoded_summary;
  } catch (const query::QueryException &e) {
    // Wrap QueryException into ClientError, because we want to allow the
    // client to fix their query.
    throw communication::bolt::ClientError(e.what());
  }
}

void BoltSession::Abort() { interpreter_.Abort(); }

bool BoltSession::Authenticate(const std::string &username,
                               const std::string &password) {
#ifdef MG_ENTERPRISE
#ifdef MG_SINGLE_NODE_HA
  return true;
#else
  if (!auth_->HasUsers()) return true;
  user_ = auth_->Authenticate(username, password);
  return !!user_;
#endif
#else
  return true;
#endif
}

std::optional<std::string> BoltSession::GetServerNameForInit() {
  if (FLAGS_bolt_server_name_for_init.empty()) return std::nullopt;
  return FLAGS_bolt_server_name_for_init;
}

BoltSession::TypedValueResultStream::TypedValueResultStream(
    TEncoder *encoder, const storage::Storage *db)
    : encoder_(encoder), db_(db) {}

void BoltSession::TypedValueResultStream::Result(
    const std::vector<query::TypedValue> &values) {
  std::vector<communication::bolt::Value> decoded_values;
  decoded_values.reserve(values.size());
  for (const auto &v : values) {
    auto maybe_value = glue::ToBoltValue(v, *db_, storage::View::NEW);
    if (maybe_value.HasError()) {
      switch (maybe_value.GetError()) {
        case storage::Error::DELETED_OBJECT:
          throw communication::bolt::ClientError(
              "Returning a deleted object as a result.");
        case storage::Error::NONEXISTENT_OBJECT:
          throw communication::bolt::ClientError(
              "Returning a nonexistent object as a result.");
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::SERIALIZATION_ERROR:
        case storage::Error::PROPERTIES_DISABLED:
          throw communication::bolt::ClientError(
              "Unexpected storage error when streaming results.");
      }
    }
    decoded_values.emplace_back(std::move(*maybe_value));
  }
  encoder_->MessageRecord(decoded_values);
}

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

  // Initialize Python
  auto *program_name = Py_DecodeLocale(argv[0], nullptr);
  CHECK(program_name);
  // Set program name, so Python can find its way to runtime libraries relative
  // to executable.
  Py_SetProgramName(program_name);
  Py_InitializeEx(0 /* = initsigs */);
  PyEval_InitThreads();
  Py_BEGIN_ALLOW_THREADS;
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
  Py_END_ALLOW_THREADS;
  // Shutdown Python
  Py_Finalize();
  PyMem_RawFree(program_name);
  return 0;
}
