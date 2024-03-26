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

#include <cstdint>
#include "audit/log.hpp"
#include "auth/auth.hpp"
#include "communication/websocket/auth.hpp"
#include "communication/websocket/server.hpp"
#include "coordination/coordinator_handlers.hpp"
#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "dbms/inmemory/replication_handlers.hpp"
#include "flags/all.hpp"
#include "glue/MonitoringServerT.hpp"
#include "glue/ServerT.hpp"
#include "glue/auth_checker.hpp"
#include "glue/auth_handler.hpp"
#include "glue/run_id.hpp"
#include "helpers.hpp"
#include "license/license_sender.hpp"
#include "memory/global_memory_control.hpp"
#include "query/auth_checker.hpp"
#include "query/auth_query_handler.hpp"
#include "query/config.hpp"
#include "query/discard_value_stream.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/procedure/callable_alias_mapper.hpp"
#include "query/procedure/module.hpp"
#include "query/procedure/py_module.hpp"
#include "replication_handler/replication_handler.hpp"
#include "replication_handler/system_replication.hpp"
#include "requests/requests.hpp"
#include "storage/v2/durability/durability.hpp"
#include "system/system.hpp"
#include "telemetry/telemetry.hpp"
#include "utils/signals.hpp"
#include "utils/sysinfo/memory.hpp"
#include "utils/system_info.hpp"
#include "utils/terminate_handler.hpp"
#include "version.hpp"

namespace {
constexpr const char *kMgUser = "MEMGRAPH_USER";
constexpr const char *kMgPassword = "MEMGRAPH_PASSWORD";
constexpr const char *kMgPassfile = "MEMGRAPH_PASSFILE";
constexpr uint64_t kMgVmMaxMapCount = 262144;

// TODO: move elsewhere so that we can remove need of interpreter.hpp
void InitFromCypherlFile(memgraph::query::InterpreterContext &ctx, memgraph::dbms::DatabaseAccess &db_acc,
                         std::string cypherl_file_path, memgraph::audit::Log *audit_log = nullptr) {
  memgraph::query::Interpreter interpreter(&ctx, db_acc);
  // Temporary empty user
  // TODO: Double check with buda
  memgraph::query::AllowEverythingAuthChecker tmp_auth_checker;
  auto tmp_user = tmp_auth_checker.GenQueryUser(std::nullopt, std::nullopt);
  interpreter.SetUser(tmp_user);

  std::ifstream file(cypherl_file_path);
  if (!file.is_open()) {
    spdlog::trace("Could not find init file {}", cypherl_file_path);
    return;
  }

  std::string line;
  while (std::getline(file, line)) {
    if (!line.empty()) {
      try {
        auto results = interpreter.Prepare(line, {}, {});
        memgraph::query::DiscardValueResultStream stream;
        interpreter.Pull(&stream, {}, results.qid);
      } catch (const memgraph::query::UserAlreadyExistsException &e) {
        spdlog::warn("{} The rest of the init-file will be run.", e.what());
      }
      if (audit_log) {
        audit_log->Record("", "", line, {}, std::string{memgraph::dbms::kDefaultDB});
      }
    }
  }

  file.close();
}

using memgraph::communication::ServerContext;

// Needed to correctly handle memgraph destruction from a signal handler.
// Without having some sort of a flag, it is possible that a signal is handled
// when we are exiting main, inside destructors of database::GraphDb and
// similar. The signal handler may then initiate another shutdown on memgraph
// which is in half destructed state, causing invalid memory access and crash.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
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

  MG_ASSERT(memgraph::utils::SignalHandler::RegisterHandler(memgraph::utils::Signal::Terminate, shutdown,
                                                            block_shutdown_signals),
            "Unable to register SIGTERM handler!");
  MG_ASSERT(memgraph::utils::SignalHandler::RegisterHandler(memgraph::utils::Signal::Interupt, shutdown,
                                                            block_shutdown_signals),
            "Unable to register SIGINT handler!");
}
}  // namespace

int main(int argc, char **argv) {
  memgraph::memory::SetHooks();
  google::SetUsageMessage("Memgraph database server");
  gflags::SetVersionString(version_string);

  // Load config before parsing arguments, so that flags from the command line
  // overwrite the config.
  LoadConfig("memgraph");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_h) {
    gflags::ShowUsageWithFlags(argv[0]);
    exit(1);
  }

  memgraph::flags::InitializeLogger();
  memgraph::flags::InitializeExperimental();

  // Unhandled exception handler init.
  std::set_terminate(&memgraph::utils::TerminateHandler);

  // Initialize Python
  auto *program_name = Py_DecodeLocale(argv[0], nullptr);
  MG_ASSERT(program_name);
  // Set program name, so Python can find its way to runtime libraries relative
  // to executable.
  Py_SetProgramName(program_name);
  PyImport_AppendInittab("_mgp", &memgraph::query::procedure::PyInitMgpModule);
  Py_InitializeEx(0 /* = initsigs */);
  PyEval_InitThreads();
  Py_BEGIN_ALLOW_THREADS;

  // Add our Python modules to sys.path
  try {
    auto exe_path = memgraph::utils::GetExecutablePath();
    auto py_support_dir = exe_path.parent_path() / "python_support";
    if (std::filesystem::is_directory(py_support_dir)) {
      auto gil = memgraph::py::EnsureGIL();
      auto maybe_exc = memgraph::py::AppendToSysPath(py_support_dir.c_str());
      if (maybe_exc) {
        spdlog::error(memgraph::utils::MessageWithLink("Unable to load support for embedded Python: {}.", *maybe_exc,
                                                       "https://memgr.ph/python"));
      } else {
        // Change how we load dynamic libraries on Python by using RTLD_NOW and
        // RTLD_DEEPBIND flags. This solves an issue with using the wrong version of
        // libstd.
        auto gil = memgraph::py::EnsureGIL();
        // NOLINTNEXTLINE(hicpp-signed-bitwise)
        auto *flag = PyLong_FromLong(RTLD_NOW);
        auto *setdl = PySys_GetObject("setdlopenflags");
        MG_ASSERT(setdl);
        auto *arg = PyTuple_New(1);
        MG_ASSERT(arg);
        MG_ASSERT(PyTuple_SetItem(arg, 0, flag) == 0);
        PyObject_CallObject(setdl, arg);
        Py_DECREF(flag);
        Py_DECREF(setdl);
        Py_DECREF(arg);
      }
    } else {
      spdlog::error(
          memgraph::utils::MessageWithLink("Unable to load support for embedded Python: missing directory {}.",
                                           py_support_dir, "https://memgr.ph/python"));
    }
  } catch (const std::filesystem::filesystem_error &e) {
    spdlog::error(memgraph::utils::MessageWithLink("Unable to load support for embedded Python: {}.", e.what(),
                                                   "https://memgr.ph/python"));
  }

  memgraph::utils::Scheduler python_gc_scheduler;
  python_gc_scheduler.Run("Python GC", std::chrono::seconds(FLAGS_storage_python_gc_cycle_sec),
                          [] { memgraph::query::procedure::PyCollectGarbage(); });

  // Initialize the communication library.
  memgraph::communication::SSLInit sslInit;

  // Initialize the requests library.
  memgraph::requests::Init();

  // Start memory warning logger.
  memgraph::utils::Scheduler mem_log_scheduler;
  if (FLAGS_memory_warning_threshold > 0) {
    auto free_ram = memgraph::utils::sysinfo::AvailableMemory();
    if (free_ram) {
      mem_log_scheduler.Run("Memory warning", std::chrono::seconds(3), [] {
        auto free_ram = memgraph::utils::sysinfo::AvailableMemory();
        if (free_ram && *free_ram / 1024 < FLAGS_memory_warning_threshold)
          spdlog::warn(memgraph::utils::MessageWithLink("Running out of available RAM, only {} MB left.",
                                                        *free_ram / 1024, "https://memgr.ph/ram"));
      });
    } else {
      // Kernel version for the `MemAvailable` value is from: man procfs
      spdlog::warn(
          "You have an older kernel version (<3.14) or the /proc "
          "filesystem isn't available so remaining memory warnings "
          "won't be available.");
    }
  }
  std::cout << "You are running Memgraph v" << gflags::VersionString() << std::endl;
  std::cout << "To get started with Memgraph, visit https://memgr.ph/start" << std::endl;

  const auto vm_max_map_count = memgraph::utils::GetVmMaxMapCount();
  if (vm_max_map_count.has_value()) {
    if (vm_max_map_count.value() < kMgVmMaxMapCount) {
      std::cout << "Max virtual memory areas vm.max_map_count " << vm_max_map_count.value()
                << " is too low, increase to at least " << kMgVmMaxMapCount << std::endl;
    }
  } else {
    std::cout << "Can't get info on vm.max_map_count, check whether it is too low, vm.max_map_count is at least "
              << kMgVmMaxMapCount << std::endl;
  }

  auto data_directory = std::filesystem::path(FLAGS_data_directory);

  memgraph::utils::EnsureDirOrDie(data_directory);
  // Verify that the user that started the process is the same user that is
  // the owner of the storage directory.
  memgraph::storage::durability::VerifyStorageDirectoryOwnerAndProcessUserOrDie(data_directory);
  // Create the lock file and open a handle to it. This will crash the
  // database if it can't open the file for writing or if any other process is
  // holding the file opened.
  memgraph::utils::OutputFile lock_file_handle;
  lock_file_handle.Open(data_directory / ".lock", memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);
  MG_ASSERT(lock_file_handle.AcquireLock(),
            "Couldn't acquire lock on the storage directory {}"
            "!\nAnother Memgraph process is currently running with the same "
            "storage directory, please stop it first before starting this "
            "process!",
            data_directory);

  const auto memory_limit = memgraph::flags::GetMemoryLimit();
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
  spdlog::info("Memory limit in config is set to {}", memgraph::utils::GetReadableSize(memory_limit));
  memgraph::utils::total_memory_tracker.SetMaximumHardLimit(memory_limit);
  memgraph::utils::total_memory_tracker.SetHardLimit(memory_limit);

  memgraph::utils::global_settings.Initialize(data_directory / "settings");
  memgraph::utils::OnScopeExit settings_finalizer([&] { memgraph::utils::global_settings.Finalize(); });

  // register all runtime settings
  memgraph::license::RegisterLicenseSettings(memgraph::license::global_license_checker,
                                             memgraph::utils::global_settings);
  memgraph::utils::OnScopeExit global_license_finalizer([] { memgraph::license::global_license_checker.Finalize(); });

  memgraph::flags::run_time::Initialize();

  memgraph::license::global_license_checker.CheckEnvLicense();
  if (!FLAGS_organization_name.empty() && !FLAGS_license_key.empty()) {
    memgraph::license::global_license_checker.SetLicenseInfoOverride(FLAGS_license_key, FLAGS_organization_name);
  }

  memgraph::license::global_license_checker.StartBackgroundLicenseChecker(memgraph::utils::global_settings);

  // All enterprise features should be constructed before the main database
  // storage. This will cause them to be destructed *after* the main database
  // storage. That way any errors that happen during enterprise features
  // destruction won't have an impact on the storage engine.
  // Example: When the main storage is destructed it makes a snapshot. When
  // audit logging is destructed it syncs all pending data to disk and that can
  // fail. That is why it must be destructed *after* the main database storage
  // to minimise the impact of their failure on the main storage.

  // Begin enterprise features initialization

#ifdef MG_ENTERPRISE
  // Audit log
  memgraph::audit::Log audit_log{data_directory / "audit", FLAGS_audit_buffer_size,
                                 FLAGS_audit_buffer_flush_interval_ms};
  // Start the log if enabled.
  if (FLAGS_audit_enabled) {
    audit_log.Start();
  }
  // Setup SIGUSR2 to be used for reopening audit log files, when e.g. logrotate
  // rotates our audit logs.
  MG_ASSERT(memgraph::utils::SignalHandler::RegisterHandler(memgraph::utils::Signal::User2,
                                                            [&audit_log]() { audit_log.ReopenLog(); }),
            "Unable to register SIGUSR2 handler!");

  // End enterprise features initialization
#endif

  // Main storage and execution engines initialization
  memgraph::storage::Config db_config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC,
             .interval = std::chrono::seconds(FLAGS_storage_gc_cycle_sec)},

      .durability = {.storage_directory = FLAGS_data_directory,
                     .recover_on_startup = FLAGS_storage_recover_on_startup || FLAGS_data_recovery_on_startup,
                     .snapshot_retention_count = FLAGS_storage_snapshot_retention_count,
                     .wal_file_size_kibibytes = FLAGS_storage_wal_file_size_kib,
                     .wal_file_flush_every_n_tx = FLAGS_storage_wal_file_flush_every_n_tx,
                     .snapshot_on_exit = FLAGS_storage_snapshot_on_exit,
                     .restore_replication_state_on_startup = FLAGS_replication_restore_state_on_startup,
                     .items_per_batch = FLAGS_storage_items_per_batch,
                     .recovery_thread_count = FLAGS_storage_recovery_thread_count,
                     // deprecated
                     .allow_parallel_index_creation = FLAGS_storage_parallel_index_recovery,
                     .allow_parallel_schema_creation = FLAGS_storage_parallel_schema_recovery},
      .transaction = {.isolation_level = memgraph::flags::ParseIsolationLevel()},
      .disk = {.main_storage_directory = FLAGS_data_directory + "/rocksdb_main_storage",
               .label_index_directory = FLAGS_data_directory + "/rocksdb_label_index",
               .label_property_index_directory = FLAGS_data_directory + "/rocksdb_label_property_index",
               .unique_constraints_directory = FLAGS_data_directory + "/rocksdb_unique_constraints",
               .name_id_mapper_directory = FLAGS_data_directory + "/rocksdb_name_id_mapper",
               .id_name_mapper_directory = FLAGS_data_directory + "/rocksdb_id_name_mapper",
               .durability_directory = FLAGS_data_directory + "/rocksdb_durability",
               .wal_directory = FLAGS_data_directory + "/rocksdb_wal"},
      .salient.items = {.properties_on_edges = FLAGS_storage_properties_on_edges,
                        .enable_schema_metadata = FLAGS_storage_enable_schema_metadata,
                        .delta_on_identical_property_update = FLAGS_storage_delta_on_identical_property_update},
      .salient.storage_mode = memgraph::flags::ParseStorageMode()};
  spdlog::info("config recover on startup {}, flags {} {}", db_config.durability.recover_on_startup,
               FLAGS_storage_recover_on_startup, FLAGS_data_recovery_on_startup);
  memgraph::utils::Scheduler jemalloc_purge_scheduler;
  jemalloc_purge_scheduler.Run("Jemalloc purge", std::chrono::seconds(FLAGS_storage_gc_cycle_sec),
                               [] { memgraph::memory::PurgeUnusedMemory(); });

  if (FLAGS_storage_snapshot_interval_sec == 0) {
    if (FLAGS_storage_wal_enabled) {
      LOG_FATAL(
          "In order to use write-ahead-logging you must enable "
          "periodic snapshots by setting the snapshot interval to a "
          "value larger than 0!");
      db_config.durability.snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::DISABLED;
    }
  } else {
    if (FLAGS_storage_wal_enabled) {
      db_config.durability.snapshot_wal_mode =
          memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    } else {
      db_config.durability.snapshot_wal_mode =
          memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT;
    }
    db_config.durability.snapshot_interval = std::chrono::seconds(FLAGS_storage_snapshot_interval_sec);
  }

  // Default interpreter configuration
  memgraph::query::InterpreterConfig interp_config{
      .query = {.allow_load_csv = FLAGS_allow_load_csv},
      .replication_replica_check_frequency = std::chrono::seconds(FLAGS_replication_replica_check_frequency_sec),
#ifdef MG_ENTERPRISE
      .instance_down_timeout_sec = std::chrono::seconds(FLAGS_instance_down_timeout_sec),
      .instance_health_check_frequency_sec = std::chrono::seconds(FLAGS_instance_health_check_frequency_sec),
      .instance_get_uuid_frequency_sec = std::chrono::seconds(FLAGS_instance_get_uuid_frequency_sec),
#endif
      .default_kafka_bootstrap_servers = FLAGS_kafka_bootstrap_servers,
      .default_pulsar_service_url = FLAGS_pulsar_service_url,
      .stream_transaction_conflict_retries = FLAGS_stream_transaction_conflict_retries,
      .stream_transaction_retry_interval = std::chrono::milliseconds(FLAGS_stream_transaction_retry_interval)};

  auto auth_glue = [](memgraph::auth::SynchedAuth *auth, std::unique_ptr<memgraph::query::AuthQueryHandler> &ah,
                      std::unique_ptr<memgraph::query::AuthChecker> &ac) {
    // Glue high level auth implementations to the query side
    ah = std::make_unique<memgraph::glue::AuthQueryHandler>(auth);
    ac = std::make_unique<memgraph::glue::AuthChecker>(auth);
    // Handle users passed via arguments
    auto *maybe_username = std::getenv(kMgUser);
    auto *maybe_password = std::getenv(kMgPassword);
    auto *maybe_pass_file = std::getenv(kMgPassfile);
    if (maybe_username && maybe_password) {
      ah->CreateUser(maybe_username, maybe_password, nullptr);
    } else if (maybe_pass_file) {
      const auto [username, password] = LoadUsernameAndPassword(maybe_pass_file);
      if (!username.empty() && !password.empty()) {
        ah->CreateUser(username, password, nullptr);
      }
    }
  };

  memgraph::auth::Auth::Config auth_config{FLAGS_auth_user_or_role_name_regex, FLAGS_auth_password_strength_regex,
                                           FLAGS_auth_password_permit_null};
  memgraph::auth::SynchedAuth auth_{data_directory / "auth", auth_config};
  std::unique_ptr<memgraph::query::AuthQueryHandler> auth_handler;
  std::unique_ptr<memgraph::query::AuthChecker> auth_checker;
  auth_glue(&auth_, auth_handler, auth_checker);

  auto system = memgraph::system::System{db_config.durability.storage_directory, FLAGS_data_recovery_on_startup};

  // singleton replication state
  memgraph::replication::ReplicationState repl_state{ReplicationStateRootPath(db_config)};

  // singleton coordinator state
#ifdef MG_ENTERPRISE
  memgraph::coordination::CoordinatorState coordinator_state;
#endif

  memgraph::dbms::DbmsHandler dbms_handler(db_config, repl_state
#ifdef MG_ENTERPRISE
                                           ,
                                           auth_, FLAGS_data_recovery_on_startup
#endif
  );

  // Note: Now that all system's subsystems are initialised (dbms & auth)
  //       We can now initialise the recovery of replication (which will include those subsystems)
  //       ReplicationHandler will handle the recovery
  auto replication_handler = memgraph::replication::ReplicationHandler{repl_state, dbms_handler
#ifdef MG_ENTERPRISE
                                                                       ,
                                                                       system, auth_
#endif
  };

#ifdef MG_ENTERPRISE
  // MAIN or REPLICA instance
  if (FLAGS_management_port) {
    memgraph::dbms::CoordinatorHandlers::Register(coordinator_state.GetCoordinatorServer(), replication_handler);
    MG_ASSERT(coordinator_state.GetCoordinatorServer().Start(), "Failed to start coordinator server!");
  }
#endif

  auto db_acc = dbms_handler.Get();

  memgraph::query::InterpreterContext interpreter_context_(interp_config, &dbms_handler, &repl_state, system,
#ifdef MG_ENTERPRISE
                                                           &coordinator_state,
#endif
                                                           auth_handler.get(), auth_checker.get(),
                                                           &replication_handler);
  MG_ASSERT(db_acc, "Failed to access the main database");

  memgraph::query::procedure::gModuleRegistry.SetModulesDirectory(memgraph::flags::ParseQueryModulesDirectory(),
                                                                  FLAGS_data_directory);
  memgraph::query::procedure::gModuleRegistry.UnloadAndLoadModulesFromDirectories();
  memgraph::query::procedure::gCallableAliasMapper.LoadMapping(FLAGS_query_callable_mappings_path);

  // TODO Make multi-tenant
  if (!FLAGS_init_file.empty()) {
    spdlog::info("Running init file...");
#ifdef MG_ENTERPRISE
    if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
      InitFromCypherlFile(interpreter_context_, db_acc, FLAGS_init_file, &audit_log);
    } else {
      InitFromCypherlFile(interpreter_context_, db_acc, FLAGS_init_file);
    }
#else
    InitFromCypherlFile(interpreter_context_, db_acc, FLAGS_init_file);
#endif
  }

#ifdef MG_ENTERPRISE
  dbms_handler.RestoreTriggers(&interpreter_context_);
  dbms_handler.RestoreStreams(&interpreter_context_);
#else
  {
    // Triggers can execute query procedures, so we need to reload the modules first and then
    // the triggers
    auto storage_accessor = db_acc->Access();
    auto dba = memgraph::query::DbAccessor{storage_accessor.get()};
    db_acc->trigger_store()->RestoreTriggers(&interpreter_context_.ast_cache, &dba, interpreter_context_.config.query,
                                             interpreter_context_.auth_checker);
  }

  // As the Stream transformations are using modules, they have to be restored after the query modules are loaded.
  db_acc->streams()->RestoreStreams(db_acc, &interpreter_context_);
#endif

  ServerContext context;
  std::string service_name = "Bolt";
  if (!FLAGS_bolt_key_file.empty() && !FLAGS_bolt_cert_file.empty()) {
    context = ServerContext(FLAGS_bolt_key_file, FLAGS_bolt_cert_file);
    service_name = "BoltS";
    spdlog::info("Using secure Bolt connection (with SSL)");
  } else {
    spdlog::warn(
        memgraph::utils::MessageWithLink("Using non-secure Bolt connection (without SSL).", "https://memgr.ph/ssl"));
  }
  auto server_endpoint = memgraph::communication::v2::ServerEndpoint{
      boost::asio::ip::address::from_string(FLAGS_bolt_address), static_cast<uint16_t>(FLAGS_bolt_port)};
#ifdef MG_ENTERPRISE
  Context session_context{&interpreter_context_, &auth_, &audit_log};
#else
  Context session_context{&interpreter_context_, &auth_};
#endif
  memgraph::glue::ServerT server(server_endpoint, &session_context, &context, FLAGS_bolt_session_inactivity_timeout,
                                 service_name, FLAGS_bolt_num_workers);

  const auto machine_id = memgraph::utils::GetMachineId();

  // Setup telemetry
  static constexpr auto telemetry_server{"https://telemetry.memgraph.com/88b5e7e8-746a-11e8-9f85-538a9e9690cc/"};
  std::optional<memgraph::telemetry::Telemetry> telemetry;
  if (FLAGS_telemetry_enabled) {
    telemetry.emplace(telemetry_server, data_directory / "telemetry", memgraph::glue::run_id_, machine_id,
                      service_name == "BoltS", FLAGS_data_directory, std::chrono::minutes(10));
    telemetry->AddStorageCollector(dbms_handler, auth_, repl_state);
#ifdef MG_ENTERPRISE
    telemetry->AddDatabaseCollector(dbms_handler, repl_state);
#else
    telemetry->AddDatabaseCollector();
#endif
    telemetry->AddClientCollector();
    telemetry->AddEventsCollector();
    telemetry->AddQueryModuleCollector();
    telemetry->AddExceptionCollector();
    telemetry->AddReplicationCollector();
  }
  memgraph::license::LicenseInfoSender license_info_sender(telemetry_server, memgraph::glue::run_id_, machine_id,
                                                           memory_limit,
                                                           memgraph::license::global_license_checker.GetLicenseInfo());

  memgraph::communication::websocket::SafeAuth websocket_auth{&auth_};
  memgraph::communication::websocket::Server websocket_server{
      {FLAGS_monitoring_address, static_cast<uint16_t>(FLAGS_monitoring_port)}, &context, websocket_auth};
  memgraph::flags::AddLoggerSink(websocket_server.GetLoggingSink());

#ifdef MG_ENTERPRISE
  // TODO: Make multi-tenant
  memgraph::glue::MonitoringServerT metrics_server{
      {FLAGS_metrics_address, static_cast<uint16_t>(FLAGS_metrics_port)}, db_acc->storage(), &context};
#endif

  // Handler for regular termination signals
  auto shutdown = [
#ifdef MG_ENTERPRISE
                      &metrics_server,
#endif
                      &websocket_server, &server, &interpreter_context_] {
    // Server needs to be shutdown first and then the database. This prevents
    // a race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
    // After the server is notified to stop accepting and processing
    // connections we tell the execution engine to stop processing all pending
    // queries.
    interpreter_context_.Shutdown();
    websocket_server.Shutdown();
#ifdef MG_ENTERPRISE
    metrics_server.Shutdown();
#endif
  };

  InitSignalHandlers(shutdown);

  // Release the temporary database access
  db_acc.reset();

  // Startup the main server
  MG_ASSERT(server.Start(), "Couldn't start the Bolt server!");
  websocket_server.Start();

#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    metrics_server.Start();
  }
#endif

  if (!FLAGS_init_data_file.empty()) {
    spdlog::info("Running init data file.");
    auto db_acc = dbms_handler.Get();
    MG_ASSERT(db_acc, "Failed to gain access to the main database");
#ifdef MG_ENTERPRISE
    if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
      InitFromCypherlFile(interpreter_context_, db_acc, FLAGS_init_data_file, &audit_log);
    } else {
      InitFromCypherlFile(interpreter_context_, db_acc, FLAGS_init_data_file);
    }
#else
    InitFromCypherlFile(interpreter_context_, db_acc, FLAGS_init_data_file);
#endif
  }

  server.AwaitShutdown();
  websocket_server.AwaitShutdown();
  memgraph::memory::UnsetHooks();
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    metrics_server.AwaitShutdown();
  }
#endif

  memgraph::query::procedure::gModuleRegistry.UnloadAllModules();

  python_gc_scheduler.Stop();
  Py_END_ALLOW_THREADS;
  // Shutdown Python
  Py_Finalize();
  PyMem_RawFree(program_name);

  memgraph::utils::total_memory_tracker.LogPeakMemoryUsage();
  return 0;
}
