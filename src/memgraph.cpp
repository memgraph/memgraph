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

#include <algorithm>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#include "audit/log.hpp"
#include "auth/auth.hpp"
#include "auth/profiles/user_profiles.hpp"
#include "communication/v2/server.hpp"
#include "communication/websocket/auth.hpp"
#include "communication/websocket/server.hpp"
#include "coordination/data_instance_management_server_handlers.hpp"
#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "flags/all.hpp"
#include "flags/bolt.hpp"
#include "flags/coord_flag_env_handler.hpp"
#include "flags/coordination.hpp"
#include "flags/experimental.hpp"
#include "flags/general.hpp"
#include "flags/log_level.hpp"
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
#include "replication/state.hpp"
#include "replication_handler/replication_handler.hpp"
#include "requests/requests.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/storage_mode.hpp"
#include "system/system.hpp"
#include "telemetry/telemetry.hpp"
#include "utils/event_gauge.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/readable_size.hpp"
#include "utils/resource_monitoring.hpp"
#include "utils/scheduler.hpp"
#include "utils/signals.hpp"
#include "utils/stat.hpp"
#include "utils/sysinfo/memory.hpp"
#include "utils/system_info.hpp"
#include "utils/terminate_handler.hpp"
#include "version.hpp"

#include <spdlog/spdlog.h>
#include <boost/asio/ip/address.hpp>

namespace memgraph::metrics {
extern const Event PeakMemoryRes;
}  // namespace memgraph::metrics

namespace {
constexpr const char *kMgUser = "MEMGRAPH_USER";
constexpr const char *kMgPassword = "MEMGRAPH_PASSWORD";
constexpr const char *kMgPassfile = "MEMGRAPH_PASSFILE";

constexpr const char *kMgExperimentalEnabled = "MEMGRAPH_EXPERIMENTAL_ENABLED";
constexpr const char *kMgBoltPort = "MEMGRAPH_BOLT_PORT";
constexpr const char *kMgHaClusterInitQueries = "MEMGRAPH_HA_CLUSTER_INIT_QUERIES";

constexpr uint64_t kMgVmMaxMapCount = 262144;

// TODO: move elsewhere so that we can remove need of interpreter.hpp
void InitFromCypherlFile(memgraph::query::InterpreterContext &ctx, memgraph::dbms::DatabaseAccess &db_acc,
                         std::string cypherl_file_path, memgraph::audit::Log *audit_log = nullptr) {
  memgraph::query::Interpreter interpreter(&ctx, db_acc);
  // Temporary empty user
  // TODO: Double check with buda
  memgraph::query::AllowEverythingAuthChecker tmp_auth_checker;
  auto tmp_user = tmp_auth_checker.GenEmptyUser();
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
        // TODO remove security issue
        spdlog::trace("Executing line: {}", line);
        auto results = interpreter.Prepare(line, memgraph::query::no_params_fn, {});
        memgraph::query::DiscardValueResultStream stream;
        interpreter.Pull(&stream, {}, results.qid);
      } catch (std::exception const &e) {
        spdlog::warn("Exception occurred while executing one line. The rest of the init-file will be run. {}",
                     e.what());
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

  auto flags_experimental = memgraph::flags::ReadExperimental(FLAGS_experimental_enabled);
  memgraph::flags::SetExperimental(flags_experimental);
  auto *maybe_experimental = std::getenv(kMgExperimentalEnabled);
  if (maybe_experimental) {
    auto env_experimental = memgraph::flags::ReadExperimental(maybe_experimental);
    memgraph::flags::AppendExperimental(env_experimental);
  }
  // Initialize the logger. Done after experimental setup so that we could print which experimental features are enabled
  // even if
  // `--also-log-to-stderr` is set to false.
  memgraph::flags::InitializeLogger();

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
        // Change how we load dynamic libraries on Python by using RTLD_NOW flag.
        // This solves an issue with using the wrong version of libstdc++.
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
  python_gc_scheduler.SetInterval(std::chrono::seconds(FLAGS_storage_python_gc_cycle_sec));
  python_gc_scheduler.Run("Python GC", [] { memgraph::query::procedure::PyCollectGarbage(); });

  // Initialize the communication library.
  memgraph::communication::SSLInit sslInit;

  // Initialize the requests library.
  memgraph::requests::Init();

  // Start memory warning logger.
  memgraph::utils::Scheduler mem_log_scheduler;
  if (FLAGS_memory_warning_threshold > 0) {
    auto free_ram = memgraph::utils::sysinfo::AvailableMemory();
    if (free_ram) {
      mem_log_scheduler.SetInterval(std::chrono::seconds(3));
      mem_log_scheduler.Run("Memory check", [] {
        auto free_ram = memgraph::utils::sysinfo::AvailableMemory();
        if (free_ram && *free_ram / 1024 < FLAGS_memory_warning_threshold)
          spdlog::warn(memgraph::utils::MessageWithLink("Running out of available RAM, only {} MB left.",
                                                        *free_ram / 1024, "https://memgr.ph/ram"));

        auto memory_res = memgraph::utils::GetMemoryRES();
        memgraph::metrics::SetGaugeValue(memgraph::metrics::PeakMemoryRes, memory_res);
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
  if (vm_max_map_count) {
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

  memgraph::license::global_license_checker.CheckEnvLicense();
  if (!FLAGS_organization_name.empty() && !FLAGS_license_key.empty()) {
    memgraph::license::global_license_checker.SetLicenseInfoOverride(FLAGS_license_key, FLAGS_organization_name);
  }

  memgraph::license::global_license_checker.StartBackgroundLicenseChecker(memgraph::utils::global_settings);

  // Has to be initialized after the storage and license startup
  memgraph::flags::run_time::Initialize();

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
                     .root_data_directory = FLAGS_data_directory,
                     .recover_on_startup = FLAGS_data_recovery_on_startup,
                     .snapshot_retention_count = FLAGS_storage_snapshot_retention_count,
                     .wal_file_size_kibibytes = FLAGS_storage_wal_file_size_kib,
                     .wal_file_flush_every_n_tx = FLAGS_storage_wal_file_flush_every_n_tx,
                     .snapshot_on_exit = FLAGS_storage_snapshot_on_exit,
                     .restore_replication_state_on_startup = FLAGS_replication_restore_state_on_startup,
                     .items_per_batch = FLAGS_storage_items_per_batch,
                     .snapshot_thread_count = FLAGS_storage_snapshot_thread_count,
                     .recovery_thread_count = FLAGS_storage_recovery_thread_count,
                     .allow_parallel_snapshot_creation = FLAGS_storage_parallel_snapshot_creation,
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
                        .enable_edges_metadata =
                            FLAGS_storage_properties_on_edges ? FLAGS_storage_enable_edges_metadata : false,
                        .enable_schema_metadata = FLAGS_storage_enable_schema_metadata,
                        .enable_schema_info = FLAGS_schema_info_enabled,
                        .enable_label_index_auto_creation = FLAGS_storage_automatic_label_index_creation_enabled,
                        .enable_edge_type_index_auto_creation =
                            FLAGS_storage_automatic_edge_type_index_creation_enabled,  // NOLINT(misc-include-cleaner)
                        .delta_on_identical_property_update = FLAGS_storage_delta_on_identical_property_update,
                        .property_store_compression_enabled = FLAGS_storage_property_store_compression_enabled},
      .salient.storage_mode = memgraph::flags::ParseStorageMode(),
      .salient.property_store_compression_level = memgraph::flags::ParseCompressionLevel(),
      .track_label_counts = FLAGS_telemetry_enabled};
  if (db_config.salient.items.enable_edge_type_index_auto_creation && !db_config.salient.items.properties_on_edges) {
    LOG_FATAL(
        "Automatic index creation on edge-types has been set but properties on edges are disabled. If you wish to use "
        "automatic edge-type index creation, enable properties on edges as well.");
  }
  if (!FLAGS_storage_properties_on_edges && FLAGS_storage_enable_edges_metadata) {
    spdlog::warn(
        "Properties on edges were not enabled, hence edges metadata will also be disabled. If you wish to utilize "
        "extra metadata on edges, enable properties on edges as well.");
  }
  spdlog::info("config recover on startup {}, flags {}", db_config.durability.recover_on_startup,
               FLAGS_data_recovery_on_startup);
  memgraph::utils::Scheduler jemalloc_purge_scheduler;
  jemalloc_purge_scheduler.SetInterval(std::chrono::seconds(FLAGS_storage_gc_cycle_sec));
  jemalloc_purge_scheduler.Run("Jemalloc purge", [] { memgraph::memory::PurgeUnusedMemory(); });

  using namespace std::chrono_literals;
  using enum memgraph::storage::StorageMode;
  using enum memgraph::storage::Config::Durability::SnapshotWalMode;

  db_config.durability.snapshot_interval = memgraph::utils::SchedulerInterval(FLAGS_storage_snapshot_interval);
  if (db_config.salient.storage_mode == IN_MEMORY_TRANSACTIONAL) {
    if (!db_config.durability.snapshot_interval) {
      if (FLAGS_storage_wal_enabled) {
        LOG_FATAL(
            "In order to use write-ahead-logging you must enable "
            "periodic snapshots by setting the snapshot interval to a "
            "value larger than 0!");
      }
      db_config.durability.snapshot_wal_mode = DISABLED;
    } else {
      if (FLAGS_storage_wal_enabled) {
        db_config.durability.snapshot_wal_mode = PERIODIC_SNAPSHOT_WITH_WAL;
      } else {
        db_config.durability.snapshot_wal_mode = PERIODIC_SNAPSHOT;
      }
    }
  } else {
    // IN_MEMORY_ANALYTICAL and ON_DISK_TRANSACTIONAL do not support periodic snapshots
    db_config.durability.snapshot_wal_mode = DISABLED;
  }

#ifdef MG_ENTERPRISE
  if (std::chrono::seconds(FLAGS_instance_down_timeout_sec) <
      std::chrono::seconds(FLAGS_instance_health_check_frequency_sec)) {
    LOG_FATAL(
        "Instance down timeout config option must be greater than or equal to instance health check frequency config "
        "option!");
  }

#endif

#ifdef MG_ENTERPRISE
  memgraph::flags::SetFinalCoordinationSetup();
  auto const &coordination_setup = memgraph::flags::CoordinationSetupInstance();
  if (coordination_setup.IsDataInstanceManagedByCoordinator() &&
      db_config.salient.storage_mode == IN_MEMORY_TRANSACTIONAL) {
    MG_ASSERT(db_config.durability.snapshot_wal_mode == PERIODIC_SNAPSHOT_WITH_WAL,
              "When running Memgraph in high availability mode, a data instance must be started with flag "
              "--storage-wal-enabled=true. One of the flags used for setting up snapshots "
              "--storage-snapshot-interval-sec or --storage-snapshot-interval also needs to be set.");
  }

#endif

  // Default interpreter configuration
  memgraph::query::InterpreterConfig const interp_config{
      .query = {.allow_load_csv = FLAGS_allow_load_csv},
      .replication_replica_check_frequency = std::chrono::seconds(FLAGS_replication_replica_check_frequency_sec),
      .default_kafka_bootstrap_servers = FLAGS_kafka_bootstrap_servers,
      .default_pulsar_service_url = FLAGS_pulsar_service_url,
      .stream_transaction_conflict_retries = FLAGS_stream_transaction_conflict_retries,
      .stream_transaction_retry_interval = std::chrono::milliseconds(FLAGS_stream_transaction_retry_interval)};

#ifdef MG_ENTERPRISE
  auto auth_glue = [&coordination_setup]
#else
  auto auth_glue = []
#endif
      (memgraph::auth::SynchedAuth *auth, std::unique_ptr<memgraph::query::AuthQueryHandler> &ah,
       std::unique_ptr<memgraph::query::AuthChecker> &ac) {
        // Glue high level auth implementations to the query side
        ah = std::make_unique<memgraph::glue::AuthQueryHandler>(auth);
        ac = std::make_unique<memgraph::glue::AuthChecker>(auth);
        // Handle users passed via arguments
        auto *maybe_username = std::getenv(kMgUser);
        auto *maybe_password = std::getenv(kMgPassword);
        auto *maybe_pass_file = std::getenv(kMgPassfile);
#ifdef MG_ENTERPRISE
        if (maybe_username && maybe_password && !coordination_setup.IsCoordinator()) {
#else
        if (maybe_username && maybe_password) {
#endif
          ah->CreateUser(maybe_username, maybe_password, nullptr);
        } else if (maybe_pass_file) {
          const auto [username, password] = LoadUsernameAndPassword(maybe_pass_file);
          if (!username.empty() && !password.empty()) {
            ah->CreateUser(username, password, nullptr);
          }
        }
      };

  memgraph::auth::Auth::Config const auth_config{FLAGS_auth_user_or_role_name_regex, FLAGS_auth_password_strength_regex,
                                                 FLAGS_auth_password_permit_null};

  std::unique_ptr<memgraph::query::AuthQueryHandler> auth_handler;
  std::unique_ptr<memgraph::query::AuthChecker> auth_checker;
  std::unique_ptr<memgraph::auth::SynchedAuth> auth_;
#ifdef MG_ENTERPRISE
  // Resource monitoring
  auto resource_monitoring = memgraph::utils::ResourceMonitoring{};
  try {
    auth_ = std::make_unique<memgraph::auth::SynchedAuth>(data_directory / "auth", auth_config, &resource_monitoring);
#else
  try {
    auth_ = std::make_unique<memgraph::auth::SynchedAuth>(data_directory / "auth", auth_config);
#endif
  } catch (std::exception const &e) {
    spdlog::error("Exception was thrown on creating SyncedAuth object, shutting down Memgraph. {}", e.what());
    exit(1);
  }
  auth_glue(auth_.get(), auth_handler, auth_checker);

  auto system = memgraph::system::System{db_config.durability.storage_directory, FLAGS_data_recovery_on_startup};

  int const extracted_bolt_port = [&]() {
    if (auto *maybe_env_bolt_port = std::getenv(kMgBoltPort); maybe_env_bolt_port) {
      return std::stoi(maybe_env_bolt_port);
    }
    return FLAGS_bolt_port;
  }();  // iile

  // singleton coordinator state
#ifdef MG_ENTERPRISE
  using memgraph::coordination::CoordinatorInstanceInitConfig;
  using memgraph::coordination::CoordinatorState;
  using memgraph::coordination::ReplicationInstanceInitConfig;
  std::optional<CoordinatorState> coordinator_state{std::nullopt};
  auto const is_valid_data_instance =
      coordination_setup.management_port && !coordination_setup.coordinator_port && !coordination_setup.coordinator_id;
  auto const is_valid_coordinator_instance = coordination_setup.management_port &&
                                             coordination_setup.coordinator_port && coordination_setup.coordinator_id &&
                                             !coordination_setup.coordinator_hostname.empty();
  auto try_init_coord_state = [&coordinator_state, &extracted_bolt_port, &is_valid_data_instance,
                               &is_valid_coordinator_instance](auto const &coordination_setup) {
    if (!(coordination_setup.management_port || coordination_setup.coordinator_port ||
          coordination_setup.coordinator_id)) {
      spdlog::trace("Aborting coordinator initialization.");
      return;
    }

    spdlog::trace("Creating coordinator state.");
    if (!(is_valid_coordinator_instance || is_valid_data_instance)) {
      throw std::runtime_error(
          "You specified invalid combination of HA flags to start coordinator instance or data instance."
          "Coordinator must be started with coordinator_id, coordinator_hostname, coordinator_port and "
          "management_port. Data instance must be "
          "started only with management port.");
    }

    if (is_valid_coordinator_instance) {
      constexpr auto kRaftDataDir = "/high_availability/raft_data";
      auto const high_availability_data_dir = FLAGS_data_directory + kRaftDataDir;
      memgraph::utils::EnsureDirOrDie(high_availability_data_dir);
      coordinator_state.emplace(CoordinatorInstanceInitConfig{
          .coordinator_id = coordination_setup.coordinator_id,
          .coordinator_port = coordination_setup.coordinator_port,
          .bolt_port = extracted_bolt_port,
          .management_port = coordination_setup.management_port,
          .durability_dir = high_availability_data_dir,
          .coordinator_hostname = coordination_setup.coordinator_hostname,
          .nuraft_log_file = coordination_setup.nuraft_log_file,
          .instance_down_timeout_sec = std::chrono::seconds(FLAGS_instance_down_timeout_sec),
          .instance_health_check_frequency_sec = std::chrono::seconds(FLAGS_instance_health_check_frequency_sec)});
    } else {
      coordinator_state.emplace(ReplicationInstanceInitConfig{.management_port = coordination_setup.management_port});
    }
  };

  try {
    try_init_coord_state(coordination_setup);
    spdlog::trace("Coordinator state initialized successfully.");
  } catch (std::exception const &e) {
    spdlog::error("Exception was thrown on coordinator state construction, shutting down Memgraph. {}", e.what());
    exit(1);
  }

#endif

  // singleton replication state
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      ReplicationStateRootPath(db_config)
#ifdef MG_ENTERPRISE
          ,
      coordinator_state.has_value() && coordinator_state->IsDataInstance()
#endif
  };

  memgraph::dbms::DbmsHandler dbms_handler(db_config, repl_state
#ifdef MG_ENTERPRISE
                                           ,
                                           *auth_, FLAGS_data_recovery_on_startup
#endif
  );

  // Note: Now that all system's subsystems are initialised (dbms & auth)
  //       We can now initialise the recovery of replication (which will include those subsystems)
  //       ReplicationHandler will handle the recovery
  auto replication_handler = memgraph::replication::ReplicationHandler{repl_state, dbms_handler
#ifdef MG_ENTERPRISE
                                                                       ,
                                                                       system, *auth_
#endif
  };

#ifdef MG_ENTERPRISE
  // MAIN or REPLICA instance
  if (is_valid_data_instance) {
    spdlog::trace("Starting data instance management server.");
    memgraph::dbms::DataInstanceManagementServerHandlers::Register(coordinator_state->GetDataInstanceManagementServer(),
                                                                   replication_handler);
    MG_ASSERT(coordinator_state->GetDataInstanceManagementServer().Start(), "Failed to start coordinator server!");
    spdlog::trace("Data instance management server started.");
  }
#endif

  auto db_acc = dbms_handler.Get();

  memgraph::query::InterpreterContextLifetimeControl interpreter_context_lifetime_control(
      interp_config, &dbms_handler, repl_state, system,
#ifdef MG_ENTERPRISE
      coordinator_state ? std::optional<std::reference_wrapper<CoordinatorState>>{std::ref(*coordinator_state)}
                        : std::nullopt,
      &resource_monitoring,
#endif
      auth_handler.get(), auth_checker.get(), &replication_handler);

  auto &interpreter_context_ = memgraph::query::InterpreterContextHolder::GetInstance();
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

  // Tied to coord initialization, must happen after coordinator is initialized
  auto *maybe_ha_init_file = std::getenv(kMgHaClusterInitQueries);
  if (maybe_ha_init_file) {
    spdlog::trace("Initializing coordinator using cypher file.");
    InitFromCypherlFile(interpreter_context_, db_acc, maybe_ha_init_file);
    spdlog::trace("Coordinator initialized using cypher file.");
  }

  // Triggers can execute query procedures, so we need to reload the modules first and then the triggers.
  // Stream transformations use modules, so we need to restored streams after the query modules have been loaded.
  if (db_config.durability.recover_on_startup) {
    dbms_handler.RestoreTriggers(&interpreter_context_);
    spdlog::trace("Triggers restored.");
    dbms_handler.RestoreStreams(&interpreter_context_);
    spdlog::trace("Streams restored.");
  }

  // Global worker pool!
  // Used by sessions to schedule tasks.
  std::optional<memgraph::utils::PriorityThreadPool> worker_pool_;
  unsigned io_n_threads = FLAGS_bolt_num_workers;

  if (GetSchedulerType() == SchedulerType::PRIORITY_QUEUE_WITH_SIDECAR) {
    worker_pool_.emplace(/* low priority */ static_cast<uint16_t>(FLAGS_bolt_num_workers),
                         /* high priority */ 1U);
    io_n_threads = 1U;
  }

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
  auto server_endpoint = memgraph::communication::v2::ServerEndpoint{boost::asio::ip::make_address(FLAGS_bolt_address),
                                                                     static_cast<uint16_t>(extracted_bolt_port)};
#ifdef MG_ENTERPRISE
  memgraph::glue::Context session_context{server_endpoint, &interpreter_context_, auth_.get(), &audit_log,
                                          worker_pool_ ? &*worker_pool_ : nullptr};
#else
  memgraph::glue::Context session_context{server_endpoint, &interpreter_context_, auth_.get(),
                                          worker_pool_ ? &*worker_pool_ : nullptr};
#endif
  memgraph::glue::ServerT server(server_endpoint, &session_context, &context, service_name, io_n_threads);

  const auto machine_id = memgraph::utils::GetMachineId();

  // Setup telemetry
  static constexpr auto telemetry_server{"https://telemetry.memgraph.com/88b5e7e8-746a-11e8-9f85-538a9e9690cc/"};
  std::optional<memgraph::telemetry::Telemetry> telemetry;
  if (FLAGS_telemetry_enabled) {
    telemetry.emplace(telemetry_server, data_directory / "telemetry", memgraph::glue::run_id_, machine_id,
                      service_name == "BoltS", FLAGS_data_directory, std::chrono::hours(8), 1);
    telemetry->AddStorageCollector(dbms_handler, *auth_);
#ifdef MG_ENTERPRISE
    telemetry->AddDatabaseCollector(dbms_handler);
    telemetry->AddCoordinatorCollector(coordinator_state);
#else
    telemetry->AddDatabaseCollector();
#endif
    telemetry->AddClientCollector();
    telemetry->AddEventsCollector();
    telemetry->AddQueryModuleCollector();
    telemetry->AddExceptionCollector();
    telemetry->AddReplicationCollector(repl_state);
    telemetry->Start();
  }
  memgraph::license::LicenseInfoSender license_info_sender(telemetry_server, memgraph::glue::run_id_, machine_id,
                                                           memory_limit,
                                                           memgraph::license::global_license_checker.GetLicenseInfo());

  memgraph::communication::websocket::SafeAuth websocket_auth{auth_.get()};
  memgraph::communication::websocket::Server websocket_server{
      {FLAGS_monitoring_address, static_cast<uint16_t>(FLAGS_monitoring_port)}, &context, websocket_auth};

  spdlog::trace("Websocket server created.");
  if (!websocket_server.HasErrorHappened()) {
    spdlog::trace("Initializing logger sync.");
    memgraph::flags::AddLoggerSink(websocket_server.GetLoggingSink());
    spdlog::trace("Logger sink added.");
  } else {
    spdlog::error("Skipping adding logger sync for websocket.");
  }

// TODO: Make multi-tenant
#ifdef MG_ENTERPRISE
  memgraph::glue::MonitoringServerT metrics_server{
      {FLAGS_metrics_address, static_cast<uint16_t>(FLAGS_metrics_port)}, db_acc->storage(), &context};
  spdlog::trace("Metrics server created.");
#endif

  // Handler for regular termination signals
  auto shutdown = [
#ifdef MG_ENTERPRISE
                      &coordinator_state, &metrics_server,
#endif
                      &websocket_server, &server, &interpreter_context_, &dbms_handler, &worker_pool_] {
    // Server needs to be shutdown first and then the database. This prevents
    // a race condition when a transaction is accepted during server shutdown.
    spdlog::trace("Shutting down handler!");
    spdlog::info("Workers shutting down.");
    if (worker_pool_) worker_pool_->ShutDown();  // Workers can enqueue io tasks, so they need to be stopped first
    // Shutdown communication server
    server.Shutdown();
    // Stop all triggers, streams and ttl
    dbms_handler.ForEach([](memgraph::dbms::DatabaseAccess acc) { acc->StopAllBackgroundTasks(); });
    // After the server is notified to stop accepting and processing
    // connections we tell the execution engine to stop processing all pending
    // queries.
    interpreter_context_.Shutdown();
    websocket_server.Shutdown();
#ifdef MG_ENTERPRISE
    metrics_server.Shutdown();
    if (coordinator_state.has_value() && coordinator_state->IsCoordinator()) {
      coordinator_state->ShutDownCoordinator();
    }
#endif
  };

  InitSignalHandlers(shutdown);
  spdlog::trace("Signal handlers initialized.");

  // Release the temporary database access
  db_acc.reset();

  // Startup the main server
  MG_ASSERT(server.Start(), "Couldn't start the Bolt server!");
  spdlog::trace("Bolt server started.");
  websocket_server.Start();
  spdlog::trace("Web socket server started.");

#ifdef MG_ENTERPRISE
  metrics_server.Start();
  spdlog::trace("Metrics server started");
#endif

  if (!FLAGS_init_data_file.empty()) {
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
    spdlog::info("Running queries from init data file successfully finished.");
  }

  spdlog::info("Memgraph successfully started!");

  if (worker_pool_) worker_pool_->AwaitShutdown();
  server.AwaitShutdown();
  websocket_server.AwaitShutdown();
  memgraph::memory::UnsetHooks();
#ifdef MG_ENTERPRISE
  metrics_server.AwaitShutdown();
#endif
  try {
    memgraph::query::procedure::gModuleRegistry.UnloadAllModules();
  } catch (memgraph::query::QueryException &) {
    spdlog::warn("Failed to unload query modules while shutting down.");
  }
  python_gc_scheduler.Stop();
  Py_END_ALLOW_THREADS;
  // Shutdown Python
  Py_Finalize();
  PyMem_RawFree(program_name);

  memgraph::utils::total_memory_tracker.LogPeakMemoryUsage();
  return 0;
}
