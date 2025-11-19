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

#include "flags/run_time_configurable.hpp"

#include <exception>
#include <optional>
#include <stdexcept>
#include <string>

#include "croncpp.h"
#include "gflags/gflags.h"

#include "flags/log_level.hpp"
#include "license/license.hpp"
#include "spdlog/spdlog.h"
#include "utils/exceptions.hpp"
#include "utils/flag_validation.hpp"
#include "utils/logging.hpp"
#include "utils/observer.hpp"
#include "utils/result.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/settings.hpp"
#include "utils/string.hpp"
#include "utils/synchronized.hpp"

namespace {
bool ValidTimezone(std::string_view tz);

template <bool FATAL>
bool ValidPeriodicSnapshot(std::string_view def);
template bool ValidPeriodicSnapshot<false>(std::string_view def);
template bool ValidPeriodicSnapshot<true>(std::string_view def);
}  // namespace

/*
 * Setup GFlags
 */

// Bolt server flags.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(bolt_server_name_for_init, "Neo4j/v5.11.0 compatible graph database server - Memgraph",
              "Server name which the database should send to the client in the "
              "Bolt INIT message.");

// Logging flags
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_bool(also_log_to_stderr, false, "Log messages go to stderr in addition to logfiles");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, misc-unused-parameters)
DEFINE_VALIDATED_string(log_level, "WARNING", memgraph::flags::GetLogLevelHelpString(),
                        { return memgraph::flags::ValidLogLevel(value); });

// Query flags
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_double(query_execution_timeout_sec, 600,
              "Maximum allowed query execution time. Queries exceeding this "
              "limit will be aborted. Value of 0 means no limit.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(hops_limit_partial_results, true,
            "If set to true, the query will return partial results if the "
            "hops limit is reached.");

// Query plan flags
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(cartesian_product_enabled, true, "Enable cartesian product expansion.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(debug_query_plans, false, "Enable DEBUG logging of potential query plans.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, misc-unused-parameters)
DEFINE_VALIDATED_string(timezone, "UTC", "Define instance's timezone (IANA format).", { return ValidTimezone(value); });

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(query_log_directory, "", "Path to directory where the query logs should be stored.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, misc-unused-parameters)
DEFINE_string(storage_snapshot_interval, "",
              "Define periodic snapshot schedule via cron format or as a period in seconds.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(storage_snapshot_interval_sec, 300,
                        "Storage snapshot creation interval (in seconds). Set "
                        "to 0 to disable periodic snapshot creation.",
                        FLAG_IN_RANGE(0, 7LU * 24 * 3600));

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, misc-unused-parameters)
DEFINE_string(aws_region, "", "Define AWS region which is used for the AWS integration.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, misc-unused-parameters)
DEFINE_string(aws_access_key, "", "Define AWS access key for the AWS integration.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, misc-unused-parameters)
DEFINE_string(aws_secret_key, "", "Define AWS secret key for the AWS integration.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, misc-unused-parameters)
DEFINE_string(aws_endpoint_url, "", "Define AWS endpoint url for the AWS integration.");

// Storage flags
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_gc_aggressive, false, "Enable aggressive garbage collection.");

namespace {
// Bolt server name
constexpr auto kServerNameSettingKey = "server.name";
constexpr auto kServerNameGFlagsKey = "bolt_server_name_for_init";
// Query timeout
constexpr auto kQueryTxSettingKey = "query.timeout";
constexpr auto kQueryTxGFlagsKey = "query_execution_timeout_sec";

// Hops limit partial results
constexpr auto kHopsLimitPartialResultsSettingKey = "hops_limit_partial_results";
constexpr auto kHopsLimitPartialResultsGFlagsKey = "hops_limit_partial_results";

// Log level
// No default value because it is not persistent
constexpr auto kLogLevelSettingKey = "log.level";
constexpr auto kLogLevelGFlagsKey = "log_level";
// Log to stderr
// No default value because it is not persistent
constexpr auto kLogToStderrSettingKey = "log.to_stderr";
constexpr auto kLogToStderrGFlagsKey = "also_log_to_stderr";

constexpr auto kCartesianProductEnabledSettingKey = "cartesian-product-enabled";
constexpr auto kCartesianProductEnabledGFlagsKey = "cartesian-product-enabled";

constexpr auto kDebugQueryPlansSettingKey = "debug-query-plans";
constexpr auto kDebugQueryPlansGFlagsKey = "debug-query-plans";

constexpr auto kStorageGcAggressiveSettingKey = "storage-gc-aggressive";
constexpr auto kStorageGcAggressiveGFlagsKey = "storage-gc-aggressive";

constexpr auto kQueryLogDirectorySettingKey = "query-log-directory";
constexpr auto kQueryLogDirectoryGFlagsKey = "query-log-directory";

constexpr auto kTimezoneSettingKey = "timezone";
constexpr auto kTimezoneGFlagsKey = kTimezoneSettingKey;

constexpr auto kSnapshotPeriodicSettingKey = "storage.snapshot.interval";
constexpr auto kSnapshotPeriodicGFlagsKey = "storage-snapshot-interval";

constexpr auto kAwsRegionSettingKey = "aws.region";
constexpr auto kAwsRegionGFlagsKey = "aws_region";

constexpr auto kAwsSecretSettingKey = "aws.secret_key";
constexpr auto kAwsSecretGFlagsKey = "aws_secret_key";

constexpr auto kAwsAccessSettingKey = "aws.access_key";
constexpr auto kAwsAccessGFlagsKey = "aws_access_key";

constexpr auto kAwsEndpointUrlSettingKey = "aws.endpoint_url";
constexpr auto kAwsEndpointUrlGFlagsKey = "aws_endpoint_url";

// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables)
// Local cache-like thing
std::atomic<double> execution_timeout_sec_;
std::atomic<bool> hops_limit_partial_results{true};
std::atomic<bool> cartesian_product_enabled_{true};
std::atomic<bool> debug_query_plans_{false};
std::atomic<const std::chrono::time_zone *> timezone_{nullptr};
std::atomic<bool> storage_gc_aggressive_{false};

class PeriodicObservable : public memgraph::utils::Observable<memgraph::utils::SchedulerInterval> {
 public:
  void Accept(std::shared_ptr<memgraph::utils::Observer<memgraph::utils::SchedulerInterval>> observer) override {
    const auto periodic_locked = periodic_.ReadLock();
    observer->Update(*periodic_locked);
  }

  void Modify(std::chrono::seconds pause) {
    *periodic_.Lock() = memgraph::utils::SchedulerInterval(pause, std::nullopt);
    Notify();
  }

  void Modify(std::string in) {
    *periodic_.Lock() = memgraph::utils::SchedulerInterval(std::move(in));
    Notify();
  }

 private:
  memgraph::utils::Synchronized<memgraph::utils::SchedulerInterval, memgraph::utils::RWSpinLock> periodic_;
} snapshot_periodic_;

// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)

auto ToLLEnum(std::string_view val) {
  const auto ll_enum = memgraph::flags::LogLevelToEnum(val);
  if (!ll_enum) {
    throw memgraph::utils::BasicException("Unsupported log level {}", val);
  }
  return *ll_enum;
}

memgraph::utils::Settings::ValidatorResult ValidBoolStr(std::string_view in) {
  const auto lc = memgraph::utils::ToLowerCase(in);
  if (lc != "false" && lc != "true") {
    return {"Boolean value supports only 'false' or 'true' as the input."};
  }
  return {};
}

auto GenHandler(std::string flag, std::string key) {
  return [key = std::move(key), flag = std::move(flag)]() -> std::string {
    const auto &val = memgraph::utils::global_settings.GetValue(key);
    MG_ASSERT(val, "Failed to read value at '{}' from settings.", key);
    gflags::SetCommandLineOption(flag.c_str(), val->c_str());
    return *val;
  };
}

auto GetTimezone(std::string_view tz) -> const std::chrono::time_zone * {
  try {
    return std::chrono::locate_zone(tz);
  } catch (const std::runtime_error &e) {
    spdlog::warn("Unsupported timezone: {}", e.what());
    return nullptr;
  }
}

int64_t ValidPeriod(std::string_view str) {
  try {
    // str = memgraph::utils::Trim(str);
    size_t n_processed = 0;
    const auto period = std::stol(str.data(), &n_processed);
    if (n_processed != str.size()) throw std::invalid_argument{"string contains more than just an integer"};
    return static_cast<int64_t>(period);
  } catch (const std::out_of_range & /* unused */) {
    // convert to invalid arg
    throw std::invalid_argument{"out of range"};
  }
}

bool ValidTimezone(std::string_view tz) { return GetTimezone(tz) != nullptr; }

template <bool FATAL>
bool ValidPeriodicSnapshot(const std::string_view def) {
  bool failure = false;
  // Empty string = disabled
  if (def.empty()) return true;
  try {
    // Try to get a period in seconds
    const auto period = ValidPeriod(def);
    return period >= 0L && period <= 7L * 24 * 3600;
  } catch (const std::invalid_argument & /* unused */) {
    // Handled later on
    failure = true;
  }
#ifdef MG_ENTERPRISE
  try {
    // NOTE: Cron is an enterprise feature
    const auto cron = cron::make_cron(def);
    if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
      constexpr std::string_view msg =
          "Defining snapshot schedule via cron expressions is an enterprise feature. Check your license status by "
          "running SHOW LICENSE INFO.";
      if constexpr (FATAL) {
        LOG_FATAL(msg);
      }
      spdlog::error(msg);
      return false;
    }
    return true;
  } catch (const cron::bad_cronexpr & /* unused */) {
    // Handled later on
    failure = true;
  }
#endif
  MG_ASSERT(failure, "Failure not handled correctly.");
  if constexpr (FATAL) {
    LOG_FATAL("Defined snapshot interval not a valid expression.");
  }
  return false;
}
}  // namespace

namespace memgraph::flags::run_time {

void Initialize() {
  constexpr bool kRestore = true;  //!< run-time flag is persistent between Memgraph restarts

  /**
   * @brief Helper function that registers a run-time flag
   *
   * @param flag - GFlag name
   * @param key - Settings key used to store the flag
   * @param restore - true if the flag is persistent between restarts
   * @param post_update - user defined callback executed post flag update
   * @param validator - user defined value correctness checker
   */
  auto register_flag = [&](
                           const std::string &flag, const std::string &key, bool restore,
                           std::function<void(const std::string &)> post_update = [](auto) {},
                           utils::Settings::Validation validator =
                               [](std::string_view) -> utils::Settings::ValidatorResult { return {}; }) {
    // Get flag info
    gflags::CommandLineFlagInfo info;
    gflags::GetCommandLineFlagInfo(flag.c_str(), &info);

    // Generate settings callback
    auto callback = [update = GenHandler(flag, key), post_update = std::move(post_update)] {
      const auto &val = update();
      post_update(val);
    };
    // Register setting
    memgraph::utils::global_settings.RegisterSetting(key, info.default_value, callback, std::move(validator));

    if (restore && info.is_default) {
      // No input from the user, restore persistent value from settings
      callback();
    } else {
      // Override with current value - user defined a new value or the run-time flag is not persistent between starts
      memgraph::utils::global_settings.SetValue(key, info.current_value);
    }
  };

  /*
   * Register bolt server name settings
   */
  register_flag(kServerNameGFlagsKey, kServerNameSettingKey, kRestore);

  /*
   * Register query timeout
   */
  register_flag(kQueryTxGFlagsKey, kQueryTxSettingKey, !kRestore, [&](const std::string &val) {
    execution_timeout_sec_ = std::stod(val);  // Cache for faster reads
  });

  /*
   * Register hops limit partial results
   */
  register_flag(
      kHopsLimitPartialResultsGFlagsKey, kHopsLimitPartialResultsSettingKey, kRestore,
      [](const std::string &val) { hops_limit_partial_results = val == "true"; }, ValidBoolStr);

  /*
   * Register log level
   */
  register_flag(
      kLogLevelGFlagsKey, kLogLevelSettingKey, !kRestore,
      [](const std::string &val) {
        const auto ll_enum = ToLLEnum(val);
        spdlog::set_level(ll_enum);
        UpdateStderr(ll_enum);  // Updates level if active
      },
      [](auto in) -> utils::Settings::ValidatorResult {
        if (!memgraph::flags::ValidLogLevel(in)) {
          return {"Unsupported log level. Log level must be defined as one of the following strings: " +
                  memgraph::flags::GetAllowedLogLevels()};
        }
        return {};
      });

  /*
   * Register logging to stderr
   */
  register_flag(
      kLogToStderrGFlagsKey, kLogToStderrSettingKey, !kRestore,
      [](const std::string &val) {
        if (val == "true") {
          // No need to check if ll_val exists, we got here, so the log_level must exist already
          const auto &ll_val = memgraph::utils::global_settings.GetValue(kLogLevelSettingKey);
          LogToStderr(ToLLEnum(*ll_val));
        } else {
          LogToStderr(spdlog::level::off);
        }
      },
      ValidBoolStr);

  /*
   * Register cartesian enable flag
   */
  register_flag(
      kCartesianProductEnabledGFlagsKey, kCartesianProductEnabledSettingKey, !kRestore,
      [](const std::string &val) { cartesian_product_enabled_ = val == "true"; }, ValidBoolStr);

  /*
   * Register debug query plans
   */
  register_flag(
      kDebugQueryPlansGFlagsKey, kDebugQueryPlansSettingKey, !kRestore,
      [](const std::string &val) { debug_query_plans_ = val == "true"; }, ValidBoolStr);

  /*
   * Register storage GC aggressive flag
   */
  register_flag(
      kStorageGcAggressiveGFlagsKey, kStorageGcAggressiveSettingKey, kRestore,
      [](const std::string &val) { storage_gc_aggressive_ = val == "true"; }, ValidBoolStr);

  /*
   * Register timezone setting
   */
  register_flag(
      kTimezoneGFlagsKey, kTimezoneSettingKey, kRestore,
      [](const std::string &val) {
        timezone_ = ::GetTimezone(val);  // Cache for faster access
      },
      [](auto in) -> utils::Settings::ValidatorResult {
        if (!ValidTimezone(in)) {
          return {"Timezone names must follow the IANA standard. Please note that the names are case-sensitive."};
        }
        return {};
      });

  /*
   * Register query log directory setting
   */
  register_flag(kQueryLogDirectoryGFlagsKey, kQueryLogDirectorySettingKey, kRestore);

  /*
   * Register periodic snapshot setting. In the case both flags are defined, --storage-snapshot-interval flag will be
   * used. Ideally, we rely on just a single flag but --storage-snapshot-interval-sec is for community,
   * --storage-snapshot-interval for enterprise.
   */
  if (FLAGS_storage_snapshot_interval_sec != 0) {
    if (FLAGS_storage_snapshot_interval.empty()) {
      FLAGS_storage_snapshot_interval = std::to_string(FLAGS_storage_snapshot_interval_sec);
    } else {
      spdlog::warn(
          "Periodic snapshot schedule defined via both --storage-snapshot-interval-sec and "
          "--storage-snapshot-interval. Memgraph will use the configuration flag from --storage-snapshot-interval!");
    }
  }

  // FATAL validation at startup; can't be part of the flag defintion, since we need to check for license
  ValidPeriodicSnapshot<true>(FLAGS_storage_snapshot_interval);
  register_flag(
      kSnapshotPeriodicGFlagsKey, kSnapshotPeriodicSettingKey, !kRestore,
      [](std::string_view val) {
        try {
          const auto period = ValidPeriod(val);
          snapshot_periodic_.Modify(std::chrono::seconds{period});
        } catch (const std::invalid_argument & /* unused */) {
          // String is not a period; pass in as a cron expression
          // Expression is guaranteed to be valid
          snapshot_periodic_.Modify(std::string{val});
        }
      },
      [](auto in) -> utils::Settings::ValidatorResult {
        if (!ValidPeriodicSnapshot<false>(in)) {
          return {
              "Snapshot interval can be defined as an integer period in seconds or as a 6-field cron expression. "
              "Please note that a valid license is needed in order to use cron expressions."};
        }
        return {};
      });

  register_flag(kAwsRegionGFlagsKey, kAwsRegionSettingKey, kRestore);
  register_flag(kAwsAccessGFlagsKey, kAwsAccessSettingKey, kRestore);
  register_flag(kAwsSecretGFlagsKey, kAwsSecretSettingKey, kRestore);
  register_flag(kAwsEndpointUrlGFlagsKey, kAwsEndpointUrlSettingKey, kRestore);
}

std::string GetServerName() {
  std::string s;
  // Thread safe read of gflag
  gflags::GetCommandLineOption(kServerNameGFlagsKey, &s);
  return s;
}

double GetExecutionTimeout() { return execution_timeout_sec_; }

bool GetHopsLimitPartialResults() { return hops_limit_partial_results; }

bool GetCartesianProductEnabled() { return cartesian_product_enabled_; }

bool GetDebugQueryPlans() { return debug_query_plans_; }

bool GetStorageGcAggressive() { return storage_gc_aggressive_; }

const std::chrono::time_zone *GetTimezone() { return timezone_; }

std::string GetQueryLogDirectory() {
  std::string s;
  // Thread safe read of gflag
  gflags::GetCommandLineOption(kQueryLogDirectoryGFlagsKey, &s);
  return s;
}

auto GetAwsAccessKey() -> std::string {
  std::string access_key;
  gflags::GetCommandLineOption(kAwsAccessGFlagsKey, &access_key);
  return access_key;
}

auto GetAwsSecretKey() -> std::string {
  std::string secret_key;
  gflags::GetCommandLineOption(kAwsSecretGFlagsKey, &secret_key);
  return secret_key;
}

auto GetAwsRegion() -> std::string {
  std::string region;
  gflags::GetCommandLineOption(kAwsRegionGFlagsKey, &region);
  return region;
}

auto GetAwsEndpointUrl() -> std::string {
  std::string endpoint_url;
  gflags::GetCommandLineOption(kAwsEndpointUrlGFlagsKey, &endpoint_url);
  return endpoint_url;
}

void SnapshotPeriodicAttach(std::shared_ptr<utils::Observer<utils::SchedulerInterval>> observer) {
  snapshot_periodic_.Attach(observer);
}

void SnapshotPeriodicDetach(std::shared_ptr<utils::Observer<utils::SchedulerInterval>> observer) {
  snapshot_periodic_.Detach(observer);
}

}  // namespace memgraph::flags::run_time
