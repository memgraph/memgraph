// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "utils/observer.hpp"
#include "utils/scheduler.hpp"
#include "utils/session_context.hpp"
#include "utils/settings.hpp"

namespace memgraph::flags::run_time {

/**
 * @brief Initialize the run-time flags (must be done before run-time flags are used).
 *
 */
void Initialize(utils::Settings &settings);

/**
 * @brief Get the bolt server name value
 *
 * @return std::string
 */
std::string GetServerName();

/**
 * @brief Get the query execution timeout value
 *
 * @return double
 */
double GetExecutionTimeout();

/**
 * @brief Get the hops limit partial results value
 *
 * @return bool
 */
bool GetHopsLimitPartialResults();

/**
 * @brief Get the cartesian product enabled value
 *
 * @return bool
 */
bool GetCartesianProductEnabled();

/**
 * @brief Get the debug query plans value
 *
 * @return bool
 */
bool GetDebugQueryPlans();

/**
 * @brief Get the storage GC aggressive value
 *
 * @return bool
 */
bool GetStorageGcAggressive();

/**
 * @brief Cheap (relaxed-atomic) read of --experimental-coro-prepare-accessor-yield. Safe to call
 * from hot release paths (e.g. Storage::NotifyMainLockReleased()) -- unlike the Settings-backed
 * getters above, this flag is NOT registered with the runtime Settings store (it is a
 * startup-only experimental flag, mirroring storage_gc_aggressive_'s cached-atomic shape but
 * without the persistence/SET machinery). Populated by RefreshCoroPrepareAccessorYieldEnabled().
 *
 * @return bool
 */
bool CoroPrepareAccessorYieldEnabled();

/**
 * @brief Refresh the cached atomic snapshot of --experimental-coro-prepare-accessor-yield from the
 * gflag. Called once at startup, after gflags::ParseCommandLineFlags (see memgraph.cpp, alongside
 * the other experimental-flag materialization) -- a static-init-time cache would observe the
 * flag's default rather than its parsed command-line value. Exposed (not file-local) so tests can
 * flip FLAGS_experimental_coro_prepare_accessor_yield and re-sync the cache without a process
 * restart.
 */
void RefreshCoroPrepareAccessorYieldEnabled();

/**
 * @brief Get the current timezone object
 *
 * @return const std::chrono::time_zone*
 */
const std::chrono::time_zone *GetTimezone();

/**
 * @brief Get the also-log-to-stderr value
 * @return bool
 */
bool GetAlsoLogToStderr();
/**
 * @brief Get the AWS region setting
 * @return std::string
 */
auto GetAwsRegion() -> std::string;

/**
 * @brief Get the AWS access key setting
 * @return std::string
 */
auto GetAwsAccessKey() -> std::string;

/**
 * @brief Get the AWS secret key setting
 * @return std::string
 */
auto GetAwsSecretKey() -> std::string;

/**
 * @brief Get the AWS endpoint URL setting
 * @return std::string
 */
auto GetAwsEndpointUrl() -> std::string;

/**
 * @brief Get the storage snapshot interval
 * @return std::string
 */
auto GetStorageSnapshotInterval() -> std::string;

/**
 * @brief Get the file_download_timeout run-time config value
 * @return uint64_t
 */
auto GetFileDownloadConnTimeoutSec() -> uint64_t;

/**
 * @brief Get the storage access timeout value
 * @return uint64_t
 */
auto GetStorageAccessTimeoutSec() -> std::chrono::seconds;

/**
 * @brief Attach observer to the global snapshor period variable
 */
void SnapshotPeriodicAttach(std::shared_ptr<utils::Observer<utils::SchedulerInterval>> observer);

/**
 * @brief Detach observer from the global snapshor period variable
 */
void SnapshotPeriodicDetach(std::shared_ptr<utils::Observer<utils::SchedulerInterval>> observer);

int64_t GetLogMinDurationMs();
bool GetLogFailedQueries();
bool GetLogQueryPlan();

// Settings keys; stored in the per-session overlay and the global settings store.
inline constexpr std::string_view kLogMinDurationMsKey = "log.min_duration_ms";
inline constexpr std::string_view kLogFailedQueriesKey = "log.failed_queries";
inline constexpr std::string_view kLogQueryPlanKey = "log.query_plan";

// Effective value of a session-overridable setting: session overlay first, then
// the cached global. One getter per setting keeps the fallback explicit.
int64_t GetEffectiveLogMinDurationMs(const logging::SessionLogContext &ctx);
bool GetEffectiveLogFailedQueries(const logging::SessionLogContext &ctx);
bool GetEffectiveLogQueryPlan(const logging::SessionLogContext &ctx);

// True if `key` is one of the per-session-overridable settings (the allow-list).
bool IsSessionSettable(std::string_view key);

// Validate a raw string `value` for `key`. Returns an error message if invalid,
// std::nullopt if valid. Precondition: IsSessionSettable(key).
std::optional<std::string> ValidateSessionSettingValue(std::string_view key, std::string_view value);

}  // namespace memgraph::flags::run_time
