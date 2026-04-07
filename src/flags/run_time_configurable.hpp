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
#include <string>

#include "utils/observer.hpp"
#include "utils/scheduler.hpp"
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
 * @brief Get the current timezone object
 *
 * @return const std::chrono::time_zone*
 */
const std::chrono::time_zone *GetTimezone();

/**
 * @brief Get the query log directory value
 *
 * @return std::string
 */
std::string GetQueryLogDirectory();

/**
 * @brief Get the failed query log directory value
 *
 * @return std::string
 */
std::string GetFailedQueryLogDir();

/**
 * @brief Get the failed query logging enabled flag (runtime-configurable).
 *
 * @return bool
 */
bool GetFailedQueryLoggingEnabled();

/**
 * @brief Get the slow query log directory value
 *
 * @return std::string
 */
std::string GetSlowQueryLogDir();

/**
 * @brief Get the slow query log threshold in milliseconds (runtime-configurable).
 *        Queries exceeding this threshold are logged. 0 means disabled.
 *
 * @return uint64_t
 */
uint64_t GetSlowQueryLogThresholdMs();

/**
 * @brief Get the slow query log auto-explain flag (runtime-configurable).
 *        When true, the EXPLAIN plan is included in slow query log entries.
 *
 * @return bool
 */
bool GetSlowQueryLogAutoExplain();

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

}  // namespace memgraph::flags::run_time
