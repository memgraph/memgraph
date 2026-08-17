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

#include "kvstore/rocksdb_options.hpp"

#include <gflags/gflags.h>
#include <spdlog/spdlog.h>

#include <cstdint>
#include <string_view>

#include "utils/flag_validation.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(storage_rocksdb_info_log_level, "INFO_LEVEL",
              "RocksDB info log level. Options: DEBUG_LEVEL, INFO_LEVEL, WARN_LEVEL, ERROR_LEVEL, "
              "FATAL_LEVEL, HEADER_LEVEL. Default is INFO_LEVEL.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(storage_rocksdb_keep_log_file_num, 1000,
                        "Maximum number of RocksDB info log files kept per RocksDB instance. Every restart rolls the "
                        "current info log, older ones are deleted. Default is 1000.",
                        FLAG_IN_RANGE(1, UINT64_MAX));

namespace memgraph::kvstore {

namespace {

rocksdb::InfoLogLevel ParseRocksDBInfoLogLevel(std::string_view level) {
  if (level == "DEBUG_LEVEL") return rocksdb::InfoLogLevel::DEBUG_LEVEL;
  if (level == "INFO_LEVEL") return rocksdb::InfoLogLevel::INFO_LEVEL;
  if (level == "WARN_LEVEL") return rocksdb::InfoLogLevel::WARN_LEVEL;
  if (level == "ERROR_LEVEL") return rocksdb::InfoLogLevel::ERROR_LEVEL;
  if (level == "FATAL_LEVEL") return rocksdb::InfoLogLevel::FATAL_LEVEL;
  if (level == "HEADER_LEVEL") return rocksdb::InfoLogLevel::HEADER_LEVEL;
  spdlog::warn("Unknown RocksDB info log level '{}', using INFO_LEVEL", level);
  return rocksdb::InfoLogLevel::INFO_LEVEL;
}

}  // namespace

void ApplyRocksDBLogConfig(rocksdb::Options &options) {
  options.info_log_level = ParseRocksDBInfoLogLevel(FLAGS_storage_rocksdb_info_log_level);
  options.keep_log_file_num = FLAGS_storage_rocksdb_keep_log_file_num;
}

}  // namespace memgraph::kvstore
