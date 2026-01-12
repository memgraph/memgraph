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

#include "kvstore/rocksdb_utils.hpp"

#include "flags/general.hpp"
#include "spdlog/spdlog.h"

namespace memgraph::utils {

namespace {

rocksdb::InfoLogLevel ParseRocksDBInfoLogLevel(const std::string &level) {
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

void ApplyRocksDBConfigFlags(rocksdb::Options &options) {
  options.info_log_level = ParseRocksDBInfoLogLevel(FLAGS_storage_rocksdb_info_log_level);
  options.enable_thread_tracking = FLAGS_storage_rocksdb_enable_thread_tracking;
}

}  // namespace memgraph::utils
