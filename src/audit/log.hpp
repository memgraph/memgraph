// Copyright 2023 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <atomic>
#include <filesystem>
#include <optional>

#include "data_structures/ring_buffer.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/file.hpp"
#include "utils/scheduler.hpp"

namespace memgraph::audit {

const uint64_t kBufferSizeDefault = 100000;
const uint64_t kBufferFlushIntervalMillisDefault = 200;

/// This class implements an audit log. Functions used for logging are
/// thread-safe, functions used for setup aren't thread-safe.
class Log {
 private:
  struct Item {
    int64_t timestamp;
    std::string address;
    std::string username;
    std::string query;
    storage::PropertyValue params;
    std::string db;
  };

 public:
  Log(std::filesystem::path storage_directory, int32_t buffer_size, int32_t buffer_flush_interval_millis);

  ~Log();

  Log(const Log &) = delete;
  Log(Log &&) = delete;
  Log &operator=(const Log &) = delete;
  Log &operator=(Log &&) = delete;

  /// Starts the audit log. If you don't want to use the audit log just don't
  /// start it. All functions can still be used when the log isn't started and
  /// they won't do anything. Isn't thread-safe.
  void Start();

  /// Adds an entry to the audit log. Thread-safe.
  void Record(const std::string &address, const std::string &username, const std::string &query,
              const storage::PropertyValue &params, const std::string &db);

  /// Reopens the log file. Used for log file rotation. Thread-safe.
  void ReopenLog();

 private:
  void Flush();

  std::filesystem::path storage_directory_;
  int32_t buffer_size_;
  int32_t buffer_flush_interval_millis_;
  std::atomic<bool> started_;

  std::optional<RingBuffer<Item>> buffer_;
  utils::Scheduler scheduler_;

  utils::OutputFile log_;
  std::mutex lock_;
};

}  // namespace memgraph::audit
