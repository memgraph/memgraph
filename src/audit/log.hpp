#pragma once

#include <atomic>
#include <experimental/filesystem>
#include <experimental/optional>

#include "data_structures/ring_buffer.hpp"
#include "storage/common/types/property_value.hpp"
#include "utils/file.hpp"
#include "utils/scheduler.hpp"

namespace audit {

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
    PropertyValue params;
  };

 public:
  Log(const std::experimental::filesystem::path &storage_directory,
      int32_t buffer_size, int32_t buffer_flush_interval_millis);

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
  void Record(const std::string &address, const std::string &username,
              const std::string &query, const PropertyValue &params);

  /// Reopens the log file. Used for log file rotation. Thread-safe.
  void ReopenLog();

 private:
  void Flush();

  std::experimental::filesystem::path storage_directory_;
  int32_t buffer_size_;
  int32_t buffer_flush_interval_millis_;
  std::atomic<bool> started_;

  std::experimental::optional<RingBuffer<Item>> buffer_;
  utils::Scheduler scheduler_;

  utils::LogFile log_;
  std::mutex lock_;
};

}  // namespace audit
