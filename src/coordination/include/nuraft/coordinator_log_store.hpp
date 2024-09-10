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

#pragma once

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_communication_config.hpp"

#include <libnuraft/nuraft.hxx>
#include "kvstore/kvstore.hpp"
#include "nuraft/logger_wrapper.hpp"

namespace memgraph::coordination {

using nuraft::buffer;
using nuraft::int32;
using nuraft::int64;
using nuraft::log_entry;
using nuraft::log_store;
using nuraft::ptr;
using nuraft::raft_server;

/**
 * Current version v1 of CoordinatorLogStore is a simple in-memory log store + durability by default.
 * If durability is set, logs are persisted to disk and loaded on startup.
 * On first startup, version is set to 1 and start index and last log entry index are stored to disk - which are 1 and 0
 * respectfully. If some log is missing, we assert failure. Logs are stored in a map with key being the index of the log
 * entry. In current version logs are also cached in memory fully. For durability we use RocksDB instance.
 */
class CoordinatorLogStore : public log_store {
 public:
  CoordinatorLogStore(LoggerWrapper logger, std::optional<LogStoreDurability> log_store_durability);
  CoordinatorLogStore(CoordinatorLogStore const &) = delete;
  CoordinatorLogStore &operator=(CoordinatorLogStore const &) = delete;
  CoordinatorLogStore(CoordinatorLogStore &&) = delete;
  CoordinatorLogStore &operator=(CoordinatorLogStore &&) = delete;
  ~CoordinatorLogStore() override;

  ulong next_slot() const override;

  ulong start_index() const override;

  ptr<log_entry> last_entry() const override;

  ulong append(ptr<log_entry> &entry) override;

  void write_at(ulong index, ptr<log_entry> &entry) override;

  ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end) override;

  ptr<log_entry> entry_at(ulong index) override;

  ulong term_at(ulong index) override;

  ptr<buffer> pack(ulong index, int32 cnt) override;

  void apply_pack(ulong index, buffer &pack) override;

  bool compact(ulong last_log_index) override;

  bool flush() override;

  void DeleteLogs(uint64_t start, uint64_t end);

  auto GetAllEntriesRange(uint64_t start, uint64_t end) -> std::vector<std::pair<int64_t, ptr<log_entry>>>;

  /*
   * Stores log entry to disk. We need to store our logs which in nuraft are encoded with log_val_type::app_log
   * Otherwise we don't need to store them, as nuraft sends either configuration logs or custom logs
   * and we don't handle them in our commit policy.
   * Other logs are stored as empty strings, and state_machine DecodeLogs function doesn't apply any action
   * on empty logs
   * @param clone - log entry to store
   * @param key_id - index of the log entry
   * @param is_newest_entry - if this is the newest entry
   * @return true if storing was successful, false otherwise
   */
  bool StoreEntryToDisk(const ptr<log_entry> &clone, uint64_t key_id, bool is_newest_entry);

 private:
  /*
   * Returns next slot, without taking lock. Should be called under the lock
   */
  auto GetNextSlot() const -> ulong;

  auto FindOrDefault_(ulong index) const -> ptr<log_entry>;

  bool HandleVersionMigration(LogStoreVersion stored_version);

  std::map<ulong, ptr<log_entry>> logs_;
  mutable std::mutex logs_lock_;
  std::atomic<ulong> start_idx_{0};
  std::atomic<ulong> next_idx_{0};
  std::shared_ptr<kvstore::KVStore> durability_;
  LoggerWrapper logger_;
};

}  // namespace memgraph::coordination
#endif
