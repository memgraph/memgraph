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

#include <libnuraft/nuraft.hxx>

namespace memgraph::coordination {

using nuraft::buffer;
using nuraft::int32;
using nuraft::int64;
using nuraft::log_entry;
using nuraft::log_store;
using nuraft::ptr;
using nuraft::raft_server;

class CoordinatorLogStore : public log_store {
 public:
  CoordinatorLogStore();
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

  // NOLINTNEXTLINE
  ptr<std::vector<ptr<log_entry>>> log_entries_ext(ulong start, ulong end, int64 batch_size_hint_in_bytes = 0) override;

  ptr<log_entry> entry_at(ulong index) override;

  ulong term_at(ulong index) override;

  ptr<buffer> pack(ulong index, int32 cnt) override;

  void apply_pack(ulong index, buffer &pack) override;

  bool compact(ulong last_log_index) override;

  bool flush() override;

  ulong last_durable_index() override;

  void Close();

  void SetDiskDelay(raft_server *raft, size_t delay_ms);

 private:
  static ptr<log_entry> MakeClone(ptr<log_entry> const &entry);

  void DiskEmulLoop();

  /**
   * Map of <log index, log data>.
   */
  std::map<ulong, ptr<log_entry>> logs_;

  /**
   * Lock for `logs_`.
   */
  mutable std::mutex logs_lock_;

  /**
   * The index of the first log.
   */
  std::atomic<ulong> start_idx_;

  /**
   * Backward pointer to Raft server.
   */
  raft_server *raft_server_bwd_pointer_;

  // Testing purpose --------------- BEGIN

  /**
   * If non-zero, this log store will emulate the disk write delay.
   */
  std::atomic<size_t> disk_emul_delay;

  /**
   * Map of <timestamp, log index>, emulating logs that is being written to disk.
   * Log index will be regarded as "durable" after the corresponding timestamp.
   */
  std::map<uint64_t, uint64_t> disk_emul_logs_being_written_;

  /**
   * Thread that will update `last_durable_index_` and call
   * `notify_log_append_completion` at proper time.
   */
  std::unique_ptr<std::thread> disk_emul_thread_;

  /**
   * Flag to terminate the thread.
   */
  std::atomic<bool> disk_emul_thread_stop_signal_;

  /**
   * Last written log index.
   */
  std::atomic<uint64_t> disk_emul_last_durable_index_;

  // Testing purpose --------------- END
};

}  // namespace memgraph::coordination
#endif
