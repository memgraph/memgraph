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

  ptr<log_entry> entry_at(ulong index) override;

  ulong term_at(ulong index) override;

  ptr<buffer> pack(ulong index, int32 cnt) override;

  void apply_pack(ulong index, buffer &pack) override;

  bool compact(ulong last_log_index) override;

  bool flush() override;

 private:
  auto FindOrDefault_(ulong index) const -> ptr<log_entry>;

  std::map<ulong, ptr<log_entry>> logs_;
  mutable std::mutex logs_lock_;
  std::atomic<ulong> start_idx_;
};

}  // namespace memgraph::coordination
#endif
