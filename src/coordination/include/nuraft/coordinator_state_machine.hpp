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

#include <spdlog/spdlog.h>
#include <libnuraft/nuraft.hxx>

namespace memgraph::coordination {

using nuraft::async_result;
using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::cluster_config;
using nuraft::int32;
using nuraft::ptr;
using nuraft::snapshot;
using nuraft::state_machine;

class CoordinatorStateMachine : public state_machine {
 public:
  CoordinatorStateMachine() = default;
  CoordinatorStateMachine(CoordinatorStateMachine const &) = delete;
  CoordinatorStateMachine &operator=(CoordinatorStateMachine const &) = delete;
  CoordinatorStateMachine(CoordinatorStateMachine &&) = delete;
  CoordinatorStateMachine &operator=(CoordinatorStateMachine &&) = delete;
  ~CoordinatorStateMachine() override {}

  ptr<buffer> pre_commit(ulong const log_idx, buffer &data) override {
    buffer_serializer bs(data);
    std::string str = bs.get_str();

    spdlog::info("pre_commit {} : {}", log_idx, str);
    return nullptr;
  }

  ptr<buffer> commit(ulong const log_idx, buffer &data) override {
    buffer_serializer bs(data);
    std::string str = bs.get_str();

    spdlog::info("commit {} : {}", log_idx, str);

    last_committed_idx_ = log_idx;
    return nullptr;
  }

  void commit_config(ulong const log_idx, ptr<cluster_config> & /*new_conf*/) override {
    last_committed_idx_ = log_idx;
  }

  void rollback(ulong const log_idx, buffer &data) override {
    buffer_serializer bs(data);
    std::string str = bs.get_str();

    spdlog::info("rollback {} : {}", log_idx, str);
  }

  int read_logical_snp_obj(snapshot & /*snapshot*/, void *& /*user_snp_ctx*/, ulong /*obj_id*/, ptr<buffer> &data_out,
                           bool &is_last_obj) override {
    // Put dummy data.
    data_out = buffer::alloc(sizeof(int32));
    buffer_serializer bs(data_out);
    bs.put_i32(0);

    is_last_obj = true;
    return 0;
  }

  void save_logical_snp_obj(snapshot &s, ulong &obj_id, buffer & /*data*/, bool /*is_first_obj*/,
                            bool /*is_last_obj*/) override {
    spdlog::info("save snapshot {} term {} object ID", s.get_last_log_idx(), s.get_last_log_term(), obj_id);
    // Request next object.
    obj_id++;
  }

  bool apply_snapshot(snapshot &s) override {
    spdlog::info("apply snapshot {} term {}", s.get_last_log_idx(), s.get_last_log_term());
    {
      auto lock = std::lock_guard{last_snapshot_lock_};
      ptr<buffer> snp_buf = s.serialize();
      last_snapshot_ = snapshot::deserialize(*snp_buf);
    }
    return true;
  }

  void free_user_snp_ctx(void *&user_snp_ctx) override {}

  ptr<snapshot> last_snapshot() override {
    auto lock = std::lock_guard{last_snapshot_lock_};
    return last_snapshot_;
  }

  ulong last_commit_index() override { return last_committed_idx_; }

  void create_snapshot(snapshot &s, async_result<bool>::handler_type &when_done) override {
    spdlog::info("create snapshot {} term {}", s.get_last_log_idx(), s.get_last_log_term());
    // Clone snapshot from `s`.
    {
      auto lock = std::lock_guard{last_snapshot_lock_};
      ptr<buffer> snp_buf = s.serialize();
      last_snapshot_ = snapshot::deserialize(*snp_buf);
    }
    ptr<std::exception> except(nullptr);
    bool ret = true;
    when_done(ret, except);
  }

 private:
  std::atomic<uint64_t> last_committed_idx_{0};

  ptr<snapshot> last_snapshot_;

  std::mutex last_snapshot_lock_;
};

}  // namespace memgraph::coordination
#endif
