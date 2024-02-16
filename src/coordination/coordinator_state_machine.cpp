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

#ifdef MG_ENTERPRISE

#include "nuraft/coordinator_state_machine.hpp"

namespace memgraph::coordination {

auto CoordinatorStateMachine::EncodeLogAction(std::string const &name, RaftLogAction log_action) -> ptr<buffer> {
  auto const str_log = [&name, log_action] {
    switch (log_action) {
      case RaftLogAction::REGISTER_REPLICATION_INSTANCE:
        return "register_" + name;
      case RaftLogAction::UNREGISTER_REPLICATION_INSTANCE:
        return "unregister_" + name;
      case RaftLogAction::SET_INSTANCE_AS_MAIN:
        return "set_main_" + name;
      case RaftLogAction::SET_INSTANCE_AS_REPLICA:
        return "set_replica_" + name;
    }
  }();

  ptr<buffer> log = buffer::alloc(sizeof(uint32_t) + str_log.size());
  buffer_serializer bs(log);
  bs.put_str(str_log);
  return log;
}

auto CoordinatorStateMachine::DecodeLog(buffer &data) -> std::pair<std::string, RaftLogAction> {
  buffer_serializer bs(data);
  auto const log_str = bs.get_str();
  spdlog::info("Received log in DecodeLog: {}", log_str);
  return {};
}

auto CoordinatorStateMachine::pre_commit(ulong const /*log_idx*/, buffer & /*data*/) -> ptr<buffer> { return nullptr; }

auto CoordinatorStateMachine::commit(ulong const log_idx, buffer &data) -> ptr<buffer> {
  buffer_serializer bs(data);
  std::string str = bs.get_str();

  spdlog::info("commit {} : {}", log_idx, str);

  last_committed_idx_ = log_idx;
  return nullptr;
}

auto CoordinatorStateMachine::commit_config(ulong const log_idx, ptr<cluster_config> & /*new_conf*/) -> void {
  last_committed_idx_ = log_idx;
}

auto CoordinatorStateMachine::rollback(ulong const log_idx, buffer &data) -> void {
  buffer_serializer bs(data);
  std::string str = bs.get_str();

  spdlog::info("rollback {} : {}", log_idx, str);
}

auto CoordinatorStateMachine::read_logical_snp_obj(snapshot & /*snapshot*/, void *& /*user_snp_ctx*/, ulong /*obj_id*/,
                                                   ptr<buffer> &data_out, bool &is_last_obj) -> int {
  // Put dummy data.
  data_out = buffer::alloc(sizeof(int32));
  buffer_serializer bs(data_out);
  bs.put_i32(0);

  is_last_obj = true;
  return 0;
}

auto CoordinatorStateMachine::save_logical_snp_obj(snapshot &s, ulong &obj_id, buffer & /*data*/, bool /*is_first_obj*/,
                                                   bool /*is_last_obj*/) -> void {
  spdlog::info("save snapshot {} term {} object ID", s.get_last_log_idx(), s.get_last_log_term(), obj_id);
  // Request next object.
  obj_id++;
}

auto CoordinatorStateMachine::apply_snapshot(snapshot &s) -> bool {
  spdlog::info("apply snapshot {} term {}", s.get_last_log_idx(), s.get_last_log_term());
  {
    auto lock = std::lock_guard{last_snapshot_lock_};
    ptr<buffer> snp_buf = s.serialize();
    last_snapshot_ = snapshot::deserialize(*snp_buf);
  }
  return true;
}

auto CoordinatorStateMachine::free_user_snp_ctx(void *&user_snp_ctx) -> void {}

auto CoordinatorStateMachine::last_snapshot() -> ptr<snapshot> {
  auto lock = std::lock_guard{last_snapshot_lock_};
  return last_snapshot_;
}

auto CoordinatorStateMachine::last_commit_index() -> ulong { return last_committed_idx_; }

auto CoordinatorStateMachine::create_snapshot(snapshot &s, async_result<bool>::handler_type &when_done) -> void {
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

}  // namespace memgraph::coordination
#endif
