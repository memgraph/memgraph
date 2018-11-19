#include "raft/raft_server.hpp"

#include <experimental/filesystem>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "raft/coordination.hpp"
#include "raft/exceptions.hpp"
#include "raft/raft_rpc_messages.hpp"
#include "utils/exceptions.hpp"

namespace raft {

namespace fs = std::experimental::filesystem;

const std::string kRaftDir = "raft";

RaftServer::RaftServer(uint16_t server_id, const std::string &durability_dir,
                       const Config &config, Coordination *coordination)
    : server_id_(server_id),
      config_(config),
      coordination_(coordination),
      disk_storage_(fs::path(durability_dir) / kRaftDir) {
  coordination_->Register<RequestVoteRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        throw utils::NotYetImplemented("RaftServer constructor");
      });

  coordination_->Register<AppendEntriesRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        throw utils::NotYetImplemented("RaftServer constructor");
      });
}

void RaftServer::Transition(const Mode &new_mode) {
  if (new_mode == Mode::LEADER)
    log_entry_buffer_.Enable();
  else
    log_entry_buffer_.Disable();

  throw utils::NotYetImplemented("RaftServer transition");
}

void RaftServer::Replicate(const std::vector<database::StateDelta> &log) {
  throw utils::NotYetImplemented("RaftServer replication");
}

void RaftServer::Emplace(const database::StateDelta &delta) {
  log_entry_buffer_.Emplace(delta);
}

RaftServer::LogEntryBuffer::LogEntryBuffer(RaftServer *raft_server)
    : raft_server_(raft_server) {
  CHECK(raft_server_) << "RaftServer can't be nullptr";
}

void RaftServer::LogEntryBuffer::Enable() {
  std::lock_guard<std::mutex> guard(lock_);
  enabled_ = true;
}

void RaftServer::LogEntryBuffer::Disable() {
  std::lock_guard<std::mutex> guard(lock_);
  enabled_ = false;
  // Clear all existing logs from buffers.
  logs_.clear();
}

void RaftServer::LogEntryBuffer::Emplace(const database::StateDelta &delta) {
  std::lock_guard<std::mutex> guard(lock_);
  if (!enabled_) return;

  tx::TransactionId tx_id = delta.transaction_id;
  if (IsStateDeltaTransactionEnd(delta)) {
    auto it = logs_.find(tx_id);
    CHECK(it != logs_.end()) << "Missing StateDeltas for transaction " << tx_id;

    std::vector<database::StateDelta> log(std::move(it->second));
    log.emplace_back(std::move(delta));
    logs_.erase(it);

    raft_server_->Replicate(log);
  } else {
    logs_[tx_id].emplace_back(std::move(delta));
  }
}

bool RaftServer::LogEntryBuffer::IsStateDeltaTransactionEnd(
    const database::StateDelta &delta) {
  switch (delta.type) {
    case database::StateDelta::Type::TRANSACTION_COMMIT:
      return true;
    case database::StateDelta::Type::TRANSACTION_ABORT:
    case database::StateDelta::Type::TRANSACTION_BEGIN:
    case database::StateDelta::Type::CREATE_VERTEX:
    case database::StateDelta::Type::CREATE_EDGE:
    case database::StateDelta::Type::SET_PROPERTY_VERTEX:
    case database::StateDelta::Type::SET_PROPERTY_EDGE:
    case database::StateDelta::Type::ADD_LABEL:
    case database::StateDelta::Type::REMOVE_LABEL:
    case database::StateDelta::Type::REMOVE_VERTEX:
    case database::StateDelta::Type::REMOVE_EDGE:
    case database::StateDelta::Type::BUILD_INDEX:
    case database::StateDelta::Type::DROP_INDEX:
      return false;
  }
}

}  // namespace raft
