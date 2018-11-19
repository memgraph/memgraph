#include "raft/raft_server.hpp"
#include "raft/exceptions.hpp"
#include "raft/raft_rpc_messages.hpp"

#include "utils/exceptions.hpp"

#include <experimental/filesystem>

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace fs = std::experimental::filesystem;

namespace raft {

RaftServer::RaftServer(uint16_t server_id, const Config &config,
                       Coordination *coordination)
    : config_(config),
      server_id_(server_id),
      disk_storage_(config.disk_storage_path) {
  coordination->Register<RequestVoteRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        throw utils::NotYetImplemented("RaftServer constructor");
      });

  coordination->Register<AppendEntriesRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        throw utils::NotYetImplemented("RaftServer constructor");
      });
}

void RaftServer::Transition(const Mode &new_mode) {
  throw utils::NotYetImplemented("RaftServer transition");
}

}  // namespace raft
