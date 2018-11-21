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
    : config_(config),
      server_id_(server_id),
      disk_storage_(fs::path(durability_dir) / kRaftDir) {
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
