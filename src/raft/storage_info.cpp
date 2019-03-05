#include "raft/storage_info.hpp"

#include <chrono>

#include "database/single_node_ha/graph_db.hpp"
#include "raft/coordination.hpp"
#include "raft/storage_info_rpc_messages.hpp"
#include "utils/future.hpp"
#include "utils/stat.hpp"

namespace raft {

using namespace std::literals::chrono_literals;
using Clock = std::chrono::system_clock;
using TimePoint = std::chrono::system_clock::time_point;

const std::chrono::duration<int64_t> kRpcTimeout = 1s;

StorageInfo::StorageInfo(database::GraphDb *db, Coordination *coordination,
                         uint16_t server_id)
    : db_(db), coordination_(coordination), server_id_(server_id) {
  CHECK(db) << "Graph DB can't be nullptr";
  CHECK(coordination) << "Coordination can't be nullptr";
}

StorageInfo::~StorageInfo() {}

void StorageInfo::Start() {
  coordination_->Register<StorageInfoRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        StorageInfoReq req;
        Load(&req, req_reader);

        StorageInfoRes res(this->server_id_, this->GetLocalStorageInfo());
        Save(res, res_builder);
      });
}

std::vector<std::pair<std::string, std::string>>
StorageInfo::GetLocalStorageInfo() const {
  std::vector<std::pair<std::string, std::string>> info;

  db_->RefreshStat();
  auto &stat = db_->GetStat();

  info.emplace_back("vertex_count", std::to_string(stat.vertex_count));
  info.emplace_back("edge_count", std::to_string(stat.edge_count));
  info.emplace_back("average_degree", std::to_string(stat.avg_degree));
  info.emplace_back("memory_usage", std::to_string(utils::GetMemoryUsage()));
  info.emplace_back("disk_usage",
                    std::to_string(db_->GetDurabilityDirDiskUsage()));

  return info;
}

std::map<std::string, std::vector<std::pair<std::string, std::string>>>
StorageInfo::GetStorageInfo() const {
  std::map<std::string, std::vector<std::pair<std::string, std::string>>> info;
  std::map<uint16_t, utils::Future<StorageInfoRes>> remote_storage_info_futures;
  std::map<uint16_t, bool> received_reply;

  auto peers = coordination_->GetWorkerIds();

  for (auto id : peers) {
    received_reply[id] = false;
    if (id == server_id_) {
      info.emplace(std::to_string(id), GetLocalStorageInfo());
      received_reply[id] = true;
    } else {
      remote_storage_info_futures.emplace(
          id, coordination_->ExecuteOnWorker<StorageInfoRes>(
                  id, [&](int worker_id, auto &client) {
                    try {
                      auto res = client.template Call<StorageInfoRpc>();
                      return res;
                    } catch (...) {
                      return StorageInfoRes(id, {});
                    }
                  }));
    }
  }

  int16_t waiting_for = peers.size() - 1;

  TimePoint start = Clock::now();
  while (Clock::now() - start <= kRpcTimeout && waiting_for > 0) {
    for (auto id : peers) {
      if (received_reply[id]) continue;
      auto &future = remote_storage_info_futures[id];
      if (!future.IsReady()) continue;

      auto reply = future.get();
      info.emplace(std::to_string(reply.server_id),
                   std::move(reply.storage_info));
      received_reply[id] = true;
      waiting_for--;
    }
  }

  return info;
}

}  // namespace raft
