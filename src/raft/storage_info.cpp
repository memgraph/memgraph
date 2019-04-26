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

  for (auto id : coordination_->GetAllNodeIds()) {
    if (id == server_id_) {
      info.emplace(std::to_string(id), GetLocalStorageInfo());
    } else {
      auto reply = coordination_->ExecuteOnOtherNode<StorageInfoRpc>(id);
      if (reply) {
        info[std::to_string(id)] = std::move(reply->storage_info);
      } else {
        info[std::to_string(id)] = {};
      }
    }
  }

  return info;
}

}  // namespace raft
