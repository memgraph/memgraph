#include "distributed/dynamic_worker.hpp"

#include "database/distributed_graph_db.hpp"
#include "distributed/dynamic_worker_rpc_messages.hpp"

namespace distributed {
using Server = communication::rpc::Server;
using ClientPool = communication::rpc::ClientPool;

DynamicWorkerAddition::DynamicWorkerAddition(database::DistributedGraphDb *db,
                                             Server *server)
    : db_(db), server_(server) {
  server_->Register<DynamicWorkerRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        DynamicWorkerReq req;
        req.Load(req_reader);
        DynamicWorkerRes res(this->GetIndicesToCreate());
        res.Save(res_builder);
      });
}

std::vector<std::pair<std::string, std::string>>
DynamicWorkerAddition::GetIndicesToCreate() {
  std::vector<std::pair<std::string, std::string>> indices;
  if (!enabled_.load()) return indices;
  for (const auto &key : db_->storage().label_property_index().Keys()) {
    auto label = db_->label_mapper().id_to_value(key.label_);
    auto property = db_->property_mapper().id_to_value(key.property_);
    indices.emplace_back(label, property);
  }
  return indices;
}

void DynamicWorkerAddition::Enable() { enabled_.store(true); }

DynamicWorkerRegistration::DynamicWorkerRegistration(ClientPool *client_pool)
    : client_pool_(client_pool) {}

std::vector<std::pair<std::string, std::string>>
DynamicWorkerRegistration::GetIndicesToCreate() {
  auto result = client_pool_->Call<DynamicWorkerRpc>();
  return result.recover_indices;
}

}  // namespace distributed
