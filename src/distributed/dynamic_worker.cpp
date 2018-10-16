#include "distributed/dynamic_worker.hpp"

#include "database/distributed/distributed_graph_db.hpp"
#include "distributed/dynamic_worker_rpc_messages.hpp"

namespace distributed {

DynamicWorkerAddition::DynamicWorkerAddition(database::DistributedGraphDb *db,
                                             distributed::Coordination *coordination)
    : db_(db), coordination_(coordination) {
  coordination_->Register<DynamicWorkerRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        DynamicWorkerReq req;
        Load(&req, req_reader);
        DynamicWorkerRes res(this->GetIndicesToCreate());
        Save(res, res_builder);
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

DynamicWorkerRegistration::DynamicWorkerRegistration(communication::rpc::ClientPool *client_pool)
    : client_pool_(client_pool) {}

std::vector<std::pair<std::string, std::string>>
DynamicWorkerRegistration::GetIndicesToCreate() {
  auto result = client_pool_->Call<DynamicWorkerRpc>();
  return result.recover_indices;
}

}  // namespace distributed
