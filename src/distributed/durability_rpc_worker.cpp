#include "distributed/durability_rpc_worker.hpp"

#include "database/distributed/distributed_graph_db.hpp"
#include "database/distributed/graph_db_accessor.hpp"
#include "distributed/durability_rpc_messages.hpp"

namespace distributed {

DurabilityRpcWorker::DurabilityRpcWorker(
    database::Worker *db, distributed::Coordination *coordination)
    : db_(db) {
  coordination->Register<MakeSnapshotRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        auto dba = db_->Access(req_reader.getMember());
        MakeSnapshotRes res(db_->MakeSnapshot(*dba));
        Save(res, res_builder);
      });

  coordination->Register<RecoverWalAndIndexesRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        durability::RecoveryData recovery_data;
        durability::Load(&recovery_data, req_reader.getMember());
        this->db_->RecoverWalAndIndexes(&recovery_data);
      });
}

}  // namespace distributed
