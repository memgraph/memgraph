#include "distributed/durability_rpc_worker.hpp"

#include "database/distributed/distributed_graph_db.hpp"
#include "database/distributed/graph_db_accessor.hpp"
#include "distributed/durability_rpc_messages.hpp"

namespace distributed {

DurabilityRpcWorker::DurabilityRpcWorker(
    database::Worker *db, distributed::Coordination *coordination)
    : db_(db) {
  coordination->Register<MakeSnapshotRpc>(
      [this](auto *req_reader, auto *res_builder) {
        MakeSnapshotReq req;
        slk::Load(&req, req_reader);
        auto dba = db_->Access(req.member);
        MakeSnapshotRes res(db_->MakeSnapshot(*dba));
        slk::Save(res, res_builder);
      });

  coordination->Register<RecoverWalAndIndexesRpc>(
      [this](auto *req_reader, auto *res_builder) {
        RecoverWalAndIndexesReq req;
        slk::Load(&req, req_reader);
        this->db_->RecoverWalAndIndexes(&req.member);
        RecoverWalAndIndexesRes res;
        slk::Save(res, res_builder);
      });
}

}  // namespace distributed
