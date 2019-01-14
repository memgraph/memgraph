/// @file

#pragma once

#include <map>
#include <mutex>

#include "distributed/bfs_rpc_messages.hpp"
#include "distributed/bfs_subcursor.hpp"
#include "distributed/coordination.hpp"

namespace distributed {

/// Along with `BfsRpcClients`, this class is used to expose `BfsSubcursor`
/// interface over the network so that subcursors can communicate during the
/// traversal. It is just a thin wrapper forwarding RPC calls to subcursors in
/// subcursor storage.
class BfsRpcServer {
 public:
  BfsRpcServer(database::DistributedGraphDb *db,
               distributed::Coordination *coordination,
               BfsSubcursorStorage *subcursor_storage)
      : db_(db), subcursor_storage_(subcursor_storage) {
    coordination->Register<CreateBfsSubcursorRpc>([this](const auto &req_reader,
                                                         auto *res_builder) {
      CreateBfsSubcursorReq req;
      auto ast_storage = std::make_unique<query::AstStorage>();
      Load(&req, req_reader, ast_storage.get());
      database::GraphDbAccessor *dba;
      {
        std::lock_guard<std::mutex> guard(lock_);
        auto it = db_accessors_.find(req.tx_id);
        if (it == db_accessors_.end()) {
          it = db_accessors_.emplace(req.tx_id, db_->Access(req.tx_id)).first;
        }
        dba = it->second.get();
      }
      query::EvaluationContext evaluation_context;
      evaluation_context.timestamp = req.timestamp;
      evaluation_context.parameters = req.parameters;
      evaluation_context.properties =
          query::NamesToProperties(ast_storage->properties_, dba);
      evaluation_context.labels =
          query::NamesToLabels(ast_storage->labels_, dba);
      auto id = subcursor_storage_->Create(
          dba, req.direction, req.edge_types, std::move(req.symbol_table),
          std::move(ast_storage), req.filter_lambda, evaluation_context);
      CreateBfsSubcursorRes res(id);
      Save(res, res_builder);
    });

    coordination->Register<RegisterSubcursorsRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          RegisterSubcursorsReq req;
          Load(&req, req_reader);
          subcursor_storage_->Get(req.subcursor_ids.at(db_->WorkerId()))
              ->RegisterSubcursors(req.subcursor_ids);
          RegisterSubcursorsRes res;
          Save(res, res_builder);
        });

    coordination->Register<ResetSubcursorRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          ResetSubcursorReq req;
          Load(&req, req_reader);
          subcursor_storage_->Get(req.subcursor_id)->Reset();
          ResetSubcursorRes res;
          Save(res, res_builder);
        });

    coordination->Register<SetSourceRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          SetSourceReq req;
          Load(&req, req_reader);
          subcursor_storage_->Get(req.subcursor_id)->SetSource(req.source);
          SetSourceRes res;
          Save(res, res_builder);
        });

    coordination->Register<ExpandLevelRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          ExpandLevelReq req;
          Load(&req, req_reader);
          auto subcursor = subcursor_storage_->Get(req.member);
          ExpandResult result;
          try {
            result = subcursor->ExpandLevel() ? ExpandResult::SUCCESS
                                              : ExpandResult::FAILURE;
          } catch (const query::QueryRuntimeException &) {
            result = ExpandResult::LAMBDA_ERROR;
          }
          ExpandLevelRes res(result);
          Save(res, res_builder);
        });

    coordination->Register<SubcursorPullRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          SubcursorPullReq req;
          Load(&req, req_reader);
          auto vertex = subcursor_storage_->Get(req.member)->Pull();
          SubcursorPullRes res(vertex);
          Save(res, res_builder, db_->WorkerId());
        });

    coordination->Register<ExpandToRemoteVertexRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          ExpandToRemoteVertexReq req;
          Load(&req, req_reader);
          ExpandToRemoteVertexRes res(
              subcursor_storage_->Get(req.subcursor_id)
                  ->ExpandToLocalVertex(req.edge, req.vertex));
          Save(res, res_builder);
        });

    coordination->Register<ReconstructPathRpc>([this](const auto &req_reader,
                                                      auto *res_builder) {
      ReconstructPathReq req;
      Load(&req, req_reader);
      auto subcursor = subcursor_storage_->Get(req.subcursor_id);
      PathSegment result;
      if (req.vertex) {
        result = subcursor->ReconstructPath(*req.vertex);
      } else if (req.edge) {
        result = subcursor->ReconstructPath(*req.edge);
      } else {
        LOG(FATAL) << "`edge` or `vertex` should be set in ReconstructPathReq";
      }
      ReconstructPathRes res(result.edges, result.next_vertex,
                             result.next_edge);
      Save(res, res_builder, db_->WorkerId());
    });

    coordination->Register<PrepareForExpandRpc>([this](const auto &req_reader,
                                                       auto *res_builder) {
      PrepareForExpandReq req;
      auto subcursor_id = req_reader.getSubcursorId();
      auto *subcursor = subcursor_storage_->Get(subcursor_id);
      Load(&req, req_reader, subcursor->db_accessor(), &db_->data_manager());
      subcursor->PrepareForExpand(req.clear, std::move(req.frame));
      PrepareForExpandRes res;
      Save(res, res_builder);
    });
  }

  void ClearTransactionalCache(tx::TransactionId oldest_active) {
    // It is unlikely this will become a performance issue, but if it does, we
    // should store database accessors in a lock-free map.
    std::lock_guard<std::mutex> guard(lock_);
    for (auto it = db_accessors_.begin(); it != db_accessors_.end();) {
      if (it->first < oldest_active) {
        it = db_accessors_.erase(it);
      } else {
        it++;
      }
    }
  }

 private:
  database::DistributedGraphDb *db_;

  std::mutex lock_;
  std::map<tx::TransactionId, std::unique_ptr<database::GraphDbAccessor>>
      db_accessors_;
  BfsSubcursorStorage *subcursor_storage_;
};

}  // namespace distributed
