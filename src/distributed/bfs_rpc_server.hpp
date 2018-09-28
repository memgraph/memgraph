/// @file

#pragma once

#include <map>

#include "communication/rpc/server.hpp"

#include "distributed/bfs_rpc_messages.hpp"
#include "distributed/bfs_subcursor.hpp"

namespace distributed {

/// Along with `BfsRpcClients`, this class is used to expose `BfsSubcursor`
/// interface over the network so that subcursors can communicate during the
/// traversal. It is just a thin wrapper forwarding RPC calls to subcursors in
/// subcursor storage.
class BfsRpcServer {
 public:
  BfsRpcServer(database::DistributedGraphDb *db,
               communication::rpc::Server *server,
               BfsSubcursorStorage *subcursor_storage)
      : db_(db), server_(server), subcursor_storage_(subcursor_storage) {
    server_->Register<CreateBfsSubcursorRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          CreateBfsSubcursorReq req;
          auto ast_storage = std::make_unique<query::AstStorage>();
          Load(&req, req_reader, ast_storage.get());
          auto db_accessor = db_->Access(req.tx_id);
          auto id = subcursor_storage_->Create(
              db_accessor.get(), req.direction, req.edge_types,
              std::move(req.symbol_table), std::move(ast_storage),
              req.filter_lambda, std::move(req.evaluation_context));
          db_accessors_[id] = std::move(db_accessor);
          CreateBfsSubcursorRes res(id);
          Save(res, res_builder);
        });

    server_->Register<RegisterSubcursorsRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          RegisterSubcursorsReq req;
          Load(&req, req_reader);
          subcursor_storage_->Get(req.subcursor_ids.at(db_->WorkerId()))
              ->RegisterSubcursors(req.subcursor_ids);
          RegisterSubcursorsRes res;
          Save(res, res_builder);
        });

    server_->Register<ResetSubcursorRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          ResetSubcursorReq req;
          Load(&req, req_reader);
          subcursor_storage_->Get(req.subcursor_id)->Reset();
          ResetSubcursorRes res;
          Save(res, res_builder);
        });

    server_->Register<RemoveBfsSubcursorRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          RemoveBfsSubcursorReq req;
          Load(&req, req_reader);
          db_accessors_.erase(req.member);
          subcursor_storage_->Erase(req.member);
          RemoveBfsSubcursorRes res;
          Save(res, res_builder);
        });

    server_->Register<SetSourceRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          SetSourceReq req;
          Load(&req, req_reader);
          subcursor_storage_->Get(req.subcursor_id)->SetSource(req.source);
          SetSourceRes res;
          Save(res, res_builder);
        });

    server_->Register<ExpandLevelRpc>(
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

    server_->Register<SubcursorPullRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          SubcursorPullReq req;
          Load(&req, req_reader);
          auto vertex = subcursor_storage_->Get(req.member)->Pull();
          SubcursorPullRes res(vertex);
          Save(res, res_builder, db_->WorkerId());
        });

    server_->Register<ExpandToRemoteVertexRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          ExpandToRemoteVertexReq req;
          Load(&req, req_reader);
          ExpandToRemoteVertexRes res(
              subcursor_storage_->Get(req.subcursor_id)
                  ->ExpandToLocalVertex(req.edge, req.vertex));
          Save(res, res_builder);
        });

    server_->Register<ReconstructPathRpc>([this](const auto &req_reader,
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

    server_->Register<PrepareForExpandRpc>([this](const auto &req_reader,
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

 private:
  database::DistributedGraphDb *db_;

  communication::rpc::Server *server_;
  std::map<int64_t, std::unique_ptr<database::GraphDbAccessor>> db_accessors_;
  BfsSubcursorStorage *subcursor_storage_;
};

}  // namespace distributed
