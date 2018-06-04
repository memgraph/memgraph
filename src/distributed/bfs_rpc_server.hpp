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
  BfsRpcServer(database::GraphDb *db, communication::rpc::Server *server,
               BfsSubcursorStorage *subcursor_storage)
      : db_(db), server_(server), subcursor_storage_(subcursor_storage) {
    server_->Register<CreateBfsSubcursorRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          CreateBfsSubcursorReq req;
          req.Load(req_reader);
          CreateBfsSubcursorRes res(subcursor_storage_->Create(
              req.tx_id, req.direction, req.edge_types, req.graph_view));
          res.Save(res_builder);
        });

    server_->Register<RegisterSubcursorsRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          RegisterSubcursorsReq req;
          req.Load(req_reader);
          subcursor_storage_->Get(req.subcursor_ids.at(db_->WorkerId()))
              ->RegisterSubcursors(req.subcursor_ids);
          RegisterSubcursorsRes res;
          res.Save(res_builder);
        });

    server_->Register<RemoveBfsSubcursorRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          RemoveBfsSubcursorReq req;
          req.Load(req_reader);
          subcursor_storage_->Erase(req.member);
          RemoveBfsSubcursorRes res;
          res.Save(res_builder);
        });

    server_->Register<SetSourceRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          SetSourceReq req;
          req.Load(req_reader);
          subcursor_storage_->Get(req.subcursor_id)->SetSource(req.source);
          SetSourceRes res;
          res.Save(res_builder);
        });

    server_->Register<ExpandLevelRpc>([this](const auto &req_reader,
                                             auto *res_builder) {
      ExpandLevelReq req;
      req.Load(req_reader);
      ExpandLevelRes res(subcursor_storage_->Get(req.member)->ExpandLevel());
      res.Save(res_builder);
    });

    server_->Register<SubcursorPullRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          SubcursorPullReq req;
          req.Load(req_reader);
          auto vertex = subcursor_storage_->Get(req.member)->Pull();
          if (!vertex) {
            SubcursorPullRes res;
            res.Save(res_builder);
            return;
          }
          SubcursorPullRes res(*vertex);
          res.Save(res_builder);
        });

    server_->Register<ExpandToRemoteVertexRpc>(
        [this](const auto &req_reader, auto *res_builder) {
          ExpandToRemoteVertexReq req;
          req.Load(req_reader);
          ExpandToRemoteVertexRes res(
              subcursor_storage_->Get(req.subcursor_id)
                  ->ExpandToLocalVertex(req.edge, req.vertex));
          res.Save(res_builder);
        });

    server_->Register<ReconstructPathRpc>([this](const auto &req_reader,
                                                 auto *res_builder) {
      ReconstructPathReq req;
      req.Load(req_reader);
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
      res.Save(res_builder);
    });

    server_->Register<PrepareForExpandRpc>([this](const auto &req_reader,
                                                  auto *res_builder) {
      PrepareForExpandReq req;
      req.Load(req_reader);
      subcursor_storage_->Get(req.subcursor_id)->PrepareForExpand(req.clear);
      PrepareForExpandRes res;
      res.Save(res_builder);
    });
  }

 private:
  database::GraphDb *db_;

  communication::rpc::Server *server_;
  BfsSubcursorStorage *subcursor_storage_;
};

}  // namespace distributed
