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
        [this](const CreateBfsSubcursorReq &req) {
          return std::make_unique<CreateBfsSubcursorRes>(
              subcursor_storage_->Create(req.tx_id, req.direction,
                                         req.edge_types, req.graph_view));
        });

    server_->Register<RegisterSubcursorsRpc>(
        [this](const RegisterSubcursorsReq &req) {
          subcursor_storage_->Get(req.subcursor_ids.at(db_->WorkerId()))
              ->RegisterSubcursors(req.subcursor_ids);
          return std::make_unique<RegisterSubcursorsRes>();
        });

    server_->Register<RemoveBfsSubcursorRpc>(
        [this](const RemoveBfsSubcursorReq &req) {
          subcursor_storage_->Erase(req.member);
          return std::make_unique<RemoveBfsSubcursorRes>();
        });

    server_->Register<SetSourceRpc>([this](const SetSourceReq &req) {
      subcursor_storage_->Get(req.subcursor_id)->SetSource(req.source);
      return std::make_unique<SetSourceRes>();
    });

    server_->Register<ExpandLevelRpc>([this](const ExpandLevelReq &req) {
      return std::make_unique<ExpandLevelRes>(
          subcursor_storage_->Get(req.member)->ExpandLevel());
    });

    server_->Register<SubcursorPullRpc>([this](const SubcursorPullReq &req) {
      auto vertex = subcursor_storage_->Get(req.member)->Pull();
      if (!vertex) {
        return std::make_unique<SubcursorPullRes>();
      }
      return std::make_unique<SubcursorPullRes>(*vertex);
    });

    server_->Register<ExpandToRemoteVertexRpc>(
        [this](const ExpandToRemoteVertexReq &req) {
          return std::make_unique<ExpandToRemoteVertexRes>(
              subcursor_storage_->Get(req.subcursor_id)
                  ->ExpandToLocalVertex(req.edge, req.vertex));
        });

    server_->Register<ReconstructPathRpc>([this](
                                              const ReconstructPathReq &req) {
      auto subcursor = subcursor_storage_->Get(req.subcursor_id);
      PathSegment result;
      if (req.vertex) {
        result = subcursor->ReconstructPath(*req.vertex);
      } else if (req.edge) {
        result = subcursor->ReconstructPath(*req.edge);
      } else {
        LOG(FATAL) << "`edge` or `vertex` should be set in ReconstructPathReq";
      }
      return std::make_unique<ReconstructPathRes>(
          result.edges, result.next_vertex, result.next_edge);
    });

    server_->Register<PrepareForExpandRpc>([this](
                                               const PrepareForExpandReq &req) {
      subcursor_storage_->Get(req.subcursor_id)->PrepareForExpand(req.clear);
      return std::make_unique<PrepareForExpandRes>();
    });
  }

 private:
  database::GraphDb *db_;

  communication::rpc::Server *server_;
  BfsSubcursorStorage *subcursor_storage_;
};

}  // namespace distributed
