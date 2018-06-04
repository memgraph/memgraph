#pragma once

#include <unordered_map>

#include "glog/logging.h"

#include "communication/raft/network_common.hpp"
#include "communication/raft/raft.hpp"
#include "communication/rpc/client.hpp"
#include "communication/rpc/server.hpp"
#include "io/network/endpoint.hpp"

/* Implementation of `RaftNetworkInterface` using RPC. Raft RPC requests and
 * responses are wrapped in `PeerRpcRequest` and `PeerRpcReply`. */

// TODO(mtomic): Unwrap RPCs and use separate request-response protocols instead
// of `PeerProtocol`, or at least use an union to avoid sending unnecessary data
// over the wire.

namespace communication::raft {

template <class State>
using PeerProtocol = rpc::RequestResponse<PeerRpcRequest<State>, PeerRpcReply>;

template <class State>
class RpcNetwork : public RaftNetworkInterface<State> {
 public:
  RpcNetwork(rpc::Server &server,
             std::unordered_map<std::string, io::network::Endpoint> directory)
      : server_(server), directory_(std::move(directory)) {}

  virtual void Start(RaftMember<State> &member) override {
    // TODO: Serialize RPC via Cap'n Proto
//    server_.Register<PeerProtocol<State>>(
//        [&member](const auto &req_reader, auto *res_builder) {
//          PeerRpcRequest<State> request;
//          request.Load(req_reader);
//          PeerRpcReply reply;
//          reply.type = request.type;
//          switch (request.type) {
//            case RpcType::REQUEST_VOTE:
//              reply.request_vote = member.OnRequestVote(request.request_vote);
//              break;
//            case RpcType::APPEND_ENTRIES:
//              reply.append_entries =
//                  member.OnAppendEntries(request.append_entries);
//              break;
//            default:
//              LOG(ERROR) << "Unknown RPC type: "
//                         << static_cast<int>(request.type);
//          }
//          reply.Save(res_builder);
//        });
  }

  virtual bool SendRequestVote(const MemberId &recipient,
                               const RequestVoteRequest &request,
                               RequestVoteReply &reply) override {
    PeerRpcRequest<State> req;
    PeerRpcReply rep;

    req.type = RpcType::REQUEST_VOTE;
    req.request_vote = request;

    if (!SendRpc(recipient, req, rep)) {
      return false;
    }

    reply = rep.request_vote;
    return true;
  }

  virtual bool SendAppendEntries(const MemberId &recipient,
                                 const AppendEntriesRequest<State> &request,
                                 AppendEntriesReply &reply) override {
    PeerRpcRequest<State> req;
    PeerRpcReply rep;

    req.type = RpcType::APPEND_ENTRIES;
    req.append_entries = request;

    if (!SendRpc(recipient, req, rep)) {
      return false;
    }

    reply = rep.append_entries;
    return true;
  }

 private:
  bool SendRpc(const MemberId &recipient, const PeerRpcRequest<State> &request,
               PeerRpcReply &reply) {
    auto &client = GetClient(recipient);
    auto response = client.template Call<PeerProtocol<State>>(request);

    if (!response) {
      return false;
    }

    reply = *response;
    return true;
  }

  rpc::Client &GetClient(const MemberId &id) {
    auto it = clients_.find(id);
    if (it == clients_.end()) {
      auto ne = directory_[id];
      it = clients_.try_emplace(id, ne).first;
    }
    return it->second;
  }

  rpc::Server &server_;
  // TODO(mtomic): how to update and distribute this?
  std::unordered_map<MemberId, io::network::Endpoint> directory_;

  std::unordered_map<MemberId, rpc::Client> clients_;
};

}  // namespace communication::raft
