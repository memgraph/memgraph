#pragma once

#include "communication/rpc/messages.hpp"
#include "communication/raft/raft.hpp"

namespace communication::raft {

enum class RpcType { REQUEST_VOTE, APPEND_ENTRIES };

template <class State>
struct PeerRpcRequest {
  RpcType type;
  RequestVoteRequest request_vote;
  AppendEntriesRequest<State> append_entries;
};

struct PeerRpcReply {
  RpcType type;
  RequestVoteReply request_vote;
  AppendEntriesReply append_entries;
};

}  // namespace communication::raft
