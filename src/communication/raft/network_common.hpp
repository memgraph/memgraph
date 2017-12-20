#pragma once

#include "cereal/cereal.hpp"

#include "communication/messaging/distributed.hpp"
#include "communication/raft/raft.hpp"

namespace communication::raft {

enum class RpcType { REQUEST_VOTE, APPEND_ENTRIES };

template <class State>
struct PeerRpcRequest : public messaging::Message {
  RpcType type;
  RequestVoteRequest request_vote;
  AppendEntriesRequest<State> append_entries;

  template <class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<messaging::Message>(this), type, request_vote,
       append_entries);
  }
};

struct PeerRpcReply : public messaging::Message {
  RpcType type;
  RequestVoteReply request_vote;
  AppendEntriesReply append_entries;

  template <class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<messaging::Message>(this), type, request_vote,
       append_entries);
  }
};

}  // namespace communication::raft
