#pragma once

#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"

#include "communication/rpc/messages.hpp"
#include "communication/raft/raft.hpp"

namespace communication::raft {

enum class RpcType { REQUEST_VOTE, APPEND_ENTRIES };

template <class State>
struct PeerRpcRequest : public rpc::Message {
  RpcType type;
  RequestVoteRequest request_vote;
  AppendEntriesRequest<State> append_entries;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<rpc::Message>(*this);
    ar &type;
    ar &request_vote;
    ar &append_entries;
  }
};

struct PeerRpcReply : public rpc::Message {
  RpcType type;
  RequestVoteReply request_vote;
  AppendEntriesReply append_entries;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<rpc::Message>(*this);
    ar &type;
    ar &request_vote;
    ar &append_entries;
  }
};

}  // namespace communication::raft
