#pragma once

#include <atomic>

#include "communication/raft/raft.hpp"
#include "communication/reactor/reactor_local.cpp"

/* This is junk, hopefully we get rid of reactor leftovers soon. */

namespace communication::raft {

using namespace communication::reactor;

template <class State>
class LocalReactorNetworkInterface : public RaftNetworkInterface<State> {
 public:
  explicit LocalReactorNetworkInterface(
      communication::reactor::System &system, const std::string &id,
      const std::function<PeerRPCReply(const PeerRPCRequest<State> &)>
          &rpc_callback)
      : id_(id),
        reactor_(system.Spawn(
            id,
            [this, rpc_callback](reactor::Reactor &r) {
              reactor::EventStream *stream = r.main_.first;
              stream->OnEvent<MPeerRPCRequest>([this, rpc_callback, &r](
                  const MPeerRPCRequest &request,
                  const reactor::Subscription &) {
                if (!connected_) {
                  return;
                }
                PeerRPCReply reply = rpc_callback(request.request);
                reactor::LocalChannelWriter channel_writer(
                    request.sender, request.reply_channel, r.system_);
                auto channel =
                    r.system_.Resolve(request.sender, request.reply_channel);
                channel_writer.Send<MPeerRPCReply>(reply);
              });
            })),
        connected_(false) {}

  ~LocalReactorNetworkInterface() {}

  void Connect() { connected_ = true; }
  void Disconnect() { connected_ = false; }

  bool SendRPC(const std::string &recipient,
               const PeerRPCRequest<State> &request, PeerRPCReply &reply) {
    if (!connected_) {
      return false;
    }
    reactor::LocalChannelWriter request_channel_writer(recipient, "main",
                                                       reactor_->system_);

    std::string reply_channel_id = fmt::format("{}", request_id_++);
    auto reply_channel = reactor_->Open(reply_channel_id).first;

    std::atomic<bool> got_reply{false};

    reply_channel->template OnEvent<MPeerRPCReply>([&reply, &got_reply](
        const MPeerRPCReply &reply_message, const reactor::Subscription &) {
      reply = reply_message.reply;
      got_reply = true;
    });

    request_channel_writer.Send<MPeerRPCRequest>(
        MPeerRPCRequest(request, id_, reply_channel_id));

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    return got_reply;
  }

 private:
  struct MPeerRPCRequest : public communication::reactor::Message {
    MPeerRPCRequest(const PeerRPCRequest<State> &request,
                    const std::string &sender, const std::string &reply_channel)
        : request(request), sender(sender), reply_channel(reply_channel) {}
    PeerRPCRequest<State> request;
    std::string sender;
    std::string reply_channel;
  };

  struct MPeerRPCReply : public communication::reactor::Message {
    MPeerRPCReply(const PeerRPCReply &reply) : reply(reply) {}
    PeerRPCReply reply;
  };

  std::string id_;
  std::unique_ptr<communication::reactor::Reactor> reactor_;
  std::atomic<bool> connected_;
  std::atomic<int> request_id_{0};
};

struct MShutdown : public Message {};

template <class State>
class RaftMemberLocalReactor {
 public:
  explicit RaftMemberLocalReactor(communication::reactor::System &system,
                                  RaftStorageInterface<State> &storage,
                                  const std::string &id,
                                  const RaftConfig &config)
      : network_(system, id,
                 [this](const PeerRPCRequest<State> &request) -> PeerRPCReply {
                   return this->OnRPC(request);
                 }),
        member_(network_, storage, id, config) {}

  void Connect() { network_.Connect(); }
  void Disconnect() { network_.Disconnect(); }

  virtual ~RaftMemberLocalReactor() {}

 private:
  LocalReactorNetworkInterface<State> network_;

  PeerRPCReply OnRPC(const PeerRPCRequest<State> &request) {
    PeerRPCReply reply;
    if (request.type == RPCType::REQUEST_VOTE) {
      reply.type = RPCType::REQUEST_VOTE;
      reply.request_vote = member_.OnRequestVote(request.request_vote);
    } else if (request.type == RPCType::APPEND_ENTRIES) {
      reply.type = RPCType::APPEND_ENTRIES;
      reply.append_entries = member_.OnAppendEntries(request.append_entries);
    } else {
      LOG(FATAL) << "Unknown Raft RPC request type";
    }
    return reply;
  }

 protected:
  RaftMember<State> member_;
};

}  // namespace communication::raft
