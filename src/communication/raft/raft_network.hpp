#pragma once

#include <mutex>

#include "communication/reactor/reactor_local.hpp"

namespace communication::raft {

struct MLeaderTimeout : public communication::reactor::Message {};
struct MShutdown : public communication::reactor::Message {};

struct RaftMessage : public communication::reactor::Message {
  RaftMessage(int term, const std::string &sender_id)
      : term(term), sender_id(sender_id) {}
  int term;
  std::string sender_id;
};

struct RaftMessageReply : public RaftMessage {
  RaftMessageReply(int term, const std::string &sender_id, bool success)
      : RaftMessage(term, sender_id), success(success) {}
  std::experimental::optional<std::string> leader;
  bool success;
};

struct MRequestVote : public RaftMessage {
  MRequestVote(int candidate_term, const std::string &candidate_id)
      : RaftMessage(candidate_term, candidate_id) {}
};

struct MRequestVoteReply : public RaftMessageReply {
  MRequestVoteReply(int term, const std::string &sender_id, bool vote_granted)
      : RaftMessageReply(term, sender_id, vote_granted) {}
};

struct MAppendEntries : public RaftMessage {
  MAppendEntries(int term, const std::string &sender_id)
      : RaftMessage(term, sender_id) {}
};

struct MAppendEntriesReply : public RaftMessageReply {
  MAppendEntriesReply(int term, const std::string &sender_id, bool success)
      : RaftMessageReply(term, sender_id, success) {}
};

class RaftNetworkInterface {
 public:
  virtual ~RaftNetworkInterface() {}

  virtual bool RequestVote(const std::string &recipient,
                           const MRequestVote &msg) = 0;
  virtual bool RequestVoteReply(const std::string &recipient,
                                const MRequestVoteReply &msg) = 0;
  virtual bool AppendEntries(const std::string &recipient,
                             const MAppendEntries &msg) = 0;
  virtual bool AppendEntriesReply(const std::string &recipient,
                                  const MAppendEntriesReply &msg) = 0;
};

class LocalReactorNetworkInterface : public RaftNetworkInterface {
 public:
  explicit LocalReactorNetworkInterface(communication::reactor::System &system)
      : system_(system) {}

  bool RequestVote(const std::string &recipient,
                   const MRequestVote &msg) override {
    return SendMessage(recipient, msg);
  }

  bool RequestVoteReply(const std::string &recipient,
                        const MRequestVoteReply &msg) override {
    return SendMessage(recipient, msg);
  }

  bool AppendEntries(const std::string &recipient,
                     const MAppendEntries &msg) override {
    return SendMessage(recipient, msg);
  }

  bool AppendEntriesReply(const std::string &recipient,
                          const MAppendEntriesReply &msg) override {
    return SendMessage(recipient, msg);
  }

 private:
  template <class TMessage>
  bool SendMessage(const std::string &recipient, const TMessage &msg) {
    auto channel = system_.FindChannel(recipient, "main");
    if (!channel) {
      return false;
    }
    channel->Send<TMessage>(msg);
    return true;
  }

  communication::reactor::System &system_;
};

class FakeNetworkInterface : public RaftNetworkInterface {
 public:
  explicit FakeNetworkInterface(communication::reactor::System &system) : system_(system) {}

  bool RequestVote(const std::string &recipient,
                   const MRequestVote &msg) override {
    return SendMessage(recipient, msg);
  }

  bool RequestVoteReply(const std::string &recipient,
                        const MRequestVoteReply &msg) override {
    return SendMessage(recipient, msg);
  }

  bool AppendEntries(const std::string &recipient,
                     const MAppendEntries &msg) override {
    return SendMessage(recipient, msg);
  }

  bool AppendEntriesReply(const std::string &recipient,
                          const MAppendEntriesReply &msg) override {
    return SendMessage(recipient, msg);
  }

  void Disconnect(const std::string &id) {
    std::lock_guard<std::mutex> guard(mutex_);
    status_[id] = false;
  }

  void Connect(const std::string &id) {
    std::lock_guard<std::mutex> guard(mutex_);
    status_[id] = true;
  }

 private:
  template <class TMessage>
  bool SendMessage(const std::string &recipient, const TMessage &msg) {
    bool ok;

    {
      std::lock_guard<std::mutex> guard(mutex_);
      ok = status_[msg.sender_id] && status_[recipient];
    }

    if (ok) {
      auto channel = system_.FindChannel(recipient, "main");
      if (!channel) {
        ok = false;
      } else {
        channel->Send<TMessage>(msg);
      }
    }

    return ok;
  }

  communication::reactor::System &system_;

  std::mutex mutex_;
  std::unordered_map<std::string, bool> status_;
};

}  // namespace communication::raft
