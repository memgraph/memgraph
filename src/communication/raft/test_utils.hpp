#include <functional>

#include "communication/raft/network_common.hpp"
#include "communication/raft/raft.hpp"

namespace communication::raft::test_utils {

struct DummyState {
  struct Change {
    bool operator==(const Change &) const { return true; }
    bool operator!=(const Change &) const { return false; }

    template <class Archive>
    void serialize(Archive &ar) {}
  };

  template <class Archive>
  void serialize(Archive &ar) {}
};

struct IntState {
  int x;

  struct Change {
    enum Type { ADD, SUB, SET };
    Type t;
    int d;

    bool operator==(const Change &rhs) const {
      return t == rhs.t && d == rhs.d;
    }
    bool operator!=(const Change &rhs) const { return !(*this == rhs); };

    template <class Archive>
    void serialize(Archive &ar) {
      ar(t, d);
    }
  };

  template <class Archive>
  void serialize(Archive &ar) {
    ar(x);
  }
};

/* Implementations of `RaftNetworkInterface` for simpler unit testing. */

/* `NoOpNetworkInterface` doesn't do anything -- it's like a server disconnected
 * from the network. */
template <class State>
class NoOpNetworkInterface : public RaftNetworkInterface<State> {
 public:
  ~NoOpNetworkInterface() {}

  virtual bool SendRequestVote(const MemberId &recipient,
                               const RequestVoteRequest &request,
                               RequestVoteReply &reply,
                               std::chrono::milliseconds timeout) override {
    return false;
  }

  virtual bool SendAppendEntries(const MemberId &recipient,
                                 const AppendEntriesRequest<State> &request,
                                 AppendEntriesReply &reply,
                                 std::chrono::milliseconds timeout) override {
    return false;
  }

  virtual void Start(RaftMember<State> &member) override {}

  virtual void Shutdown() override {}
};

/* `NextReplyNetworkInterface` has two fields: `on_request_` and `next_reply_`
 * which is optional. `on_request_` is a callback that will be called before
 * processing requets. If `next_reply_` is not set, `Send*` functions will
 * return false, otherwise they return that reply. */
template <class State>
class NextReplyNetworkInterface : public RaftNetworkInterface<State> {
 public:
  ~NextReplyNetworkInterface() {}

  virtual bool SendRequestVote(const MemberId &recipient,
                               const RequestVoteRequest &request,
                               RequestVoteReply &reply,
                               std::chrono::milliseconds timeout) override {
    PeerRpcRequest<State> req;
    req.type = RpcType::REQUEST_VOTE;
    req.request_vote = request;
    on_request_(req);
    if (!next_reply_) {
      return false;
    }
    DCHECK(next_reply_->type == RpcType::REQUEST_VOTE)
        << "`next_reply_` type doesn't match the request type";
    reply = next_reply_->request_vote;
    return true;
  }

  virtual bool SendAppendEntries(const MemberId &recipient,
                                 const AppendEntriesRequest<State> &request,
                                 AppendEntriesReply &reply,
                                 std::chrono::milliseconds timeout) override {
    PeerRpcRequest<State> req;
    req.type = RpcType::APPEND_ENTRIES;
    req.append_entries = request;
    on_request_(req);
    if (!next_reply_) {
      return false;
    }
    DCHECK(next_reply_->type == RpcType::APPEND_ENTRIES)
        << "`next_reply_` type doesn't match the request type";
    reply = next_reply_->append_entries;
    return true;
  }

  virtual void Start(RaftMember<State> &member) override {}

  virtual void Shutdown() override {}

  std::function<void(const PeerRpcRequest<State> &)> on_request_;
  std::experimental::optional<PeerRpcReply> next_reply_;
};

template <class State>
class NoOpStorageInterface : public RaftStorageInterface<State> {
 public:
  NoOpStorageInterface() {}

  void WriteTermAndVotedFor(const TermId,
                            const std::experimental::optional<std::string> &) {}

  std::pair<TermId, std::experimental::optional<MemberId>>
  GetTermAndVotedFor() {
    return {0, {}};
  }
  void AppendLogEntry(const LogEntry<State> &) {}
  TermId GetLogTerm(const LogIndex) { return 0; }
  LogEntry<State> GetLogEntry(const LogIndex) { assert(false); }
  std::vector<LogEntry<State>> GetLogSuffix(const LogIndex) { return {}; }
  LogIndex GetLastLogIndex() { return 0; }
  void TruncateLogSuffix(const LogIndex) {}

  TermId term_;
  std::experimental::optional<MemberId> voted_for_;
  std::vector<LogEntry<State>> log_;
};

template <class State>
class InMemoryStorageInterface : public RaftStorageInterface<State> {
 public:
  InMemoryStorageInterface(
      const TermId term,
      const std::experimental::optional<std::string> &voted_for,
      const std::vector<LogEntry<State>> log)
      : term_(term), voted_for_(voted_for), log_(log) {}

  void WriteTermAndVotedFor(
      const TermId term,
      const std::experimental::optional<std::string> &voted_for) {
    term_ = term;
    voted_for_ = voted_for;
  }

  std::pair<TermId, std::experimental::optional<MemberId>>
  GetTermAndVotedFor() {
    return {term_, voted_for_};
  }
  void AppendLogEntry(const LogEntry<State> &entry) { log_.push_back(entry); }
  TermId GetLogTerm(const LogIndex index) {
    return index > 0 ? log_[index - 1].term : 0;
  }
  LogEntry<State> GetLogEntry(const LogIndex index) { return log_[index - 1]; }
  std::vector<LogEntry<State>> GetLogSuffix(const LogIndex index) {
    return std::vector<LogEntry<State>>(log_.begin() + index - 1, log_.end());
  }
  LogIndex GetLastLogIndex(void) { return log_.size(); }
  void TruncateLogSuffix(const LogIndex index) {
    log_.erase(log_.begin() + index - 1, log_.end());
  }

  TermId term_;
  std::experimental::optional<MemberId> voted_for_;
  std::vector<LogEntry<State>> log_;
};

}  // namespace communication::raft::test_utils
