#include <functional>

#include "communication/raft/network_common.hpp"
#include "communication/raft/raft.hpp"

namespace communication::raft::test_utils {

struct DummyState {
  struct Change {
    bool operator==(const Change &) const { return true; }
    bool operator!=(const Change &) const { return false; }

    template <class TArchive>
    void serialize(TArchive &, unsigned int) {}
  };

  template <class TArchive>
  void serialize(TArchive &, unsigned int) {}
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

    template <class TArchive>
    void serialize(TArchive &ar, unsigned int) {
      ar &t;
      ar &d;
    }
  };

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &x;
  }
};

/* Implementations of `RaftNetworkInterface` for simpler unit testing. */

/* `NoOpNetworkInterface` doesn't do anything -- it's like a server disconnected
 * from the network. */
template <class State>
class NoOpNetworkInterface : public RaftNetworkInterface<State> {
 public:
  ~NoOpNetworkInterface() {}

  virtual bool SendRequestVote(const MemberId &, const RequestVoteRequest &,
                               RequestVoteReply &,
                               std::chrono::milliseconds) override {
    return false;
  }

  virtual bool SendAppendEntries(const MemberId &,
                                 const AppendEntriesRequest<State> &,
                                 AppendEntriesReply &,
                                 std::chrono::milliseconds) override {
    return false;
  }

  virtual void Start(RaftMember<State> &) override {}
};

/* `NextReplyNetworkInterface` has two fields: `on_request_` and `next_reply_`
 * which is optional. `on_request_` is a callback that will be called before
 * processing requets. If `next_reply_` is not set, `Send*` functions will
 * return false, otherwise they return that reply. */
template <class State>
class NextReplyNetworkInterface : public RaftNetworkInterface<State> {
 public:
  ~NextReplyNetworkInterface() {}

  virtual bool SendRequestVote(const MemberId &,
                               const RequestVoteRequest &request,
                               RequestVoteReply &reply,
                               std::chrono::milliseconds) override {
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

  virtual bool SendAppendEntries(const MemberId &,
                                 const AppendEntriesRequest<State> &request,
                                 AppendEntriesReply &reply,
                                 std::chrono::milliseconds) override {
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

  virtual void Start(RaftMember<State> &) override {}

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

}  // namespace communication::raft::test_utils
