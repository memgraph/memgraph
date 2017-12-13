#include <functional>

#include "communication/raft/raft.hpp"

namespace communication::raft::test_utils {

struct DummyState {
  struct Change {
    bool operator==(const Change &) const { return true; }
    bool operator!=(const Change &) const { return false; }
  };
  struct Query {};
  struct Result {};
};

struct IntState {
  int x;

  struct Change {
    enum Type { ADD, SUB, SET };
    Type t;
    int d;

    bool operator==(const Change &rhs) { return t == rhs.t && d == rhs.d; }
    bool operator!=(const Change &rhs) { return !(*this == rhs); };
  };
};

template <class State>
class NoOpNetworkInterface : public RaftNetworkInterface<State> {
 public:
  ~NoOpNetworkInterface() {}

  bool SendRPC(const MemberId &recipient, const PeerRPCRequest<State> &request,
               PeerRPCReply &reply) {
    return false;
  }
};

template <class State>
class NextReplyNetworkInterface : public RaftNetworkInterface<State> {
 public:
  ~NextReplyNetworkInterface() {}

  bool SendRPC(const MemberId &recipient, const PeerRPCRequest<State> &request,
               PeerRPCReply &reply) {
    on_request_(request);
    if (!next_reply_) {
      return false;
    }
    reply = *next_reply_;
    return true;
  }

  std::function<void(const PeerRPCRequest<State> &)> on_request_;
  std::experimental::optional<PeerRPCReply> next_reply_;
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
