#pragma once

#include "communication/raft/raft.hpp"

namespace communication::raft {

template <class State>
class InMemoryStorage : public RaftStorageInterface<State> {
 public:
  InMemoryStorage()
      : term_(0), voted_for_(std::experimental::nullopt), log_() {}

  InMemoryStorage(const TermId term,
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
    CHECK(0 <= index && index <= log_.size())
        << "Trying to read nonexistent log entry";
    return index > 0 ? log_[index - 1].term : 0;
  }

  LogEntry<State> GetLogEntry(const LogIndex index) {
    CHECK(1 <= index && index <= log_.size())
        << "Trying to get nonexistent log entry";
    return log_[index - 1];
  }

  std::vector<LogEntry<State>> GetLogSuffix(const LogIndex index) {
    CHECK(1 <= index && index <= log_.size())
        << "Trying to get nonexistent log entries";
    return std::vector<LogEntry<State>>(log_.begin() + index - 1, log_.end());
  }

  LogIndex GetLastLogIndex(void) { return log_.size(); }

  void TruncateLogSuffix(const LogIndex index) {
    CHECK(1 <= index <= log_.size())
        << "Trying to remove nonexistent log entries";
    log_.erase(log_.begin() + index - 1, log_.end());
  }

  TermId term_;
  std::experimental::optional<MemberId> voted_for_;
  std::vector<LogEntry<State>> log_;
};

}  // namespace communication::raft
