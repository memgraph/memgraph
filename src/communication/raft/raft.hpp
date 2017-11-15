#pragma once

#include <chrono>
#include <experimental/optional>
#include <set>
#include <vector>

#include "communication/reactor/reactor_local.hpp"
#include "utils/watchdog.hpp"

namespace communication::raft {

struct MAppendEntries;
struct MAppendEntriesReply;
struct MRequestVote;
struct MRequestVoteReply;

class RaftNetworkInterface;

struct RaftConfig {
  std::vector<std::string> members;
  std::chrono::milliseconds leader_timeout_min;
  std::chrono::milliseconds leader_timeout_max;
  std::chrono::milliseconds heartbeat_interval;
};

class RaftMember {
 public:
  RaftMember(communication::reactor::System &system, const std::string &id,
             const RaftConfig &config, RaftNetworkInterface &network);
  virtual ~RaftMember();

 protected:
  std::string id_;
  std::experimental::optional<std::string> leader_;

 private:
  enum class Mode { FOLLOWER, CANDIDATE, LEADER };

  communication::reactor::System &system_;
  RaftConfig config_;
  RaftNetworkInterface &network_;
  Mode mode_;
  uint64_t term_ = 0;
  std::experimental::optional<std::string> voted_for_;

  std::set<std::string> votes_;

  Watchdog leader_watchdog_;
  Watchdog heartbeat_watchdog_;

  std::unique_ptr<reactor::Reactor> reactor_;

  void RunElection();
  void TransitionToFollower();
  void TransitionToCandidate();
  void TransitionToLeader();
  void UpdateTerm(int new_term);

  MRequestVoteReply OnRequestVote(const MRequestVote &);
  void OnRequestVoteReply(const MRequestVoteReply &);

  MAppendEntriesReply OnAppendEntries(const MAppendEntries &);
  void OnAppendEntriesReply(const MAppendEntriesReply &);

  template <class... Args>
  void LogInfo(const std::string &, Args &&...);
};

}  // namespace communication::raft
