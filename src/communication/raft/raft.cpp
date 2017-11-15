#include "raft.hpp"

#include <iostream>

#include "fmt/format.h"
#include "glog/logging.h"

#include "communication/raft/raft_network.hpp"

using std::experimental::optional;
using std::placeholders::_1;
using std::placeholders::_2;

using namespace communication::reactor;
using namespace std::chrono_literals;

namespace communication::raft {

RaftMember::RaftMember(System &system, const std::string &id,
                       const RaftConfig &config, RaftNetworkInterface &network)
    : id_(id),
      system_(system),
      config_(config),
      network_(network),
      mode_(Mode::FOLLOWER),
      leader_watchdog_(config_.leader_timeout_min, config_.leader_timeout_max,
                       [this]() {
                         auto channel = system_.FindChannel(id_, "main");
                         if (channel) {
                           channel->Send<MLeaderTimeout>();
                         }
                       }),
      heartbeat_watchdog_(
          config_.heartbeat_interval, config_.heartbeat_interval,
          [this]() {
            for (const auto &member : config_.members) {
              if (id_ != member) {
                network_.AppendEntries(member, MAppendEntries(term_, id_));
              }
            }
          },
          true),
      reactor_(system.Spawn(id, [this](Reactor &r) {
        EventStream *stream = r.main_.first;

        stream->OnEvent<MLeaderTimeout>([this](
            const MLeaderTimeout &, const Subscription &) { RunElection(); });

        stream->OnEvent<MRequestVote>(
            [this](const MRequestVote &req, const Subscription &) {
              network_.RequestVoteReply(req.sender_id, OnRequestVote(req));
            });
        stream->OnEvent<MRequestVoteReply>(
            [this](const MRequestVoteReply &req, const Subscription &) {
              OnRequestVoteReply(req);
            });

        stream->OnEvent<MAppendEntries>(
            [this](const MAppendEntries &req, const Subscription &) {
              network_.AppendEntriesReply(req.sender_id, OnAppendEntries(req));
            });
        stream->OnEvent<MAppendEntriesReply>(
            [this](const MAppendEntriesReply &rep, const Subscription &) {
              OnAppendEntriesReply(rep);
            });
      })) {}

RaftMember::~RaftMember() { LogInfo("Shutting down..."); }

template <class... Args>
void RaftMember::LogInfo(const std::string &format, Args &&... args) {
  LOG(INFO) << fmt::format("(node = {}, term = {}) ", id_, term_)
            << fmt::format(format, std::forward<Args>(args)...) << std::endl;
}

void RaftMember::TransitionToFollower() {
  /* Stop sending heartbeats to followers, start listening for them. */
  heartbeat_watchdog_.Block();
  leader_watchdog_.Unblock();

  mode_ = Mode::FOLLOWER;
}

void RaftMember::TransitionToCandidate() {
  /* This transition only happens if we are in follower or candidate mode.
   * Leader timeout watchdog is not blocked because it also triggers new
   * elections if the current one times out. */
  DCHECK(mode_ != Mode::LEADER)
      << "Transition to candidate mode from leader mode.";
  mode_ = Mode::CANDIDATE;
  votes_ = {};
}

void RaftMember::TransitionToLeader() {
  /* This transition only happens if we are in candidate mode. */
  DCHECK(mode_ == Mode::CANDIDATE)
      << "Transition to leader mode from leader or follower mode.";

  /* Stop listening for leader heartbeats, start sending them. */
  leader_watchdog_.Block();
  heartbeat_watchdog_.Unblock();

  mode_ = Mode::LEADER;
  leader_ = id_;
}

void RaftMember::UpdateTerm(int new_term) {
  term_ = new_term;
  voted_for_ = {};
  leader_ = {};
}

void RaftMember::RunElection() {
  /* Elections will be skipped if we believe we are the leader. This can happen
   * if leader timeout message was delayed for some reason.  */
  if (mode_ == Mode::LEADER) {
    return;
  }

  LogInfo("Running for leader.");

  /* [Raft paper, Section 5.2.]
   * "To begin an election, a follower increments its current term and
   * transitions to candidate state. It then votes for itself and issues
   * RequestVote RPCs in parallel to each of the other servers in
   * the cluster." */
  TransitionToCandidate();
  UpdateTerm(term_ + 1);

  voted_for_ = id_;
  votes_.insert(id_);

  for (const auto &member_id : config_.members) {
    if (member_id != id_) {
      network_.RequestVote(member_id, MRequestVote(term_, id_));
    }
  }
}

MRequestVoteReply RaftMember::OnRequestVote(const MRequestVote &req) {
  LogInfo("Vote requested from {}, candidate_term = {}", req.sender_id,
          req.term);
  /* [Raft paper, Section 5.1]
   * "Current terms are exchanged whenever servers communicate; if one server's
   * current term is smaller than the other's, then it updates its current term
   * to the larger value. If a candidate or leader discovers that its term is
   * out of date, it immediately reverts to follower state." */
  if (req.term > term_) {
    TransitionToFollower();
    UpdateTerm(req.term);
  }

  /* [Raft paper, Figure 2]
   * "Reply false if term < currentTerm." */
  if (req.term < term_) {
    return MRequestVoteReply(term_, id_, false);
  }

  /* [Raft paper, Figure 2]
   * "If votedFor is null or candidateId, and candidate's log is at least as
   * up-to-date as receiver's log, grant vote." */
  if (voted_for_ && *voted_for_ != req.sender_id) {
    return MRequestVoteReply(term_, id_, false);
  }

  LogInfo("Granting vote to {}.", req.sender_id);

  voted_for_ = req.sender_id;
  /* [Raft paper, Section 5.2]
   * "A server remains in follower state as long as it receives valid RPCs from
   * a leader or candidate." */
  leader_watchdog_.Notify();

  DCHECK(mode_ != Mode::LEADER) << "Granted vote as a leader.";

  return MRequestVoteReply(term_, id_, true);
}

void RaftMember::OnRequestVoteReply(const MRequestVoteReply &rep) {
  /* Ignore leftover messages from old elections. */
  if (mode_ != Mode::CANDIDATE || rep.term < term_) {
    return;
  }

  /* [Raft paper, Section 5.1]
   * "Current terms are exchanged whenever servers communicate; if one server's
   * current term is smaller than the other's, then it updates its current term
   * to the larger value. If a candidate or leader discovers that its term is
   * out of date, it immediately reverts to follower state." */
  if (rep.term > term_) {
    LogInfo(
        "Vote denied from {} with greater term {}, transitioning to follower "
        "mode.",
        rep.sender_id, rep.term);
    TransitionToFollower();
    UpdateTerm(rep.term);
    return;
  }

  if (!rep.success) {
    LogInfo("Vote rejected from {}.", rep.sender_id);
    return;
  }

  LogInfo("Vote granted from {}.", rep.sender_id);
  votes_.insert(rep.sender_id);
  if (2 * votes_.size() > config_.members.size()) {
    LogInfo("Elected as leader.");
    TransitionToLeader();
  }
}

MAppendEntriesReply RaftMember::OnAppendEntries(const MAppendEntries &req) {
  LogInfo("Append entries from {}.", req.sender_id);

  /* [Raft paper, Section 5.1]
   * "If a server receives a request with a stale term number, it rejects
   * the request." */
  if (req.term < term_) {
    return MAppendEntriesReply(term_, id_, false);
  }

  /* [Raft paper, Section 5.1]
   * "Current terms are exchanged whenever servers communicate; if one server's
   * current term is smaller than the other's, then it updates its current term
   * to the larger value. If a candidate or leader discovers that its term is
   * out of date, it immediately reverts to follower state." */
  if (req.term > term_) {
    TransitionToFollower();
    UpdateTerm(req.term);
  }

  /* [Raft paper, Section 5.2]
   * "While waiting for votes, a candidate may receive an AppendEntries RPC from
   * another server claiming to be leader. If the leader’s term (included in its
   * RPC) is at least as large as the candidate’s current term, then the
   * candidate recognizes the leader as legitimate and returns to follower
   * state." */
  if (req.term == term_ && mode_ == Mode::CANDIDATE) {
    TransitionToFollower();
    UpdateTerm(req.term);
  }

  /* [Raft paper, Section 5.2]
   * "A server remains in follower state as long as it receives
   * valid RPCs from a leader or candidate." */
  leader_ = req.sender_id;
  leader_watchdog_.Notify();

  return MAppendEntriesReply(term_, id_, true);
}

void RaftMember::OnAppendEntriesReply(const MAppendEntriesReply &rep) {
  /* [Raft paper, Section 5.1]
   * "Current terms are exchanged whenever servers communicate; if one server's
   * current term is smaller than the other's, then it updates its current term
   * to the larger value. If a candidate or leader discovers that its term is
   * out of date, it immediately reverts to follower state." */
  if (rep.term > term_) {
    TransitionToFollower();
    UpdateTerm(rep.term);
  }
}

}  // namespace communication::raft
