#include "gtest/gtest.h"

#include <chrono>
#include <experimental/optional>
#include <thread>

#include "communication/raft/raft.hpp"
#include "communication/raft/test_utils.hpp"

using namespace std::chrono_literals;
using namespace communication::raft;
using namespace communication::raft::test_utils;

using testing::Values;

const RaftConfig test_config2{{"a", "b"}, 150ms, 300ms, 70ms};
const RaftConfig test_config3{{"a", "b", "c"}, 150ms, 300ms, 70ms};
const RaftConfig test_config5{{"a", "b", "c", "d", "e"}, 150ms, 300ms, 70ms};

using communication::raft::impl::RaftMemberImpl;
using communication::raft::impl::RaftMode;

class RaftMemberTest : public ::testing::Test {
 public:
  RaftMemberTest()
      : storage_(1, "a", {}), member(network_, storage_, "a", test_config5) {}

  void SetLog(std::vector<LogEntry<DummyState>> log) {
    storage_.log_ = std::move(log);
  }

  NoOpNetworkInterface<DummyState> network_;
  InMemoryStorageInterface<DummyState> storage_;
  RaftMemberImpl<DummyState> member;
};

TEST_F(RaftMemberTest, Constructor) {
  EXPECT_EQ(member.mode_, RaftMode::FOLLOWER);
  EXPECT_EQ(member.term_, 1);
  EXPECT_EQ(*member.voted_for_, "a");
  EXPECT_EQ(member.commit_index_, 0);
}

TEST_F(RaftMemberTest, CandidateOrLeaderTransitionToFollower) {
  member.mode_ = RaftMode::CANDIDATE;
  member.CandidateTransitionToLeader();

  member.CandidateOrLeaderTransitionToFollower();
  EXPECT_EQ(member.mode_, RaftMode::FOLLOWER);
  EXPECT_EQ(member.leader_, std::experimental::nullopt);
  EXPECT_LT(member.next_election_time_, TimePoint::max());
}

TEST_F(RaftMemberTest, CandidateTransitionToLeader) {
  member.mode_ = RaftMode::CANDIDATE;
  member.CandidateTransitionToLeader();

  EXPECT_EQ(member.mode_, RaftMode::LEADER);
  EXPECT_EQ(*member.leader_, "a");
  EXPECT_EQ(member.next_election_time_, TimePoint::max());
}

TEST_F(RaftMemberTest, StartNewElection) {
  member.StartNewElection();

  EXPECT_EQ(member.mode_, RaftMode::CANDIDATE);
  EXPECT_EQ(member.term_, 2);
  EXPECT_EQ(member.voted_for_, member.id_);
}

TEST_F(RaftMemberTest, CountVotes) {
  member.StartNewElection();
  EXPECT_FALSE(member.CountVotes());

  member.peer_states_["b"]->voted_for_me = true;
  EXPECT_FALSE(member.CountVotes());

  member.peer_states_["c"]->voted_for_me = true;
  EXPECT_TRUE(member.CountVotes());
}

TEST_F(RaftMemberTest, AdvanceCommitIndex) {
  SetLog({{1}, {1}, {1}, {1}, {2}, {2}, {2}, {2}});

  member.mode_ = RaftMode::LEADER;
  member.term_ = 2;

  member.peer_states_["b"]->match_index = 4;
  member.peer_states_["c"]->match_index = 4;

  EXPECT_EQ(member.commit_index_, 0);
  member.AdvanceCommitIndex();
  EXPECT_EQ(member.commit_index_, 0);

  member.peer_states_["b"]->match_index = 4;
  member.peer_states_["c"]->match_index = 4;
  member.AdvanceCommitIndex();
  EXPECT_EQ(member.commit_index_, 0);

  member.peer_states_["b"]->match_index = 5;
  member.AdvanceCommitIndex();
  EXPECT_EQ(member.commit_index_, 0);

  member.peer_states_["c"]->match_index = 5;
  member.AdvanceCommitIndex();
  EXPECT_EQ(member.commit_index_, 5);

  member.peer_states_["d"]->match_index = 6;
  member.peer_states_["e"]->match_index = 7;
  member.AdvanceCommitIndex();
  EXPECT_EQ(member.commit_index_, 6);

  member.peer_states_["c"]->match_index = 8;
  member.AdvanceCommitIndex();
  EXPECT_EQ(member.commit_index_, 7);

  member.peer_states_["a"]->match_index = 8;
  member.AdvanceCommitIndex();
  EXPECT_EQ(member.commit_index_, 8);
}

TEST(RequestVote, SimpleElection) {
  NextReplyNetworkInterface<DummyState> network;
  InMemoryStorageInterface<DummyState> storage(1, {}, {{1}, {1}});
  RaftMemberImpl<DummyState> member(network, storage, "a", test_config5);

  member.StartNewElection();

  std::unique_lock<std::mutex> lock(member.mutex_);

  PeerRPCReply next_reply;
  next_reply.type = PeerRPCReply::Type::REQUEST_VOTE;

  network.on_request_ = [](const PeerRPCRequest<DummyState> &request) {
    ASSERT_EQ(request.type, PeerRPCRequest<DummyState>::Type::REQUEST_VOTE);
    ASSERT_EQ(request.request_vote.candidate_term, 2);
    ASSERT_EQ(request.request_vote.candidate_id, "a");
  };

  /* member 'b' first voted for us */
  next_reply.request_vote.term = 2;
  next_reply.request_vote.vote_granted = true;
  network.next_reply_ = next_reply;
  member.RequestVote("b", *member.peer_states_["b"], lock);
  EXPECT_EQ(member.mode_, RaftMode::CANDIDATE);
  EXPECT_TRUE(member.peer_states_["b"]->request_vote_done);
  EXPECT_TRUE(member.peer_states_["b"]->voted_for_me);

  /* member 'c' didn't */
  next_reply.request_vote.vote_granted = false;
  network.next_reply_ = next_reply;
  member.RequestVote("c", *member.peer_states_["c"], lock);
  EXPECT_TRUE(member.peer_states_["c"]->request_vote_done);
  EXPECT_FALSE(member.peer_states_["c"]->voted_for_me);
  EXPECT_EQ(member.mode_, RaftMode::CANDIDATE);

  /* but member 'd' did */
  next_reply.request_vote.vote_granted = true;
  network.next_reply_ = next_reply;
  member.RequestVote("d", *member.peer_states_["d"], lock);
  EXPECT_TRUE(member.peer_states_["d"]->request_vote_done);
  EXPECT_TRUE(member.peer_states_["d"]->voted_for_me);
  EXPECT_EQ(member.mode_, RaftMode::LEADER);

  /* no-op entry should be at the end of leader's log */
  EXPECT_EQ(storage.log_.back().term, 2);
  EXPECT_EQ(storage.log_.back().command, std::experimental::nullopt);
}

TEST(AppendEntries, SimpleLogSync) {
  NextReplyNetworkInterface<DummyState> network;
  InMemoryStorageInterface<DummyState> storage(3, {}, {{1}, {1}, {2}, {3}});
  RaftMemberImpl<DummyState> member(network, storage, "a", test_config2);

  member.mode_ = RaftMode::LEADER;

  std::unique_lock<std::mutex> lock(member.mutex_);

  PeerRPCReply reply;
  reply.type = PeerRPCReply::Type::APPEND_ENTRIES;

  reply.append_entries.term = 3;
  reply.append_entries.success = false;
  network.next_reply_ = reply;

  LogIndex expected_prev_log_index;
  TermId expected_prev_log_term;
  std::vector<LogEntry<DummyState>> expected_entries;

  network.on_request_ = [&](const PeerRPCRequest<DummyState> &request) {
    EXPECT_EQ(request.type, PeerRPCRequest<DummyState>::Type::APPEND_ENTRIES);
    EXPECT_EQ(request.append_entries.leader_term, 3);
    EXPECT_EQ(request.append_entries.leader_id, "a");
    EXPECT_EQ(request.append_entries.prev_log_index, expected_prev_log_index);
    EXPECT_EQ(request.append_entries.prev_log_term, expected_prev_log_term);
    EXPECT_EQ(request.append_entries.entries, expected_entries);
  };

  /* initial state after election */
  auto &peer_state = *member.peer_states_["b"];
  peer_state.match_index = 0;
  peer_state.next_index = 5;
  peer_state.suppress_log_entries = true;

  /* send a heartbeat and find out logs don't match */
  expected_prev_log_index = 4;
  expected_prev_log_term = 3;
  expected_entries = {};
  member.AppendEntries("b", peer_state, lock);
  EXPECT_EQ(peer_state.match_index, 0);
  EXPECT_EQ(peer_state.next_index, 4);
  EXPECT_EQ(member.commit_index_, 0);

  /* move `next_index` until we find a match, `expected_entries` will be empty
   * because `suppress_log_entries` will be true */
  expected_entries = {};

  expected_prev_log_index = 3;
  expected_prev_log_term = 2;
  member.AppendEntries("b", peer_state, lock);
  EXPECT_EQ(peer_state.match_index, 0);
  EXPECT_EQ(peer_state.next_index, 3);
  EXPECT_EQ(peer_state.suppress_log_entries, true);
  EXPECT_EQ(member.commit_index_, 0);

  expected_prev_log_index = 2;
  expected_prev_log_term = 1;
  member.AppendEntries("b", peer_state, lock);
  EXPECT_EQ(peer_state.match_index, 0);
  EXPECT_EQ(peer_state.next_index, 2);
  EXPECT_EQ(peer_state.suppress_log_entries, true);
  EXPECT_EQ(member.commit_index_, 0);

  /* we found a match */
  reply.append_entries.success = true;
  network.next_reply_ = reply;

  expected_prev_log_index = 1;
  expected_prev_log_term = 1;
  member.AppendEntries("b", peer_state, lock);
  EXPECT_EQ(peer_state.match_index, 1);
  EXPECT_EQ(peer_state.next_index, 2);
  EXPECT_EQ(peer_state.suppress_log_entries, false);
  EXPECT_EQ(member.commit_index_, 4);

  /* now sync them */
  expected_prev_log_index = 1;
  expected_prev_log_term = 1;
  expected_entries = {{1}, {2}, {3}};
  member.AppendEntries("b", peer_state, lock);
  EXPECT_EQ(peer_state.match_index, 4);
  EXPECT_EQ(peer_state.next_index, 5);
  EXPECT_EQ(peer_state.suppress_log_entries, false);
  EXPECT_EQ(member.commit_index_, 4);

  /* heartbeat after successful log sync */
  expected_prev_log_index = 4;
  expected_prev_log_term = 3;
  expected_entries = {};
  member.AppendEntries("b", peer_state, lock);
  EXPECT_EQ(peer_state.match_index, 4);
  EXPECT_EQ(peer_state.next_index, 5);
  EXPECT_EQ(member.commit_index_, 4);

  /* replicate a newly appended entry */
  storage.AppendLogEntry({3});

  expected_prev_log_index = 4;
  expected_prev_log_term = 3;
  expected_entries = {{3}};
  member.AppendEntries("b", peer_state, lock);
  EXPECT_EQ(peer_state.match_index, 5);
  EXPECT_EQ(peer_state.next_index, 6);
  EXPECT_EQ(member.commit_index_, 5);
}

template <class TestParam>
class RaftMemberParamTest : public ::testing::TestWithParam<TestParam> {
 public:
  virtual void SetUp() {
    /* Some checks to verify that test case is valid. */

    /* Member's term should be greater than or equal to last log term. */
    ASSERT_GE(storage_.term_, storage_.GetLogTerm(storage_.GetLastLogIndex()));

    ASSERT_GE(peer_storage_.term_,
              peer_storage_.GetLogTerm(peer_storage_.GetLastLogIndex()));

    /* If two logs match at some index, the entire prefix should match. */
    LogIndex pos =
        std::min(storage_.GetLastLogIndex(), peer_storage_.GetLastLogIndex());

    for (; pos > 0; --pos) {
      if (storage_.GetLogEntry(pos) == peer_storage_.GetLogEntry(pos)) {
        break;
      }
    }

    for (; pos > 0; --pos) {
      ASSERT_EQ(storage_.GetLogEntry(pos), peer_storage_.GetLogEntry(pos));
    }
  }

  RaftMemberParamTest(InMemoryStorageInterface<DummyState> storage,
                      InMemoryStorageInterface<DummyState> peer_storage)
      : network_(NoOpNetworkInterface<DummyState>()),
        storage_(storage),
        member_(network_, storage_, "a", test_config3),
        peer_storage_(peer_storage) {}

  NoOpNetworkInterface<DummyState> network_;
  InMemoryStorageInterface<DummyState> storage_;
  RaftMemberImpl<DummyState> member_;

  InMemoryStorageInterface<DummyState> peer_storage_;
};

struct OnRequestVoteTestParam {
  TermId term;
  std::experimental::optional<MemberId> voted_for;
  std::vector<LogEntry<DummyState>> log;

  TermId peer_term;
  std::vector<LogEntry<DummyState>> peer_log;

  bool expected_reply;
};

class OnRequestVoteTest : public RaftMemberParamTest<OnRequestVoteTestParam> {
 public:
  OnRequestVoteTest()
      : RaftMemberParamTest(
            InMemoryStorageInterface<DummyState>(
                GetParam().term, GetParam().voted_for, GetParam().log),
            InMemoryStorageInterface<DummyState>(GetParam().peer_term, {},
                                                 GetParam().peer_log)) {}
  virtual ~OnRequestVoteTest() {}
};

TEST_P(OnRequestVoteTest, RequestVoteTest) {
  auto reply = member_.OnRequestVote(
      {GetParam().peer_term, "b", peer_storage_.GetLastLogIndex(),
       peer_storage_.GetLogTerm(peer_storage_.GetLastLogIndex())});

  EXPECT_EQ(reply.vote_granted, GetParam().expected_reply);

  /* If we accepted the request, our term should be equal to candidate's term
   * and voted_for should be set. */
  EXPECT_EQ(reply.term,
            reply.vote_granted ? GetParam().peer_term : GetParam().term);
  EXPECT_EQ(storage_.term_,
            reply.vote_granted ? GetParam().peer_term : GetParam().term);
  EXPECT_EQ(storage_.voted_for_,
            reply.vote_granted ? "b" : GetParam().voted_for);
}

/* Member 'b' is starting an election for term 5 and sending RequestVote RPC
 * to 'a'. Logs are empty so log-up-to-date check will always pass. */
INSTANTIATE_TEST_CASE_P(
    TermAndVotedForCheck, OnRequestVoteTest,
    Values(
        /* we didn't vote for anyone in a smaller term -> accept */
        OnRequestVoteTestParam{3, {}, {}, 5, {}, true},
        /* we voted for someone in smaller term -> accept */
        OnRequestVoteTestParam{4, "c", {}, 5, {}, true},
        /* equal term but we didn't vote for anyone in it -> accept */
        OnRequestVoteTestParam{5, {}, {}, 5, {}, true},
        /* equal term but we voted for this candidate-> accept */
        OnRequestVoteTestParam{5, "b", {}, 5, {}, true},
        /* equal term but we voted for someone else -> decline */
        OnRequestVoteTestParam{5, "c", {}, 5, {}, false},
        /* larger term and haven't voted for anyone -> decline */
        OnRequestVoteTestParam{6, {}, {}, 5, {}, false},
        /* larger term and we voted for someone else -> decline */
        OnRequestVoteTestParam{6, "a", {}, 5, {}, false}));

/* Member 'a' log:
 *     1   2   3   4   5   6   7
 *   | 1 | 1 | 1 | 2 | 3 | 3 |
 *
 * It is in term 5.
 */

/* Member 'b' is sending RequestVote RPC to 'a' for term 8.  */
INSTANTIATE_TEST_CASE_P(
    LogUpToDateCheck, OnRequestVoteTest,
    Values(
        /* candidate's last log term is smaller -> decline */
        OnRequestVoteTestParam{5,
                               {},
                               {{1}, {1}, {1}, {2}, {3}, {3}},
                               8,
                               {{1}, {1}, {1}, {2}},
                               false},
        /* candidate's last log term is smaller -> decline */
        OnRequestVoteTestParam{5,
                               {},
                               {{1}, {1}, {1}, {2}, {3}, {3}},
                               8,
                               {{1}, {1}, {1}, {2}, {2}, {2}, {2}},
                               false},
        /* candidate's term is equal, but our log is longer -> decline */
        OnRequestVoteTestParam{5,
                               {},
                               {{1}, {1}, {1}, {2}, {3}, {3}},
                               8,
                               {{1}, {1}, {1}, {2}, {3}},
                               false},
        /* equal logs -> accept */
        OnRequestVoteTestParam{5,
                               {},
                               {{1}, {1}, {1}, {2}, {3}, {3}},
                               8,
                               {{1}, {1}, {1}, {2}, {3}, {3}},
                               true},
        /* candidate's term is larger -> accept */
        OnRequestVoteTestParam{5,
                               {},
                               {{1}, {1}, {1}, {2}, {3}, {3}},
                               8,
                               {{1}, {1}, {1}, {2}, {4}},
                               true},
        /* equal terms, but candidate's log is longer -> accept */
        OnRequestVoteTestParam{5,
                               {},
                               {{1}, {1}, {1}, {2}, {3}, {3}},
                               8,
                               {{1}, {1}, {1}, {2}, {3}, {3}, {3}},
                               true},
        /* candidate's last log term is larger -> accept */
        OnRequestVoteTestParam{5,
                               {},
                               {{1}, {1}, {1}, {2}, {3}, {3}},
                               8,
                               {{1}, {2}, {3}, {4}, {5}},
                               true}));

struct OnAppendEntriesTestParam {
  TermId term;
  std::vector<LogEntry<DummyState>> log;

  TermId peer_term;
  std::vector<LogEntry<DummyState>> peer_log;
  LogIndex peer_next_index;

  bool expected_reply;
  TermId expected_term;
  std::vector<LogEntry<DummyState>> expected_log;
};

class OnAppendEntriesTest
    : public RaftMemberParamTest<OnAppendEntriesTestParam> {
 public:
  OnAppendEntriesTest()
      : RaftMemberParamTest(
            InMemoryStorageInterface<DummyState>(GetParam().term, {},
                                                 GetParam().log),
            InMemoryStorageInterface<DummyState>(GetParam().peer_term, {},
                                                 GetParam().peer_log)) {}
  virtual ~OnAppendEntriesTest() {}
};

TEST_P(OnAppendEntriesTest, All) {
  auto last_log_index = GetParam().peer_next_index - 1;
  auto last_log_term = peer_storage_.GetLogTerm(last_log_index);
  auto entries = peer_storage_.GetLogSuffix(GetParam().peer_next_index);
  auto reply = member_.OnAppendEntries(
      {GetParam().peer_term, "b", last_log_index, last_log_term, entries, 0});

  EXPECT_EQ(reply.success, GetParam().expected_reply);
  EXPECT_EQ(reply.term, GetParam().expected_term);
  EXPECT_EQ(storage_.log_, GetParam().expected_log);
}
/* Member 'a' recieved AppendEntries RPC from member 'b'. The request will
 * contain no log entries, representing just a heartbeat, as it is not
 * important in these scenarios. */
INSTANTIATE_TEST_CASE_P(
    TermAndLogConsistencyCheck, OnAppendEntriesTest,
    Values(
        /* sender has stale term -> decline */
        OnAppendEntriesTestParam{/* my term*/ 8,
                                 {{1}, {1}, {2}},
                                 7,
                                 {{1}, {1}, {2}, {3}, {4}, {5}, {5}, {6}},
                                 7,
                                 false,
                                 8,
                                 {{1}, {1}, {2}}},
        /* we're missing entries 4, 5 and 6 -> decline, but update term */
        OnAppendEntriesTestParam{4,
                                 {{1}, {1}, {2}},
                                 8,
                                 {{1}, {1}, {2}, {3}, {4}, {5}, {5}, {6}},
                                 7,
                                 false,
                                 8,
                                 {{1}, {1}, {2}}},
        /* we're missing entry 4 -> decline, but update term */
        OnAppendEntriesTestParam{5,
                                 {{1}, {1}, {2}},
                                 8,
                                 {{1}, {1}, {2}, {3}, {4}, {5}, {5}, {6}},
                                 5,
                                 false,
                                 8,
                                 {{1}, {1}, {2}}},
        /* log terms don't match at entry 4 -> decline, but update term */
        OnAppendEntriesTestParam{5,
                                 {{1}, {1}, {2}},
                                 8,
                                 {{1}, {1}, {3}, {3}, {4}, {5}, {5}, {6}},
                                 4,
                                 false,
                                 8,
                                 {{1}, {1}, {2}}},
        /* logs match -> accept and update term */
        OnAppendEntriesTestParam{5,
                                 {{1}, {1}, {2}},
                                 8,
                                 {{1}, {1}, {2}, {3}, {4}, {5}, {5}, {6}},
                                 4,
                                 true,
                                 8,
                                 {{1}, {1}, {2}, {3}, {4}, {5}, {5}, {6}}},
        /* now follow some log truncation tests */
        /* no truncation, append a single entry */
        OnAppendEntriesTestParam{
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}},
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}},
            9,
            true,
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}}},
        /* no truncation, append multiple entries */
        OnAppendEntriesTestParam{
            8,
            {{1}, {1}, {1}, {4}},
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}},
            4,
            true,
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}}},
        /* no truncation, leader's log is prefix of ours */
        OnAppendEntriesTestParam{
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}, {6}},
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}},
            4,
            true,
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}, {6}}},
        /* another one, now with entries from newer term */
        OnAppendEntriesTestParam{
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}, {7}, {7}},
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}},
            4,
            true,
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}, {7}, {7}}},
        /* no truncation, partial match between our log and appended entries
           */
        OnAppendEntriesTestParam{
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}},
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}},
            4,
            true,
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}}},
        /* truncate suffix */
        OnAppendEntriesTestParam{
            8,
            {{1}, {1}, {1}, {4}, {4}, {4}, {4}},
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}},
            5,
            true,
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}}},
        /* truncate suffix, with partial match between our log and appened
           entries */
        OnAppendEntriesTestParam{
            8,
            {{1}, {1}, {1}, {4}, {4}, {4}, {4}},
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}},
            4,
            true,
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}}},
        /* delete whole log */
        OnAppendEntriesTestParam{
            8,
            {{5}},
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}},
            1,
            true,
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}}},
        /* append on empty log */
        OnAppendEntriesTestParam{
            8,
            {{}},
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}},
            1,
            true,
            8,
            {{1}, {1}, {1}, {4}, {4}, {5}, {5}, {6}, {6}, {6}}}));
