#include "gtest/gtest.h"

#include <chrono>
#include <thread>

#include "communication/raft/raft.hpp"
#include "communication/raft/raft_network.hpp"
#include "communication/reactor/reactor_local.hpp"

using namespace std::chrono_literals;
using namespace communication::raft;

class RaftMemberTest : RaftMember {
 public:
  std::string Id() { return id_; }
  std::string Leader() { return *leader_; }

  using RaftMember::RaftMember;
};

const RaftConfig test_config{{"a", "b", "c", "d", "e"}, 150ms, 300ms, 70ms};

TEST(Raft, InitialElection) {
  communication::reactor::System sys;
  FakeNetworkInterface network(sys);

  {
    std::vector<std::unique_ptr<RaftMemberTest>> members;
    for (const auto &member_id : test_config.members) {
      members.push_back(std::make_unique<RaftMemberTest>(sys, member_id,
                                                         test_config, network));
      network.Connect(member_id);
    }

    std::this_thread::sleep_for(500ms);

    std::string leader = members[0]->Leader();
    for (const auto &member : members) {
      EXPECT_EQ(member->Leader(), leader);
    }
  }
}

TEST(Raft, Reelection) {
  communication::reactor::System sys;
  FakeNetworkInterface network(sys);

  {
    std::vector<std::unique_ptr<RaftMemberTest>> members;
    for (const auto &member_id : test_config.members) {
      members.push_back(std::make_unique<RaftMemberTest>(sys, member_id,
                                                         test_config, network));
      network.Connect(member_id);
    }

    std::this_thread::sleep_for(500ms);

    std::string first_leader = members[0]->Leader();
    for (const auto &member : members) {
      EXPECT_EQ(member->Leader(), first_leader);
    }

    network.Disconnect(first_leader);

    std::this_thread::sleep_for(500ms);

    std::string second_leader = members[0]->Id() == first_leader
                                    ? members[1]->Leader()
                                    : members[0]->Leader();
    network.Connect(first_leader);

    std::this_thread::sleep_for(100ms);

    for (const auto &member : members) {
      EXPECT_EQ(member->Leader(), second_leader);
    }
  }
}
