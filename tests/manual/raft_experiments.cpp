#include <algorithm>

#include "fmt/format.h"
#include "glog/logging.h"

#include "communication/raft/raft.hpp"
#include "communication/raft/raft_network.hpp"

using std::chrono::milliseconds;
using std::experimental::optional;
using namespace communication::raft;
using namespace communication::reactor;
using namespace std::chrono_literals;

class RaftMemberTest : RaftMember {
 public:
  std::string Id() const { return id_; }
  optional<std::string> Leader() const { return leader_; }

  using RaftMember::RaftMember;
};

milliseconds InitialElection(const RaftConfig &config) {
  System sys;
  FakeNetworkInterface network(sys);

  std::chrono::system_clock::time_point start, end;

  LOG(INFO) << "Starting..." << std::endl;

  {
    std::vector<std::unique_ptr<RaftMemberTest>> members;

    start = std::chrono::system_clock::now();

    for (const auto &member_id : config.members) {
      members.push_back(
          std::make_unique<RaftMemberTest>(sys, member_id, config, network));
      network.Connect(member_id);
    }

    bool leader_elected = false;
    do {
      for (const auto &member : members) {
        if (member->Leader()) {
          leader_elected = true;
          break;
        }
      }
    } while (!leader_elected);
    end = std::chrono::system_clock::now();
  }

  sys.AwaitShutdown();

  return std::chrono::duration_cast<milliseconds>(end - start);
}

milliseconds Reelection(const RaftConfig &config) {
  System sys;
  FakeNetworkInterface network(sys);

  std::chrono::system_clock::time_point start, end;

  LOG(INFO) << "Starting..." << std::endl;

  {
    std::vector<std::unique_ptr<RaftMemberTest>> members;

    for (const auto &member_id : config.members) {
      members.push_back(
          std::make_unique<RaftMemberTest>(sys, member_id, config, network));
      network.Connect(member_id);
    }

    bool leader_elected = false;
    std::string first_leader;
    do {
      for (const auto &member : members) {
        if (member->Leader()) {
          leader_elected = true;
          first_leader = *member->Leader();
          break;
        }
      }
    } while (!leader_elected);

    // Let leader notify followers
    std::this_thread::sleep_for(config.heartbeat_interval);

    start = std::chrono::system_clock::now();
    network.Disconnect(first_leader);

    leader_elected = false;
    do {
      for (const auto &member : members) {
        if (member->Leader() && *member->Leader() != first_leader) {
          leader_elected = true;
          break;
        }
      }
    } while (!leader_elected);

    end = std::chrono::system_clock::now();
  }

  sys.AwaitShutdown();

  return std::chrono::duration_cast<milliseconds>(end - start);
}

std::vector<milliseconds> RunTest(const std::string &name,
                                  const std::function<milliseconds()> &test,
                                  const int runs) {
  std::vector<milliseconds> results(runs);
  for (int i = 0; i < runs; ++i) {
    results[i] = test();
  }
  sort(results.begin(), results.end());

  fmt::print("{} : min = {}ms, max = {}ms, median = {}ms\n", name,
             results[0].count(), results[runs - 1].count(),
             results[(runs - 1) / 2].count());

  return results;
}

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);

  RaftConfig config{{"a", "b", "c", "d", "e"}, 150ms, 300ms, 70ms};

  const int RUNS = 100;

  auto InitialElectionTest = [config]() { return InitialElection(config); };
  auto ReelectionTest = [config]() { return Reelection(config); };

  RunTest("InitialElection", InitialElectionTest, RUNS);
  RunTest("ReelectionTest", ReelectionTest, RUNS);

  return 0;
}
