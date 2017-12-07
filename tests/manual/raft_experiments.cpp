#include <algorithm>

#include "fmt/format.h"
#include "glog/logging.h"

#include "communication/raft/raft_reactor.hpp"
#include "communication/raft/test_utils.hpp"

using std::chrono::milliseconds;
using std::experimental::optional;
using namespace communication::raft;
using namespace communication::raft::test_utils;
using namespace communication::reactor;
using namespace std::chrono_literals;

template <class State>
class RaftMemberTest : public RaftMemberLocalReactor<State> {
 public:
  MemberId Id() { return member_.Id(); }
  std::experimental::optional<MemberId> Leader() { return member_.Leader(); }

 private:
  using RaftMemberLocalReactor<State>::RaftMemberLocalReactor;
  using RaftMemberLocalReactor<State>::member_;
};

using RaftMemberDummy = RaftMemberTest<DummyState>;

milliseconds InitialElection(const RaftConfig &config) {
  System sys;
  NoOpStorageInterface<DummyState> storage;

  std::chrono::system_clock::time_point start, end;

  LOG(INFO) << "Starting..." << std::endl;

  {
    std::vector<std::unique_ptr<RaftMemberDummy>> members;

    start = std::chrono::system_clock::now();

    for (const auto &member_id : config.members) {
      members.push_back(
          std::make_unique<RaftMemberDummy>(sys, storage, member_id, config));
      members.back()->Connect();
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

  return std::chrono::duration_cast<milliseconds>(end - start);
}

milliseconds Reelection(const RaftConfig &config) {
  System sys;
  NoOpStorageInterface<DummyState> storage;

  std::chrono::system_clock::time_point start, end;

  LOG(INFO) << "Starting..." << std::endl;

  {
    std::vector<std::unique_ptr<RaftMemberDummy>> members;

    for (const auto &member_id : config.members) {
      members.push_back(
          std::make_unique<RaftMemberDummy>(sys, storage, member_id, config));
      members.back()->Connect();
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
    for (const auto &member : members) {
      if (member->Id() == first_leader) {
        member->Disconnect();
        break;
      }
    }

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

int main(int, char *argv[]) {
  google::InitGoogleLogging(argv[0]);

  RaftConfig config{{"a", "b", "c", "d", "e"}, 150ms, 300ms, 70ms};

  const int RUNS = 100;

  auto InitialElectionTest = [config]() { return InitialElection(config); };
  auto ReelectionTest = [config]() { return Reelection(config); };

  RunTest("InitialElection", InitialElectionTest, RUNS);
  RunTest("ReelectionTest", ReelectionTest, RUNS);

  return 0;
}
