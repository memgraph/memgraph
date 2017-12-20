#include "communication/messaging/distributed.hpp"
#include "communication/raft/rpc.hpp"
#include "communication/raft/test_utils.hpp"

namespace raft = communication::raft;

using io::network::NetworkEndpoint;
using raft::RaftConfig;
using raft::RpcNetwork;
using raft::test_utils::DummyState;
using raft::test_utils::InMemoryStorageInterface;

DEFINE_string(member_id, "", "id of RaftMember");

CEREAL_REGISTER_TYPE(raft::PeerRpcReply);
CEREAL_REGISTER_TYPE(raft::PeerRpcRequest<DummyState>);

/* Start cluster members with:
 * ./raft_rpc --member-id a
 * ./raft_rpc --member-id b
 * ./raft_rpc --member-id c
 *
 * Enjoy democracy!
 */

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::unordered_map<std::string, NetworkEndpoint> directory = {
      {"a", NetworkEndpoint("127.0.0.1", 12345)},
      {"b", NetworkEndpoint("127.0.0.1", 12346)},
      {"c", NetworkEndpoint("127.0.0.1", 12347)}};

  communication::messaging::System my_system("127.0.0.1",
                                             directory[FLAGS_member_id].port());
  RpcNetwork<DummyState> network(my_system, directory);
  raft::test_utils::InMemoryStorageInterface<DummyState> storage(0, {}, {});

  raft::RaftConfig config{{"a", "b", "c"}, 150ms, 300ms, 70ms, 60ms, 30ms};

  {
    raft::RaftMember<DummyState> raft_member(network, storage, FLAGS_member_id,
                                             config);
    while (true)
      ;
  }

  my_system.Shutdown();

  return 0;
}
