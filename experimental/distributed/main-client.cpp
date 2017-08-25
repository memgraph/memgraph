#include "reactors_distributed.hpp"

#include "memgraph_config.hpp"
#include "memgraph_distributed.hpp"
#include "memgraph_transactions.hpp"

#include <fstream>
#include <iostream>
#include <memory>

/**
 * This is the client that issues some hard-coded queries.
 */
class Client : public Reactor {
 public:
  Client(std::string name) : Reactor(name) {
  }

  void IssueQueries(std::shared_ptr<ChannelWriter> channel_to_leader) {
    const int NUM_VERTS = 10;
    // (concurrently) create a couple of vertices
    for (int num_vert = 0; num_vert < NUM_VERTS; ++num_vert) {
      // register callback

      std::string channel_name = "create-node-" + std::to_string(num_vert);
      // TODO(zuza): this is actually pretty bad because if SuccessQueryCreateVertex arrives, then
      //             FailureQueryCreateVertex never gets unsubscribed. This could cause memory leaks
      //             in the future (not currently since all callbacks get destroyed when channel is closed).
      //             The best thing to do is to implement a ThenOnce and Either. Perhaps even a ThenClose.
      auto stream = Open(channel_name).first;
      stream
        ->OnEventOnce()
        .ChainOnce<SuccessQueryCreateVertex>([this, num_vert](const SuccessQueryCreateVertex&, const Subscription& sub) {
            LOG(INFO) << "successfully created vertex " << num_vert+1 << std::endl;
            sub.CloseChannel();
          });

      stream
        ->OnEventOnce()
        .ChainOnce<FailureQueryCreateVertex>([this, num_vert](const FailureQueryCreateVertex&, const Subscription& sub) {
            LOG(INFO) << "failed on creating vertex " << num_vert+1 << std::endl;
            sub.CloseChannel();
          });

      // then issue the query (to avoid race conditions)
      LOG(INFO) << "Issuing command to create vertex " << num_vert+1;
      channel_to_leader->Send<QueryCreateVertex>(channel_name);
    }
  }

  virtual void Run() {
    MemgraphDistributed& memgraph = MemgraphDistributed::GetInstance();
    int mnid = memgraph.LeaderMnid();

    memgraph.FindChannel(mnid, "master", "client-queries")
      ->OnEventOnce()
      .ChainOnce<ChannelResolvedMessage>([this](const ChannelResolvedMessage &msg, const Subscription& sub) {
          sub.CloseChannel();
          IssueQueries(msg.channelWriter());
        });
  }
};

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  System &system = System::GetInstance();
  Distributed &distributed = Distributed::GetInstance();
  MemgraphDistributed& memgraph = MemgraphDistributed::GetInstance();
  memgraph.RegisterConfig(ParseConfig());
  distributed.StartServices();

  system.Spawn<Client>("client");

  system.AwaitShutdown();
  distributed.StopServices();

  return 0;
}
