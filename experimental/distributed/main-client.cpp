#include <fstream>
#include <iostream>
#include <memory>

#include "reactors_distributed.hpp"
#include "memgraph_config.hpp"
#include "memgraph_distributed.hpp"
#include "memgraph_transactions.hpp"

/**
 * List of queries that should be executed.
 */
std::vector<std::string> queries = {{
  "create vertex",
  "create vertex",
  "create vertex",
  "create vertex",
  "create vertex",
  "create vertex",
  "create vertex",
  "create vertex",
  "create vertex",
  "create vertex",
  "vertex count",
  "create vertex",
  "create vertex",
  "vertex count"
}};

/**
 * This is the client that issues some hard-coded queries.
 */
class Client : public Reactor {
 public:
  Client(std::string name) : Reactor(name) {
  }

  void IssueQueries(std::shared_ptr<ChannelWriter> channel_to_leader) {
    // (concurrently) create a couple of vertices
    for (int query_idx = 0; query_idx < queries.size(); ++query_idx) {
      // register callback
      std::string channel_name = "query-" + std::to_string(query_idx);
      auto stream = Open(channel_name).first;
      stream
        ->OnEventOnce()
        .ChainOnce<ResultMsg>([this, query_idx](const ResultMsg &msg,
                                               const Subscription &sub){
          std::cout << "Result of query " << query_idx << " ("
                    << queries[query_idx] << "):" << std::endl
                    << "  " << msg.result() << std::endl;
          sub.CloseChannel();
        });

      // then issue the query (to avoid race conditions)
      std::cout << "Issuing command " << query_idx << " ("
                << queries[query_idx] << ")" << std::endl;
      channel_to_leader->Send<QueryMsg>(channel_name, queries[query_idx]);
    }
  }

  virtual void Run() {
    MemgraphDistributed& memgraph = MemgraphDistributed::GetInstance();
    auto mnid = memgraph.LeaderMnid();

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
