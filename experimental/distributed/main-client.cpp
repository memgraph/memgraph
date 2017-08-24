#include "memgraph_distributed.hpp"
#include "memgraph_config.hpp"

#include "reactors_distributed.hpp"

#include <iostream>
#include <fstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

/**
 * This is the client that issues some hard-coded queries.
 */
class Client : public Reactor {
 public:
  Client(std::string name) : Reactor(name) {
  }

  virtual void Run() {

  }
};

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

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
