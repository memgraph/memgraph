#include "memgraph_config.hpp"
#include "memgraph_distributed.hpp"
#include "memgraph_transactions.hpp"

#include "reactors_distributed.hpp"

#include <iostream>
#include <fstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_uint64(my_mnid, -1, "Memgraph node id"); // TODO(zuza): this should be assigned by the leader once in the future

/**
 * Sends a text message and has a return address.
 */
class TextMessage : public ReturnAddressMsg {
 public:
  TextMessage(std::string reactor, std::string channel, std::string s)
    : ReturnAddressMsg(reactor, channel), text(s) {}

  template <class Archive>
  void serialize(Archive &archive) {
    archive(cereal::virtual_base_class<ReturnAddressMsg>(this), text);
  }

  std::string text;

 protected:
  friend class cereal::access;
  TextMessage() {} // Cereal needs access to a default constructor.
};
CEREAL_REGISTER_TYPE(TextMessage);

class Master : public Reactor {
 public:
  Master(std::string name, MnidT mnid)
    : Reactor(name), mnid_(mnid) {
    MemgraphDistributed& memgraph = MemgraphDistributed::GetInstance();
    worker_mnids_ = memgraph.GetAllMnids();
    // remove the leader (itself), because its not a worker
    auto leader_it = std::find(worker_mnids_.begin(), worker_mnids_.end(), memgraph.LeaderMnid());
    worker_mnids_.erase(leader_it);
  }

  virtual void Run() {
    Distributed &distributed = Distributed::GetInstance();

    LOG(INFO) << "Master (" << mnid_ << ") @ " << distributed.network().Address()
              << ":" << distributed.network().Port() << std::endl;

    // TODO(zuza): check if all workers are up

    auto stream = Open("client-queries").first;
    stream->OnEvent<QueryCreateVertex>([this](const QueryCreateVertex& msg, const Subscription&) {
        std::random_device rd; // slow random number generator

        // succeed and fail with 50-50
        if (rd() % 2 == 0) {
          msg.GetReturnChannelWriter()
            ->Send<SuccessQueryCreateVertex>();
        } else {
          msg.GetReturnChannelWriter()
            ->Send<FailureQueryCreateVertex>();
        }
      });

  }

 protected:
  MnidT workers_seen = 0;
  const MnidT mnid_;
  std::vector<MnidT> worker_mnids_;
};

class Worker : public Reactor {
 public:
  Worker(std::string name, MnidT mnid)
      : Reactor(name), mnid_(mnid) {}

  virtual void Run() {
    Distributed &distributed = Distributed::GetInstance();

    LOG(INFO) << "Worker (" << mnid_ << ") @ " << distributed.network().Address()
              << ":" << distributed.network().Port() << std::endl;
  }

 protected:
  const MnidT mnid_;
};

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, /* remove flags from command line */ true);
  std::string logging_name = std::string(argv[0]) + "-mnid-" + std::to_string(FLAGS_my_mnid);
  google::InitGoogleLogging(logging_name.c_str());

  System &system = System::GetInstance();
  Distributed& distributed = Distributed::GetInstance();
  MemgraphDistributed& memgraph = MemgraphDistributed::GetInstance();
  memgraph.RegisterConfig(ParseConfig());
  distributed.StartServices();

  if (FLAGS_my_mnid == memgraph.LeaderMnid()) {
    system.Spawn<Master>("master", FLAGS_my_mnid);
  } else {
    system.Spawn<Worker>("worker", FLAGS_my_mnid);
  }
  system.AwaitShutdown();
  distributed.StopServices();

  return 0;
}
