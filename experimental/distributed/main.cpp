#include <iostream>
#include <fstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "memgraph_config.hpp"
#include "memgraph_distributed.hpp"
#include "memgraph_transactions.hpp"
#include "reactors_distributed.hpp"
#include "storage.hpp"

DEFINE_uint64(my_mnid, -1, "Memgraph node id"); // TODO(zuza): this should be assigned by the leader once in the future

class Master : public Reactor {
 public:
  Master(std::string name, MnidT mnid) : Reactor(name), mnid_(mnid) {
    MemgraphDistributed& memgraph = MemgraphDistributed::GetInstance();
    worker_mnids_ = memgraph.GetAllMnids();
    // remove the leader (itself), because it is not a worker
    auto leader_it = std::find(worker_mnids_.begin(), worker_mnids_.end(), memgraph.LeaderMnid());
    worker_mnids_.erase(leader_it);
  }

  virtual void Run() {
    Distributed &distributed = Distributed::GetInstance();

    std::cout << "Master (" << mnid_ << ") @ " << distributed.network().Address()
              << ":" << distributed.network().Port() << std::endl;

    // TODO(zuza): check if all workers are up

    // start listening on queries arriving from the client
    auto stream = Open("client-queries").first;
    stream->OnEvent<QueryMsg>([this](const QueryMsg &msg, const Subscription &){
      // process query message
      if (msg.query() == "create vertex") {
        InstallMakeVertex(msg.GetReturnChannelWriter());
      } else if (msg.query() == "vertex count") {
        InstallVertexCount(msg.GetReturnChannelWriter());
      } else {
        std::cerr << "unknown query" << std::endl;
      }
    });
  }

 private:
  /**
   * Organizes communication with all workers and performs VertexCount.
   */
  void InstallVertexCount(std::shared_ptr<ChannelWriter> return_channel) {
    // open channel through which answers will arrive
    auto channel_name = "response" + std::to_string(xid++);
    auto result = Open(channel_name).first;

    // create struct to keep track of responses
    struct VertexCountResponse {
      VertexCountResponse(int64_t count, int64_t remaining)
          : count_(count), remaining_(remaining) {}

      int64_t count_;
      int64_t remaining_;
    };

    // allocate it dynamically so it lives outside the scope of this function
    // it will be deallocated once all responses arrive and channel is closed
    auto response = std::make_shared<VertexCountResponse>(0, worker_mnids_.size());

    // register callbacks
    result->OnEvent<ResultQueryVertexCount>(
      [this, response, return_channel](const ResultQueryVertexCount &msg,
                                       const Subscription &sub){
        response->count_ += msg.count();
        --response->remaining_;
        if (response->remaining_ == 0) {
          sub.CloseChannel();
          return_channel->Send<ResultMsg>(std::to_string(response->count_));
        }
      });

    // instruct workers to count vertices
    for (auto wmnid : worker_mnids_)
      VertexCount(wmnid, channel_name);
  }

  /**
   * Asynchronously counts vertices on the given node.
   *
   * @param mnid Id of the node whose vertices should be counted.
   * @param channel_name Name of the channel on which response will arrive.
   */
  void VertexCount(MnidT mnid, std::string channel_name) {
    MemgraphDistributed::GetInstance().FindChannel(mnid, "worker", "main")
      ->OnEventOnceThenClose<ChannelResolvedMessage>(
          [this, channel_name](const ChannelResolvedMessage &msg){
            msg.channelWriter()->Send<QueryVertexCount>(channel_name);
          });
  }

  /**
   * Organizes communication with a random worker and performs MakeVertex.
   */
  void InstallMakeVertex(std::shared_ptr<ChannelWriter> return_channel) {
    // choose worker on random and instruct it to make vertex
    auto wmnid = worker_mnids_[rand() % worker_mnids_.size()];

    // open channel through which answer will arrive
    auto channel_name = "response" + std::to_string(xid++);
    auto result = Open(channel_name).first;

    // register callbacks for the answer
    // TODO(zuza): this is actually pretty bad because if SuccessQueryCreateVertex arrives, then
    //             FailureQueryCreateVertex never gets unsubscribed. This could cause memory leaks
    //             in the future (not currently since all callbacks get destroyed when channel is closed).
    //             The best thing to do is to implement a ThenOnce and Either. Perhaps even a ThenClose.
    //             An Either in conjunction with a failure detector event stream should eventually fail
    //             the transaction and close the channel.
    result->OnEventOnceThenClose<SuccessQueryCreateVertex>(
        [this, return_channel](const SuccessQueryCreateVertex &) {
          return_channel->Send<ResultMsg>("success");
      });
    result->OnEventOnceThenClose<FailureQueryCreateVertex>(
        [this, return_channel](const FailureQueryCreateVertex &) {
          return_channel->Send<ResultMsg>("failure");
      });

    // instruct worker to make vertex
    MakeVertex(wmnid, channel_name);
  }

  /**
   * Asynchronously creates vertex on the give node.
   *
   * @param mnid Id of the node on which vertex should be created.
   * @param channel_name Name of the channel on which response will arrive.
   */
  void MakeVertex(MnidT mnid, std::string channel_name) {
    MemgraphDistributed::GetInstance().FindChannel(mnid, "worker", "main")
      ->OnEventOnceThenClose<ChannelResolvedMessage>(
        [this, channel_name](const ChannelResolvedMessage &msg){
          msg.channelWriter()->Send<QueryCreateVertex>(channel_name);
      });
  }

 protected:
  // node id
  const MnidT mnid_;

  // transaction id
  int64_t xid{0};

  // list of ids of nodes that act as worker
  std::vector<MnidT> worker_mnids_;
};

class Worker : public Reactor {
 public:
  Worker(std::string name, MnidT mnid)
      : Reactor(name), mnid_(mnid), storage_(mnid) {}

  virtual void Run() {
    Distributed &distributed = Distributed::GetInstance();

    std::cout << "Worker (" << mnid_ << ") @ " << distributed.network().Address()
              << ":" << distributed.network().Port() << std::endl;

    main_.first->OnEvent<QueryCreateVertex>([this](const QueryCreateVertex& msg,
                                                   const Subscription &) {
      std::random_device rd; // slow random number generator

      // succeed and fail with 50-50 (just for testing)
      // TODO: remove random failure
      if (rd() % 2 == 0) {
        storage_.MakeVertex();
        std::cout << "Vertex created" << std::endl;
        msg.GetReturnChannelWriter()->Send<SuccessQueryCreateVertex>();
      } else {
        msg.GetReturnChannelWriter()->Send<FailureQueryCreateVertex>();
      }
    });

    main_.first->OnEvent<QueryVertexCount>([this](const QueryVertexCount &msg,
                                                  const Subscription &){
      auto count = storage_.VertexCount();
      msg.GetReturnChannelWriter()->Send<ResultQueryVertexCount>(count);
    });
  }

 protected:
  const MnidT mnid_;
  ShardedStorage storage_;
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
