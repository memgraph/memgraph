#include "memgraph_distributed.hpp"
#include "memgraph_config.hpp"

#include "reactors_distributed.hpp"

#include <iostream>
#include <fstream>

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
    worker_mnids_ = MemgraphDistributed::GetInstance().GetAllMnids();
    worker_mnids_.erase(worker_mnids_.begin()); // remove the master from the beginning
  }

  virtual void Run() {
    MemgraphDistributed &memgraph = MemgraphDistributed::GetInstance();
    Distributed &distributed = Distributed::GetInstance();

    std::cout << "Master (" << mnid_ << ") @ " << distributed.network().Address()
              << ":" << distributed.network().Port() << std::endl;

    auto stream = main_.first;

    // wait until every worker sends a ReturnAddressMsg back, then close
    stream->OnEvent<TextMessage>([this](const TextMessage &msg,
                                        const Subscription &subscription) {
      std::cout << "Message from " << msg.Address() << ":" << msg.Port() << " .. " << msg.text << "\n";
      ++workers_seen;
      if (workers_seen == worker_mnids_.size()) {
        subscription.Unsubscribe();
        // Sleep for a while so we can read output in the terminal.
        // (start_distributed.py runs each process in a new tab which is
        //  closed immediately after process has finished)
        std::this_thread::sleep_for(std::chrono::seconds(4));
        CloseChannel("main");
      }
    });

    // send a TextMessage to each worker
    for (auto wmnid : worker_mnids_) {
      std::cout << "wmnid_ = " << wmnid << std::endl;

      auto stream = memgraph.FindChannel(wmnid, "worker", "main");
      stream->OnEventOnce()
        .ChainOnce<ChannelResolvedMessage>([this, stream](const ChannelResolvedMessage &msg, const Subscription&){
          msg.channelWriter()->Send<TextMessage>("master", "main", "hi from master");
          stream->Close();
        });
    }
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

    std::cout << "Worker (" << mnid_ << ") @ " << distributed.network().Address()
              << ":" << distributed.network().Port() << std::endl;

    auto stream = main_.first;
    // wait until master sends us a TextMessage, then reply back and close
    stream->OnEventOnce()
      .ChainOnce<TextMessage>([this](const TextMessage &msg, const Subscription&) {
      std::cout << "Message from " << msg.Address() << ":" << msg.Port() << " .. " << msg.text << "\n";

      msg.GetReturnChannelWriter()
        ->Send<TextMessage>("worker", "main", "hi from worker");

      // Sleep for a while so we can read output in the terminal.
      std::this_thread::sleep_for(std::chrono::seconds(4));
      CloseChannel("main");
    });
  }

 protected:
  const MnidT mnid_;
};

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

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
