#include "reactors_distributed.hpp"

#include <iostream>
#include <fstream>

#include <glog/logging.h>

DEFINE_int64(my_mnid, 0, "Memgraph node id"); // TODO(zuza): this should be assigned by the leader once in the future
DEFINE_string(config_filename, "", "File containing list of all processes");

class MemgraphDistributed : public Distributed {
 private:
  using Location = std::pair<std::string, uint16_t>;

 public:
  MemgraphDistributed(System &system) : Distributed() {}

  /** Register memgraph node id to the given location. */
  void RegisterMemgraphNode(int64_t mnid, const std::string &address, uint16_t port) {
    std::unique_lock<std::recursive_mutex> lock(mutex_);
    mnodes_[mnid] = Location(address, port);
  }

  EventStream* FindChannel(int64_t mnid,
                           const std::string &reactor,
                           const std::string &channel) {
    std::unique_lock<std::recursive_mutex> lock(mutex_);
    const auto &location = mnodes_.at(mnid);
    return Distributed::FindChannel(location.first, location.second, reactor, channel);
  }

 private:
  std::recursive_mutex mutex_;
  std::unordered_map<int64_t, Location> mnodes_;
};

/**
 * About config file
 *
 * Each line contains three strings:
 *   memgraph node id, ip address of the worker, and port of the worker
 * Data on the first line is used to start master.
 * Data on the remaining lines is used to start workers.
 */

/**
 * Parse config file and register processes into system.
 *
 * @return Pair (master mnid, list of worker's id).
 */
std::pair<int64_t, std::vector<int64_t>>
  ParseConfigAndRegister(const std::string &filename,
                         MemgraphDistributed &distributed) {
  std::ifstream file(filename, std::ifstream::in);
  assert(file.good());
  int64_t master_mnid;
  std::vector<int64_t> worker_mnids;
  int64_t mnid;
  std::string address;
  uint16_t port;
  file >> master_mnid >> address >> port;
  distributed.RegisterMemgraphNode(master_mnid, address, port);
  while (file.good()) {
    file >> mnid >> address >> port;
    if (file.eof())
      break ;
    distributed.RegisterMemgraphNode(mnid, address, port);
    worker_mnids.push_back(mnid);
  }
  file.close();
  return std::make_pair(master_mnid, worker_mnids);
}

/**
 * Interface to the Memgraph distributed system.
 *
 * E.g. get channel to other Memgraph nodes.
 */
class MemgraphReactor : public Reactor {
 public:
  MemgraphReactor(std::string name,
                  MemgraphDistributed &distributed)
    : Reactor(name), distributed_(distributed) {}

 protected:
  MemgraphDistributed &distributed_;
};

/**
 * Sends a text message and has a return address.
 */
class TextMessage : public SenderMessage {
public:
  TextMessage(std::string reactor, std::string channel, std::string s)
    : SenderMessage(reactor, channel), text(s) {}

  template <class Archive>
  void serialize(Archive &archive) {
    archive(cereal::virtual_base_class<SenderMessage>(this), text);
  }

  std::string text;

protected:
  friend class cereal::access;
  TextMessage() {} // Cereal needs access to a default constructor.
};
CEREAL_REGISTER_TYPE(TextMessage);


class Master : public MemgraphReactor {
 public:
  Master(std::string name, MemgraphDistributed &distributed,
         int64_t mnid, std::vector<int64_t> &&worker_mnids)
    : MemgraphReactor(name, distributed), mnid_(mnid),
      worker_mnids_(std::move(worker_mnids)) {}

  virtual void Run() {
    std::cout << "Master (" << mnid_ << ") @ " << distributed_.network().Address()
              << ":" << distributed_.network().Port() << std::endl;

    auto stream = main_.first;

    // wait until every worker sends a SenderMessage back, then close
    stream->OnEvent<TextMessage>([this](const TextMessage &msg,
                                          const EventStream::Subscription &subscription) {
      std::cout << "Message from " << msg.Address() << ":" << msg.Port() << " .. " << msg.text << "\n";
      ++workers_seen;
      if (workers_seen == worker_mnids_.size()) {
        subscription.unsubscribe();
        // Sleep for a while so we can read output in the terminal.
        // (start_distributed.py runs each process in a new tab which is
        //  closed immediately after process has finished)
        std::this_thread::sleep_for(std::chrono::seconds(4));
        CloseConnector("main");
      }
    });

    // send a TextMessage to each worker
    for (auto wmnid : worker_mnids_) {
      auto stream = distributed_.FindChannel(wmnid, "worker", "main");
      stream->OnEventOnce()
        .ChainOnce<ChannelResolvedMessage>([this, stream](const ChannelResolvedMessage &msg){
          msg.channel()->Send<TextMessage>("master", "main", "hi from master");
          stream->Close();
        });
    }
  }

 protected:
  int64_t workers_seen = 0;
  const int64_t mnid_;
  std::vector<int64_t> worker_mnids_;
};

class Worker : public MemgraphReactor {
 public:
  Worker(std::string name, MemgraphDistributed &distributed,
         int64_t mnid, int64_t master_mnid)
      : MemgraphReactor(name, distributed), mnid_(mnid),
        master_mnid_(master_mnid) {}

  virtual void Run() {
    std::cout << "Worker (" << mnid_ << ") @ " << distributed_.network().Address()
              << ":" << distributed_.network().Port() << std::endl;

    auto stream = main_.first;
    // wait until master sends us a TextMessage, then reply back and close
    stream->OnEventOnce()
      .ChainOnce<TextMessage>([this](const TextMessage &msg) {
      std::cout << "Message from " << msg.Address() << ":" << msg.Port() << " .. " << msg.text << "\n";

      msg.GetChannelToSender(&distributed_)
        ->Send<TextMessage>("worker", "main", "hi from worker");

      // Sleep for a while so we can read output in the terminal.
      std::this_thread::sleep_for(std::chrono::seconds(4));
      CloseConnector("main");
    });
  }

 protected:
  const int64_t mnid_;
  const int64_t master_mnid_;
};


int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  System &system = System::GetInstance();
  MemgraphDistributed distributed(system);
  auto mnids = ParseConfigAndRegister(FLAGS_config_filename, distributed);
  distributed.StartServices();
  if (FLAGS_my_mnid == mnids.first)
    system.Spawn<Master>("master", distributed, FLAGS_my_mnid, std::move(mnids.second));
  else
    system.Spawn<Worker>("worker", distributed, FLAGS_my_mnid, mnids.first);
  system.AwaitShutdown();
  distributed.StopServices();

  return 0;
}
