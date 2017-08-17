#include <iostream>
#include <fstream>

#include "communication.hpp"

DEFINE_int64(my_mnid, 0, "Memgraph node id");
DEFINE_string(config_filename, "", "File containing list of all processes");

class MemgraphDistributed : public Distributed {
 private:
  using Location = std::pair<std::string, uint16_t>;

 public:
  MemgraphDistributed(System &system) : Distributed(system) {}

  /** Register memgraph node id to the given location. */
  void RegisterMemgraphNode(int64_t mnid, const std::string& address, uint16_t port) {
    std::unique_lock<std::recursive_mutex> lock(mutex_);
    mnodes_[mnid] = Location(address, port);
  }

  EventStream* FindChannel(int64_t mnid,
                           const std::string &reactor,
                           const std::string &channel) {
    std::unique_lock<std::recursive_mutex> lock(mutex_);
    const auto& location = mnodes_.at(mnid);
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
ParseConfigAndRegister(const std::string& filename,
                       MemgraphDistributed& distributed) {
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


class MemgraphReactor : public Reactor {
 public:
  MemgraphReactor(System* system, std::string name,
                     MemgraphDistributed &distributed)
    : Reactor(system, name), distributed_(distributed) {}

 protected:
  MemgraphDistributed &distributed_;
};


class Master : public MemgraphReactor {
 public:
  Master(System* system, std::string name, MemgraphDistributed &distributed,
         int64_t mnid, std::vector<int64_t>&& worker_mnids)
    : MemgraphReactor(system, name, distributed), mnid_(mnid),
      worker_mnids_(std::move(worker_mnids)) {}

  virtual void Run() {
    std::cout << "Master (" << mnid_ << ") @ " << distributed_.network().Address()
              << ":" << distributed_.network().Port() << std::endl;

    auto stream = main_.first;
    stream->OnEvent<SenderMessage>([this](const SenderMessage &msg,
                                          const EventStream::Subscription& subscription) {
      std::cout << "Message from " << msg.Address() << ":" << msg.Port() << "\n";
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

    for (auto wmnid : worker_mnids_) {
      auto stream = distributed_.FindChannel(wmnid, "worker", "main");
      stream->OnEventOnce()
        .ChainOnce<ChannelResolvedMessage>([this, stream](const ChannelResolvedMessage &msg){
          msg.channel()->Send<SenderMessage>("master", "main");
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
  Worker(System* system, std::string name, MemgraphDistributed &distributed,
         int64_t mnid, int64_t master_mnid)
      : MemgraphReactor(system, name, distributed), mnid_(mnid),
        master_mnid_(master_mnid) {}

  virtual void Run() {
    std::cout << "Worker (" << mnid_ << ") @ " << distributed_.network().Address()
              << ":" << distributed_.network().Port() << std::endl;

    auto stream = main_.first;
    stream->OnEventOnce()
      .ChainOnce<SenderMessage>([this](const SenderMessage &msg) {
      std::cout << "Message from " << msg.Address() << ":" << msg.Port() << "\n";
      // Sleep for a while so we can read output in the terminal.
      std::this_thread::sleep_for(std::chrono::seconds(4));
      CloseConnector("main");
    });

    auto remote_stream = distributed_.FindChannel(master_mnid_, "master", "main");
    remote_stream->OnEventOnce()
      .ChainOnce<ChannelResolvedMessage>([this, remote_stream](const ChannelResolvedMessage &msg){
        msg.channel()->Send<SenderMessage>("worker", "main");
        remote_stream->Close();
      });
  }

 protected:
  const int64_t mnid_;
  const int64_t master_mnid_;
};


int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  System system;
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