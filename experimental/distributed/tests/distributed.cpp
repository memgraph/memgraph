#include <iostream>
#include <fstream>

#include "communication.hpp"

DEFINE_int64(my_mnid, 0, "Memgraph node id");
DEFINE_string(config_filename, "", "File containing list of all processes");

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
ParseConfigAndRegister(const std::string& filename, System& system) {
  std::ifstream file(filename, std::ifstream::in);
  assert(file.good());
  int64_t master_mnid;
  std::vector<int64_t> worker_mnids;
  int64_t mnid;
	std::string address;
	uint16_t port;
	file >> master_mnid >> address >> port;
	system.RegisterMemgraphNode(master_mnid, address, port);
  while (!(file >> mnid).eof()) {
		file >> address >> port;
    system.RegisterMemgraphNode(mnid, address, port);
    worker_mnids.push_back(mnid);
	}
	file.close();
	return std::make_pair(master_mnid, worker_mnids);
}


class Master : public Reactor {
 public:
  Master(System* system, std::string name, int64_t mnid,
         std::vector<int64_t>&& worker_mnids)
    : Reactor(system, name), mnid_(mnid), worker_mnids_(std::move(worker_mnids)) {}

  virtual void Run() {
    std::cout << "Master (" << mnid_ << ") @ " << system_->network().Address()
              << ":" << system_->network().Port() << std::endl;

    auto stream = main_.first;
    stream->OnEvent<SenderMessage>([this](const SenderMessage &msg,
                                          const EventStream::Subscription& subscription) {
      std::cout << "Message from " << msg.Address() << ":" << msg.Port() << "\n";
      ++workers_seen;
      if (workers_seen == worker_mnids_.size()) {
        subscription.unsubscribe();
        std::this_thread::sleep_for(std::chrono::seconds(4));
        CloseConnector("main");
      }
    });

    for (auto wmnid : worker_mnids_) {
      std::shared_ptr<Channel> channel;
      while (!(channel = system_->FindChannel(wmnid, "worker", "main")))
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      channel->Send<SenderMessage>("master", "main");
    }
  }

 protected:
  int64_t workers_seen = 0;
  const int64_t mnid_;
  std::vector<int64_t> worker_mnids_;
};

class Worker : public Reactor {
 public:
  Worker(System* system, std::string name, int64_t mnid, int64_t master_mnid)
      : Reactor(system, name), mnid_(mnid), master_mnid_(master_mnid) {}

  virtual void Run() {
    std::cout << "Worker (" << mnid_ << ") @ " << system_->network().Address()
              << ":" << system_->network().Port() << std::endl;

    auto stream = main_.first;
    stream->OnEvent<SenderMessage>([this](const SenderMessage &msg,
                                          const EventStream::Subscription &subscription) {
      std::cout << "Message from " << msg.Address() << ":" << msg.Port() << "\n";
      subscription.unsubscribe();
      std::this_thread::sleep_for(std::chrono::seconds(4));
      CloseConnector("main");
    });

    std::shared_ptr<Channel> channel;
    while(!(channel = system_->FindChannel(master_mnid_, "master", "main")))
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    channel->Send<SenderMessage>("worker", "main");
  }

 protected:
  const int64_t mnid_;
  const int64_t master_mnid_;
};


int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  System system;
  auto mnids = ParseConfigAndRegister(FLAGS_config_filename, system);
  system.StartServices();
  if (FLAGS_my_mnid == mnids.first)
    system.Spawn<Master>("master", FLAGS_my_mnid, std::move(mnids.second));
  else
    system.Spawn<Worker>("worker", FLAGS_my_mnid, mnids.first);
  system.AwaitShutdown();

  return 0;
}