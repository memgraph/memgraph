#include <fstream>
#include <iostream>

#include <glog/logging.h>

#include "memgraph_config.hpp"
#include "reactors_distributed.hpp"

DEFINE_int64(my_mnid, 0, "Memgraph node id");  // TODO(zuza): this should be
                                               // assigned by the leader once in
                                               // the future

class MemgraphDistributed {
 private:
  using Location = std::pair<std::string, uint16_t>;

 public:
  /**
   * Get the (singleton) instance of MemgraphDistributed.
   *
   * More info:
   * https://stackoverflow.com/questions/1008019/c-singleton-design-pattern
   */
  static MemgraphDistributed &GetInstance() {
    static MemgraphDistributed
        memgraph;  // guaranteed to be destroyed, initialized on first use
    return memgraph;
  }

  /** Register memgraph node id to the given location. */
  void RegisterMemgraphNode(int64_t mnid, const std::string &address,
                            uint16_t port) {
    std::unique_lock<std::mutex> lock(mutex_);
    mnodes_[mnid] = Location(address, port);
  }

  EventStream *FindChannel(int64_t mnid, const std::string &reactor,
                           const std::string &channel) {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto &location = mnodes_.at(mnid);
    return Distributed::GetInstance().FindChannel(
        location.first, location.second, reactor, channel);
  }

 protected:
  MemgraphDistributed() {}

 private:
  std::mutex mutex_;
  std::unordered_map<int64_t, Location> mnodes_;

  MemgraphDistributed(const MemgraphDistributed &) = delete;
  MemgraphDistributed(MemgraphDistributed &&) = delete;
  MemgraphDistributed &operator=(const MemgraphDistributed &) = delete;
  MemgraphDistributed &operator=(MemgraphDistributed &&) = delete;
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
std::pair<int64_t, std::vector<int64_t>> ParseConfigAndRegister(
    const std::string &filename) {
  std::ifstream file(filename, std::ifstream::in);
  assert(file.good());
  int64_t master_mnid;
  std::vector<int64_t> worker_mnids;
  int64_t mnid;
  std::string address;
  uint16_t port;
  file >> master_mnid >> address >> port;
  MemgraphDistributed &memgraph = MemgraphDistributed::GetInstance();
  memgraph.RegisterMemgraphNode(master_mnid, address, port);
  while (file.good()) {
    file >> mnid >> address >> port;
    if (file.eof()) break;
    memgraph.RegisterMemgraphNode(mnid, address, port);
    worker_mnids.push_back(mnid);
  }
  file.close();
  return std::make_pair(master_mnid, worker_mnids);
}

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
  TextMessage() {}  // Cereal needs access to a default constructor.
};
CEREAL_REGISTER_TYPE(TextMessage);

class Master : public Reactor {
 public:
  Master(std::string name, int64_t mnid, std::vector<int64_t> &&worker_mnids)
      : Reactor(name), mnid_(mnid), worker_mnids_(std::move(worker_mnids)) {}

  virtual void Run() {
    MemgraphDistributed &memgraph = MemgraphDistributed::GetInstance();
    Distributed &distributed = Distributed::GetInstance();

    std::cout << "Master (" << mnid_ << ") @ "
              << distributed.network().Address() << ":"
              << distributed.network().Port() << std::endl;

    auto stream = main_.first;

    // wait until every worker sends a ReturnAddressMsg back, then close
    stream->OnEvent<TextMessage>(
        [this](const TextMessage &msg, const Subscription &subscription) {
          std::cout << "Message from " << msg.Address() << ":" << msg.Port()
                    << " .. " << msg.text << "\n";
          ++workers_seen;
          if (workers_seen == static_cast<int64_t>(worker_mnids_.size())) {
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
      auto stream = memgraph.FindChannel(wmnid, "worker", "main");
      stream->OnEventOnce().ChainOnce<ChannelResolvedMessage>([this, stream](
          const ChannelResolvedMessage &msg, const Subscription &) {
        msg.channelWriter()->Send<TextMessage>("master", "main",
                                               "hi from master");
        stream->Close();
      });
    }
  }

 protected:
  int64_t workers_seen = 0;
  const int64_t mnid_;
  std::vector<int64_t> worker_mnids_;
};

class Worker : public Reactor {
 public:
  Worker(std::string name, int64_t mnid, int64_t master_mnid)
      : Reactor(name), mnid_(mnid), master_mnid_(master_mnid) {}

  virtual void Run() {
    Distributed &distributed = Distributed::GetInstance();

    std::cout << "Worker (" << mnid_ << ") @ "
              << distributed.network().Address() << ":"
              << distributed.network().Port() << std::endl;

    auto stream = main_.first;
    // wait until master sends us a TextMessage, then reply back and close
    stream->OnEventOnce().ChainOnce<TextMessage>(
        [this](const TextMessage &msg, const Subscription &) {
          std::cout << "Message from " << msg.Address() << ":" << msg.Port()
                    << " .. " << msg.text << "\n";

          msg.GetReturnChannelWriter()->Send<TextMessage>("worker", "main",
                                                          "hi from worker");

          // Sleep for a while so we can read output in the terminal.
          std::this_thread::sleep_for(std::chrono::seconds(4));
          CloseChannel("main");
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
  auto mnids = ParseConfigAndRegister(FLAGS_config_filename);
  Distributed::GetInstance().StartServices();
  if (FLAGS_my_mnid == mnids.first)
    system.Spawn<Master>("master", FLAGS_my_mnid, std::move(mnids.second));
  else
    system.Spawn<Worker>("worker", FLAGS_my_mnid, mnids.first);
  system.AwaitShutdown();
  Distributed::GetInstance().StopServices();

  return 0;
}
