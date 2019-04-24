#include "raft/coordination.hpp"

#include <thread>

#include "glog/logging.h"
#include "json/json.hpp"

#include "utils/file.hpp"
#include "utils/string.hpp"

namespace raft {

namespace fs = std::filesystem;

Coordination::Coordination(
    uint16_t server_workers_count, uint16_t client_workers_count,
    uint16_t worker_id,
    std::unordered_map<uint16_t, io::network::Endpoint> workers)
    : server_(workers[worker_id], server_workers_count),
      worker_id_(worker_id),
      workers_(workers) {
  for (const auto &worker : workers_) {
    client_locks_[worker.first] = std::make_unique<std::mutex>();
  }
}

Coordination::~Coordination() {
  CHECK(!alive_) << "You must call Shutdown and AwaitShutdown on Coordination!";
}

std::unordered_map<uint16_t, io::network::Endpoint> Coordination::LoadFromFile(
    const std::string &coordination_config_file) {
  if (!fs::exists(coordination_config_file))
    throw RaftCoordinationConfigException(coordination_config_file);

  std::unordered_map<uint16_t, io::network::Endpoint> workers;
  nlohmann::json data;
  try {
    data = nlohmann::json::parse(
        utils::Join(utils::ReadLines(coordination_config_file), ""));
  } catch (const nlohmann::json::parse_error &e) {
    throw RaftCoordinationConfigException(coordination_config_file);
  }

  if (!data.is_array())
    throw RaftCoordinationConfigException(coordination_config_file);

  for (auto &it : data) {
    if (!it.is_array() || it.size() != 3)
      throw RaftCoordinationConfigException(coordination_config_file);

    workers[it[0]] = io::network::Endpoint{it[1], it[2]};
  }

  return workers;
}

io::network::Endpoint Coordination::GetEndpoint(int worker_id) {
  std::lock_guard<std::mutex> guard(lock_);
  auto found = workers_.find(worker_id);
  CHECK(found != workers_.end())
      << "No endpoint registered for worker id: " << worker_id;
  return found->second;
}

io::network::Endpoint Coordination::GetServerEndpoint() {
  return server_.endpoint();
}

std::vector<int> Coordination::GetWorkerIds() {
  std::lock_guard<std::mutex> guard(lock_);
  std::vector<int> worker_ids;
  for (auto worker : workers_) worker_ids.push_back(worker.first);
  return worker_ids;
}

uint16_t Coordination::WorkerCount() { return workers_.size(); }

bool Coordination::Start() {
  if (!server_.Start()) return false;
  AddWorker(worker_id_, server_.endpoint());
  return true;
}

void Coordination::AwaitShutdown(
    std::function<void(void)> call_before_shutdown) {
  // Wait for a shutdown notification.
  while (alive_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Call the before shutdown callback.
  call_before_shutdown();

  // Shutdown our RPC server.
  server_.Shutdown();
  server_.AwaitShutdown();
}

void Coordination::Shutdown() { alive_.store(false); }

std::string Coordination::GetWorkerName(const io::network::Endpoint &endpoint) {
  std::lock_guard<std::mutex> guard(lock_);
  for (const auto &worker : workers_) {
    if (worker.second == endpoint) {
      return fmt::format("worker {} ({})", worker.first, worker.second);
    }
  }
  return fmt::format("unknown worker ({})", endpoint);
}

void Coordination::AddWorker(int worker_id,
                             const io::network::Endpoint &endpoint) {
  std::lock_guard<std::mutex> guard(lock_);
  workers_.insert({worker_id, endpoint});
}

}  // namespace raft
