#include "database/distributed_counters.hpp"

#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/server.hpp"
#include "database/counters_rpc_messages.hpp"

namespace database {

MasterCounters::MasterCounters(communication::rpc::Server *server)
    : rpc_server_(server) {
  rpc_server_->Register<CountersGetRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        CountersGetRes res(Get(req_reader.getName()));
        res.Save(res_builder);
      });
  rpc_server_->Register<CountersSetRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        Set(req_reader.getName(), req_reader.getValue());
        return std::make_unique<CountersSetRes>();
      });
}

int64_t MasterCounters::Get(const std::string &name) {
  return counters_.access()
      .emplace(name, std::make_tuple(name), std::make_tuple(0))
      .first->second.fetch_add(1);
}

void MasterCounters::Set(const std::string &name, int64_t value) {
  auto name_counter_pair = counters_.access().emplace(
      name, std::make_tuple(name), std::make_tuple(value));
  if (!name_counter_pair.second) name_counter_pair.first->second.store(value);
}

WorkerCounters::WorkerCounters(
    communication::rpc::ClientPool *master_client_pool)
    : master_client_pool_(master_client_pool) {}

int64_t WorkerCounters::Get(const std::string &name) {
  auto response = master_client_pool_->Call<CountersGetRpc>(name);
  CHECK(response) << "CountersGetRpc failed";
  return response->value;
}

void WorkerCounters::Set(const std::string &name, int64_t value) {
  auto response = master_client_pool_->Call<CountersSetRpc>(name, value);
  CHECK(response) << "CountersSetRpc failed";
}

}  // namespace database
