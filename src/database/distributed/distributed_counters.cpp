#include "database/distributed/distributed_counters.hpp"

#include "database/distributed/counters_rpc_messages.hpp"

namespace database {

MasterCounters::MasterCounters(distributed::Coordination *coordination) {
  coordination->Register<CountersGetRpc>(
      [this](auto *req_reader, auto *res_builder) {
        CountersGetReq req;
        slk::Load(&req, req_reader);
        CountersGetRes res(Get(req.name));
        slk::Save(res, res_builder);
      });
  coordination->Register<CountersSetRpc>(
      [this](auto *req_reader, auto *res_builder) {
        CountersSetReq req;
        slk::Load(&req, req_reader);
        Set(req.name, req.value);
        CountersSetRes res;
        slk::Save(res, res_builder);
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
  return master_client_pool_->Call<CountersGetRpc>(name).value;
}

void WorkerCounters::Set(const std::string &name, int64_t value) {
  master_client_pool_->Call<CountersSetRpc>(name, value);
}

}  // namespace database
